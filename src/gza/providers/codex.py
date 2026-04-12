"""OpenAI Codex CLI provider implementation."""

from __future__ import annotations

import json
import os
import subprocess
import time
from collections.abc import Callable
from pathlib import Path
from typing import TYPE_CHECKING, Any

from .base import (
    DockerConfig,
    Provider,
    RunResult,
    build_docker_cmd,
    ensure_docker_image,
    verify_docker_credentials,
)
from .output_formatter import StreamOutputFormatter, truncate_text

if TYPE_CHECKING:
    from ..config import Config


# OpenAI Codex pricing per million tokens (input, output)
# https://openai.com/api/pricing/
CODEX_PRICING = {
    "gpt-5.2-codex": (2.50, 10.00),
    "gpt-5.3-codex": (2.50, 10.00),
    "o3": (10.00, 40.00),
    "default": (2.50, 10.00),
}


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

    def verify_credentials(self, config: Config) -> bool:
        """Verify Codex credentials by testing the codex command."""
        if config.use_docker:
            return self._verify_docker(config)
        return self._verify_direct()

    def _verify_docker(self, config: Config) -> bool:
        """Verify credentials work in Docker."""
        docker_config = _get_docker_config(f"{config.docker_image}-codex")
        if not ensure_docker_image(docker_config, config.project_dir):
            print("Error: Failed to build Docker image")
            return False
        return verify_docker_credentials(
            docker_config=docker_config,
            version_cmd=["codex", "--version"],
            error_patterns=["Invalid API key", "authentication", "unauthorized"],
            error_message=(
                "Error: Invalid or missing Codex credentials\n"
                "  Run 'codex login' or set CODEX_API_KEY in .env"
            ),
        )

    def _verify_direct(self) -> bool:
        """Verify credentials work directly."""
        try:
            result = subprocess.run(
                ["codex", "--version"],
                capture_output=True,
                timeout=10,
                text=True,
            )
            output = result.stdout + result.stderr
            if "Invalid API key" in output or "authentication" in output.lower() or "unauthorized" in output.lower():
                print("Error: Invalid or missing Codex credentials")
                print("  Run 'codex login' or set CODEX_API_KEY in .env")
                return False
            if result.returncode == 0:
                return True
        except (subprocess.TimeoutExpired, FileNotFoundError) as e:
            if isinstance(e, FileNotFoundError):
                print("Error: 'codex' command not found")
                print("  Install with: npm install -g @openai/codex")
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
    ) -> RunResult:
        """Run Codex to execute a task."""
        if config.use_docker:
            return self._run_docker(config, prompt, log_file, work_dir, resume_session_id, on_session_id, on_step_count)
        return self._run_direct(config, prompt, log_file, work_dir, resume_session_id, on_session_id, on_step_count)

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
        """Run Codex in Docker container."""
        docker_config = _get_docker_config(f"{config.docker_image}-codex")

        if not ensure_docker_image(docker_config, config.project_dir):
            print("Error: Failed to build Docker image")
            return RunResult(exit_code=1)

        cmd = build_docker_cmd(docker_config, work_dir, config.timeout_minutes, config.docker_volumes, config.docker_setup_command)

        if resume_session_id:
            cmd.extend([
                "codex", "exec", "resume", "--json",
                "--dangerously-bypass-approvals-and-sandbox",
                resume_session_id,
                "-",  # Read resume prompt from stdin
            ])

            # Add model if specified
            if config.model:
                cmd.extend(["-m", config.model])
        else:
            cmd.extend([
                "codex", "exec", "--json",
                "--dangerously-bypass-approvals-and-sandbox",  # Bypass sandbox for headless operation
                "--skip-git-repo-check",  # Worktree metadata may be unavailable inside containers
                "-C", "/workspace",  # Set working directory explicitly
                "-",  # Read prompt from stdin
            ])

            # Add model if specified
            if config.model:
                cmd.extend(["-m", config.model])

        return self._run_with_output_parsing(
            cmd, log_file, config.timeout_minutes, stdin_input=prompt,
            model=config.model, max_steps=config.max_steps,
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
        """Run Codex directly (no Docker)."""
        cmd = [
            "timeout", f"{config.timeout_minutes}m",
        ]

        if resume_session_id:
            cmd.extend([
                "codex", "exec", "resume", "--json",
                "--dangerously-bypass-approvals-and-sandbox",
                resume_session_id,
                "-",  # Read resume prompt from stdin
            ])

            # Add model if specified
            if config.model:
                cmd.extend(["-m", config.model])
        else:
            cmd.extend([
                "codex", "exec", "--json",
                "--dangerously-bypass-approvals-and-sandbox",  # Bypass sandbox for headless operation
                "--skip-git-repo-check",  # Worktree metadata may be unavailable in detached review contexts
                "-C", str(work_dir),
                "-",  # Read prompt from stdin
            ])

            # Add model if specified
            if config.model:
                cmd.extend(["-m", config.model])

        return self._run_with_output_parsing(
            cmd, log_file, config.timeout_minutes, cwd=work_dir,
            stdin_input=prompt, model=config.model,
            max_steps=config.max_steps,
            chat_text_display_length=config.chat_text_display_length,
            on_session_id=on_session_id,
            on_step_count=on_step_count,
        )

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
    ) -> RunResult:
        """Run command and parse Codex's JSON output."""
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

                if event_type == "thread.started":
                    thread_id = event.get("thread_id")
                    if thread_id and "thread_id" not in data:
                        data["thread_id"] = thread_id
                        if on_session_id:
                            on_session_id(thread_id)
                    elif thread_id:
                        data["thread_id"] = thread_id

                elif event_type == "turn.started":
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
                    _ensure_step_store(data)
                    legacy_turn_id = _current_turn_id(data)
                    if legacy_turn_id:
                        counters = data.get("_legacy_event_count_by_turn")
                        if isinstance(counters, dict):
                            counters.setdefault(legacy_turn_id, 0)

                    # Step headers are now printed when agent_message items
                    # arrive, so each model response is a logical step (like claude).

                elif event_type == "item.completed":
                    item = event.get("item", {})
                    item_type = item.get("type")
                    data["item_count"] = data.get("item_count", 0) + 1
                    data["item_count_in_turn"] = data.get("item_count_in_turn", 0) + 1

                    if item_type == "command_execution":
                        command = item.get("command", "")
                        aggregated_output = item.get("aggregated_output", "")
                        data["approx_input_chars"] = data.get("approx_input_chars", 0) + len(command) + len(aggregated_output)
                        current_step = data.get("_current_step_event")
                        legacy_turn_id = _current_turn_id(data)
                        if current_step is None:
                            current_step = _start_step(
                                data,
                                None,
                                legacy_turn_id,
                                legacy_event_id=_allocate_legacy_event_id(data, legacy_turn_id),
                                summary="Pre-message tool activity",
                            )
                            _maybe_mark_max_steps_exceeded(data)
                        call_id = item.get("id")
                        retry_of_call_id = item.get("retry_of_call_id") or item.get("retry_of")

                        if retry_of_call_id:
                            current_step["substeps"].append(
                                {
                                    "type": "tool_retry",
                                    "source": "provider",
                                    "call_id": call_id,
                                    "payload": {"retry_of_call_id": retry_of_call_id},
                                    "legacy_turn_id": legacy_turn_id,
                                    "legacy_event_id": _allocate_legacy_event_id(data, legacy_turn_id),
                                }
                            )

                        current_step["substeps"].append(
                            {
                                "type": "tool_call",
                                "source": "provider",
                                "call_id": call_id,
                                "payload": {
                                    "tool_name": "Bash",
                                    "command": command,
                                    "tool_input": {"command": command},
                                    "retry_of_call_id": retry_of_call_id,
                                },
                                "legacy_turn_id": legacy_turn_id,
                                "legacy_event_id": _allocate_legacy_event_id(data, legacy_turn_id),
                            }
                        )
                        exit_code = item.get("exit_code")
                        if not isinstance(exit_code, int):
                            maybe_exit = item.get("status_code")
                            exit_code = maybe_exit if isinstance(maybe_exit, int) else None
                        if isinstance(exit_code, int):
                            substep_type = "tool_output" if exit_code == 0 else "tool_error"
                            current_step["substeps"].append(
                                {
                                    "type": substep_type,
                                    "source": "provider",
                                    "call_id": call_id,
                                    "payload": {
                                        "exit_code": exit_code,
                                        "output": aggregated_output,
                                    },
                                    "legacy_turn_id": legacy_turn_id,
                                    "legacy_event_id": _allocate_legacy_event_id(data, legacy_turn_id),
                                }
                            )
                        elif aggregated_output:
                            current_step["substeps"].append(
                                {
                                    "type": "tool_output",
                                    "source": "provider",
                                    "call_id": call_id,
                                    "payload": {"output": aggregated_output},
                                    "legacy_turn_id": legacy_turn_id,
                                    "legacy_event_id": _allocate_legacy_event_id(data, legacy_turn_id),
                                }
                            )
                        # Truncate to 80 chars
                        command = truncate_text(command, 80)
                        formatter.print_tool_event("Bash", command)

                    elif item_type == "agent_message":
                        data["computed_turn_count"] = data.get("computed_turn_count", 0) + 1
                        raw_text = item.get("text", "")
                        data["approx_output_chars"] = data.get("approx_output_chars", 0) + len(raw_text)

                        # Treat each agent_message as a new logical step
                        # (like claude does with each unique msg_id)
                        data["computed_step_count"] = data.get("computed_step_count", 0) + 1
                        step_num = data["computed_step_count"]

                        elapsed_seconds = int(time.time() - data.get("start_time", time.time()))
                        display_input_tokens, display_output_tokens = _step_header_usage(data)
                        total_tokens = display_input_tokens + display_output_tokens
                        cost = calculate_cost(display_input_tokens, display_output_tokens, model)

                        if log_handle:
                            from datetime import datetime
                            timestamp_str = datetime.now().astimezone().strftime("%Y-%m-%d %H:%M:%S %Z")
                            log_handle.write(f"--- Step {step_num} at {timestamp_str} ---\n")
                            log_handle.flush()

                        formatter.print_step_header(
                            step_num,
                            total_tokens,
                            cost,
                            elapsed_seconds,
                            blank_line_before=step_num > 1,
                        )

                        legacy_turn_id = _current_turn_id(data)
                        _start_step(
                            data,
                            raw_text.strip() or None,
                            legacy_turn_id,
                            legacy_event_id=_allocate_legacy_event_id(data, legacy_turn_id),
                        )
                        _maybe_mark_max_steps_exceeded(data)

                        text = raw_text.strip()
                        if text:
                            if chat_text_display_length == 0:
                                formatter.print_agent_message(text)
                            else:
                                first_line = text.split("\n")[0]
                                formatter.print_agent_message(
                                    truncate_text(first_line, chat_text_display_length)
                                )

                    elif item_type == "reasoning":
                        # Optional: show reasoning (currently skipped)
                        pass

                # Codex usage may appear in different completion/error events depending on
                # execution mode. Capture usage from all completion/error events.
                if (
                    isinstance(event_type, str)
                    and (event_type.endswith(".completed") or event_type.endswith(".error"))
                    and isinstance(event.get("usage"), dict)
                ):
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
                        # Rebase estimate deltas so already-priced turns are not
                        # counted again in later step headers.
                        data["estimate_input_chars_baseline"] = _as_nonnegative_int(data.get("approx_input_chars"))
                        data["estimate_output_chars_baseline"] = _as_nonnegative_int(data.get("approx_output_chars"))

                elif isinstance(event_type, str) and "error" in event_type:
                    message = event.get("message") or event.get("error") or json.dumps(event)
                    formatter.print_error(f"Error: {message}")

            except json.JSONDecodeError:
                # Non-JSON output, just display it
                if line == data.get("_startup_line"):
                    return
                formatter.print_error(line)

        result = self.run_with_logging(
            cmd, log_file, timeout_minutes, cwd=cwd, parse_output=parse_codex_output, stdin_input=stdin_input
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
