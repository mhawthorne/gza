"""OpenAI Codex CLI provider implementation."""

from __future__ import annotations

import json
import os
import subprocess
import time
from pathlib import Path
from typing import TYPE_CHECKING

from .base import (
    Provider,
    RunResult,
    DockerConfig,
    ensure_docker_image,
    build_docker_cmd,
    verify_docker_credentials,
)

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


def _get_docker_config(image_name: str, use_oauth: bool = True) -> DockerConfig:
    """Get Docker configuration for Codex.

    Auth priority: OAuth (~/.codex) if available, otherwise CODEX_API_KEY.
    OAuth is preferred as it uses ChatGPT pricing (typically cheaper).

    Args:
        image_name: Docker image name to use.
        use_oauth: If True and OAuth credentials exist, mount ~/.codex.
                   If False, force API key auth (don't mount ~/.codex).
    """
    # Prefer OAuth if credentials exist, otherwise use API key
    if use_oauth and _has_codex_oauth():
        config_dir = ".codex"
        env_vars = []  # Don't need API key when using OAuth
    else:
        config_dir = None
        env_vars = ["CODEX_API_KEY"]

    return DockerConfig(
        image_name=image_name,
        npm_package="@openai/codex",
        cli_command="codex",
        config_dir=config_dir,
        env_vars=env_vars,
    )


class CodexProvider(Provider):
    """OpenAI Codex CLI provider."""

    @property
    def name(self) -> str:
        return "Codex"

    @property
    def credential_setup_hint(self) -> str:
        return "Set OPENAI_API_KEY in ~/.gza/.env or run 'codex --login' to authenticate"

    def check_credentials(self) -> bool:
        """Check for Codex credentials (OAuth or API key)."""
        codex_config = Path.home() / ".codex"
        if codex_config.is_dir():
            return True
        if os.getenv("CODEX_API_KEY"):
            return True
        return False

    def verify_credentials(self, config: Config) -> bool:
        """Verify Codex credentials by testing the codex command."""
        if config.use_docker:
            return self._verify_docker(config)
        return self._verify_direct()

    def _verify_docker(self, config: Config) -> bool:
        """Verify credentials work in Docker."""
        docker_config = _get_docker_config(config.docker_image)
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
    ) -> RunResult:
        """Run Codex to execute a task."""
        if config.use_docker:
            return self._run_docker(config, prompt, log_file, work_dir, resume_session_id)
        return self._run_direct(config, prompt, log_file, work_dir, resume_session_id)

    def _run_docker(
        self,
        config: Config,
        prompt: str,
        log_file: Path,
        work_dir: Path,
        resume_session_id: str | None = None,
    ) -> RunResult:
        """Run Codex in Docker container."""
        docker_config = _get_docker_config(config.docker_image)

        if not ensure_docker_image(docker_config, config.project_dir):
            print("Error: Failed to build Docker image")
            return RunResult(exit_code=1)

        cmd = build_docker_cmd(docker_config, work_dir, config.timeout_minutes, config.docker_volumes, config.docker_setup_command)

        if resume_session_id:
            cmd.extend(["codex", "exec", "resume", "--json", "--last"])
        else:
            cmd.extend([
                "codex", "exec", "--json",
                "--dangerously-bypass-approvals-and-sandbox",  # Bypass sandbox for headless operation
                "-C", "/workspace",  # Set working directory explicitly
                "-",  # Read prompt from stdin
            ])

            # Add model if specified
            if config.model:
                cmd.extend(["-m", config.model])

        return self._run_with_output_parsing(
            cmd, log_file, config.timeout_minutes, stdin_input=prompt if not resume_session_id else None,
            model=config.model, max_turns=config.max_turns,
        )

    def _run_direct(
        self,
        config: Config,
        prompt: str,
        log_file: Path,
        work_dir: Path,
        resume_session_id: str | None = None,
    ) -> RunResult:
        """Run Codex directly (no Docker)."""
        cmd = [
            "timeout", f"{config.timeout_minutes}m",
        ]

        if resume_session_id:
            cmd.extend(["codex", "exec", "resume", "--json", "--last"])
        else:
            cmd.extend([
                "codex", "exec", "--json",
                "--dangerously-bypass-approvals-and-sandbox",  # Bypass sandbox for headless operation
                "-C", str(work_dir),
                "-",  # Read prompt from stdin
            ])

            # Add model if specified
            if config.model:
                cmd.extend(["-m", config.model])

        return self._run_with_output_parsing(
            cmd, log_file, config.timeout_minutes, cwd=work_dir if not resume_session_id else None,
            stdin_input=prompt if not resume_session_id else None, model=config.model,
            max_turns=config.max_turns,
        )

    def _run_with_output_parsing(
        self,
        cmd: list[str],
        log_file: Path,
        timeout_minutes: int,
        cwd: Path | None = None,
        stdin_input: str | None = None,
        model: str = "",
        max_turns: int = 50,
    ) -> RunResult:
        """Run command and parse Codex's JSON output."""

        def parse_codex_output(line: str, data: dict, log_handle=None) -> None:
            try:
                event = json.loads(line)
                event_type = event.get("type")

                if event_type == "thread.started":
                    data["thread_id"] = event.get("thread_id")

                elif event_type == "turn.started":
                    if "turn_count" not in data:
                        data["turn_count"] = 0
                        data["start_time"] = time.time()
                        data["item_count"] = 0
                        data["item_count_in_turn"] = 0
                        data["computed_turn_count"] = 0
                    data["turn_count"] += 1
                    data["item_count_in_turn"] = 0

                    # Check if we've exceeded max_turns
                    if data["turn_count"] > max_turns:
                        data["exceeded_max_turns"] = True
                        # Note: We can't kill the process from here, so we just mark it
                        # The actual process will be killed by the timeout mechanism

                    # Calculate runtime
                    elapsed_seconds = int(time.time() - data["start_time"])
                    if elapsed_seconds >= 60:
                        runtime_str = f"{elapsed_seconds // 60}m {elapsed_seconds % 60}s"
                    else:
                        runtime_str = f"{elapsed_seconds}s"

                    # Display turn info
                    total_tokens = data.get("input_tokens", 0) + data.get("output_tokens", 0)
                    if total_tokens > 1_000_000:
                        token_str = f"{total_tokens / 1_000_000:.1f}M tokens"
                    elif total_tokens > 1000:
                        token_str = f"{total_tokens // 1000}k tokens"
                    else:
                        token_str = f"{total_tokens} tokens"

                    # Calculate estimated cost
                    cost = calculate_cost(
                        data.get("input_tokens", 0),
                        data.get("output_tokens", 0),
                        model,
                    )
                    cost_str = f"${cost:.2f}"

                    # Add blank line before turn (except first turn)
                    if data["turn_count"] > 1:
                        print()

                    # Print turn info
                    print(f"| Turn {data['turn_count']} | {token_str} | {cost_str} | {runtime_str} |")

                elif event_type == "item.completed":
                    item = event.get("item", {})
                    item_type = item.get("type")
                    data["item_count"] = data.get("item_count", 0) + 1
                    data["item_count_in_turn"] = data.get("item_count_in_turn", 0) + 1
                    turn_count = data.get("turn_count", 0)
                    item_idx = data.get("item_count_in_turn", 0)
                    item_prefix = f"[T{turn_count}.{item_idx}] " if turn_count > 0 else ""

                    if item_type == "command_execution":
                        command = item.get("command", "")
                        # Truncate to 80 chars
                        if len(command) > 80:
                            command = command[:77] + "..."
                        print(f"  {item_prefix}→ Bash {command}")

                    elif item_type == "agent_message":
                        data["computed_turn_count"] = data.get("computed_turn_count", 0) + 1
                        text = item.get("text", "").strip()
                        if text:
                            # Truncate to first line and 80 chars
                            first_line = text.split("\n")[0]
                            if len(first_line) > 80:
                                first_line = first_line[:77] + "..."
                            print(f"  {item_prefix}{first_line}")

                    elif item_type == "reasoning":
                        # Optional: show reasoning (currently skipped)
                        pass

                elif event_type == "turn.completed":
                    usage = event.get("usage", {})
                    if "input_tokens" not in data:
                        data["input_tokens"] = 0
                        data["output_tokens"] = 0
                        data["cached_tokens"] = 0
                    data["input_tokens"] += usage.get("input_tokens", 0)
                    data["output_tokens"] += usage.get("output_tokens", 0)
                    data["cached_tokens"] += usage.get("cached_input_tokens", 0)

            except json.JSONDecodeError:
                # Non-JSON output, just display it
                if line == data.get("_startup_line"):
                    return
                print(line)

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

            # Set token counts
            if "input_tokens" in accumulated:
                result.input_tokens = accumulated["input_tokens"]
            if "output_tokens" in accumulated:
                result.output_tokens = accumulated["output_tokens"]

            # Calculate cost
            if result.input_tokens is not None and result.output_tokens is not None:
                result.cost_usd = calculate_cost(
                    result.input_tokens,
                    result.output_tokens,
                    model,
                )

            # Check if we exceeded max turns
            if accumulated.get("exceeded_max_turns"):
                result.error_type = "max_turns"

            # Store session ID for resume capability
            if "thread_id" in accumulated:
                result.session_id = accumulated["thread_id"]

        return result
