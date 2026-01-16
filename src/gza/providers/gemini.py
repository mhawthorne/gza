"""Gemini CLI provider implementation."""

from __future__ import annotations

import json
import os
import subprocess
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


# Gemini pricing per token (as of Dec 2024)
# https://ai.google.dev/pricing
GEMINI_PRICING = {
    # Format: model_prefix -> (input_price_per_million, output_price_per_million)
    "gemini-2.5-pro": (1.25, 10.00),
    "gemini-2.5-flash": (0.15, 0.60),
    "gemini-2.0-flash": (0.10, 0.40),
    "gemini-1.5-pro": (1.25, 5.00),
    "gemini-1.5-flash": (0.075, 0.30),
    # Default fallback for unknown models
    "default": (1.25, 10.00),
}


def calculate_cost(model: str, input_tokens: int, output_tokens: int) -> float:
    """Calculate cost in USD based on model and token counts."""
    # Find matching pricing (prefix match)
    pricing = GEMINI_PRICING.get("default")
    for model_prefix, prices in GEMINI_PRICING.items():
        if model_prefix != "default" and model.startswith(model_prefix):
            pricing = prices
            break

    input_price, output_price = pricing
    cost = (input_tokens * input_price / 1_000_000) + (output_tokens * output_price / 1_000_000)
    return round(cost, 6)


def _get_docker_config(image_name: str) -> DockerConfig:
    """Get Docker configuration for Gemini."""
    return DockerConfig(
        image_name=image_name,
        npm_package="@google/gemini-cli",
        cli_command="gemini",
        config_dir=None,  # Use API key auth, no need to mount ~/.gemini
        env_vars=["GEMINI_API_KEY", "GOOGLE_API_KEY", "GOOGLE_APPLICATION_CREDENTIALS"],
    )


class GeminiProvider(Provider):
    """Gemini CLI provider."""

    @property
    def name(self) -> str:
        return "Gemini"

    def check_credentials(self) -> bool:
        """Check for Gemini credentials.

        Gemini CLI supports:
        - GEMINI_API_KEY: Primary API key
        - GOOGLE_API_KEY: For Vertex AI express mode
        - GOOGLE_APPLICATION_CREDENTIALS: Service account JSON file
        - OAuth login via 'gemini auth'
        """
        # Check for API keys
        if os.getenv("GEMINI_API_KEY"):
            return True
        if os.getenv("GOOGLE_API_KEY"):
            return True
        if os.getenv("GOOGLE_APPLICATION_CREDENTIALS"):
            return True
        # Check for OAuth credentials (stored after 'gemini auth')
        gemini_config = Path.home() / ".gemini"
        if gemini_config.is_dir():
            return True
        return False

    def verify_credentials(self, config: Config) -> bool:
        """Verify Gemini credentials by testing the gemini command."""
        if config.use_docker:
            return self._verify_docker(config)
        return self._verify_direct()

    def _verify_docker(self, config: Config) -> bool:
        """Verify credentials work in Docker."""
        # Use gemini-specific image name
        image_name = f"{config.project_name}-gza-gemini"
        docker_config = _get_docker_config(image_name)
        if not ensure_docker_image(docker_config, config.project_dir):
            print("Error: Failed to build Docker image")
            return False
        return verify_docker_credentials(
            docker_config=docker_config,
            version_cmd=["gemini", "--version"],
            error_patterns=["authentication", "api key", "unauthorized"],
            error_message=(
                "Error: Invalid or missing Gemini credentials\n"
                "  Run 'gemini auth' or set GEMINI_API_KEY in .env"
            ),
        )

    def _verify_direct(self) -> bool:
        """Verify credentials work directly."""
        try:
            result = subprocess.run(
                ["gemini", "--version"],
                capture_output=True,
                timeout=10,
                text=True,
            )
            if result.returncode == 0:
                return True
            output = result.stdout + result.stderr
            if "not found" in output.lower() or "command not found" in output.lower():
                print("Error: 'gemini' command not found")
                print("  Install with: npm install -g @google/gemini-cli")
                return False
            # Check for auth errors
            if "authentication" in output.lower() or "api key" in output.lower():
                print("Error: Invalid or missing Gemini credentials")
                print("  Run 'gemini auth' or set GEMINI_API_KEY in .env")
                return False
        except subprocess.TimeoutExpired:
            print("Error: Gemini CLI timed out")
            return False
        except FileNotFoundError:
            print("Error: 'gemini' command not found")
            print("  Install with: npm install -g @google/gemini-cli")
            return False
        return False

    def run(
        self,
        config: Config,
        prompt: str,
        log_file: Path,
        work_dir: Path,
    ) -> RunResult:
        """Run Gemini to execute a task."""
        if config.use_docker:
            return self._run_docker(config, prompt, log_file, work_dir)
        return self._run_direct(config, prompt, log_file, work_dir)

    def _run_docker(
        self,
        config: Config,
        prompt: str,
        log_file: Path,
        work_dir: Path,
    ) -> RunResult:
        """Run Gemini in Docker container."""
        # Use gemini-specific image name
        image_name = f"{config.project_name}-gza-gemini"
        docker_config = _get_docker_config(image_name)

        if not ensure_docker_image(docker_config, config.project_dir):
            print("Error: Failed to build Docker image")
            return RunResult(exit_code=1)

        cmd = build_docker_cmd(docker_config, work_dir, config.timeout_minutes)
        # Insert GEMINI_SHELL_ENABLED before the image name (last element from build_docker_cmd)
        cmd.insert(-1, "-e")
        cmd.insert(-1, "GEMINI_SHELL_ENABLED=true")
        cmd.extend([
            "gemini", "-p", prompt,
            "--output-format", "stream-json",
            "--allowed-tools", "run_shell_command",
        ])

        # Add model if specified
        if config.model:
            cmd.extend(["-m", config.model])

        return self._run_with_output_parsing(cmd, log_file, config.timeout_minutes, config.model)

    def _run_direct(
        self,
        config: Config,
        prompt: str,
        log_file: Path,
        work_dir: Path,
    ) -> RunResult:
        """Run Gemini directly."""
        cmd = [
            "env", "GEMINI_SHELL_ENABLED=true",
            "timeout", f"{config.timeout_minutes}m",
            "gemini", "-p", prompt,
            "--output-format", "stream-json",
            "--allowed-tools", "run_shell_command",
        ]

        # Add model if specified
        if config.model:
            cmd.extend(["-m", config.model])

        return self._run_with_output_parsing(cmd, log_file, config.timeout_minutes, config.model, cwd=work_dir)

    def _run_with_output_parsing(
        self,
        cmd: list[str],
        log_file: Path,
        timeout_minutes: int,
        model: str,
        cwd: Path | None = None,
    ) -> RunResult:
        """Run command and parse Gemini's stream-json output."""

        def parse_gemini_output(line: str, data: dict) -> None:
            try:
                event = json.loads(line)
                event_type = event.get("type")

                if event_type == "init":
                    # Store model from init event if not specified
                    if not data.get("model") and event.get("model"):
                        data["model"] = event["model"]

                elif event_type == "message":
                    # Show assistant messages
                    if event.get("role") == "assistant":
                        content = event.get("content", "")
                        if content and not event.get("delta"):
                            first_line = content.split("\n")[0][:80]
                            print(f"  {first_line}")

                elif event_type == "tool_use":
                    tool_name = event.get("tool_name", "unknown")
                    tool_input = event.get("tool_input", {})
                    # Extract file path for file-related tools
                    file_path = tool_input.get("file_path") or tool_input.get("path")
                    if file_path:
                        print(f"  → {tool_name} {file_path}")
                    else:
                        print(f"  → {tool_name}")

                elif event_type == "result":
                    data["result"] = event

            except json.JSONDecodeError:
                # Non-JSON output, just display it
                print(line)

        result = self.run_with_logging(
            cmd, log_file, timeout_minutes, cwd=cwd, parse_output=parse_gemini_output
        )

        # Extract stats from result event
        accumulated = getattr(result, "_accumulated_data", {})
        result_data = accumulated.get("result", {})

        if result_data:
            stats = result_data.get("stats", {})

            # Extract token counts
            input_tokens = stats.get("input_tokens")
            output_tokens = stats.get("output_tokens")

            if input_tokens is not None:
                result.input_tokens = input_tokens
            if output_tokens is not None:
                result.output_tokens = output_tokens

            # Get tool calls as proxy for turns
            tool_calls = stats.get("tool_calls")
            if tool_calls is not None:
                result.num_turns = tool_calls

            # Calculate cost from tokens
            if input_tokens is not None and output_tokens is not None:
                used_model = model or accumulated.get("model", "default")
                result.cost_usd = calculate_cost(used_model, input_tokens, output_tokens)

        return result
