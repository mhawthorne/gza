"""Claude Code provider implementation."""

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

    def check_credentials(self) -> bool:
        """Check for Claude credentials (OAuth or API key)."""
        claude_config = Path.home() / ".claude"
        if claude_config.is_dir():
            return True
        if os.getenv("ANTHROPIC_API_KEY"):
            return True
        return False

    def verify_credentials(self, config: Config) -> bool:
        """Verify Claude credentials by testing the claude command."""
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
            version_cmd=["claude", "--version"],
            error_patterns=["Invalid API key", "Please run /login", "/login"],
            error_message=(
                "Error: Invalid or missing Claude credentials\n"
                "  Run 'claude login' or set ANTHROPIC_API_KEY in .env"
            ),
        )

    def _verify_direct(self) -> bool:
        """Verify credentials work directly."""
        try:
            result = subprocess.run(
                ["claude", "--version"],
                capture_output=True,
                timeout=10,
                text=True,
            )
            output = result.stdout + result.stderr
            if "Invalid API key" in output or "Please run /login" in output or "/login" in output:
                print("Error: Invalid or missing Claude credentials")
                print("  Run 'claude login' or set ANTHROPIC_API_KEY in .env")
                return False
            if result.returncode == 0:
                return True
        except (subprocess.TimeoutExpired, FileNotFoundError) as e:
            if isinstance(e, FileNotFoundError):
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
    ) -> RunResult:
        """Run Claude to execute a task."""
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
        """Run Claude in Docker container."""
        docker_config = _get_docker_config(config.docker_image)

        if not ensure_docker_image(docker_config, config.project_dir):
            print("Error: Failed to build Docker image")
            return RunResult(exit_code=1)

        cmd = build_docker_cmd(docker_config, work_dir, config.timeout_minutes)
        cmd.extend(["claude", "-p", prompt, "--output-format", "stream-json", "--verbose"])
        cmd.extend(config.claude_args)
        cmd.extend(["--max-turns", str(config.max_turns)])

        return self._run_with_output_parsing(cmd, log_file, config.timeout_minutes)

    def _run_direct(
        self,
        config: Config,
        prompt: str,
        log_file: Path,
        work_dir: Path,
    ) -> RunResult:
        """Run Claude directly (no Docker)."""
        cmd = [
            "timeout", f"{config.timeout_minutes}m",
            "claude", "-p", prompt,
            "--output-format", "stream-json", "--verbose",
        ]
        cmd.extend(config.claude_args)
        cmd.extend(["--max-turns", str(config.max_turns)])

        return self._run_with_output_parsing(cmd, log_file, config.timeout_minutes, cwd=work_dir)

    def _run_with_output_parsing(
        self,
        cmd: list[str],
        log_file: Path,
        timeout_minutes: int,
        cwd: Path | None = None,
    ) -> RunResult:
        """Run command and parse Claude's stream-json output."""

        def parse_claude_output(line: str, data: dict) -> None:
            try:
                event = json.loads(line)
                event_type = event.get("type")

                if event_type == "assistant":
                    message = event.get("message", {})
                    for content in message.get("content", []):
                        if content.get("type") == "tool_use":
                            tool_name = content.get("name", "unknown")
                            tool_input = content.get("input", {})
                            # Extract file path for file-related tools
                            file_path = tool_input.get("file_path") or tool_input.get("path")
                            if file_path:
                                print(f"  → {tool_name} {file_path}")
                            else:
                                print(f"  → {tool_name}")
                        elif content.get("type") == "text":
                            text = content.get("text", "").strip()
                            if text:
                                first_line = text.split("\n")[0][:80]
                                print(f"  {first_line}")

                elif event_type == "result":
                    data["result"] = event

            except json.JSONDecodeError:
                # Non-JSON output, just display it
                print(line)

        result = self.run_with_logging(
            cmd, log_file, timeout_minutes, cwd=cwd, parse_output=parse_claude_output
        )

        # Extract stats and error info from result event
        result_data = getattr(result, "_accumulated_data", {}).get("result", {})
        if result_data:
            if "num_turns" in result_data:
                result.num_turns = result_data["num_turns"]
            if "total_cost_usd" in result_data:
                result.cost_usd = result_data["total_cost_usd"]
            # Check for error subtypes (e.g., error_max_turns)
            subtype = result_data.get("subtype", "")
            if subtype == "error_max_turns":
                result.error_type = "max_turns"

        return result
