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
        resume_session_id: str | None = None,
    ) -> RunResult:
        """Run Claude to execute a task."""
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
        """Run Claude in Docker container."""
        docker_config = _get_docker_config(config.docker_image)

        if not ensure_docker_image(docker_config, config.project_dir):
            print("Error: Failed to build Docker image")
            return RunResult(exit_code=1)

        cmd = build_docker_cmd(docker_config, work_dir, config.timeout_minutes)
        cmd.extend(["claude", "-p", "-", "--output-format", "stream-json", "--verbose"])

        if resume_session_id:
            cmd.extend(["--resume", resume_session_id])

        cmd.extend(config.claude_args)
        cmd.extend(["--max-turns", str(config.max_turns)])

        return self._run_with_output_parsing(cmd, log_file, config.timeout_minutes, stdin_input=prompt)

    def _run_direct(
        self,
        config: Config,
        prompt: str,
        log_file: Path,
        work_dir: Path,
        resume_session_id: str | None = None,
    ) -> RunResult:
        """Run Claude directly (no Docker)."""
        cmd = [
            "timeout", f"{config.timeout_minutes}m",
            "claude", "-p", "-",
            "--output-format", "stream-json", "--verbose",
        ]

        if resume_session_id:
            cmd.extend(["--resume", resume_session_id])

        cmd.extend(config.claude_args)
        cmd.extend(["--max-turns", str(config.max_turns)])

        return self._run_with_output_parsing(cmd, log_file, config.timeout_minutes, cwd=work_dir, stdin_input=prompt)

    def _run_with_output_parsing(
        self,
        cmd: list[str],
        log_file: Path,
        timeout_minutes: int,
        cwd: Path | None = None,
        stdin_input: str | None = None,
    ) -> RunResult:
        """Run command and parse Claude's stream-json output."""

        def parse_claude_output(line: str, data: dict) -> None:
            try:
                event = json.loads(line)
                event_type = event.get("type")

                if event_type == "assistant":
                    message = event.get("message", {})
                    msg_id = message.get("id")

                    # Track unique message IDs as turn proxy
                    if "seen_msg_ids" not in data:
                        data["seen_msg_ids"] = set()
                    if msg_id and msg_id not in data["seen_msg_ids"]:
                        data["seen_msg_ids"].add(msg_id)
                        turn_count = len(data["seen_msg_ids"])

                        # Accumulate token usage for cost estimation
                        usage = message.get("usage", {})
                        if "total_input_tokens" not in data:
                            data["total_input_tokens"] = 0
                            data["total_output_tokens"] = 0
                        data["total_input_tokens"] += usage.get("input_tokens", 0)
                        data["total_input_tokens"] += usage.get("cache_creation_input_tokens", 0)
                        data["total_input_tokens"] += usage.get("cache_read_input_tokens", 0)
                        data["total_output_tokens"] += usage.get("output_tokens", 0)

                        # Display live stats
                        total_tokens = data["total_input_tokens"] + data["total_output_tokens"]
                        if total_tokens > 1_000_000:
                            token_str = f"{total_tokens / 1_000_000:.1f}M tokens"
                        elif total_tokens > 1000:
                            token_str = f"{total_tokens // 1000}k tokens"
                        else:
                            token_str = f"{total_tokens} tokens"
                        print(f"  [turn {turn_count}, {token_str}]")

                    for content in message.get("content", []):
                        if content.get("type") == "tool_use":
                            tool_name = content.get("name", "unknown")
                            tool_input = content.get("input", {})

                            # Extract file path for file-related tools
                            file_path = tool_input.get("file_path") or tool_input.get("path")

                            # Enhanced logging for specific tools
                            if tool_name == "Glob":
                                pattern = tool_input.get("pattern", "")
                                print(f"  → {tool_name} {pattern}")
                            elif tool_name == "TodoWrite":
                                todos = tool_input.get("todos", [])
                                todos_summary = f"{len(todos)} todos"
                                # Show status breakdown if available
                                if todos:
                                    pending = sum(1 for t in todos if t.get("status") == "pending")
                                    in_progress = sum(1 for t in todos if t.get("status") == "in_progress")
                                    completed = sum(1 for t in todos if t.get("status") == "completed")
                                    todos_summary += f" (pending: {pending}, in_progress: {in_progress}, completed: {completed})"
                                print(f"  → {tool_name} {todos_summary}")
                            elif file_path:
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
            cmd, log_file, timeout_minutes, cwd=cwd, parse_output=parse_claude_output, stdin_input=stdin_input
        )

        # Extract stats and error info from result event
        result_data = getattr(result, "_accumulated_data", {}).get("result", {})
        if result_data:
            if "num_turns" in result_data:
                result.num_turns = result_data["num_turns"]
            if "total_cost_usd" in result_data:
                result.cost_usd = result_data["total_cost_usd"]
            if "session_id" in result_data:
                result.session_id = result_data["session_id"]
            # Check for error subtypes (e.g., error_max_turns)
            subtype = result_data.get("subtype", "")
            if subtype == "error_max_turns":
                result.error_type = "max_turns"

        return result
