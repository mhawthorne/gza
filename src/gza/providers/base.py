"""Base provider interface and common utilities."""

from __future__ import annotations

import os
import subprocess
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

from ..config import APP_NAME

if TYPE_CHECKING:
    from ..config import Config


# Base Dockerfile template - providers customize the npm package and command
DOCKERFILE_TEMPLATE = """\
FROM node:20-slim

# Install the CLI tool globally
RUN npm install -g {npm_package}

# Create gza user for isolation
RUN useradd -m -s /bin/bash gza
USER gza
WORKDIR /home/gza

# Default command
CMD ["{cli_command}"]
"""


@dataclass
class DockerConfig:
    """Configuration for Docker execution."""
    image_name: str
    npm_package: str
    cli_command: str
    config_dir: str | None  # e.g., ".claude" or ".gemini", None to skip mount
    env_vars: list[str]  # e.g., ["ANTHROPIC_API_KEY", "GEMINI_API_KEY"]


def _get_image_created_time(image_name: str) -> float | None:
    """Get the creation timestamp of a Docker image.

    Returns:
        Unix timestamp of image creation, or None if image doesn't exist.
    """
    import json

    result = subprocess.run(
        ["docker", "image", "inspect", image_name, "--format", "{{.Created}}"],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        return None

    # Parse ISO 8601 timestamp (e.g., "2025-01-08T10:30:00.123456789Z")
    from datetime import datetime

    timestamp_str = result.stdout.strip()
    try:
        # Handle nanoseconds by truncating to microseconds
        if "." in timestamp_str:
            base, frac = timestamp_str.rsplit(".", 1)
            frac = frac.rstrip("Z")[:6]  # Keep only 6 digits for microseconds
            timestamp_str = f"{base}.{frac}Z"
        dt = datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))
        return dt.timestamp()
    except ValueError:
        return None


def ensure_docker_image(docker_config: DockerConfig, project_dir: Path) -> bool:
    """Ensure Docker image exists, building if needed.

    Args:
        docker_config: Docker configuration
        project_dir: Project directory for storing Dockerfile

    Returns:
        True if image is available, False on failure
    """
    etc_dir = project_dir / "etc"
    etc_dir.mkdir(parents=True, exist_ok=True)
    dockerfile_path = etc_dir / f"Dockerfile.{docker_config.cli_command}"

    # Check if image exists and is up-to-date
    image_time = _get_image_created_time(docker_config.image_name)
    if image_time is not None:
        # Image exists - check if Dockerfile is newer
        if dockerfile_path.exists():
            dockerfile_time = dockerfile_path.stat().st_mtime
            if dockerfile_time > image_time:
                print(f"Dockerfile changed, rebuilding {docker_config.image_name}...")
            else:
                return True  # Image is up-to-date
        else:
            return True  # No Dockerfile to compare, image exists

    # Generate Dockerfile if it doesn't exist (preserve custom Dockerfiles)
    if not dockerfile_path.exists():
        dockerfile_content = DOCKERFILE_TEMPLATE.format(
            npm_package=docker_config.npm_package,
            cli_command=docker_config.cli_command,
        )
        dockerfile_path.write_text(dockerfile_content)

    print(f"Building Docker image {docker_config.image_name}...")
    result = subprocess.run(
        ["docker", "build", "-t", docker_config.image_name,
         "-f", str(dockerfile_path), str(etc_dir)],
    )
    return result.returncode == 0


def build_docker_cmd(
    docker_config: DockerConfig,
    work_dir: Path,
    timeout_minutes: int,
) -> list[str]:
    """Build the base Docker run command.

    Args:
        docker_config: Docker configuration
        work_dir: Working directory to mount
        timeout_minutes: Timeout in minutes

    Returns:
        List of command arguments (without the actual CLI command)
    """
    cmd = [
        "timeout", f"{timeout_minutes}m",
        "docker", "run", "--rm", "-i",
        "-v", f"{work_dir}:/workspace",
        "-w", "/workspace",
    ]

    # Mount config directory if specified (for OAuth credentials)
    if docker_config.config_dir:
        cmd.insert(-2, "-v")
        cmd.insert(-2, f"{Path.home()}/{docker_config.config_dir}:/home/gza/{docker_config.config_dir}")

    # Pass environment variables if set
    for env_var in docker_config.env_vars:
        if os.getenv(env_var):
            cmd.extend(["-e", env_var])

    cmd.append(docker_config.image_name)
    return cmd


def is_docker_running() -> bool:
    """Check if Docker daemon is running."""
    try:
        result = subprocess.run(
            ["docker", "info"],
            capture_output=True,
            timeout=5,
        )
        return result.returncode == 0
    except (subprocess.TimeoutExpired, FileNotFoundError):
        return False


def verify_docker_credentials(
    docker_config: DockerConfig,
    version_cmd: list[str],
    error_patterns: list[str],
    error_message: str,
) -> bool:
    """Verify credentials work in Docker by running a version check.

    Args:
        docker_config: Docker configuration
        version_cmd: Command to run for version check (e.g., ["claude", "--version"])
        error_patterns: Strings that indicate auth errors in output
        error_message: Message to print on auth error

    Returns:
        True if credentials are valid
    """
    if not is_docker_running():
        print("Error: Docker daemon is not running")
        print("  Start Docker Desktop or use --no-docker flag")
        return False

    try:
        cmd = ["docker", "run", "--rm"]
        # Mount config directory if specified (for OAuth credentials)
        if docker_config.config_dir:
            cmd.extend(["-v", f"{Path.home()}/{docker_config.config_dir}:/home/gza/{docker_config.config_dir}"])
        for env_var in docker_config.env_vars:
            if os.getenv(env_var):
                cmd.extend(["-e", env_var])
        cmd.append(docker_config.image_name)
        cmd.extend(version_cmd)

        result = subprocess.run(
            cmd,
            capture_output=True,
            timeout=30,
            text=True,
        )
        output = result.stdout + result.stderr

        for pattern in error_patterns:
            if pattern in output:
                print(error_message)
                return False

        if result.returncode == 0:
            return True
    except subprocess.TimeoutExpired:
        print("Error: Docker command timed out")
        return False
    except FileNotFoundError:
        print("Error: Docker not found")
        return False

    return False


@dataclass
class RunResult:
    """Result from running a code generation provider."""
    exit_code: int
    duration_seconds: float = 0.0
    num_turns: int | None = None
    cost_usd: float | None = None
    input_tokens: int | None = None
    output_tokens: int | None = None
    error_type: str | None = None  # e.g., "max_turns" when turn limit exceeded
    session_id: str | None = None  # Claude session ID for resume capability


class Provider(ABC):
    """Base class for AI code generation providers."""

    @property
    @abstractmethod
    def name(self) -> str:
        """Provider name for display."""
        ...

    @abstractmethod
    def check_credentials(self) -> bool:
        """Check if credentials are configured (quick check)."""
        ...

    @abstractmethod
    def verify_credentials(self, config: Config) -> bool:
        """Verify credentials work by testing the command."""
        ...

    @abstractmethod
    def run(
        self,
        config: Config,
        prompt: str,
        log_file: Path,
        work_dir: Path,
    ) -> RunResult:
        """Run the provider to execute a task.

        Args:
            config: Gza configuration
            prompt: The task prompt
            log_file: Path to write logs
            work_dir: Working directory for execution

        Returns:
            RunResult with exit code and statistics
        """
        ...

    def run_with_logging(
        self,
        cmd: list[str],
        log_file: Path,
        timeout_minutes: int,
        cwd: Path | None = None,
        parse_output: callable = None,
        stdin_input: str | None = None,
    ) -> RunResult:
        """Run command with output to both console and log file.

        This is a utility method that providers can use for common logging behavior.

        Args:
            cmd: Command and arguments to run
            log_file: Path to log file
            timeout_minutes: Timeout in minutes
            cwd: Working directory
            parse_output: Optional callback to parse each line of output.
                         Called with (line: str, accumulated_data: dict).
                         The callback should update accumulated_data in place.
            stdin_input: Optional string to pass to stdin

        Returns:
            RunResult with exit code and duration. Stats should be filled
            in by parse_output callback or by caller.
        """
        print(f"Logging to: {log_file}")
        print(f"Timeout: {timeout_minutes} minutes")
        print("")

        start_time = time.time()
        accumulated_data: dict = {}

        with open(log_file, "w") as log:
            process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                stdin=subprocess.PIPE if stdin_input else None,
                text=True,
                cwd=cwd,
            )

            # Write stdin if provided
            if stdin_input:
                process.stdin.write(stdin_input)
                process.stdin.close()

            for line in process.stdout:
                log.write(line)
                line = line.strip()
                if not line:
                    continue

                if parse_output:
                    parse_output(line, accumulated_data)

            process.wait()

        duration_seconds = time.time() - start_time

        result = RunResult(
            exit_code=process.returncode,
            duration_seconds=round(duration_seconds, 1),
        )

        # Let caller extract stats from accumulated_data
        result._accumulated_data = accumulated_data
        return result


def get_provider(config: Config) -> Provider:
    """Get the appropriate provider based on config."""
    from .claude import ClaudeProvider
    from .gemini import GeminiProvider

    providers = {
        "claude": ClaudeProvider,
        "gemini": GeminiProvider,
    }

    provider_class = providers.get(config.provider)
    if not provider_class:
        raise ValueError(f"Unknown provider: {config.provider}")

    return provider_class()
