"""Base provider interface and common utilities."""

from __future__ import annotations

import os
import shlex
import shutil
import subprocess
import time
from abc import ABC, abstractmethod
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from ..config import Config


# Dockerfile template with all common dependencies
DOCKERFILE_TEMPLATE = """\
FROM node:20-slim

RUN apt-get update && apt-get install -y \\
    ca-certificates \\
    curl \\
    git \\
    ripgrep \\
    python3 \\
    python3-pip \\
    python3-venv \\
    && rm -rf /var/lib/apt/lists/*

RUN curl -LsSf https://astral.sh/uv/install.sh | sh
RUN cp /root/.local/bin/uv /usr/local/bin/uv

RUN npm install -g {npm_package}

# Run optional setup command from env var, then exec the CLI
RUN printf '#!/bin/bash\\nset -e\\nif [ -n "$GZA_DOCKER_SETUP_COMMAND" ]; then\\n    eval "$GZA_DOCKER_SETUP_COMMAND"\\nfi\\nexec "$@"\\n' > /usr/local/bin/entrypoint.sh && chmod +x /usr/local/bin/entrypoint.sh

RUN useradd -m -s /bin/bash gza
USER gza
ENV PATH="/usr/local/bin:/usr/bin:/bin:/home/gza/.local/bin"
WORKDIR /home/gza

ENTRYPOINT ["entrypoint.sh"]
CMD ["{cli_command}"]
"""

GZA_SHIM_SETUP_COMMAND = """\
mkdir -p /tmp/gza-shims
cat > /tmp/gza-shims/gza <<'EOF'
#!/bin/sh
if [ -x /workspace/bin/gza ]; then
    exec /workspace/bin/gza "$@"
fi
if command -v uv >/dev/null 2>&1; then
    exec uv run --directory /workspace gza "$@"
fi
echo "Error: gza command unavailable in container. Use 'uv run gza ...' from /workspace." >&2
exit 127
EOF
chmod +x /tmp/gza-shims/gza
export PATH="/tmp/gza-shims:/workspace/bin:$PATH"
"""


@dataclass
class DockerConfig:
    """Configuration for Docker execution."""
    image_name: str
    npm_package: str
    cli_command: str
    config_dir: str | None  # e.g., ".claude" or ".gemini", None to skip mount
    env_vars: list[str]  # e.g., ["ANTHROPIC_API_KEY", "GEMINI_API_KEY"]


def _get_config_dir_volume_args(docker_config: DockerConfig) -> list[str]:
    """Return Docker -v args for mounting provider config dir and JSON file.

    Handles the shutil.copy2 workaround for Docker Desktop not sharing
    individual files.
    """
    if not docker_config.config_dir:
        return []
    args: list[str] = []
    config_dir = Path.home() / docker_config.config_dir
    args.extend(["-v", f"{config_dir}:/home/gza/{docker_config.config_dir}"])
    # Also mount the config file (e.g., ~/.claude.json) if it exists
    # Docker Desktop can't share individual files, so copy it into the config dir
    config_file = Path.home() / f"{docker_config.config_dir}.json"
    if config_file.exists():
        config_dir.mkdir(parents=True, exist_ok=True)
        dest = config_dir / f"{docker_config.config_dir}.json"
        shutil.copy2(config_file, dest)
        args.extend(["-v", f"{dest}:/home/gza/{docker_config.config_dir}.json"])
    return args


def _get_image_created_time(image_name: str) -> float | None:
    """Get the creation timestamp of a Docker image.

    Returns:
        Unix timestamp of image creation, or None if image doesn't exist.
    """

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


def _get_image_label(image_name: str, label_key: str) -> str | None:
    """Get a specific label value from a Docker image.

    Returns:
        Label value, or None if image/label does not exist.
    """
    result = subprocess.run(
        [
            "docker",
            "image",
            "inspect",
            image_name,
            "--format",
            f'{{{{index .Config.Labels "{label_key}"}}}}',
        ],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        return None
    value = result.stdout.strip()
    if not value or value == "<no value>":
        return None
    return value


def ensure_docker_image(docker_config: DockerConfig, project_dir: Path) -> bool:
    """Ensure Docker image exists, building if needed.

    Args:
        docker_config: Docker configuration
        project_dir: Project directory for storing Dockerfile

    Returns:
        True if image is available, False on failure
    """
    if not is_docker_running():
        print("Error: Docker daemon is not running")
        print("  Start Docker Desktop or use --no-docker flag")
        return False

    etc_dir = project_dir / "etc"
    etc_dir.mkdir(parents=True, exist_ok=True)
    dockerfile_path = etc_dir / f"Dockerfile.{docker_config.cli_command}"

    # Check if image exists and is up-to-date
    rebuild_reason: str | None = None
    image_time = _get_image_created_time(docker_config.image_name)
    if image_time is not None:
        # Image exists - check if Dockerfile is newer
        if dockerfile_path.exists():
            dockerfile_time = dockerfile_path.stat().st_mtime
            if dockerfile_time > image_time:
                rebuild_reason = f"{dockerfile_path.name} is newer than image"
            else:
                print(
                    f"Using Docker image {docker_config.image_name} "
                    f"(up-to-date for {docker_config.cli_command})"
                )
                return True  # Image is up-to-date
        else:
            print(
                f"Using Docker image {docker_config.image_name} "
                f"(up-to-date for {docker_config.cli_command}; no {dockerfile_path.name} timestamp to compare)"
            )
            return True  # No Dockerfile to compare, image exists
    else:
        rebuild_reason = "image not found"

    # Generate Dockerfile if it doesn't exist (preserve custom Dockerfiles)
    if not dockerfile_path.exists():
        dockerfile_content = DOCKERFILE_TEMPLATE.format(
            npm_package=docker_config.npm_package,
            cli_command=docker_config.cli_command,
        )
        dockerfile_path.write_text(dockerfile_content)

    print(f"Rebuilding Docker image {docker_config.image_name}: {rebuild_reason}")
    result = subprocess.run(
        [
            "docker",
            "build",
            "-t",
            docker_config.image_name,
            "--label",
            f"gza.cli_command={docker_config.cli_command}",
            "--label",
            f"gza.npm_package={docker_config.npm_package}",
            "-f",
            str(dockerfile_path),
            str(etc_dir),
        ],
    )
    return result.returncode == 0


def build_docker_cmd(
    docker_config: DockerConfig,
    work_dir: Path,
    timeout_minutes: int,
    docker_volumes: list[str] | None = None,
    docker_setup_command: str = "",
    interactive: bool = False,
) -> list[str]:
    """Build the base Docker run command.

    Args:
        docker_config: Docker configuration
        work_dir: Working directory to mount
        timeout_minutes: Timeout in minutes
        docker_volumes: Optional list of custom volume mounts (e.g., ["/host:/container:ro"])
        docker_setup_command: Optional setup command to run inside container before CLI starts
        interactive: If True, allocate a TTY (-it) for interactive use (e.g. attach handoff).
            When False, attach only stdin (-i) which is required for streaming-json pipe mode.

    Returns:
        List of command arguments (without the actual CLI command)
    """
    stdio_flag = "-it" if interactive else "-i"
    cmd = [
        "timeout", f"{timeout_minutes}m",
        "docker", "run", "--rm", stdio_flag,
        "-v", f"{work_dir}:/workspace",
        "-w", "/workspace",
    ]

    # If work_dir is a git worktree, mount the host .git directory so git
    # commands work inside the container.  Worktree .git files reference an
    # absolute host path (e.g. /Users/.../project/.git/worktrees/<name>) that
    # doesn't exist in the container.  Mounting the main .git dir at the same
    # host path makes the reference resolve transparently.
    git_file = work_dir / ".git"
    if git_file.is_file():
        try:
            first_line = git_file.read_text().splitlines()[0].strip()
            if first_line.startswith("gitdir:"):
                gitdir = Path(first_line.split(":", 1)[1].strip())
                # gitdir is .git/worktrees/<name>; main .git is two levels up
                main_git_dir = gitdir.parent.parent
                if main_git_dir.is_dir():
                    cmd.extend(["-v", f"{main_git_dir}:{main_git_dir}"])
        except (OSError, IndexError):
            pass  # Non-fatal — git just won't work inside the container

    # Mount config directory if specified (for OAuth credentials)
    for arg in _get_config_dir_volume_args(docker_config):
        cmd.insert(-2, arg)

    # Add custom volume mounts
    if docker_volumes:
        for volume in docker_volumes:
            cmd.extend(["-v", volume])

    # Pass environment variables if set
    for env_var in docker_config.env_vars:
        if os.getenv(env_var):
            cmd.extend(["-e", env_var])

    # Pass git identity into the container so git commit/rebase works.
    # Read from host git config; GIT_* env vars override if already set.
    for env_var, git_key in [
        ("GIT_AUTHOR_NAME", "user.name"),
        ("GIT_AUTHOR_EMAIL", "user.email"),
        ("GIT_COMMITTER_NAME", "user.name"),
        ("GIT_COMMITTER_EMAIL", "user.email"),
    ]:
        if os.getenv(env_var):
            cmd.extend(["-e", env_var])
        else:
            try:
                result = subprocess.run(
                    ["git", "config", git_key],
                    capture_output=True, text=True, timeout=5,
                )
                if result.returncode == 0 and result.stdout.strip():
                    cmd.extend(["-e", f"{env_var}={result.stdout.strip()}"])
            except (subprocess.TimeoutExpired, FileNotFoundError):
                pass

    # Always install a `gza` shim so bare `gza ...` works inside task containers.
    setup_commands: list[str] = [GZA_SHIM_SETUP_COMMAND.strip()]
    if docker_setup_command.strip():
        setup_commands.append(docker_setup_command.strip())
    combined_setup_command = "\n".join(setup_commands)
    cmd.extend(["-e", f"GZA_DOCKER_SETUP_COMMAND={combined_setup_command}"])

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


def _tail(text: str, max_chars: int = 2000) -> str:
    """Return the last ``max_chars`` of ``text`` (non-destructive)."""
    if not text:
        return ""
    if len(text) <= max_chars:
        return text
    return text[-max_chars:]


def write_preflight_entry(
    log_file: Path | None,
    *,
    event: str,
    command: list[str],
    returncode: int | None,
    stdout_tail: str,
    stderr_tail: str,
    message: str,
) -> None:
    """Append a preflight JSONL entry describing a verify subprocess call."""
    if log_file is None:
        return
    import json
    entry = {
        "type": "gza",
        "subtype": "preflight",
        "event": event,
        "command": _format_command_for_log(command),
        "returncode": returncode,
        "stdout_tail": _tail(stdout_tail),
        "stderr_tail": _tail(stderr_tail),
        "message": message,
    }
    try:
        with open(log_file, "a") as f:
            f.write(json.dumps(entry) + "\n")
            f.flush()
    except OSError:
        pass


def verify_docker_credentials(
    docker_config: DockerConfig,
    version_cmd: list[str],
    error_patterns: list[str],
    error_message: str,
    log_file: Path | None = None,
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
        write_preflight_entry(
            log_file,
            event="docker_daemon_missing",
            command=["docker", "info"],
            returncode=None,
            stdout_tail="",
            stderr_tail="",
            message="Docker daemon is not running",
        )
        print("Error: Docker daemon is not running")
        print("  Start Docker Desktop or use --no-docker flag")
        return False

    try:
        cmd = ["docker", "run", "--rm"]
        # Mount config directory if specified (for OAuth credentials)
        cmd.extend(_get_config_dir_volume_args(docker_config))
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
        write_preflight_entry(
            log_file,
            event="verify_credentials_docker",
            command=cmd,
            returncode=result.returncode,
            stdout_tail=result.stdout,
            stderr_tail=result.stderr,
            message=f"docker {' '.join(version_cmd)} exited {result.returncode}",
        )

        for pattern in error_patterns:
            if pattern in output:
                print(error_message)
                return False

        if result.returncode == 0:
            return True

        # Non-zero exit code without matching error patterns
        print(f"Error: Credential verification failed (exit code {result.returncode})")
        if output.strip():
            # Show last few lines of output for debugging
            lines = output.strip().split("\n")
            if len(lines) > 5:
                print("  Last 5 lines of output:")
                for line in lines[-5:]:
                    print(f"    {line}")
            else:
                print("  Output:")
                for line in lines:
                    print(f"    {line}")
        else:
            print("  (no output)")

        # Check if config directory doesn't exist
        if docker_config.config_dir:
            config_path = Path.home() / docker_config.config_dir
            if not config_path.exists():
                print(f"\n  Hint: {config_path} directory not found")
                print("  You may need to set the API key environment variable or run the login command")

        return False
    except subprocess.TimeoutExpired:
        write_preflight_entry(
            log_file,
            event="verify_credentials_timeout",
            command=["docker", "run", "--rm", docker_config.image_name, *version_cmd],
            returncode=None,
            stdout_tail="",
            stderr_tail="",
            message="Docker command timed out during verify",
        )
        print("Error: Docker command timed out")
        return False
    except FileNotFoundError:
        write_preflight_entry(
            log_file,
            event="verify_credentials_missing_binary",
            command=["docker"],
            returncode=None,
            stdout_tail="",
            stderr_tail="",
            message="Docker binary not found on PATH",
        )
        print("Error: Docker not found")
        return False

    return False


@dataclass
class RunResult:
    """Result from running a code generation provider."""
    exit_code: int
    duration_seconds: float = 0.0
    num_steps_reported: int | None = None  # Step count reported by the provider
    num_steps_computed: int | None = None  # Step count computed internally
    num_turns_reported: int | None = None  # Turn count reported by the provider (e.g., Claude's result event)
    num_turns_computed: int | None = None  # Turn count computed internally (e.g., by counting unique message IDs)
    cost_usd: float | None = None
    input_tokens: int | None = None
    output_tokens: int | None = None
    tokens_estimated: bool = False
    cost_estimated: bool = False
    error_type: str | None = None  # e.g., "max_steps" when step limit exceeded
    session_id: str | None = None  # Claude session ID for resume capability
    _accumulated_data: dict[str, Any] | None = None  # Internal data for parsing


class Provider(ABC):
    """Base class for AI code generation providers."""

    @property
    @abstractmethod
    def name(self) -> str:
        """Provider name for display."""
        ...

    @property
    def supports_interactive_foreground(self) -> bool:
        """Return whether provider supports interactive foreground inline runs."""
        return False

    @property
    def credential_setup_hint(self) -> str:
        """Return a hint for setting up credentials.

        Override in subclasses to provide provider-specific instructions.
        """
        return "Check the provider documentation for credential setup."

    @abstractmethod
    def check_credentials(self) -> bool:
        """Check if credentials are configured (quick check)."""
        ...

    @abstractmethod
    def verify_credentials(self, config: Config, log_file: Path | None = None) -> bool:
        """Verify credentials work by testing the command.

        If ``log_file`` is provided, implementations should append a JSONL
        entry capturing the subprocess command, return code, and a tail of its
        stdout+stderr so preflight failures leave a breadcrumb on disk.
        """
        ...

    @abstractmethod
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
        """Run the provider to execute a task.

        Args:
            config: Gza configuration
            prompt: The task prompt
            log_file: Path to write logs
            work_dir: Working directory for execution
            resume_session_id: Optional session ID to resume from
            on_session_id: Optional callback invoked with the session_id as soon
                as it is first observed in the streaming output.  Use this to
                persist the session_id before the run completes so that
                interrupted tasks can still be resumed.
            on_step_count: Optional callback invoked with the current step count
                whenever the step count changes during streaming.  Use this to
                update the task record in real time.
            interactive: If True, run in provider-specific interactive foreground
                mode when supported.

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
        parse_output: Callable[[str, dict[str, Any], Any], None] | None = None,
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
                         Called with (line: str, accumulated_data: dict, log_handle).
                         The callback should update accumulated_data in place.
                         log_handle can be used to write additional formatted output.
            stdin_input: Optional string to pass to stdin

        Returns:
            RunResult with exit code and duration. Stats should be filled
            in by parse_output callback or by caller.
        """
        print(f"Running command: {_format_command_for_log(cmd)}")
        print(f"Logging to: {log_file}")
        print(f"Timeout: {timeout_minutes} minutes")
        print("")

        # Write a breadcrumb so the exact command is captured even if the
        # subprocess hangs before producing any output.
        import json
        try:
            with open(log_file, "a") as log_breadcrumb:
                log_breadcrumb.write(
                    json.dumps({
                        "type": "gza",
                        "subtype": "command",
                        "event": "provider_exec_start",
                        "command": _format_command_for_log(cmd),
                        "cwd": str(cwd) if cwd else None,
                        "timeout_minutes": timeout_minutes,
                    }) + "\n"
                )
                log_breadcrumb.flush()
        except OSError:
            pass

        start_time = time.time()
        accumulated_data: dict = {}
        startup_logged = False

        with open(log_file, "a") as log:
            stdin_target = subprocess.PIPE if stdin_input is not None else subprocess.DEVNULL
            env = os.environ.copy()
            env.setdefault("RUST_BACKTRACE", "1")
            process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                stdin=stdin_target,
                text=True,
                cwd=cwd,
                env=env,
            )

            # Write stdin if provided
            if stdin_input is not None and process.stdin:
                process.stdin.write(stdin_input)
                process.stdin.close()

            if process.stdout:
                for line in process.stdout:
                    log.write(line)
                    line = line.strip()
                    if not line:
                        continue

                    if not startup_logged:
                        startup_line = _extract_startup_log_line(line)
                        if startup_line:
                            print(f"Startup: {startup_line}")
                            accumulated_data["_startup_line"] = line
                            startup_logged = True

                    if parse_output:
                        parse_output(line, accumulated_data, log)
                    if accumulated_data.get("__terminate_process__"):
                        process.terminate()
                        accumulated_data["__terminated_by_parser__"] = True
                        break

            process.wait()

        duration_seconds = time.time() - start_time

        result = RunResult(
            exit_code=process.returncode,
            duration_seconds=round(duration_seconds, 1),
        )
        if accumulated_data.get("__terminated_by_parser__"):
            # Keep successful parser-directed termination as non-shell-failure.
            result.exit_code = 0

        # Let caller extract stats from accumulated_data
        result._accumulated_data = accumulated_data
        return result


def _format_command_for_log(cmd: list[str]) -> str:
    """Format command for display while redacting sensitive values."""

    def redact(arg: str) -> str:
        if "=" not in arg:
            return arg
        key, value = arg.split("=", 1)
        key_upper = key.upper()
        if key_upper == "GZA_DOCKER_SETUP_COMMAND":
            return f"{key}=***"
        sensitive_markers = ("KEY", "TOKEN", "SECRET", "PASSWORD")
        if any(marker in key_upper for marker in sensitive_markers):
            return f"{key}=***"
        return f"{key}={value}"

    return shlex.join([redact(arg) for arg in cmd])


def _extract_startup_log_line(line: str, max_len: int = 180) -> str | None:
    """Return a concise startup line, skipping structured JSON output."""
    if line.startswith("{") and line.endswith("}"):
        return None
    if len(line) > max_len:
        return f"{line[: max_len - 3]}..."
    return line


def get_provider(config: Config) -> Provider:
    """Get the appropriate provider based on config."""
    from .claude import ClaudeProvider
    from .codex import CodexProvider
    from .gemini import GeminiProvider

    providers: dict[str, type[ClaudeProvider] | type[CodexProvider] | type[GeminiProvider]] = {
        "claude": ClaudeProvider,
        "codex": CodexProvider,
        "gemini": GeminiProvider,
    }

    provider_class = providers.get(config.provider)
    if not provider_class:
        raise ValueError(f"Unknown provider: {config.provider}")

    return provider_class()
