"""Configuration for Theo."""

import os
import sys
from dataclasses import dataclass, field
from pathlib import Path

import yaml

CONFIG_FILENAME = "theo.yaml"


class ConfigError(Exception):
    """Raised when configuration is invalid or missing."""
    pass


DEFAULT_TASKS_FILE = "tasks.yaml"
DEFAULT_DB_FILE = ".theo/theo.db"
DEFAULT_LOG_DIR = ".theo/logs"
DEFAULT_TIMEOUT_MINUTES = 10
DEFAULT_USE_DOCKER = True
DEFAULT_BRANCH_MODE = "multi"  # "single" or "multi"
DEFAULT_MAX_TURNS = 50
DEFAULT_WORKTREE_DIR = "/tmp/theo-worktrees"
DEFAULT_WORK_COUNT = 1  # Number of tasks to run in a work session
DEFAULT_PROVIDER = "claude"  # "claude" or "gemini"
DEFAULT_CLAUDE_ARGS = [
    "--allowedTools", "Read", "Write", "Edit", "Glob", "Grep", "Bash",
]


@dataclass
class Config:
    project_dir: Path
    project_name: str  # Required - no default
    tasks_file: str = DEFAULT_TASKS_FILE
    log_dir: str = DEFAULT_LOG_DIR
    use_docker: bool = DEFAULT_USE_DOCKER
    docker_image: str = ""
    timeout_minutes: int = DEFAULT_TIMEOUT_MINUTES
    branch_mode: str = DEFAULT_BRANCH_MODE  # "single" or "multi"
    max_turns: int = DEFAULT_MAX_TURNS
    claude_args: list[str] = field(default_factory=lambda: list(DEFAULT_CLAUDE_ARGS))
    worktree_dir: str = DEFAULT_WORKTREE_DIR
    work_count: int = DEFAULT_WORK_COUNT
    provider: str = DEFAULT_PROVIDER  # "claude" or "gemini"
    model: str = ""  # Provider-specific model name (optional)

    def __post_init__(self):
        if not self.docker_image:
            self.docker_image = f"{self.project_name}-theo"

    @property
    def worktree_path(self) -> Path:
        return Path(self.worktree_dir) / self.project_name

    @property
    def tasks_path(self) -> Path:
        return self.project_dir / self.tasks_file

    @property
    def db_path(self) -> Path:
        return self.project_dir / DEFAULT_DB_FILE

    @property
    def log_path(self) -> Path:
        return self.project_dir / self.log_dir

    @classmethod
    def config_path(cls, project_dir: Path) -> Path:
        """Get the path to the config file."""
        return project_dir / CONFIG_FILENAME

    @classmethod
    def load(cls, project_dir: Path) -> "Config":
        """Load config from theo.yaml in project root.

        Raises ConfigError if config file is missing or project_name is not set.
        """
        config_path = cls.config_path(project_dir)

        if not config_path.exists():
            raise ConfigError(
                f"Configuration file not found: {config_path}\n"
                f"Run 'theo init' to create one."
            )

        with open(config_path) as f:
            data = yaml.safe_load(f) or {}

        # Validate and warn about unknown keys
        valid_fields = {
            "project_name", "tasks_file", "log_dir", "use_docker",
            "docker_image", "timeout_minutes", "branch_mode", "max_turns",
            "claude_args", "worktree_dir", "work_count", "provider", "model"
        }
        for key in data.keys():
            if key not in valid_fields:
                print(f"Warning: Unknown configuration field '{key}' in {config_path}", file=sys.stderr)

        # Require project_name
        if "project_name" not in data or not data["project_name"]:
            raise ConfigError(
                f"'project_name' is required in {config_path}\n"
                f"Add 'project_name: your-project-name' to the config file."
            )

        # Environment variables override file config
        use_docker = data.get("use_docker", DEFAULT_USE_DOCKER)
        if os.getenv("THEO_USE_DOCKER"):
            use_docker = os.getenv("THEO_USE_DOCKER").lower() != "false"

        timeout_minutes = data.get("timeout_minutes", DEFAULT_TIMEOUT_MINUTES)
        if os.getenv("THEO_TIMEOUT_MINUTES"):
            timeout_minutes = int(os.getenv("THEO_TIMEOUT_MINUTES"))

        branch_mode = data.get("branch_mode", DEFAULT_BRANCH_MODE)
        if os.getenv("THEO_BRANCH_MODE"):
            branch_mode = os.getenv("THEO_BRANCH_MODE")

        max_turns = data.get("max_turns", DEFAULT_MAX_TURNS)
        if os.getenv("THEO_MAX_TURNS"):
            max_turns = int(os.getenv("THEO_MAX_TURNS"))

        worktree_dir = data.get("worktree_dir", DEFAULT_WORKTREE_DIR)
        if os.getenv("THEO_WORKTREE_DIR"):
            worktree_dir = os.getenv("THEO_WORKTREE_DIR")

        work_count = data.get("work_count", DEFAULT_WORK_COUNT)
        if os.getenv("THEO_WORK_COUNT"):
            work_count = int(os.getenv("THEO_WORK_COUNT"))

        provider = data.get("provider", DEFAULT_PROVIDER)
        if os.getenv("THEO_PROVIDER"):
            provider = os.getenv("THEO_PROVIDER")

        model = data.get("model", "")
        if os.getenv("THEO_MODEL"):
            model = os.getenv("THEO_MODEL")

        return cls(
            project_dir=project_dir,
            project_name=data["project_name"],  # Already validated above
            tasks_file=data.get("tasks_file", DEFAULT_TASKS_FILE),
            log_dir=data.get("log_dir", DEFAULT_LOG_DIR),
            use_docker=use_docker,
            docker_image=data.get("docker_image", ""),
            timeout_minutes=timeout_minutes,
            branch_mode=branch_mode,
            max_turns=max_turns,
            claude_args=data.get("claude_args", list(DEFAULT_CLAUDE_ARGS)),
            worktree_dir=worktree_dir,
            work_count=work_count,
            provider=provider,
            model=model,
        )

    @classmethod
    def validate(cls, project_dir: Path) -> tuple[bool, list[str], list[str]]:
        """Validate theo.yaml configuration file.

        Returns:
            Tuple of (is_valid, list of error messages, list of warning messages)
        """
        config_path = cls.config_path(project_dir)
        errors = []
        warnings = []

        # Check if file exists
        if not config_path.exists():
            errors.append(f"Configuration file not found: {config_path}")
            return False, errors, warnings

        # Try to parse YAML
        try:
            with open(config_path) as f:
                data = yaml.safe_load(f)
        except yaml.YAMLError as e:
            errors.append(f"Invalid YAML syntax: {e}")
            return False, errors, warnings
        except Exception as e:
            errors.append(f"Error reading file: {e}")
            return False, errors, warnings

        # If empty file, project_name is required
        if data is None:
            errors.append("'project_name' is required")
            return False, errors, warnings

        # Check if it's a dict
        if not isinstance(data, dict):
            errors.append("Configuration must be a YAML dictionary/object")
            return False, errors, warnings

        # Validate known fields - unknown keys are warnings, not errors
        valid_fields = {
            "project_name", "tasks_file", "log_dir", "use_docker",
            "docker_image", "timeout_minutes", "branch_mode", "max_turns", "claude_args",
            "worktree_dir", "work_count", "provider", "model"
        }

        for key in data.keys():
            if key not in valid_fields:
                warnings.append(f"Unknown configuration field: '{key}'")

        # Require project_name
        if "project_name" not in data or not data["project_name"]:
            errors.append("'project_name' is required")
        elif not isinstance(data["project_name"], str):
            errors.append("'project_name' must be a string")

        if "tasks_file" in data and not isinstance(data["tasks_file"], str):
            errors.append("'tasks_file' must be a string")

        if "log_dir" in data and not isinstance(data["log_dir"], str):
            errors.append("'log_dir' must be a string")

        if "use_docker" in data and not isinstance(data["use_docker"], bool):
            errors.append("'use_docker' must be a boolean (true/false)")

        if "docker_image" in data and not isinstance(data["docker_image"], str):
            errors.append("'docker_image' must be a string")

        if "timeout_minutes" in data:
            if not isinstance(data["timeout_minutes"], int):
                errors.append("'timeout_minutes' must be an integer")
            elif data["timeout_minutes"] <= 0:
                errors.append("'timeout_minutes' must be positive")

        if "branch_mode" in data:
            if not isinstance(data["branch_mode"], str):
                errors.append("'branch_mode' must be a string")
            elif data["branch_mode"] not in ("single", "multi"):
                errors.append("'branch_mode' must be either 'single' or 'multi'")

        if "max_turns" in data:
            if not isinstance(data["max_turns"], int):
                errors.append("'max_turns' must be an integer")
            elif data["max_turns"] <= 0:
                errors.append("'max_turns' must be positive")

        if "claude_args" in data:
            if not isinstance(data["claude_args"], list):
                errors.append("'claude_args' must be a list")
            else:
                for i, arg in enumerate(data["claude_args"]):
                    if not isinstance(arg, str):
                        errors.append(f"'claude_args[{i}]' must be a string")

        if "worktree_dir" in data and not isinstance(data["worktree_dir"], str):
            errors.append("'worktree_dir' must be a string")

        if "work_count" in data:
            if not isinstance(data["work_count"], int):
                errors.append("'work_count' must be an integer")
            elif data["work_count"] <= 0:
                errors.append("'work_count' must be positive")

        if "provider" in data:
            if not isinstance(data["provider"], str):
                errors.append("'provider' must be a string")
            elif data["provider"] not in ("claude", "gemini"):
                errors.append("'provider' must be either 'claude' or 'gemini'")

        if "model" in data and not isinstance(data["model"], str):
            errors.append("'model' must be a string")

        return len(errors) == 0, errors, warnings
