"""Configuration for Gza."""

import os
import sys
import warnings
from dataclasses import dataclass, field
from pathlib import Path

import yaml

APP_NAME = "gza"
CONFIG_FILENAME = f"{APP_NAME}.yaml"


class ConfigError(Exception):
    """Raised when configuration is invalid or missing."""
    pass


DEFAULT_TASKS_FILE = "tasks.yaml"
DEFAULT_DB_FILE = f".{APP_NAME}/{APP_NAME}.db"
DEFAULT_LOG_DIR = f".{APP_NAME}/logs"
DEFAULT_WORKERS_DIR = f".{APP_NAME}/workers"
DEFAULT_TIMEOUT_MINUTES = 10
DEFAULT_USE_DOCKER = True
DEFAULT_BRANCH_MODE = "multi"  # "single" or "multi"
DEFAULT_MAX_TURNS = 50
DEFAULT_WORKTREE_DIR = f"/tmp/{APP_NAME}-worktrees"
DEFAULT_WORK_COUNT = 1  # Number of tasks to run in a work session
DEFAULT_PROVIDER = "claude"  # "claude" or "gemini"
DEFAULT_CHAT_TEXT_DISPLAY_LENGTH = 0  # 0 means unlimited (show all)
DEFAULT_BRANCH_STRATEGY = "monorepo"  # Default branch naming strategy
DEFAULT_CLAUDE_ARGS = [
    "--allowedTools", "Read", "Write", "Edit", "Glob", "Grep", "Bash",
]


@dataclass
class TaskTypeConfig:
    """Configuration for a specific task type."""
    model: str | None = None
    max_turns: int | None = None


@dataclass
class ClaudeConfig:
    """Claude-specific configuration."""
    fetch_auth_token_from_keychain: bool = False
    args: list[str] = field(default_factory=lambda: list(DEFAULT_CLAUDE_ARGS))


@dataclass
class BranchStrategy:
    """Configuration for branch naming strategy."""
    pattern: str
    default_type: str = "feature"

    def __post_init__(self):
        """Validate the branch strategy configuration."""
        # Validate pattern contains valid variables
        valid_vars = {"{project}", "{task_id}", "{date}", "{slug}", "{type}"}
        # Check for invalid characters that would break git branch names
        invalid_chars = [" ", "~", "^", ":", "?", "*", "[", "\\"]
        for char in invalid_chars:
            if char in self.pattern:
                raise ConfigError(f"Invalid character '{char}' in branch_strategy pattern")

        # Check for consecutive dots or slashes
        if ".." in self.pattern:
            raise ConfigError("Branch strategy pattern cannot contain consecutive dots (..)")
        if "//" in self.pattern:
            raise ConfigError("Branch strategy pattern cannot contain consecutive slashes (//)")

        # Check pattern doesn't start with dot or slash
        if self.pattern.startswith("."):
            raise ConfigError("Branch strategy pattern cannot start with a dot")
        if self.pattern.startswith("/"):
            raise ConfigError("Branch strategy pattern cannot start with a slash")

        # Check pattern doesn't end with slash or .lock
        if self.pattern.endswith("/"):
            raise ConfigError("Branch strategy pattern cannot end with a slash")
        if self.pattern.endswith(".lock"):
            raise ConfigError("Branch strategy pattern cannot end with .lock")


@dataclass
class Config:
    project_dir: Path
    project_name: str  # Required - no default
    tasks_file: str = DEFAULT_TASKS_FILE
    log_dir: str = DEFAULT_LOG_DIR
    use_docker: bool = DEFAULT_USE_DOCKER
    docker_image: str = ""
    docker_volumes: list[str] = field(default_factory=list)
    timeout_minutes: int = DEFAULT_TIMEOUT_MINUTES
    branch_mode: str = DEFAULT_BRANCH_MODE  # "single" or "multi"
    max_turns: int = DEFAULT_MAX_TURNS
    claude: ClaudeConfig = field(default_factory=ClaudeConfig)
    worktree_dir: str = DEFAULT_WORKTREE_DIR
    work_count: int = DEFAULT_WORK_COUNT
    provider: str = DEFAULT_PROVIDER  # "claude" or "gemini"
    model: str = ""  # Provider-specific model name (optional)
    task_types: dict[str, TaskTypeConfig] = field(default_factory=dict)  # Per-task-type config
    branch_strategy: BranchStrategy | None = None  # Branch naming strategy
    chat_text_display_length: int = DEFAULT_CHAT_TEXT_DISPLAY_LENGTH  # 0 = unlimited
    docker_setup_command: str = ""  # Command to run inside container before CLI starts
    verify_command: str = ""  # Command to run before finishing (e.g., mypy + pytest)

    def __post_init__(self):
        if not self.docker_image:
            self.docker_image = f"{self.project_name}-gza"

        # Set default branch strategy if not provided
        if self.branch_strategy is None:
            self.branch_strategy = BranchStrategy(
                pattern="{project}/{task_id}",
                default_type="feature"
            )

    def get_model_for_task_type(self, task_type: str) -> str | None:
        """Get the model for a given task type, falling back to defaults.

        Args:
            task_type: The task type (e.g., "plan", "review", "implement")

        Returns:
            The model name to use for this task type
        """
        # Check task_types config first
        if task_type in self.task_types and self.task_types[task_type].model:
            return self.task_types[task_type].model
        # Fall back to default model
        return self.model

    def get_max_turns_for_task_type(self, task_type: str) -> int | None:
        """Get the max_turns for a given task type, falling back to defaults.

        Args:
            task_type: The task type (e.g., "plan", "review", "implement")

        Returns:
            The max_turns to use for this task type
        """
        # Check task_types config first
        if task_type in self.task_types and self.task_types[task_type].max_turns is not None:
            return self.task_types[task_type].max_turns
        # Fall back to default max_turns
        return self.max_turns

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

    @property
    def workers_path(self) -> Path:
        return self.project_dir / DEFAULT_WORKERS_DIR

    @classmethod
    def config_path(cls, project_dir: Path) -> Path:
        """Get the path to the config file."""
        return project_dir / CONFIG_FILENAME

    @classmethod
    def load(cls, project_dir: Path) -> "Config":
        """Load config from gza.yaml in project root.

        Raises ConfigError if config file is missing or project_name is not set.
        """
        config_path = cls.config_path(project_dir)

        if not config_path.exists():
            raise ConfigError(
                f"Configuration file not found: {config_path}\n"
                f"Run 'gza init' to create one."
            )

        with open(config_path) as f:
            data = yaml.safe_load(f) or {}

        # Validate and warn about unknown keys
        valid_fields = {
            "project_name", "tasks_file", "log_dir", "use_docker",
            "docker_image", "docker_volumes", "docker_setup_command", "timeout_minutes", "branch_mode", "max_turns",
            "claude_args", "claude", "worktree_dir", "work_count", "provider", "model",
            "defaults", "task_types", "branch_strategy", "verify_command"
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

        # Support both new "defaults" section and old flat structure
        # If "defaults" exists, use it; otherwise use top-level fields
        defaults = data.get("defaults", {})

        # Environment variables override file config
        use_docker = data.get("use_docker", DEFAULT_USE_DOCKER)
        env_use_docker = os.getenv("GZA_USE_DOCKER")
        if env_use_docker:
            use_docker = env_use_docker.lower() != "false"

        timeout_minutes = data.get("timeout_minutes", DEFAULT_TIMEOUT_MINUTES)
        env_timeout = os.getenv("GZA_TIMEOUT_MINUTES")
        if env_timeout:
            timeout_minutes = int(env_timeout)

        branch_mode = data.get("branch_mode", DEFAULT_BRANCH_MODE)
        if os.getenv("GZA_BRANCH_MODE"):
            branch_mode = os.getenv("GZA_BRANCH_MODE")

        # max_turns: check defaults section first, then top-level
        max_turns = defaults.get("max_turns") or data.get("max_turns", DEFAULT_MAX_TURNS)
        env_max_turns = os.getenv("GZA_MAX_TURNS")
        if env_max_turns:
            max_turns = int(env_max_turns)

        worktree_dir = data.get("worktree_dir", DEFAULT_WORKTREE_DIR)
        if os.getenv("GZA_WORKTREE_DIR"):
            worktree_dir = os.getenv("GZA_WORKTREE_DIR")

        work_count = data.get("work_count", DEFAULT_WORK_COUNT)
        env_work_count = os.getenv("GZA_WORK_COUNT")
        if env_work_count:
            work_count = int(env_work_count)

        chat_text_display_length = data.get("chat_text_display_length", DEFAULT_CHAT_TEXT_DISPLAY_LENGTH)
        env_chat_text_display_length = os.getenv("GZA_CHAT_TEXT_DISPLAY_LENGTH")
        if env_chat_text_display_length:
            chat_text_display_length = int(env_chat_text_display_length)

        provider = data.get("provider", DEFAULT_PROVIDER)
        if os.getenv("GZA_PROVIDER"):
            provider = os.getenv("GZA_PROVIDER")

        # model: check defaults section first, then top-level
        model = defaults.get("model") or data.get("model", "")
        if os.getenv("GZA_MODEL"):
            model = os.getenv("GZA_MODEL")

        # docker_volumes: can be overridden by environment variable
        docker_volumes = data.get("docker_volumes", [])
        env_docker_volumes = os.getenv("GZA_DOCKER_VOLUMES")
        if env_docker_volumes:
            # Parse comma-separated volumes
            docker_volumes = [v.strip() for v in env_docker_volumes.split(",") if v.strip()]

        # Expand tilde in volume paths
        expanded_volumes = []
        for volume in docker_volumes:
            # Split on first colon to separate source:dest[:mode]
            parts = volume.split(":", 1)
            if parts:
                # Expand tilde in source path
                parts[0] = os.path.expanduser(parts[0])
                expanded_volumes.append(":".join(parts))
            else:
                expanded_volumes.append(volume)
        docker_volumes = expanded_volumes

        # Parse task_types configuration
        task_types = {}
        if "task_types" in data and isinstance(data["task_types"], dict):
            for task_type, config_data in data["task_types"].items():
                if isinstance(config_data, dict):
                    task_types[task_type] = TaskTypeConfig(
                        model=config_data.get("model"),
                        max_turns=config_data.get("max_turns")
                    )

        # Parse branch_strategy configuration
        branch_strategy = None
        if "branch_strategy" in data:
            bs_data = data["branch_strategy"]
            # Handle preset names
            if isinstance(bs_data, str):
                if bs_data == "monorepo":
                    branch_strategy = BranchStrategy(
                        pattern="{project}/{task_id}",
                        default_type="feature"
                    )
                elif bs_data == "conventional":
                    branch_strategy = BranchStrategy(
                        pattern="{type}/{slug}",
                        default_type="feature"
                    )
                elif bs_data == "simple":
                    branch_strategy = BranchStrategy(
                        pattern="{slug}",
                        default_type="feature"
                    )
                elif bs_data == "date_slug":
                    branch_strategy = BranchStrategy(
                        pattern="{date}-{slug}",
                        default_type="feature"
                    )
                else:
                    raise ConfigError(
                        f"Unknown branch_strategy preset: '{bs_data}'\n"
                        f"Valid presets are: monorepo, conventional, simple, date_slug\n"
                        f"Or use a dict with 'pattern' key for custom patterns."
                    )
            # Handle custom pattern dict
            elif isinstance(bs_data, dict):
                if "pattern" not in bs_data:
                    raise ConfigError("branch_strategy dict must have a 'pattern' key")
                branch_strategy = BranchStrategy(
                    pattern=bs_data["pattern"],
                    default_type=bs_data.get("default_type", "feature")
                )

        # Parse claude configuration section
        claude_config = ClaudeConfig()
        claude_data = data.get("claude")
        if isinstance(claude_data, dict):
            if "fetch_auth_token_from_keychain" in claude_data:
                claude_config.fetch_auth_token_from_keychain = bool(claude_data["fetch_auth_token_from_keychain"])
            if "args" in claude_data:
                claude_config.args = claude_data["args"]

        # Backward compat: top-level claude_args still works but is deprecated
        if "claude_args" in data:
            if isinstance(claude_data, dict) and "args" in claude_data:
                # claude.args takes precedence; warn about both being set
                warnings.warn(
                    "Both 'claude_args' and 'claude.args' are set in gza.yaml. "
                    "Using 'claude.args'. Please remove deprecated 'claude_args'.",
                    DeprecationWarning,
                    stacklevel=2,
                )
            else:
                warnings.warn(
                    "'claude_args' is deprecated. Migrate to 'claude.args' in gza.yaml.",
                    DeprecationWarning,
                    stacklevel=2,
                )
                claude_config.args = data["claude_args"]

        return cls(
            project_dir=project_dir,
            project_name=data["project_name"],  # Already validated above
            tasks_file=data.get("tasks_file", DEFAULT_TASKS_FILE),
            log_dir=data.get("log_dir", DEFAULT_LOG_DIR),
            use_docker=use_docker,
            docker_image=data.get("docker_image", ""),
            docker_volumes=docker_volumes,
            docker_setup_command=data.get("docker_setup_command", ""),
            timeout_minutes=timeout_minutes,
            branch_mode=branch_mode,
            max_turns=max_turns,
            claude=claude_config,
            worktree_dir=worktree_dir,
            work_count=work_count,
            provider=provider,
            model=model,
            task_types=task_types,
            branch_strategy=branch_strategy,
            chat_text_display_length=chat_text_display_length,
            verify_command=data.get("verify_command", ""),
        )

    @classmethod
    def validate(cls, project_dir: Path) -> tuple[bool, list[str], list[str]]:
        """Validate gza.yaml configuration file.

        Returns:
            Tuple of (is_valid, list of error messages, list of warning messages)
        """
        config_path = cls.config_path(project_dir)
        errors: list[str] = []
        warnings: list[str] = []

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
            "docker_image", "docker_volumes", "docker_setup_command", "timeout_minutes", "branch_mode", "max_turns",
            "claude_args", "claude", "worktree_dir", "work_count", "provider", "model",
            "defaults", "task_types", "branch_strategy", "verify_command"
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

        if "docker_setup_command" in data and not isinstance(data["docker_setup_command"], str):
            errors.append("'docker_setup_command' must be a string")

        if "docker_volumes" in data:
            if not isinstance(data["docker_volumes"], list):
                errors.append("'docker_volumes' must be a list")
            else:
                for i, volume in enumerate(data["docker_volumes"]):
                    if not isinstance(volume, str):
                        errors.append(f"'docker_volumes[{i}]' must be a string")
                    elif ":" not in volume:
                        warnings.append(
                            f"'docker_volumes[{i}]' missing colon separator "
                            "(expected 'source:dest' or 'source:dest:mode')"
                        )
                    else:
                        parts = volume.split(":")
                        if len(parts) < 2:
                            warnings.append(
                                f"'docker_volumes[{i}]' should have format "
                                "'source:dest' or 'source:dest:mode'"
                            )
                        elif len(parts) == 3 and parts[2] not in ["ro", "rw", "z", "Z"]:
                            warnings.append(
                                f"'docker_volumes[{i}]' has unknown mode '{parts[2]}' "
                                "(common modes: ro, rw, z, Z)"
                            )

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
            warnings.append("'claude_args' is deprecated. Migrate to 'claude.args'.")
            if not isinstance(data["claude_args"], list):
                errors.append("'claude_args' must be a list")
            else:
                for i, arg in enumerate(data["claude_args"]):
                    if not isinstance(arg, str):
                        errors.append(f"'claude_args[{i}]' must be a string")

        if "claude" in data:
            if not isinstance(data["claude"], dict):
                errors.append("'claude' must be a dictionary")
            else:
                claude_data = data["claude"]
                if "fetch_auth_token_from_keychain" in claude_data:
                    if not isinstance(claude_data["fetch_auth_token_from_keychain"], bool):
                        errors.append("'claude.fetch_auth_token_from_keychain' must be a boolean")
                if "args" in claude_data:
                    if not isinstance(claude_data["args"], list):
                        errors.append("'claude.args' must be a list")
                    else:
                        for i, arg in enumerate(claude_data["args"]):
                            if not isinstance(arg, str):
                                errors.append(f"'claude.args[{i}]' must be a string")
                # Warn about unknown keys
                valid_claude_keys = {"fetch_auth_token_from_keychain", "args"}
                for key in claude_data.keys():
                    if key not in valid_claude_keys:
                        warnings.append(f"Unknown field in 'claude': '{key}'")

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
            elif data["provider"] not in ("claude", "codex", "gemini"):
                errors.append("'provider' must be one of: 'claude', 'codex', 'gemini'")

        if "model" in data and not isinstance(data["model"], str):
            errors.append("'model' must be a string")

        if "verify_command" in data and not isinstance(data["verify_command"], str):
            errors.append("'verify_command' must be a string")

        # Validate defaults section
        if "defaults" in data:
            if not isinstance(data["defaults"], dict):
                errors.append("'defaults' must be a dictionary")
            else:
                defaults = data["defaults"]
                if "model" in defaults and not isinstance(defaults["model"], str):
                    errors.append("'defaults.model' must be a string")
                if "max_turns" in defaults:
                    if not isinstance(defaults["max_turns"], int):
                        errors.append("'defaults.max_turns' must be an integer")
                    elif defaults["max_turns"] <= 0:
                        errors.append("'defaults.max_turns' must be positive")
                # Warn about unknown keys in defaults
                valid_defaults_keys = {"model", "max_turns"}
                for key in defaults.keys():
                    if key not in valid_defaults_keys:
                        warnings.append(f"Unknown field in 'defaults': '{key}'")

        # Validate task_types section
        if "task_types" in data:
            if not isinstance(data["task_types"], dict):
                errors.append("'task_types' must be a dictionary")
            else:
                for task_type, config in data["task_types"].items():
                    if not isinstance(config, dict):
                        errors.append(f"'task_types.{task_type}' must be a dictionary")
                    else:
                        if "model" in config and not isinstance(config["model"], str):
                            errors.append(f"'task_types.{task_type}.model' must be a string")
                        if "max_turns" in config:
                            if not isinstance(config["max_turns"], int):
                                errors.append(f"'task_types.{task_type}.max_turns' must be an integer")
                            elif config["max_turns"] <= 0:
                                errors.append(f"'task_types.{task_type}.max_turns' must be positive")
                        # Warn about unknown keys
                        valid_task_type_keys = {"model", "max_turns"}
                        for key in config.keys():
                            if key not in valid_task_type_keys:
                                warnings.append(f"Unknown field in 'task_types.{task_type}': '{key}'")

        # Validate branch_strategy section
        if "branch_strategy" in data:
            bs_data = data["branch_strategy"]
            if isinstance(bs_data, str):
                # Validate preset names
                valid_presets = {"monorepo", "conventional", "simple", "date_slug"}
                if bs_data not in valid_presets:
                    errors.append(
                        f"'branch_strategy' preset '{bs_data}' is invalid. "
                        f"Valid presets: {', '.join(sorted(valid_presets))}"
                    )
            elif isinstance(bs_data, dict):
                # Validate custom pattern dict
                if "pattern" not in bs_data:
                    errors.append("'branch_strategy' dict must have a 'pattern' key")
                elif not isinstance(bs_data["pattern"], str):
                    errors.append("'branch_strategy.pattern' must be a string")
                else:
                    # Try to validate the pattern by creating a BranchStrategy
                    try:
                        BranchStrategy(
                            pattern=bs_data["pattern"],
                            default_type=bs_data.get("default_type", "feature")
                        )
                    except ConfigError as e:
                        errors.append(f"'branch_strategy.pattern' is invalid: {e}")

                if "default_type" in bs_data and not isinstance(bs_data["default_type"], str):
                    errors.append("'branch_strategy.default_type' must be a string")

                # Warn about unknown keys
                valid_bs_keys = {"pattern", "default_type"}
                for key in bs_data.keys():
                    if key not in valid_bs_keys:
                        warnings.append(f"Unknown field in 'branch_strategy': '{key}'")
            else:
                errors.append("'branch_strategy' must be a string (preset name) or dict (custom pattern)")

        return len(errors) == 0, errors, warnings
