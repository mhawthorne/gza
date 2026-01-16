"""Configuration for Gza."""

import os
import sys
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
    timeout_minutes: int = DEFAULT_TIMEOUT_MINUTES
    branch_mode: str = DEFAULT_BRANCH_MODE  # "single" or "multi"
    max_turns: int = DEFAULT_MAX_TURNS
    claude_args: list[str] = field(default_factory=lambda: list(DEFAULT_CLAUDE_ARGS))
    worktree_dir: str = DEFAULT_WORKTREE_DIR
    work_count: int = DEFAULT_WORK_COUNT
    provider: str = DEFAULT_PROVIDER  # "claude" or "gemini"
    model: str = ""  # Provider-specific model name (optional)
    task_types: dict[str, TaskTypeConfig] = field(default_factory=dict)  # Per-task-type config
    branch_strategy: BranchStrategy | None = None  # Branch naming strategy

    def __post_init__(self):
        if not self.docker_image:
            self.docker_image = f"{self.project_name}-gza"

        # Set default branch strategy if not provided
        if self.branch_strategy is None:
            self.branch_strategy = BranchStrategy(
                pattern="{project}/{task_id}",
                default_type="feature"
            )

    def get_model_for_task_type(self, task_type: str) -> str:
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

    def get_max_turns_for_task_type(self, task_type: str) -> int:
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
            "docker_image", "timeout_minutes", "branch_mode", "max_turns",
            "claude_args", "worktree_dir", "work_count", "provider", "model",
            "defaults", "task_types", "branch_strategy"
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
        if os.getenv("GZA_USE_DOCKER"):
            use_docker = os.getenv("GZA_USE_DOCKER").lower() != "false"

        timeout_minutes = data.get("timeout_minutes", DEFAULT_TIMEOUT_MINUTES)
        if os.getenv("GZA_TIMEOUT_MINUTES"):
            timeout_minutes = int(os.getenv("GZA_TIMEOUT_MINUTES"))

        branch_mode = data.get("branch_mode", DEFAULT_BRANCH_MODE)
        if os.getenv("GZA_BRANCH_MODE"):
            branch_mode = os.getenv("GZA_BRANCH_MODE")

        # max_turns: check defaults section first, then top-level
        max_turns = defaults.get("max_turns") or data.get("max_turns", DEFAULT_MAX_TURNS)
        if os.getenv("GZA_MAX_TURNS"):
            max_turns = int(os.getenv("GZA_MAX_TURNS"))

        worktree_dir = data.get("worktree_dir", DEFAULT_WORKTREE_DIR)
        if os.getenv("GZA_WORKTREE_DIR"):
            worktree_dir = os.getenv("GZA_WORKTREE_DIR")

        work_count = data.get("work_count", DEFAULT_WORK_COUNT)
        if os.getenv("GZA_WORK_COUNT"):
            work_count = int(os.getenv("GZA_WORK_COUNT"))

        provider = data.get("provider", DEFAULT_PROVIDER)
        if os.getenv("GZA_PROVIDER"):
            provider = os.getenv("GZA_PROVIDER")

        # model: check defaults section first, then top-level
        model = defaults.get("model") or data.get("model", "")
        if os.getenv("GZA_MODEL"):
            model = os.getenv("GZA_MODEL")

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
                else:
                    raise ConfigError(
                        f"Unknown branch_strategy preset: '{bs_data}'\n"
                        f"Valid presets are: monorepo, conventional, simple\n"
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
            task_types=task_types,
            branch_strategy=branch_strategy,
        )

    @classmethod
    def validate(cls, project_dir: Path) -> tuple[bool, list[str], list[str]]:
        """Validate gza.yaml configuration file.

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
            "worktree_dir", "work_count", "provider", "model", "defaults", "task_types",
            "branch_strategy"
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
                valid_presets = {"monorepo", "conventional", "simple"}
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
