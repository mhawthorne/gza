"""Configuration for Gza."""

import copy
import hashlib
import logging
import os
import re
import sys
import warnings
from dataclasses import dataclass, field
from pathlib import Path
from typing import cast

import yaml

APP_NAME = "gza"
CONFIG_FILENAME = f"{APP_NAME}.yaml"
LOCAL_CONFIG_FILENAME = f"{APP_NAME}.local.yaml"
USER_CONFIG_FILENAME = "config.yaml"
logger = logging.getLogger(__name__)

# Compiled regex for validating project_prefix values.
# Only lowercase alphanumeric — no hyphens, since the hyphen is the separator
# between prefix and numeric suffix in task IDs (e.g., "gza-42").
_PREFIX_RE = re.compile(r'^[a-z0-9]+$')
_PROJECT_ID_RE = re.compile(r"^[a-z0-9]{1,64}$")

__all__ = [
    "APP_NAME",
    "CONFIG_FILENAME",
    "LOCAL_CONFIG_FILENAME",
    "ConfigError",
    "Config",
    "TaskTypeConfig",
    "BranchStrategy",
    "discover_project_dir",
]


class ConfigError(Exception):
    """Raised when configuration is invalid or missing."""
    pass


DEFAULT_TASKS_FILE = "tasks.yaml"
DEFAULT_DB_FILE = f".{APP_NAME}/{APP_NAME}.db"
DEFAULT_LOG_DIR = f".{APP_NAME}/logs"
DEFAULT_WORKERS_DIR = f".{APP_NAME}/workers"
DEFAULT_TIMEOUT_MINUTES = 10
DEFAULT_INNER_VERIFY_COMMAND = ""
DEFAULT_USE_DOCKER = True
DEFAULT_ENFORCE_PROJECT_SCOPE = True
DEFAULT_BRANCH_MODE = "multi"  # "single" or "multi"
DEFAULT_MAX_STEPS = 50
DEFAULT_MAX_TURNS = 50
DEFAULT_WORKTREE_DIR = f"/tmp/{APP_NAME}-worktrees"
DEFAULT_WORK_COUNT = 1  # Number of tasks to run in a work session
DEFAULT_PROVIDER = "claude"  # "claude", "codex", or "gemini"
KNOWN_PROVIDERS = ("claude", "codex", "gemini")
DEFAULT_CHAT_TEXT_DISPLAY_LENGTH = 0  # 0 means unlimited (show all)
DEFAULT_BRANCH_STRATEGY = "monorepo"  # Default branch naming strategy
DEFAULT_NO_COLOR = False
DEFAULT_CLAUDE_ARGS = [
    "--allowedTools", "Read", "Write", "Edit", "Glob", "Grep", "Bash",
]
DEFAULT_ADVANCE_CREATE_REVIEWS = True
DEFAULT_REQUIRE_REVIEW_BEFORE_MERGE = True
DEFAULT_PR_INTEGRATION = True
REMOVED_ADVANCE_REVIEW_KEY = "advance_requires_review"
RENAMED_REQUIRE_REVIEW_KEY = "require_review_before_merge"
DEFAULT_ADVANCE_MODE = "default"
DEFAULT_MAX_RESUME_ATTEMPTS = 1
DEFAULT_MAX_REVIEW_CYCLES = 3
DEFAULT_MAX_NOOP_IMPROVE_CYCLES = 2
DEFAULT_WATCH_BATCH = 5
DEFAULT_WATCH_POLL = 300
DEFAULT_WATCH_NO_ACTIVITY_TIMEOUT = 60
DEFAULT_WATCH_MAX_IDLE: int | None = None
DEFAULT_WATCH_MAX_ITERATIONS = 10
DEFAULT_WATCH_FAILURE_BACKOFF_INITIAL = 60
DEFAULT_WATCH_FAILURE_BACKOFF_MAX = 3600
DEFAULT_WATCH_FAILURE_HALT_AFTER: int | None = 10
DEFAULT_WATCH_RESTART_FAILED_BATCH = 1
DEFAULT_ITERATE_MAX_ITERATIONS = 3
DEFAULT_INTERACTIVE_WORKTREE_DIR = ""
DEFAULT_MERGE_SQUASH_THRESHOLD = 0
DEFAULT_MAIN_CHECKOUT_ISOLATE = False
DEFAULT_CLEANUP_DAYS = 30
DEFAULT_REVIEW_DIFF_SMALL_THRESHOLD = 500
DEFAULT_REVIEW_DIFF_MEDIUM_THRESHOLD = 2000
DEFAULT_REVIEW_CONTEXT_FILE_LIMIT = 12
DEFAULT_REVIEW_VERIFY_TIMEOUT_SECONDS = 120
DEFAULT_CODE_TASK_DIFF_TIMEOUT_MEDIUM_THRESHOLD = 400
DEFAULT_CODE_TASK_DIFF_TIMEOUT_LARGE_THRESHOLD = 1200
DEFAULT_CODE_TASK_DIFF_TIMEOUT_MEDIUM_MINUTES = 30
DEFAULT_CODE_TASK_DIFF_TIMEOUT_LARGE_MINUTES = 45
DEFAULT_CODE_TASK_DIFF_TIMEOUT_CAP_MINUTES = 45
DEFAULT_RECOMMEND_REBASE_BEHIND_COMMITS = 1
DEFAULT_LEARNINGS_WINDOW = 25
DEFAULT_LEARNINGS_INTERVAL = 5
DEFAULT_LEARNINGS_MAX_ITEMS = 50
VALID_CONFIG_FIELDS = {
    "project_name", "project_id", "project_prefix", "tasks_file", "log_dir", "db_path", "use_docker",
    "enforce_project_scope",
    "docker_image", "docker_volumes", "docker_setup_command", "timeout_minutes", "branch_mode", "max_steps",
    "max_turns", "claude_args", "claude", "worktree_dir", "work_count", "provider", "task_providers", "model",
    "reasoning_effort", "defaults", "task_types", "providers", "branch_strategy", "chat_text_display_length",
    "verify_command", "inner_verify_command",
    "advance_create_reviews", "require_review_before_merge", "pr_integration", "advance_mode", "max_resume_attempts",
    "max_review_cycles", "max_noop_improve_cycles", "iterate_max_iterations", "watch", "interactive_worktree_dir",
    "merge_squash_threshold", "main_checkout_isolate", "cleanup_days", "review_diff_small_threshold",
    "review_diff_medium_threshold", "review_context_file_limit", "review_verify_timeout_seconds",
    "code_task_diff_timeout_medium_threshold", "code_task_diff_timeout_large_threshold",
    "code_task_diff_timeout_medium_minutes", "code_task_diff_timeout_large_minutes",
    "code_task_diff_timeout_cap_minutes",
    "recommend_rebase_behind_commits", "tmux", "learnings_window",
    "learnings_interval", "learnings_max_items", "theme", "colors", "no_color",
}
LOCAL_OVERRIDE_ALLOWED_SCHEMA: dict[str, object] = {
    "db_path": None,
    "use_docker": None,
    "enforce_project_scope": None,
    "docker_image": None,
    "docker_volumes": None,
    "docker_setup_command": None,
    "timeout_minutes": None,
    "max_steps": None,
    "max_turns": None,
    "worktree_dir": None,
    "work_count": None,
    "provider": None,
    "task_providers": {
        "*": None,
    },
    "model": None,
    "reasoning_effort": None,
    "defaults": {
        "model": None,
        "reasoning_effort": None,
        "max_steps": None,
        "max_turns": None,
    },
    "task_types": {
        "*": {
            "model": None,
            "reasoning_effort": None,
            "max_steps": None,
            "max_turns": None,
            "timeout_minutes": None,
        },
    },
    "providers": {
        "*": {
            "model": None,
            "reasoning_effort": None,
            "task_types": {
                "*": {
                    "model": None,
                    "reasoning_effort": None,
                    "max_steps": None,
                    "max_turns": None,
                    "timeout_minutes": None,
                },
            },
        },
    },
    "claude": {
        "fetch_auth_token_from_keychain": None,
        "args": None,
    },
    "tmux": {
        "enabled": None,
        "auto_accept_timeout": None,
        "max_idle_timeout": None,
        "detach_grace": None,
        "terminal_size": None,
    },
    "chat_text_display_length": None,
    "verify_command": None,
    "inner_verify_command": None,
    "advance_create_reviews": None,
    "require_review_before_merge": None,
    "pr_integration": None,
    "max_resume_attempts": None,
    "max_review_cycles": None,
    "max_noop_improve_cycles": None,
    "watch": {
        "batch": None,
        "poll": None,
        "no_activity_timeout": None,
        "max_idle": None,
        "max_iterations": None,
        "restart_failed_batch": None,
        "failure_backoff_initial": None,
        "failure_backoff_max": None,
        "failure_halt_after": None,
    },
    "iterate_max_iterations": None,
    "interactive_worktree_dir": None,
    "merge_squash_threshold": None,
    "main_checkout_isolate": None,
    "cleanup_days": None,
    "review_diff_small_threshold": None,
    "review_diff_medium_threshold": None,
    "review_context_file_limit": None,
    "review_verify_timeout_seconds": None,
    "code_task_diff_timeout_medium_threshold": None,
    "code_task_diff_timeout_large_threshold": None,
    "code_task_diff_timeout_medium_minutes": None,
    "code_task_diff_timeout_large_minutes": None,
    "code_task_diff_timeout_cap_minutes": None,
    "recommend_rebase_behind_commits": None,
    "theme": None,
    "no_color": None,
    "colors": {
        "*": None,
    },
}
USER_CONFIG_ALLOWED_SCHEMA: dict[str, object] = {
    "db_path": None,
    "use_docker": None,
    "enforce_project_scope": None,
    "docker_image": None,
    "docker_volumes": None,
    "docker_setup_command": None,
    "timeout_minutes": None,
    "max_steps": None,
    "max_turns": None,
    "worktree_dir": None,
    "work_count": None,
    "provider": None,
    "task_providers": {
        "*": None,
    },
    "model": None,
    "reasoning_effort": None,
    "defaults": {
        "model": None,
        "reasoning_effort": None,
        "max_steps": None,
        "max_turns": None,
    },
    "task_types": {
        "*": {
            "model": None,
            "reasoning_effort": None,
            "max_steps": None,
            "max_turns": None,
            "timeout_minutes": None,
        },
    },
    "providers": {
        "*": {
            "model": None,
            "reasoning_effort": None,
            "task_types": {
                "*": {
                    "model": None,
                    "reasoning_effort": None,
                    "max_steps": None,
                    "max_turns": None,
                    "timeout_minutes": None,
                },
            },
        },
    },
    "claude": {
        "fetch_auth_token_from_keychain": None,
        "args": None,
    },
    "tmux": {
        "enabled": None,
        "auto_accept_timeout": None,
        "max_idle_timeout": None,
        "detach_grace": None,
        "terminal_size": None,
    },
    "chat_text_display_length": None,
    "verify_command": None,
    "inner_verify_command": None,
    "advance_create_reviews": None,
    "require_review_before_merge": None,
    "pr_integration": None,
    "watch": {
        "batch": None,
        "poll": None,
        "no_activity_timeout": None,
        "max_idle": None,
        "max_iterations": None,
        "restart_failed_batch": None,
        "failure_backoff_initial": None,
        "failure_backoff_max": None,
        "failure_halt_after": None,
    },
    "iterate_max_iterations": None,
    "max_resume_attempts": None,
    "max_review_cycles": None,
    "max_noop_improve_cycles": None,
    "interactive_worktree_dir": None,
    "merge_squash_threshold": None,
    "main_checkout_isolate": None,
    "cleanup_days": None,
    "review_diff_small_threshold": None,
    "review_diff_medium_threshold": None,
    "review_context_file_limit": None,
    "review_verify_timeout_seconds": None,
    "code_task_diff_timeout_medium_threshold": None,
    "code_task_diff_timeout_large_threshold": None,
    "code_task_diff_timeout_medium_minutes": None,
    "code_task_diff_timeout_large_minutes": None,
    "code_task_diff_timeout_cap_minutes": None,
    "recommend_rebase_behind_commits": None,
    "learnings_window": None,
    "learnings_interval": None,
    "learnings_max_items": None,
    "theme": None,
    "no_color": None,
    "colors": {
        "*": None,
    },
}

_LOCAL_OVERRIDE_NOTICE_SHOWN: set[str] = set()


def discover_project_dir(start: Path) -> Path:
    """Find the nearest ancestor directory that contains gza.yaml."""
    current = start.resolve()
    if current.is_file():
        current = current.parent
    while True:
        if (current / CONFIG_FILENAME).exists():
            return current
        parent = current.parent
        if parent == current:
            raise ConfigError(
                f"Configuration file not found from: {start}\n"
                f"Run 'gza init' to create one."
            )
        current = parent


def _generate_project_id(project_dir: Path, project_name: str) -> str:
    """Generate a readable project ID for persisted config writes."""
    del project_dir  # Project IDs for new configs derive from the project name only.
    project_id = re.sub(r"[^a-z0-9]+", "", project_name.strip().lower())
    if not project_id:
        raise ConfigError(
            f"'project_name' {project_name!r} cannot be converted into a valid 'project_id'. "
            "Set an explicit lowercase alphanumeric project_id in gza.yaml."
        )
    return project_id[:64]


def _generate_legacy_project_id(project_dir: Path, project_name: str) -> str:
    """Generate the legacy hashed project ID for omitted-id runtime fallback."""
    canonical_root = str(project_dir.resolve())
    seed = f"{canonical_root}\n{project_name.strip().lower()}"
    digest = hashlib.sha256(seed.encode("utf-8")).hexdigest()
    # 32 chars keeps the ID compact and valid against _PROJECT_ID_RE.
    return f"p{digest[:31]}"


def _detect_model_provider_family(model: str) -> str | None:
    """Detect provider family implied by a model name."""
    model_norm = model.strip().lower()
    if not model_norm:
        return None
    if model_norm.startswith("claude"):
        return "claude"
    if model_norm.startswith("gemini"):
        return "gemini"
    if "codex" in model_norm or model_norm.startswith(("gpt-", "o1", "o3", "o4")):
        return "codex"
    return None


def _validate_optional_string_field(value: object, field_name: str, *, default: str = "") -> str:
    """Return a string config field or raise when an explicit value has the wrong type."""
    if value is None:
        return default
    if not isinstance(value, str):
        raise ConfigError(f"'{field_name}' must be a string")
    return value


def _is_model_compatible_with_provider(provider: str, model: str | None) -> bool:
    """Return True if model appears compatible with provider."""
    if not model or not isinstance(model, str):
        return True
    family = _detect_model_provider_family(model)
    if family is None:
        return True
    return family == provider


def _provider_model_mismatch_error(path: str, provider: str, model: str) -> str:
    return (
        f"'{path}' model '{model}' appears incompatible with provider '{provider}'. "
        f"Use a model for '{provider}' or change provider."
    )


def _is_strict_int(value: object) -> bool:
    """Return True only for real integer scalars (exclude booleans)."""
    return isinstance(value, int) and not isinstance(value, bool)


def _load_strict_int_field(data: dict, field_name: str, default: int) -> int:
    """Load an integer config field without coercion."""
    value = data.get(field_name, default)
    if not _is_strict_int(value):
        raise ConfigError(f"'{field_name}' must be an integer")
    return value


def _validate_optional_positive_int_field(
    value: object,
    field_name: str,
    *,
    errors: list[str] | None = None,
) -> int | None:
    """Validate an optional positive integer field with shared load/validate errors."""

    def _record_error(message: str) -> None:
        if errors is None:
            raise ConfigError(message)
        errors.append(message)

    if value is None:
        return None
    if not _is_strict_int(value):
        _record_error(f"'{field_name}' must be an integer")
        return None
    validated_value = cast(int, value)
    if validated_value <= 0:
        _record_error(f"'{field_name}' must be positive")
        return None
    return validated_value


def _code_task_timeout_scaling_order_errors(
    *,
    medium_threshold: int,
    large_threshold: int,
    medium_minutes: int,
    large_minutes: int,
) -> list[str]:
    """Return ordering errors for resolved code-task timeout scaling values."""
    errors: list[str] = []
    if large_threshold < medium_threshold:
        errors.append(
            "'code_task_diff_timeout_large_threshold' must be greater than or equal to "
            "'code_task_diff_timeout_medium_threshold'"
        )
    if large_minutes < medium_minutes:
        errors.append(
            "'code_task_diff_timeout_large_minutes' must be greater than or equal to "
            "'code_task_diff_timeout_medium_minutes'"
        )
    return errors


def _resolve_code_task_timeout_scaling_fields(
    data: dict,
    *,
    errors: list[str] | None = None,
) -> tuple[int, int, int, int, int] | None:
    """Resolve and validate code-task timeout scaling fields.

    When ``errors`` is provided, validation issues are appended there and
    ``None`` is returned. Otherwise the first issue raises ``ConfigError``.
    """

    def _record_error(message: str) -> None:
        if errors is None:
            raise ConfigError(message)
        errors.append(message)

    values: dict[str, int] = {}
    for key, default in (
        ("code_task_diff_timeout_medium_threshold", DEFAULT_CODE_TASK_DIFF_TIMEOUT_MEDIUM_THRESHOLD),
        ("code_task_diff_timeout_large_threshold", DEFAULT_CODE_TASK_DIFF_TIMEOUT_LARGE_THRESHOLD),
        ("code_task_diff_timeout_medium_minutes", DEFAULT_CODE_TASK_DIFF_TIMEOUT_MEDIUM_MINUTES),
        ("code_task_diff_timeout_large_minutes", DEFAULT_CODE_TASK_DIFF_TIMEOUT_LARGE_MINUTES),
        ("code_task_diff_timeout_cap_minutes", DEFAULT_CODE_TASK_DIFF_TIMEOUT_CAP_MINUTES),
    ):
        value = data.get(key, default)
        if not _is_strict_int(value):
            _record_error(f"'{key}' must be an integer")
            continue
        if value <= 0:
            _record_error(f"'{key}' must be positive")
            continue
        values[key] = value

    if len(values) != 5:
        return None

    order_errors = _code_task_timeout_scaling_order_errors(
        medium_threshold=values["code_task_diff_timeout_medium_threshold"],
        large_threshold=values["code_task_diff_timeout_large_threshold"],
        medium_minutes=values["code_task_diff_timeout_medium_minutes"],
        large_minutes=values["code_task_diff_timeout_large_minutes"],
    )
    if order_errors:
        if errors is None:
            raise ConfigError(order_errors[0])
        errors.extend(order_errors)
        return None

    return (
        values["code_task_diff_timeout_medium_threshold"],
        values["code_task_diff_timeout_large_threshold"],
        values["code_task_diff_timeout_medium_minutes"],
        values["code_task_diff_timeout_large_minutes"],
        values["code_task_diff_timeout_cap_minutes"],
    )


def _read_yaml_dict(path: Path) -> dict:
    with open(path) as f:
        data = yaml.safe_load(f) or {}
    if not isinstance(data, dict):
        raise ConfigError(f"Configuration in {path} must be a YAML dictionary/object")
    return data


def _resolve_config_db_path(project_dir: Path, db_path_raw: str | None) -> Path:
    """Resolve a config db_path relative to the project root."""
    if db_path_raw:
        resolved_db = Path(os.path.expanduser(db_path_raw))
        if not resolved_db.is_absolute():
            resolved_db = project_dir / resolved_db
        return resolved_db.resolve()
    return (project_dir / DEFAULT_DB_FILE).resolve()


def _shared_db_project_id_required_message(project_dir: Path, project_name: str, resolved_db: Path) -> str:
    """Return the shared-DB remediation message for omitted project_id."""
    legacy_project_id = _generate_legacy_project_id(project_dir, project_name)
    config_path = project_dir / CONFIG_FILENAME
    return (
        f"'project_id' is required when shared DB mode is active (db_path: {resolved_db}). "
        f"Add 'project_id: {legacy_project_id}' to {config_path} to preserve this project's existing shared-DB rows, "
        "or run 'uv run gza migrate --import-local-db --yes' to persist that identity while importing any legacy local DB."
    )


def persist_project_id_if_missing(project_dir: Path, project_id: str) -> bool:
    """Persist project_id into gza.yaml if it is currently absent.

    Returns True when the file was updated, False when project_id already exists.
    """
    config_path = Config.config_path(project_dir)
    data = _read_yaml_dict(config_path)
    existing = data.get("project_id")
    if isinstance(existing, str) and existing.strip():
        return False

    lines = config_path.read_text(encoding="utf-8").splitlines()
    insertion = f"project_id: {project_id}"
    insert_at = None
    for idx, line in enumerate(lines):
        if re.match(r"^\s*project_name\s*:", line):
            insert_at = idx + 1
            break
    if insert_at is None:
        lines.append(insertion)
    else:
        lines.insert(insert_at, insertion)
    config_path.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")
    return True


def bootstrap_missing_shared_project_id(project_dir: Path, *, dry_run: bool = False) -> tuple[str | None, bool]:
    """Persist the legacy shared-DB project_id for migrate --import-local-db.

    Returns `(project_id, updated)` when shared DB mode omitted the project_id and
    this helper derived the legacy identity for persistence. Returns `(None, False)`
    when no bootstrap action is needed.
    """
    config_path = project_dir / CONFIG_FILENAME
    (
        data,
        _source_map,
        _user_config_path,
        _user_config_active,
        _local_override_path,
        _local_overrides_active,
    ) = Config._load_merged_config_data(project_dir)
    project_data = _read_yaml_dict(config_path)

    if "project_name" not in data or not data["project_name"]:
        raise ConfigError(
            f"'project_name' is required in {config_path}\n"
            "Add 'project_name: your-project-name' to the config file."
        )

    db_path_raw = os.environ.get("GZA_DB_PATH")
    if not db_path_raw:
        raw_value = data.get("db_path", "")
        if raw_value and not isinstance(raw_value, str):
            raise ConfigError("'db_path' must be a string")
        db_path_raw = raw_value

    resolved_db = _resolve_config_db_path(project_dir, db_path_raw)
    local_db_path = (project_dir / DEFAULT_DB_FILE).resolve()
    if resolved_db == local_db_path:
        return None, False

    existing = project_data.get("project_id", "")
    if existing:
        if not isinstance(existing, str):
            raise ConfigError("'project_id' must be a string")
        if not _PROJECT_ID_RE.match(existing):
            raise ConfigError("'project_id' must be 1-64 lowercase alphanumeric characters")
        return None, False

    project_id = _generate_legacy_project_id(project_dir, str(data["project_name"]))
    if dry_run:
        return project_id, False
    updated = persist_project_id_if_missing(project_dir, project_id)
    return project_id, updated


def _remove_source_subtree(source_map: dict[str, str], prefix: str) -> None:
    keys_to_delete = [key for key in source_map if key == prefix or key.startswith(f"{prefix}.")]
    for key in keys_to_delete:
        del source_map[key]


def _record_leaf_sources(data: dict, source: str, source_map: dict[str, str], path_prefix: str = "") -> None:
    for key, value in data.items():
        path = f"{path_prefix}.{key}" if path_prefix else key
        if isinstance(value, dict):
            _record_leaf_sources(value, source, source_map, path)
        else:
            source_map[path] = source


def _deep_merge_dicts(
    base: dict,
    override: dict,
    source_map: dict[str, str],
    *,
    source: str,
    path_prefix: str = "",
) -> dict:
    merged = copy.deepcopy(base)
    for key, value in override.items():
        path = f"{path_prefix}.{key}" if path_prefix else key
        if value is None and isinstance(merged.get(key), dict):
            continue
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _deep_merge_dicts(merged[key], value, source_map, source=source, path_prefix=path)
            continue

        merged[key] = copy.deepcopy(value)
        _remove_source_subtree(source_map, path)
        if isinstance(value, dict):
            _record_leaf_sources(value, source, source_map, path)
        else:
            source_map[path] = source
    return merged


_CONFIG_LAYER_PRIORITY = {
    "default": 0,
    "derived": 0,
    "user": 1,
    "base": 2,
    "local": 3,
    "env": 4,
}


def _get_nested_config_value(data: dict, path: str) -> tuple[bool, object]:
    current: object = data
    for part in path.split("."):
        if not isinstance(current, dict) or part not in current:
            return False, None
        current = current[part]
    return True, current


def _resolve_compat_value(
    data: dict,
    source_map: dict[str, str],
    candidates: list[str],
) -> tuple[object | None, str | None]:
    """Resolve compatibility aliases by config layer first, then by path priority."""
    winner: tuple[int, int, object, str] | None = None
    total_candidates = len(candidates)
    for idx, path in enumerate(candidates):
        exists, value = _get_nested_config_value(data, path)
        if not exists or value is None:
            continue
        source = source_map.get(path, "default")
        layer_priority = _CONFIG_LAYER_PRIORITY.get(source, 0)
        shape_priority = total_candidates - idx
        candidate = (layer_priority, shape_priority, value, path)
        if winner is None or candidate[:2] > winner[:2]:
            winner = candidate
    if winner is None:
        return None, None
    return winner[2], winner[3]


def _validate_local_override_data(data: dict, schema: dict, path_prefix: str = "") -> None:
    for key, value in data.items():
        path = f"{path_prefix}.{key}" if path_prefix else key
        _raise_removed_config_key_error(path, LOCAL_CONFIG_FILENAME)
        allowed = schema.get(key, schema.get("*"))
        if allowed is None and key not in schema and "*" not in schema:
            raise ConfigError(
                f"Invalid local override key '{path}' in {LOCAL_CONFIG_FILENAME}. "
                "Only approved machine-local settings may be overridden."
            )
        if isinstance(allowed, dict):
            if value is None:
                continue  # Empty block (e.g. commented-out contents), skip
            if not isinstance(value, dict):
                raise ConfigError(
                    f"Invalid local override value for '{path}' in {LOCAL_CONFIG_FILENAME}: "
                    "expected a dictionary."
                )
            _validate_local_override_data(value, allowed, path)
        elif isinstance(value, dict):
            raise ConfigError(
                f"Invalid local override value for '{path}' in {LOCAL_CONFIG_FILENAME}: "
                "nested object is not allowed here."
            )


def _validate_user_config_data(data: dict, schema: dict, path_prefix: str = "") -> None:
    user_config_display = Config.user_config_display_path()
    for key, value in data.items():
        path = f"{path_prefix}.{key}" if path_prefix else key
        _raise_removed_config_key_error(path, user_config_display)
        allowed = schema.get(key, schema.get("*"))
        if allowed is None and key not in schema and "*" not in schema:
            raise ConfigError(
                f"Invalid user config key '{path}' in {user_config_display}. "
                "Put project-specific settings in gza.yaml."
            )
        if isinstance(allowed, dict):
            if value is None:
                continue
            if not isinstance(value, dict):
                raise ConfigError(
                    f"Invalid user config value for '{path}' in {user_config_display}: "
                    "expected a dictionary."
                )
            _validate_user_config_data(value, allowed, path)
        elif isinstance(value, dict):
            raise ConfigError(
                f"Invalid user config value for '{path}' in {user_config_display}: "
                "nested object is not allowed here."
            )


def _raise_removed_config_key_error(path: str, config_display_path: str) -> None:
    if path != REMOVED_ADVANCE_REVIEW_KEY:
        return
    raise ConfigError(
        f"Invalid configuration key '{REMOVED_ADVANCE_REVIEW_KEY}' in {config_display_path}: "
        f"renamed to '{RENAMED_REQUIRE_REVIEW_KEY}'. Update your config and try again."
    )


@dataclass
class TaskTypeConfig:
    """Configuration for a specific task type."""
    model: str | None = None
    reasoning_effort: str | None = None
    max_steps: int | None = None
    max_turns: int | None = None
    timeout_minutes: int | None = None


@dataclass
class ProviderConfig:
    """Configuration scoped to a specific provider."""
    model: str | None = None
    reasoning_effort: str | None = None
    task_types: dict[str, TaskTypeConfig] = field(default_factory=dict)


@dataclass
class ClaudeConfig:
    """Claude-specific configuration."""
    fetch_auth_token_from_keychain: bool = False
    args: list[str] = field(default_factory=lambda: list(DEFAULT_CLAUDE_ARGS))


@dataclass
class TmuxConfig:
    """Configuration for running tasks inside tmux sessions."""
    enabled: bool = False
    auto_accept_timeout: float = 10.0   # seconds of quiescence before auto-accept
    max_idle_timeout: float = 300.0     # seconds before assuming stuck (5 min)
    detach_grace: float = 5.0           # seconds after detach before auto-accept resumes
    terminal_size: list[int] = field(default_factory=lambda: [200, 50])  # [cols, rows]
    session_name: str | None = None     # set at runtime by the work command


@dataclass
class WatchConfig:
    """Configuration for the `gza watch` loop."""
    batch: int = DEFAULT_WATCH_BATCH
    poll: int = DEFAULT_WATCH_POLL
    no_activity_timeout: int = DEFAULT_WATCH_NO_ACTIVITY_TIMEOUT
    max_idle: int | None = DEFAULT_WATCH_MAX_IDLE
    max_iterations: int = DEFAULT_WATCH_MAX_ITERATIONS
    restart_failed_batch: int = DEFAULT_WATCH_RESTART_FAILED_BATCH
    failure_backoff_initial: int = DEFAULT_WATCH_FAILURE_BACKOFF_INITIAL
    failure_backoff_max: int = DEFAULT_WATCH_FAILURE_BACKOFF_MAX
    failure_halt_after: int | None = DEFAULT_WATCH_FAILURE_HALT_AFTER


@dataclass
class BranchStrategy:
    """Configuration for branch naming strategy."""
    pattern: str
    default_type: str = "feature"

    def __post_init__(self):
        """Validate the branch strategy configuration."""
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
    project_id: str = ""
    project_prefix: str = ""  # Short prefix for task slugs; defaults to project_name if empty
    db_path_value: str = ""
    tasks_file: str = DEFAULT_TASKS_FILE
    log_dir: str = DEFAULT_LOG_DIR
    use_docker: bool = DEFAULT_USE_DOCKER
    enforce_project_scope: bool = DEFAULT_ENFORCE_PROJECT_SCOPE
    docker_image: str = ""
    docker_volumes: list[str] = field(default_factory=list)
    timeout_minutes: int = DEFAULT_TIMEOUT_MINUTES
    branch_mode: str = DEFAULT_BRANCH_MODE  # "single" or "multi"
    max_steps: int = DEFAULT_MAX_STEPS
    max_turns: int = DEFAULT_MAX_TURNS
    claude: ClaudeConfig = field(default_factory=ClaudeConfig)
    worktree_dir: str = DEFAULT_WORKTREE_DIR
    work_count: int = DEFAULT_WORK_COUNT
    provider: str = DEFAULT_PROVIDER  # "claude", "codex", or "gemini"
    task_providers: dict[str, str] = field(default_factory=dict)  # Per-task-type provider routing
    model: str = ""  # Provider-specific model name (optional)
    reasoning_effort: str = ""  # Provider-specific reasoning effort override (optional; Codex only)
    task_types: dict[str, TaskTypeConfig] = field(default_factory=dict)  # Per-task-type config
    providers: dict[str, ProviderConfig] = field(default_factory=dict)  # Provider-scoped config
    branch_strategy: BranchStrategy | None = None  # Branch naming strategy
    chat_text_display_length: int = DEFAULT_CHAT_TEXT_DISPLAY_LENGTH  # 0 = unlimited
    docker_setup_command: str = ""  # Pre-warm command run synchronously before provider CLI starts
    verify_command: str = ""  # Command to run before finishing (e.g., mypy + pytest)
    inner_verify_command: str = DEFAULT_INNER_VERIFY_COMMAND
    advance_create_reviews: bool = DEFAULT_ADVANCE_CREATE_REVIEWS
    require_review_before_merge: bool = DEFAULT_REQUIRE_REVIEW_BEFORE_MERGE
    pr_integration: bool = DEFAULT_PR_INTEGRATION
    advance_mode: str = DEFAULT_ADVANCE_MODE
    max_resume_attempts: int = DEFAULT_MAX_RESUME_ATTEMPTS
    max_review_cycles: int = DEFAULT_MAX_REVIEW_CYCLES
    max_noop_improve_cycles: int = DEFAULT_MAX_NOOP_IMPROVE_CYCLES
    interactive_worktree_dir: str = DEFAULT_INTERACTIVE_WORKTREE_DIR
    merge_squash_threshold: int = DEFAULT_MERGE_SQUASH_THRESHOLD
    main_checkout_isolate: bool = DEFAULT_MAIN_CHECKOUT_ISOLATE
    watch: WatchConfig = field(default_factory=WatchConfig)
    iterate_max_iterations: int = DEFAULT_ITERATE_MAX_ITERATIONS
    cleanup_days: int = DEFAULT_CLEANUP_DAYS
    review_diff_small_threshold: int = DEFAULT_REVIEW_DIFF_SMALL_THRESHOLD
    review_diff_medium_threshold: int = DEFAULT_REVIEW_DIFF_MEDIUM_THRESHOLD
    review_context_file_limit: int = DEFAULT_REVIEW_CONTEXT_FILE_LIMIT
    review_verify_timeout_seconds: int = DEFAULT_REVIEW_VERIFY_TIMEOUT_SECONDS
    code_task_diff_timeout_medium_threshold: int = DEFAULT_CODE_TASK_DIFF_TIMEOUT_MEDIUM_THRESHOLD
    code_task_diff_timeout_large_threshold: int = DEFAULT_CODE_TASK_DIFF_TIMEOUT_LARGE_THRESHOLD
    code_task_diff_timeout_medium_minutes: int = DEFAULT_CODE_TASK_DIFF_TIMEOUT_MEDIUM_MINUTES
    code_task_diff_timeout_large_minutes: int = DEFAULT_CODE_TASK_DIFF_TIMEOUT_LARGE_MINUTES
    code_task_diff_timeout_cap_minutes: int = DEFAULT_CODE_TASK_DIFF_TIMEOUT_CAP_MINUTES
    recommend_rebase_behind_commits: int = DEFAULT_RECOMMEND_REBASE_BEHIND_COMMITS  # Deprecated compatibility key; ignored.
    learnings_window: int = DEFAULT_LEARNINGS_WINDOW
    learnings_interval: int = DEFAULT_LEARNINGS_INTERVAL
    learnings_max_items: int = DEFAULT_LEARNINGS_MAX_ITEMS
    tmux: TmuxConfig = field(default_factory=TmuxConfig)  # Tmux session configuration
    theme: str | None = "minimal"  # Named color theme (default: 'minimal')
    no_color: bool = DEFAULT_NO_COLOR
    colors: dict[str, str] = field(default_factory=dict)  # Ad-hoc per-field color overrides
    source_map: dict[str, str] = field(default_factory=dict)  # Key source attribution (base/user/local/env)
    user_config_file: Path | None = None
    user_config_active: bool = False
    local_override_path: Path | None = None
    local_overrides_active: bool = False
    provider_cwd: Path | None = None
    docker_workdir: str = "/workspace"

    def __post_init__(self):
        if not self.project_id:
            self.project_id = _generate_legacy_project_id(self.project_dir, self.project_name)
        if not _PROJECT_ID_RE.match(self.project_id):
            raise ConfigError(
                "'project_id' must be 1-64 lowercase alphanumeric characters"
            )

        if not self.docker_image:
            self.docker_image = f"{self.project_name}-gza"

        # Default project_prefix to project_name if not explicitly set
        if not self.project_prefix:
            # Sanitize: lowercase, strip non-alphanumeric chars, truncate to 12 chars.
            # No hyphens — the hyphen is the separator between prefix and suffix in IDs.
            sanitized = re.sub(r'[^a-z0-9]', '', self.project_name.lower())
            if not sanitized:
                raise ConfigError(
                    f"'project_name' {self.project_name!r} produces an empty 'project_prefix' "
                    "after stripping non-alphanumeric characters. "
                    "Set 'project_prefix' explicitly in gza.yaml (e.g. project_prefix: myproject)."
                )
            self.project_prefix = sanitized[:12]

        # Set default branch strategy if not provided
        if self.branch_strategy is None:
            self.branch_strategy = BranchStrategy(
                pattern="{project}/{date}-{slug}",
                default_type="feature"
            )

    def get_model_for_task(self, task_type: str, provider: str) -> str | None:
        """Get model for task type within provider scope.

        Precedence:
        1. providers.<provider>.task_types.<task_type>.model
        2. providers.<provider>.model
        3. task_types.<task_type>.model (legacy)
        4. model (legacy)
        5. None (provider runtime default)
        """
        provider_config = self.providers.get(provider)
        if provider_config:
            provider_task_type = provider_config.task_types.get(task_type)
            if provider_task_type and provider_task_type.model:
                return provider_task_type.model
            if provider_config.model:
                return provider_config.model

        legacy_task_type = self.task_types.get(task_type)
        if legacy_task_type and legacy_task_type.model:
            return legacy_task_type.model

        return self.model or None

    def get_provider_for_task(self, task_type: str) -> str:
        """Get effective provider for a task type.

        Precedence:
        1. task_providers.<task_type>
        2. provider
        """
        return self.task_providers.get(task_type, self.provider)

    def get_model_for_task_type(self, task_type: str) -> str | None:
        """Get the model for a given task type, falling back to defaults.

        Args:
            task_type: The task type (e.g., "plan", "review", "implement")

        Returns:
            The model name to use for this task type
        """
        return self.get_model_for_task(task_type, self.provider)

    def get_reasoning_effort_for_task(self, task_type: str, provider: str) -> str | None:
        """Get reasoning effort for task type within provider scope.

        Precedence:
        1. providers.<provider>.task_types.<task_type>.reasoning_effort
        2. providers.<provider>.reasoning_effort
        3. task_types.<task_type>.reasoning_effort (legacy)
        4. reasoning_effort / defaults.reasoning_effort (legacy)
        5. None (provider runtime default)
        """
        provider_config = self.providers.get(provider)
        if provider_config:
            provider_task_type = provider_config.task_types.get(task_type)
            if provider_task_type and provider_task_type.reasoning_effort:
                return provider_task_type.reasoning_effort
            if provider_config.reasoning_effort:
                return provider_config.reasoning_effort

        legacy_task_type = self.task_types.get(task_type)
        if legacy_task_type and legacy_task_type.reasoning_effort:
            return legacy_task_type.reasoning_effort

        return self.reasoning_effort or None

    def get_reasoning_effort_for_task_type(self, task_type: str) -> str | None:
        """Get reasoning effort for a task type using configured default provider."""
        return self.get_reasoning_effort_for_task(task_type, self.provider)

    def get_max_steps_for_task(self, task_type: str, provider: str) -> int:
        """Get max_steps for task type within provider scope.

        Precedence:
        1. providers.<provider>.task_types.<task_type>.max_steps
        2. providers.<provider>.task_types.<task_type>.max_turns (legacy)
        3. task_types.<task_type>.max_steps
        4. task_types.<task_type>.max_turns (legacy)
        5. max_steps
        6. max_turns (legacy global)
        7. default (50)
        """
        provider_config = self.providers.get(provider)
        if provider_config:
            provider_task_type = provider_config.task_types.get(task_type)
            if provider_task_type and provider_task_type.max_steps is not None:
                return provider_task_type.max_steps
            if provider_task_type and provider_task_type.max_turns is not None:
                return provider_task_type.max_turns

        legacy_task_type = self.task_types.get(task_type)
        if legacy_task_type and legacy_task_type.max_steps is not None:
            return legacy_task_type.max_steps
        if legacy_task_type and legacy_task_type.max_turns is not None:
            return legacy_task_type.max_turns

        if self.max_steps is not None:
            return self.max_steps
        if self.max_turns is not None:
            return self.max_turns
        return DEFAULT_MAX_STEPS

    def get_max_turns_for_task(self, task_type: str, provider: str) -> int:
        """Backward-compatible alias for step budget resolution."""
        return self.get_max_steps_for_task(task_type, provider)

    def get_max_steps_for_task_type(self, task_type: str) -> int:
        """Get max_steps for a task type using the configured default provider."""
        return self.get_max_steps_for_task(task_type, self.provider)

    def get_max_turns_for_task_type(self, task_type: str) -> int:
        """Get the max_turns for a given task type, falling back to defaults.

        Args:
            task_type: The task type (e.g., "plan", "review", "implement")

        Returns:
            The max_turns to use for this task type
        """
        return self.get_max_steps_for_task(task_type, self.provider)

    def get_timeout_minutes_for_task(self, task_type: str, provider: str) -> int:
        """Get timeout_minutes for task type within provider/task-type scope.

        Precedence:
        1. providers.<provider>.task_types.<task_type>.timeout_minutes
        2. task_types.<task_type>.timeout_minutes
        3. timeout_minutes
        4. default (10)
        """
        provider_config = self.providers.get(provider)
        if provider_config:
            provider_task_type = provider_config.task_types.get(task_type)
            if provider_task_type and provider_task_type.timeout_minutes is not None:
                return provider_task_type.timeout_minutes

        legacy_task_type = self.task_types.get(task_type)
        if legacy_task_type and legacy_task_type.timeout_minutes is not None:
            return legacy_task_type.timeout_minutes

        if self.timeout_minutes is not None:
            return self.timeout_minutes
        return DEFAULT_TIMEOUT_MINUTES

    def get_timeout_minutes_for_task_type(self, task_type: str) -> int:
        """Get timeout_minutes for a task type using the configured default provider."""
        return self.get_timeout_minutes_for_task(task_type, self.provider)

    @property
    def worktree_path(self) -> Path:
        return Path(self.worktree_dir) / self.project_name

    @property
    def main_checkout_integration_path(self) -> Path:
        return self.worktree_path / "main-integration"

    @property
    def tasks_path(self) -> Path:
        return self.project_dir / self.tasks_file

    @property
    def db_path(self) -> Path:
        if self.db_path_value:
            resolved = Path(os.path.expanduser(self.db_path_value))
            if not resolved.is_absolute():
                resolved = self.project_dir / resolved
            return resolved
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
    def local_config_path(cls, project_dir: Path) -> Path:
        """Get the path to the local override config file."""
        return project_dir / LOCAL_CONFIG_FILENAME

    @classmethod
    def user_config_path(cls) -> Path:
        """Get the path to the user-level config file."""
        return Path.home() / f".{APP_NAME}" / USER_CONFIG_FILENAME

    @classmethod
    def user_config_display_path(cls) -> str:
        """Return the canonical user config path for display."""
        return f"~/.{APP_NAME}/{USER_CONFIG_FILENAME}"

    @classmethod
    def _load_user_config_data(cls) -> tuple[dict, Path | None, bool]:
        """Load and validate user-level config data if present."""
        user_path = cls.user_config_path()
        if not user_path.exists():
            return {}, None, False

        try:
            user_data = _read_yaml_dict(user_path)
        except yaml.YAMLError as exc:
            raise ConfigError(
                f"Invalid YAML syntax in {cls.user_config_display_path()}: {exc}"
            ) from exc
        if user_data:
            _validate_user_config_data(user_data, USER_CONFIG_ALLOWED_SCHEMA)
        return user_data, user_path, bool(user_data)

    @classmethod
    def _load_merged_config_data(
        cls, project_dir: Path
    ) -> tuple[dict, dict[str, str], Path | None, bool, Path | None, bool]:
        """Load user/base/local config layers with deep-merge and source attribution."""
        config_path = cls.config_path(project_dir)

        if not config_path.exists():
            raise ConfigError(
                f"Configuration file not found: {config_path}\n"
                f"Run 'gza init' to create one."
            )

        user_data, user_path, user_active = cls._load_user_config_data()
        source_map: dict[str, str] = {}
        merged_data = copy.deepcopy(user_data)
        if user_data:
            _record_leaf_sources(user_data, "user", source_map)

        base_data = _read_yaml_dict(config_path)
        if merged_data:
            merged_data = _deep_merge_dicts(merged_data, base_data, source_map, source="base")
        else:
            merged_data = copy.deepcopy(base_data)
            _record_leaf_sources(base_data, "base", source_map)

        local_path = cls.local_config_path(project_dir)
        local_active = False
        if local_path.exists():
            local_data = _read_yaml_dict(local_path)
            if local_data:
                _validate_local_override_data(local_data, LOCAL_OVERRIDE_ALLOWED_SCHEMA)
                merged_data = _deep_merge_dicts(merged_data, local_data, source_map, source="local")
                local_active = True

        return (
            merged_data,
            source_map,
            user_path,
            user_active,
            (local_path if local_path.exists() else None),
            local_active,
        )

    @classmethod
    def load(
        cls,
        project_dir: Path,
        *,
        discover: bool = False,
        allow_derived_shared_project_id: bool = False,
    ) -> "Config":
        """Load config from gza.yaml in project root.

        Raises ConfigError if config file is missing or project_name is not set.
        """
        if discover:
            project_dir = discover_project_dir(project_dir)

        (
            data,
            source_map,
            user_config_path,
            user_config_active,
            local_override_path,
            local_overrides_active,
        ) = cls._load_merged_config_data(project_dir)
        return cls._build_config_from_merged_data(
            project_dir,
            data,
            source_map,
            user_config_path=user_config_path,
            user_config_active=user_config_active,
            local_override_path=local_override_path,
            local_overrides_active=local_overrides_active,
            allow_derived_shared_project_id=allow_derived_shared_project_id,
        )

    @classmethod
    def _build_config_from_merged_data(
        cls,
        project_dir: Path,
        data: dict,
        source_map: dict[str, str],
        *,
        user_config_path: Path | None,
        user_config_active: bool,
        local_override_path: Path | None,
        local_overrides_active: bool,
        allow_derived_shared_project_id: bool = False,
    ) -> "Config":
        """Build a Config from already-merged config data."""
        config_path = cls.config_path(project_dir)

        # if local_overrides_active and local_override_path:
        #     project_key = str(project_dir.resolve())
        #     if project_key not in _LOCAL_OVERRIDE_NOTICE_SHOWN:
        #         print(
        #             f"Notice: local config overrides active from {local_override_path.name}",
        #             file=sys.stderr,
        #         )
        #         _LOCAL_OVERRIDE_NOTICE_SHOWN.add(project_key)

        if REMOVED_ADVANCE_REVIEW_KEY in data:
            raise ConfigError(
                f"'{REMOVED_ADVANCE_REVIEW_KEY}' has been renamed to '{RENAMED_REQUIRE_REVIEW_KEY}'. "
                "Update your config and try again."
            )

        # Validate and warn about unknown keys
        for key in data.keys():
            if key not in VALID_CONFIG_FIELDS:
                print(f"Warning: Unknown configuration field '{key}' in {config_path}", file=sys.stderr)

        # Require project_name
        if "project_name" not in data or not data["project_name"]:
            raise ConfigError(
                f"'project_name' is required in {config_path}\n"
                f"Add 'project_name: your-project-name' to the config file."
            )

        db_path_raw = os.environ.get("GZA_DB_PATH")
        if db_path_raw:
            source_map["db_path"] = "env"
        else:
            db_path_raw = data.get("db_path", "")
            if db_path_raw and not isinstance(db_path_raw, str):
                raise ConfigError("'db_path' must be a string")

        project_name_raw = data["project_name"]
        local_db_path = (project_dir / DEFAULT_DB_FILE).resolve()
        resolved_db = _resolve_config_db_path(project_dir, db_path_raw)

        project_id_raw = data.get("project_id", "")
        if project_id_raw:
            if not isinstance(project_id_raw, str):
                raise ConfigError("'project_id' must be a string")
            if not _PROJECT_ID_RE.match(project_id_raw):
                raise ConfigError("'project_id' must be 1-64 lowercase alphanumeric characters")
        else:
            if resolved_db == local_db_path:
                project_id_raw = "default"
            elif allow_derived_shared_project_id:
                project_id_raw = _generate_legacy_project_id(project_dir, str(project_name_raw))
                source_map["project_id"] = "derived"
            else:
                raise ConfigError(
                    _shared_db_project_id_required_message(project_dir, str(project_name_raw), resolved_db)
                )
        if resolved_db != local_db_path and project_id_raw == "default":
            raise ConfigError(
                "'project_id: default' is only valid with local DB mode (db_path: .gza/gza.db). "
                "Set a unique project_id for shared DB mode."
            )

        # Parse and validate project_prefix
        project_prefix_raw = data.get("project_prefix", "")
        if project_prefix_raw:
            if not isinstance(project_prefix_raw, str):
                raise ConfigError("'project_prefix' must be a string")
            if len(project_prefix_raw) > 12:
                raise ConfigError("'project_prefix' must be between 1 and 12 characters")
            if not _PREFIX_RE.match(project_prefix_raw):
                raise ConfigError(
                    "'project_prefix' must contain only lowercase alphanumeric characters (no hyphens)"
                )

        # Support both new "defaults" section and old flat structure
        # If "defaults" exists, use it; otherwise use top-level fields
        defaults = data.get("defaults", {})

        use_docker = data.get("use_docker", DEFAULT_USE_DOCKER)
        timeout_minutes = _validate_optional_positive_int_field(
            data.get("timeout_minutes", DEFAULT_TIMEOUT_MINUTES),
            "timeout_minutes",
        )
        assert timeout_minutes is not None
        branch_mode = data.get("branch_mode", DEFAULT_BRANCH_MODE)

        # Compatibility aliases resolve by layer first; defaults.* only wins within the same layer.
        max_steps, max_steps_source_key = _resolve_compat_value(
            data,
            source_map,
            ["defaults.max_steps", "max_steps"],
        )
        max_turns, max_turns_source_key = _resolve_compat_value(
            data,
            source_map,
            ["defaults.max_turns", "max_turns"],
        )

        # Migration behavior: if max_steps isn't set, fall back to max_turns with warning.
        if max_steps is None:
            if max_turns is not None:
                if not isinstance(max_turns, int):
                    raise ConfigError("'max_turns' must be an integer")
                warnings.warn(
                    "'max_turns' is deprecated; use 'max_steps'.",
                    DeprecationWarning,
                    stacklevel=2,
                )
                max_steps = max_turns
                max_steps_source_key = max_turns_source_key
            else:
                max_steps = DEFAULT_MAX_STEPS
        elif not isinstance(max_steps, int):
            raise ConfigError("'max_steps' must be an integer")

        # Keep max_turns populated for backward-compatible call sites.
        if max_turns is None:
            max_turns = max_steps
            max_turns_source_key = max_steps_source_key
        elif not isinstance(max_turns, int):
            raise ConfigError("'max_turns' must be an integer")

        worktree_dir = data.get("worktree_dir", DEFAULT_WORKTREE_DIR)
        work_count = data.get("work_count", DEFAULT_WORK_COUNT)
        chat_text_display_length = data.get("chat_text_display_length", DEFAULT_CHAT_TEXT_DISPLAY_LENGTH)
        provider = data.get("provider", DEFAULT_PROVIDER)

        # task_providers routing
        task_providers: dict[str, str] = {}
        task_providers_data = data.get("task_providers")
        if task_providers_data is not None:
            if not isinstance(task_providers_data, dict):
                raise ConfigError("'task_providers' must be a dictionary")
            for task_type, provider_name in task_providers_data.items():
                if not isinstance(provider_name, str):
                    raise ConfigError(f"'task_providers.{task_type}' must be a string")
                if provider_name not in KNOWN_PROVIDERS:
                    raise ConfigError(
                        f"'task_providers.{task_type}' must be one of: {', '.join(KNOWN_PROVIDERS)}"
                    )
                task_providers[task_type] = provider_name

        model, model_source_key = _resolve_compat_value(
            data,
            source_map,
            ["defaults.model", "model"],
        )
        if model is None:
            model = ""
        elif not isinstance(model, str):
            raise ConfigError("'model' must be a string")
        reasoning_effort, reasoning_effort_source_key = _resolve_compat_value(
            data,
            source_map,
            ["defaults.reasoning_effort", "reasoning_effort"],
        )
        if reasoning_effort is None:
            reasoning_effort = ""
        elif not isinstance(reasoning_effort, str):
            raise ConfigError("'reasoning_effort' must be a string")
        verify_command = _validate_optional_string_field(data.get("verify_command"), "verify_command")
        inner_verify_command = _validate_optional_string_field(
            data.get("inner_verify_command"),
            "inner_verify_command",
            default=DEFAULT_INNER_VERIFY_COMMAND,
        )

        docker_volumes = data.get("docker_volumes", [])
        enforce_project_scope = data.get("enforce_project_scope", DEFAULT_ENFORCE_PROJECT_SCOPE)
        if not isinstance(enforce_project_scope, bool):
            raise ConfigError("'enforce_project_scope' must be a boolean (true/false)")

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
                    task_type_timeout_minutes = _validate_optional_positive_int_field(
                        config_data.get("timeout_minutes"),
                        f"task_types.{task_type}.timeout_minutes",
                    )
                    task_types[task_type] = TaskTypeConfig(
                        model=config_data.get("model"),
                        reasoning_effort=config_data.get("reasoning_effort"),
                        max_steps=config_data.get("max_steps"),
                        max_turns=config_data.get("max_turns"),
                        timeout_minutes=task_type_timeout_minutes,
                    )

        # Parse provider-scoped configuration
        providers: dict[str, ProviderConfig] = {}
        if "providers" in data:
            providers_data = data["providers"]
            if not isinstance(providers_data, dict):
                raise ConfigError("'providers' must be a dictionary")
            for provider_name, provider_config_data in providers_data.items():
                if provider_name not in KNOWN_PROVIDERS:
                    raise ConfigError(
                        f"'providers.{provider_name}' is invalid. "
                        f"Known providers: {', '.join(KNOWN_PROVIDERS)}"
                    )
                if provider_config_data is None:
                    continue  # Empty block, skip
                if not isinstance(provider_config_data, dict):
                    raise ConfigError(f"'providers.{provider_name}' must be a dictionary")

                provider_model = provider_config_data.get("model")
                if provider_model is not None and not isinstance(provider_model, str):
                    raise ConfigError(f"'providers.{provider_name}.model' must be a string")
                provider_reasoning_effort = provider_config_data.get("reasoning_effort")
                if provider_reasoning_effort is not None and not isinstance(provider_reasoning_effort, str):
                    raise ConfigError(f"'providers.{provider_name}.reasoning_effort' must be a string")

                provider_task_types: dict[str, TaskTypeConfig] = {}
                provider_task_types_data = provider_config_data.get("task_types")
                if provider_task_types_data is not None:
                    if not isinstance(provider_task_types_data, dict):
                        raise ConfigError(f"'providers.{provider_name}.task_types' must be a dictionary")
                    for task_type, task_type_config_data in provider_task_types_data.items():
                        if task_type_config_data is None:
                            continue  # Empty block, skip
                        if not isinstance(task_type_config_data, dict):
                            raise ConfigError(f"'providers.{provider_name}.task_types.{task_type}' must be a dictionary")
                        provider_task_model = task_type_config_data.get("model")
                        if provider_task_model is not None and not isinstance(provider_task_model, str):
                            raise ConfigError(
                                f"'providers.{provider_name}.task_types.{task_type}.model' must be a string"
                            )
                        provider_task_reasoning_effort = task_type_config_data.get("reasoning_effort")
                        if (
                            provider_task_reasoning_effort is not None
                            and not isinstance(provider_task_reasoning_effort, str)
                        ):
                            raise ConfigError(
                                f"'providers.{provider_name}.task_types.{task_type}.reasoning_effort' must be a string"
                            )
                        provider_task_max_turns = task_type_config_data.get("max_turns")
                        provider_task_max_steps = task_type_config_data.get("max_steps")
                        provider_task_timeout_minutes = _validate_optional_positive_int_field(
                            task_type_config_data.get("timeout_minutes"),
                            f"providers.{provider_name}.task_types.{task_type}.timeout_minutes",
                        )
                        if provider_task_max_steps is not None:
                            if not isinstance(provider_task_max_steps, int):
                                raise ConfigError(
                                    f"'providers.{provider_name}.task_types.{task_type}.max_steps' must be an integer"
                                )
                            if provider_task_max_steps <= 0:
                                raise ConfigError(
                                    f"'providers.{provider_name}.task_types.{task_type}.max_steps' must be positive"
                                )
                        if provider_task_max_turns is not None:
                            if not isinstance(provider_task_max_turns, int):
                                raise ConfigError(
                                    f"'providers.{provider_name}.task_types.{task_type}.max_turns' must be an integer"
                                )
                            if provider_task_max_turns <= 0:
                                raise ConfigError(
                                    f"'providers.{provider_name}.task_types.{task_type}.max_turns' must be positive"
                                )
                        provider_task_types[task_type] = TaskTypeConfig(
                            model=provider_task_model,
                            reasoning_effort=provider_task_reasoning_effort,
                            max_steps=provider_task_max_steps,
                            max_turns=provider_task_max_turns,
                            timeout_minutes=provider_task_timeout_minutes,
                        )

                providers[provider_name] = ProviderConfig(
                    model=provider_model,
                    reasoning_effort=provider_reasoning_effort,
                    task_types=provider_task_types,
                )

        # Warn when provider-scoped and broader fallback fields are both set for the same semantic target.
        legacy_model_set = "model" in data or ("defaults" in data and isinstance(defaults, dict) and "model" in defaults)
        if legacy_model_set:
            for provider_name, provider_config in providers.items():
                if provider_config.model:
                    warnings.warn(
                        f"Both provider-scoped model ('providers.{provider_name}.model') and default model "
                        f"('model'/'defaults.model') are set. Using provider-scoped value for provider '{provider_name}'.",
                        stacklevel=2,
                    )
        legacy_reasoning_effort_set = "reasoning_effort" in data or (
            "defaults" in data and isinstance(defaults, dict) and "reasoning_effort" in defaults
        )
        if legacy_reasoning_effort_set:
            for provider_name, provider_config in providers.items():
                if provider_config.reasoning_effort:
                    warnings.warn(
                        f"Both provider-scoped reasoning_effort ('providers.{provider_name}.reasoning_effort') and "
                        f"default reasoning_effort ('reasoning_effort'/'defaults.reasoning_effort') are set. "
                        f"Using provider-scoped value for provider '{provider_name}'.",
                        stacklevel=2,
                    )

        legacy_task_types_data = data.get("task_types")
        if isinstance(legacy_task_types_data, dict):
            for provider_name, provider_config in providers.items():
                for task_type, provider_task_type in provider_config.task_types.items():
                    legacy_task_type = legacy_task_types_data.get(task_type)
                    if not isinstance(legacy_task_type, dict):
                        continue
                    if provider_task_type.model is not None and "model" in legacy_task_type:
                        warnings.warn(
                            f"Both provider-scoped and task-type default model are set for task type '{task_type}' "
                            f"(providers.{provider_name}.task_types.{task_type}.model and task_types.{task_type}.model). "
                            f"Using provider-scoped value for provider '{provider_name}'.",
                            stacklevel=2,
                        )
                    if provider_task_type.reasoning_effort is not None and "reasoning_effort" in legacy_task_type:
                        warnings.warn(
                            f"Both provider-scoped and task-type default reasoning_effort are set for task type '{task_type}' "
                            f"(providers.{provider_name}.task_types.{task_type}.reasoning_effort and "
                            f"task_types.{task_type}.reasoning_effort). "
                            f"Using provider-scoped value for provider '{provider_name}'.",
                            stacklevel=2,
                        )
                    if provider_task_type.max_turns is not None and "max_turns" in legacy_task_type:
                        warnings.warn(
                            f"Both provider-scoped and task-type default max_turns are set for task type '{task_type}' "
                            f"(providers.{provider_name}.task_types.{task_type}.max_turns and task_types.{task_type}.max_turns). "
                            f"Using provider-scoped value for provider '{provider_name}'.",
                            stacklevel=2,
                        )
                    if provider_task_type.max_steps is not None and "max_steps" in legacy_task_type:
                        warnings.warn(
                            f"Both provider-scoped and task-type default max_steps are set for task type '{task_type}' "
                            f"(providers.{provider_name}.task_types.{task_type}.max_steps and task_types.{task_type}.max_steps). "
                            f"Using provider-scoped value for provider '{provider_name}'.",
                            stacklevel=2,
                        )

        # Deprecation warnings for legacy max_turns usage without max_steps.
        if isinstance(defaults, dict) and "max_turns" in defaults and "max_steps" not in defaults:
            warnings.warn("'defaults.max_turns' is deprecated; use 'defaults.max_steps'.", DeprecationWarning, stacklevel=2)
        if "max_turns" in data and "max_steps" not in data:
            warnings.warn("'max_turns' is deprecated; use 'max_steps'.", DeprecationWarning, stacklevel=2)
        if isinstance(legacy_task_types_data, dict):
            for task_type, legacy_task_type in legacy_task_types_data.items():
                if isinstance(legacy_task_type, dict) and "max_turns" in legacy_task_type and "max_steps" not in legacy_task_type:
                    warnings.warn(
                        f"'task_types.{task_type}.max_turns' is deprecated; use 'task_types.{task_type}.max_steps'.",
                        DeprecationWarning,
                        stacklevel=2,
                    )
        providers_data = data.get("providers")
        if isinstance(providers_data, dict):
            for provider_name, provider_data in providers_data.items():
                if not isinstance(provider_data, dict):
                    continue
                provider_task_types_data = provider_data.get("task_types")
                if not isinstance(provider_task_types_data, dict):
                    continue
                for task_type, task_type_data in provider_task_types_data.items():
                    if isinstance(task_type_data, dict) and "max_turns" in task_type_data and "max_steps" not in task_type_data:
                        warnings.warn(
                            f"'providers.{provider_name}.task_types.{task_type}.max_turns' is deprecated; use "
                            f"'providers.{provider_name}.task_types.{task_type}.max_steps'.",
                            DeprecationWarning,
                            stacklevel=2,
                        )

        # Validate provider/model compatibility with effective loaded settings.
        model_compat_errors: list[str] = []
        if not _is_model_compatible_with_provider(provider, model):
            model_compat_errors.append(_provider_model_mismatch_error("model", provider, model))
        for task_type, task_cfg in task_types.items():
            task_provider = task_providers.get(task_type, provider)
            if task_cfg.model and not _is_model_compatible_with_provider(task_provider, task_cfg.model):
                model_compat_errors.append(
                    _provider_model_mismatch_error(f"task_types.{task_type}.model", task_provider, task_cfg.model)
                )
        for provider_name, provider_cfg in providers.items():
            if provider_cfg.model and not _is_model_compatible_with_provider(provider_name, provider_cfg.model):
                model_compat_errors.append(
                    _provider_model_mismatch_error(
                        f"providers.{provider_name}.model",
                        provider_name,
                        provider_cfg.model,
                    )
                )
            for task_type, task_cfg in provider_cfg.task_types.items():
                if task_cfg.model and not _is_model_compatible_with_provider(provider_name, task_cfg.model):
                    model_compat_errors.append(
                        _provider_model_mismatch_error(
                            f"providers.{provider_name}.task_types.{task_type}.model",
                            provider_name,
                            task_cfg.model,
                        )
                    )
        if model_compat_errors:
            raise ConfigError("Invalid provider/model configuration:\n- " + "\n- ".join(model_compat_errors))

        # Parse branch_strategy configuration
        branch_strategy = None
        if "branch_strategy" in data:
            bs_data = data["branch_strategy"]
            # Handle preset names
            if isinstance(bs_data, str):
                if bs_data == "monorepo":
                    raise ConfigError(
                        "branch_strategy preset 'monorepo' was removed. "
                        "Use 'project_date_slug' (the new default) instead, "
                        "or define a custom pattern via "
                        "'branch_strategy: {pattern: ...}'."
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
                elif bs_data == "project_date_slug":
                    branch_strategy = BranchStrategy(
                        pattern="{project}/{date}-{slug}",
                        default_type="feature"
                    )
                else:
                    raise ConfigError(
                        f"Unknown branch_strategy preset: '{bs_data}'\n"
                        f"Valid presets are: project_date_slug, conventional, simple, date_slug\n"
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
                source_map["claude.args"] = source_map.get("claude_args", "base")

        # Resolve sources for semantic fields whose winning value may come from defaults.*.
        if max_steps_source_key is not None:
            source_map["max_steps"] = source_map.get(max_steps_source_key, "base")
        if max_turns_source_key is not None:
            source_map["max_turns"] = source_map.get(max_turns_source_key, "base")
        if model_source_key is not None:
            source_map["model"] = source_map.get(model_source_key, "base")
        if reasoning_effort_source_key is not None:
            source_map["reasoning_effort"] = source_map.get(reasoning_effort_source_key, "base")

        if "advance_create_reviews" in data and not isinstance(data["advance_create_reviews"], bool):
            raise ConfigError("'advance_create_reviews' must be a boolean (true/false)")
        advance_create_reviews = bool(data.get("advance_create_reviews", DEFAULT_ADVANCE_CREATE_REVIEWS))

        if "require_review_before_merge" in data and not isinstance(data["require_review_before_merge"], bool):
            raise ConfigError("'require_review_before_merge' must be a boolean (true/false)")
        require_review_before_merge = bool(
            data.get("require_review_before_merge", DEFAULT_REQUIRE_REVIEW_BEFORE_MERGE)
        )
        if "pr_integration" in data and not isinstance(data["pr_integration"], bool):
            raise ConfigError("'pr_integration' must be a boolean (true/false)")
        pr_integration = bool(data.get("pr_integration", DEFAULT_PR_INTEGRATION))
        advance_mode = str(data.get("advance_mode", DEFAULT_ADVANCE_MODE))
        if advance_mode not in {"default", "iterate"}:
            raise ConfigError("'advance_mode' must be 'default' or 'iterate'")
        max_resume_attempts = _load_strict_int_field(data, "max_resume_attempts", DEFAULT_MAX_RESUME_ATTEMPTS)
        if max_resume_attempts < 0:
            raise ConfigError("'max_resume_attempts' must be non-negative")
        max_review_cycles = _load_strict_int_field(data, "max_review_cycles", DEFAULT_MAX_REVIEW_CYCLES)
        if max_review_cycles <= 0:
            raise ConfigError("'max_review_cycles' must be positive")
        max_noop_improve_cycles = _load_strict_int_field(
            data, "max_noop_improve_cycles", DEFAULT_MAX_NOOP_IMPROVE_CYCLES
        )
        if max_noop_improve_cycles <= 0:
            raise ConfigError("'max_noop_improve_cycles' must be positive")

        iterate_max_iterations = _load_strict_int_field(
            data, "iterate_max_iterations", DEFAULT_ITERATE_MAX_ITERATIONS
        )
        if iterate_max_iterations <= 0:
            raise ConfigError("'iterate_max_iterations' must be positive")
        watch_data = data.get("watch") or {}
        if not isinstance(watch_data, dict):
            raise ConfigError("'watch' must be a dictionary")
        try:
            watch_batch = int(watch_data.get("batch", DEFAULT_WATCH_BATCH))
        except (TypeError, ValueError):
            raise ConfigError("watch.batch must be a positive integer")
        if watch_batch < 1:
            raise ConfigError("watch.batch must be a positive integer")
        try:
            watch_poll = int(watch_data.get("poll", DEFAULT_WATCH_POLL))
        except (TypeError, ValueError):
            raise ConfigError("watch.poll must be a positive integer")
        if watch_poll < 1:
            raise ConfigError("watch.poll must be a positive integer")
        try:
            watch_no_activity_timeout = int(
                watch_data.get("no_activity_timeout", DEFAULT_WATCH_NO_ACTIVITY_TIMEOUT)
            )
        except (TypeError, ValueError):
            raise ConfigError("watch.no_activity_timeout must be a positive integer")
        if watch_no_activity_timeout < 1:
            raise ConfigError("watch.no_activity_timeout must be a positive integer")
        watch_max_idle_raw = watch_data.get("max_idle", DEFAULT_WATCH_MAX_IDLE)
        if watch_max_idle_raw is None:
            watch_max_idle = None
        else:
            try:
                watch_max_idle = int(watch_max_idle_raw)
            except (TypeError, ValueError):
                raise ConfigError("watch.max_idle must be null or a positive integer")
            if watch_max_idle < 1:
                raise ConfigError("watch.max_idle must be null or a positive integer")
        try:
            watch_max_iterations = int(watch_data.get("max_iterations", DEFAULT_WATCH_MAX_ITERATIONS))
        except (TypeError, ValueError):
            raise ConfigError("watch.max_iterations must be a positive integer")
        if watch_max_iterations < 1:
            raise ConfigError("watch.max_iterations must be a positive integer")
        try:
            watch_restart_failed_batch = int(
                watch_data.get("restart_failed_batch", DEFAULT_WATCH_RESTART_FAILED_BATCH)
            )
        except (TypeError, ValueError):
            raise ConfigError("watch.restart_failed_batch must be a positive integer")
        if watch_restart_failed_batch < 1:
            raise ConfigError("watch.restart_failed_batch must be a positive integer")
        try:
            watch_failure_backoff_initial = int(
                watch_data.get("failure_backoff_initial", DEFAULT_WATCH_FAILURE_BACKOFF_INITIAL)
            )
        except (TypeError, ValueError):
            raise ConfigError("watch.failure_backoff_initial must be a positive integer")
        if watch_failure_backoff_initial < 1:
            raise ConfigError("watch.failure_backoff_initial must be a positive integer")
        try:
            watch_failure_backoff_max = int(
                watch_data.get("failure_backoff_max", DEFAULT_WATCH_FAILURE_BACKOFF_MAX)
            )
        except (TypeError, ValueError):
            raise ConfigError("watch.failure_backoff_max must be a positive integer")
        if watch_failure_backoff_max < 1:
            raise ConfigError("watch.failure_backoff_max must be a positive integer")
        if watch_failure_backoff_max < watch_failure_backoff_initial:
            raise ConfigError("watch.failure_backoff_max must be >= watch.failure_backoff_initial")
        watch_failure_halt_after_raw = watch_data.get(
            "failure_halt_after", DEFAULT_WATCH_FAILURE_HALT_AFTER
        )
        if watch_failure_halt_after_raw is None:
            watch_failure_halt_after = None
        else:
            try:
                watch_failure_halt_after = int(watch_failure_halt_after_raw)
            except (TypeError, ValueError):
                raise ConfigError("watch.failure_halt_after must be null or a positive integer")
            if watch_failure_halt_after < 1:
                raise ConfigError("watch.failure_halt_after must be null or a positive integer")

        watch_config = WatchConfig(
            batch=watch_batch,
            poll=watch_poll,
            no_activity_timeout=watch_no_activity_timeout,
            max_idle=watch_max_idle,
            max_iterations=watch_max_iterations,
            restart_failed_batch=watch_restart_failed_batch,
            failure_backoff_initial=watch_failure_backoff_initial,
            failure_backoff_max=watch_failure_backoff_max,
            failure_halt_after=watch_failure_halt_after,
        )
        interactive_worktree_dir = data.get("interactive_worktree_dir", DEFAULT_INTERACTIVE_WORKTREE_DIR)

        try:
            merge_squash_threshold = int(data.get("merge_squash_threshold", DEFAULT_MERGE_SQUASH_THRESHOLD))
        except (TypeError, ValueError):
            raise ConfigError("merge_squash_threshold must be a non-negative integer")
        if merge_squash_threshold < 0:
            raise ConfigError("merge_squash_threshold must be a non-negative integer")
        main_checkout_isolate = data.get("main_checkout_isolate", DEFAULT_MAIN_CHECKOUT_ISOLATE)
        if not isinstance(main_checkout_isolate, bool):
            raise ConfigError("'main_checkout_isolate' must be a boolean (true/false)")

        try:
            cleanup_days = int(data.get("cleanup_days", DEFAULT_CLEANUP_DAYS))
        except (TypeError, ValueError):
            raise ConfigError("cleanup_days must be a positive integer")
        if cleanup_days < 1:
            raise ConfigError("cleanup_days must be a positive integer")

        try:
            review_diff_small_threshold = int(
                data.get("review_diff_small_threshold", DEFAULT_REVIEW_DIFF_SMALL_THRESHOLD)
            )
        except (TypeError, ValueError):
            raise ConfigError("review_diff_small_threshold must be a positive integer")
        if review_diff_small_threshold < 1:
            raise ConfigError("review_diff_small_threshold must be a positive integer")

        try:
            review_diff_medium_threshold = int(
                data.get("review_diff_medium_threshold", DEFAULT_REVIEW_DIFF_MEDIUM_THRESHOLD)
            )
        except (TypeError, ValueError):
            raise ConfigError("review_diff_medium_threshold must be a positive integer")
        if review_diff_medium_threshold < 1:
            raise ConfigError("review_diff_medium_threshold must be a positive integer")

        if review_diff_medium_threshold < review_diff_small_threshold:
            raise ConfigError(
                "review_diff_medium_threshold must be greater than or equal to review_diff_small_threshold"
            )

        try:
            review_context_file_limit = int(
                data.get("review_context_file_limit", DEFAULT_REVIEW_CONTEXT_FILE_LIMIT)
            )
        except (TypeError, ValueError):
            raise ConfigError("review_context_file_limit must be a positive integer")
        if review_context_file_limit < 1:
            raise ConfigError("review_context_file_limit must be a positive integer")

        review_verify_timeout_seconds = _load_strict_int_field(
            data,
            "review_verify_timeout_seconds",
            DEFAULT_REVIEW_VERIFY_TIMEOUT_SECONDS,
        )
        if review_verify_timeout_seconds < 1:
            raise ConfigError("'review_verify_timeout_seconds' must be positive")

        resolved_code_task_timeout_scaling = _resolve_code_task_timeout_scaling_fields(data)
        assert resolved_code_task_timeout_scaling is not None
        (
            code_task_diff_timeout_medium_threshold,
            code_task_diff_timeout_large_threshold,
            code_task_diff_timeout_medium_minutes,
            code_task_diff_timeout_large_minutes,
            code_task_diff_timeout_cap_minutes,
        ) = resolved_code_task_timeout_scaling

        recommend_rebase_behind_commits = _load_strict_int_field(
            data,
            "recommend_rebase_behind_commits",
            DEFAULT_RECOMMEND_REBASE_BEHIND_COMMITS,
        )
        if recommend_rebase_behind_commits < 0:
            raise ConfigError("'recommend_rebase_behind_commits' must be non-negative")

        try:
            learnings_window = int(data.get("learnings_window", DEFAULT_LEARNINGS_WINDOW))
        except (TypeError, ValueError):
            raise ConfigError("learnings_window must be a positive integer")
        if learnings_window < 1:
            raise ConfigError("learnings_window must be a positive integer")

        try:
            learnings_interval = int(data.get("learnings_interval", DEFAULT_LEARNINGS_INTERVAL))
        except (TypeError, ValueError):
            raise ConfigError("learnings_interval must be a non-negative integer")
        if learnings_interval < 0:
            raise ConfigError("learnings_interval must be a non-negative integer")

        try:
            learnings_max_items = int(data.get("learnings_max_items", DEFAULT_LEARNINGS_MAX_ITEMS))
        except (TypeError, ValueError):
            raise ConfigError("learnings_max_items must be a positive integer")
        if learnings_max_items < 1:
            raise ConfigError("learnings_max_items must be a positive integer")

        # Parse tmux configuration
        tmux_data = data.get("tmux") or {}
        if not isinstance(tmux_data, dict):
            raise ConfigError("'tmux' must be a dictionary")

        try:
            auto_accept_timeout = float(tmux_data.get("auto_accept_timeout", 10.0))
        except (TypeError, ValueError):
            raise ConfigError("tmux.auto_accept_timeout must be a positive number")
        if auto_accept_timeout <= 0:
            raise ConfigError("tmux.auto_accept_timeout must be a positive number")

        try:
            max_idle_timeout = float(tmux_data.get("max_idle_timeout", 300.0))
        except (TypeError, ValueError):
            raise ConfigError("tmux.max_idle_timeout must be a positive number")
        if max_idle_timeout <= 0:
            raise ConfigError("tmux.max_idle_timeout must be a positive number")

        try:
            detach_grace = float(tmux_data.get("detach_grace", 5.0))
        except (TypeError, ValueError):
            raise ConfigError("tmux.detach_grace must be a positive number")
        if detach_grace <= 0:
            raise ConfigError(
                "tmux.detach_grace must be a positive number "
                "(seconds to wait after human detaches before auto-accept resumes)"
            )

        terminal_size = tmux_data.get("terminal_size", [200, 50])
        if (
            not isinstance(terminal_size, list)
            or len(terminal_size) != 2
            or not all(isinstance(v, int) and v > 0 for v in terminal_size)
        ):
            raise ConfigError(
                "tmux.terminal_size must be a list of two positive integers [cols, rows]"
            )

        tmux_config = TmuxConfig(
            enabled=tmux_data.get("enabled", False),
            auto_accept_timeout=auto_accept_timeout,
            max_idle_timeout=max_idle_timeout,
            detach_grace=detach_grace,
            terminal_size=terminal_size,
        )

        # Parse theme and ad-hoc color overrides.
        from .colors import (
            BUILT_IN_THEMES,  # noqa: PLC0415
            set_theme as _set_theme,
        )

        theme_name: str | None = data.get("theme", "minimal")
        if theme_name is not None:
            if not isinstance(theme_name, str):
                raise ConfigError("'theme' must be a string")
            if theme_name not in BUILT_IN_THEMES:
                raise ConfigError(
                    f"'theme' must be one of: {', '.join(sorted(BUILT_IN_THEMES))}"
                )

        raw_colors = data.get("colors") or {}
        if not isinstance(raw_colors, dict):
            raise ConfigError("'colors' must be a dictionary of field-name: color-value pairs")
        colors: dict[str, str] = {}
        for k, v in raw_colors.items():
            if not isinstance(k, str) or not isinstance(v, str):
                raise ConfigError("'colors' keys and values must both be strings")
            colors[k] = v

        no_color = data.get("no_color", DEFAULT_NO_COLOR)
        if not isinstance(no_color, bool):
            raise ConfigError("'no_color' must be a boolean")

        from .console import set_config_no_color as _set_config_no_color  # noqa: PLC0415

        # Apply theme to module-level color singletons so all subsequent code
        # sees the correct themed values when accessing gza.colors.*.
        _set_theme(theme_name, colors)
        # Config no_color and NO_COLOR are a logical OR. Consoles consult the
        # environment directly, so only the config bit needs to be persisted here.
        _set_config_no_color(no_color)

        return cls(
            project_dir=project_dir,
            project_name=data["project_name"],  # Already validated above
            project_id=project_id_raw,
            project_prefix=project_prefix_raw,
            tasks_file=data.get("tasks_file", DEFAULT_TASKS_FILE),
            log_dir=data.get("log_dir", DEFAULT_LOG_DIR),
            db_path_value=db_path_raw or "",
            use_docker=use_docker,
            enforce_project_scope=enforce_project_scope,
            docker_image=data.get("docker_image", ""),
            docker_volumes=docker_volumes,
            docker_setup_command=data.get("docker_setup_command", ""),
            timeout_minutes=timeout_minutes,
            branch_mode=branch_mode,
            max_steps=max_steps,
            max_turns=max_turns,
            claude=claude_config,
            worktree_dir=worktree_dir,
            work_count=work_count,
            provider=provider,
            task_providers=task_providers,
            model=model,
            reasoning_effort=reasoning_effort,
            task_types=task_types,
            providers=providers,
            branch_strategy=branch_strategy,
            chat_text_display_length=chat_text_display_length,
            verify_command=verify_command,
            inner_verify_command=inner_verify_command,
            advance_create_reviews=advance_create_reviews,
            require_review_before_merge=require_review_before_merge,
            pr_integration=pr_integration,
            advance_mode=advance_mode,
            max_resume_attempts=max_resume_attempts,
            max_review_cycles=max_review_cycles,
            max_noop_improve_cycles=max_noop_improve_cycles,
            watch=watch_config,
            iterate_max_iterations=iterate_max_iterations,
            interactive_worktree_dir=interactive_worktree_dir,
            merge_squash_threshold=merge_squash_threshold,
            main_checkout_isolate=main_checkout_isolate,
            cleanup_days=cleanup_days,
            review_diff_small_threshold=review_diff_small_threshold,
            review_diff_medium_threshold=review_diff_medium_threshold,
            review_context_file_limit=review_context_file_limit,
            review_verify_timeout_seconds=review_verify_timeout_seconds,
            code_task_diff_timeout_medium_threshold=code_task_diff_timeout_medium_threshold,
            code_task_diff_timeout_large_threshold=code_task_diff_timeout_large_threshold,
            code_task_diff_timeout_medium_minutes=code_task_diff_timeout_medium_minutes,
            code_task_diff_timeout_large_minutes=code_task_diff_timeout_large_minutes,
            code_task_diff_timeout_cap_minutes=code_task_diff_timeout_cap_minutes,
            recommend_rebase_behind_commits=recommend_rebase_behind_commits,
            learnings_window=learnings_window,
            learnings_interval=learnings_interval,
            learnings_max_items=learnings_max_items,
            tmux=tmux_config,
            theme=theme_name,
            no_color=no_color,
            colors=colors,
            source_map=source_map,
            user_config_file=user_config_path,
            user_config_active=user_config_active,
            local_override_path=local_override_path,
            local_overrides_active=local_overrides_active,
        )

    @classmethod
    def preflight_init_user_config(cls, project_dir: Path, *, project_name: str, project_id: str) -> None:
        """Validate user config semantics for `gza init` before writing project files."""
        user_data, user_path, user_active = cls._load_user_config_data()
        if not user_data:
            return

        source_map: dict[str, str] = {}
        _record_leaf_sources(user_data, "user", source_map)
        candidate_data = copy.deepcopy(user_data)
        candidate_data["project_name"] = project_name
        candidate_data["project_id"] = project_id
        source_map["project_name"] = "base"
        source_map["project_id"] = "base"

        try:
            cls._build_config_from_merged_data(
                project_dir,
                candidate_data,
                source_map,
                user_config_path=user_path,
                user_config_active=user_active,
                local_override_path=None,
                local_overrides_active=False,
            )
        except ConfigError as exc:
            raise ConfigError(
                f"Invalid user config in {cls.user_config_display_path()}: {exc}"
            ) from exc

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

        # Try to parse and merge YAML config layers
        try:
            (
                data,
                _source_map,
                user_config_path,
                user_config_active,
                local_override_path,
                local_overrides_active,
            ) = cls._load_merged_config_data(project_dir)
        except yaml.YAMLError as e:
            errors.append(f"Invalid YAML syntax: {e}")
            return False, errors, warnings
        except ConfigError as e:
            errors.append(str(e))
            return False, errors, warnings
        except Exception as e:
            logger.error(
                "Unexpected error while validating config at %s",
                config_path,
                exc_info=True,
            )
            errors.append(f"Error reading file: {e}")
            return False, errors, warnings

        if user_config_active and user_config_path:
            warnings.append(f"User config active: {cls.user_config_display_path()}")
        if local_overrides_active and local_override_path:
            warnings.append(f"Local overrides active: {local_override_path.name}")

        for key in data.keys():
            if key == REMOVED_ADVANCE_REVIEW_KEY:
                errors.append(
                    f"Unknown configuration field: '{REMOVED_ADVANCE_REVIEW_KEY}' "
                    f"(renamed to '{RENAMED_REQUIRE_REVIEW_KEY}')"
                )
            elif key not in VALID_CONFIG_FIELDS:
                warnings.append(f"Unknown configuration field: '{key}'")

        # Require project_name
        if "project_name" not in data or not data["project_name"]:
            errors.append("'project_name' is required")
        elif not isinstance(data["project_name"], str):
            errors.append("'project_name' must be a string")

        if "project_id" in data and data["project_id"]:
            project_id = data["project_id"]
            if not isinstance(project_id, str):
                errors.append("'project_id' must be a string")
            elif not _PROJECT_ID_RE.match(project_id):
                errors.append("'project_id' must be 1-64 lowercase alphanumeric characters")

        if "db_path" in data and data["db_path"] is not None and not isinstance(data["db_path"], str):
            errors.append("'db_path' must be a string")

        project_name_raw = data.get("project_name")
        project_name_for_id = str(project_name_raw) if isinstance(project_name_raw, str) else ""
        db_path_raw = data.get("db_path")
        local_db_path = (project_dir / DEFAULT_DB_FILE).resolve()
        resolved_db = _resolve_config_db_path(project_dir, db_path_raw if isinstance(db_path_raw, str) else None)
        project_id_effective = data.get("project_id")
        if not isinstance(project_id_effective, str) or not project_id_effective:
            if resolved_db == local_db_path:
                project_id_effective = "default"
            else:
                errors.append(
                    _shared_db_project_id_required_message(project_dir, project_name_for_id, resolved_db)
                )
                project_id_effective = ""
        if resolved_db != local_db_path and project_id_effective == "default":
            errors.append(
                "'project_id: default' is only valid with local DB mode (db_path: .gza/gza.db). "
                "Set a unique project_id for shared DB mode."
            )
        if "project_prefix" in data and data["project_prefix"]:
            prefix_val = data["project_prefix"]
            if not isinstance(prefix_val, str):
                errors.append("'project_prefix' must be a string")
            elif len(prefix_val) > 12:
                errors.append("'project_prefix' must be between 1 and 12 characters")
            elif not _PREFIX_RE.match(prefix_val):
                errors.append(
                    "'project_prefix' must contain only lowercase alphanumeric characters (no hyphens)"
                )

        if "tasks_file" in data and not isinstance(data["tasks_file"], str):
            errors.append("'tasks_file' must be a string")

        if "log_dir" in data and not isinstance(data["log_dir"], str):
            errors.append("'log_dir' must be a string")

        if "use_docker" in data and not isinstance(data["use_docker"], bool):
            errors.append("'use_docker' must be a boolean (true/false)")

        if "enforce_project_scope" in data and not isinstance(data["enforce_project_scope"], bool):
            errors.append("'enforce_project_scope' must be a boolean (true/false)")

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
            _validate_optional_positive_int_field(
                data["timeout_minutes"],
                "timeout_minutes",
                errors=errors,
            )

        if "branch_mode" in data:
            if not isinstance(data["branch_mode"], str):
                errors.append("'branch_mode' must be a string")
            elif data["branch_mode"] not in ("single", "multi"):
                errors.append("'branch_mode' must be either 'single' or 'multi'")

        if "max_steps" in data:
            if not isinstance(data["max_steps"], int):
                errors.append("'max_steps' must be an integer")
            elif data["max_steps"] <= 0:
                errors.append("'max_steps' must be positive")

        if "max_turns" in data:
            if not isinstance(data["max_turns"], int):
                errors.append("'max_turns' must be an integer")
            elif data["max_turns"] <= 0:
                errors.append("'max_turns' must be positive")
            if "max_steps" not in data and not (
                isinstance(data.get("defaults"), dict) and "max_steps" in data["defaults"]
            ):
                warnings.append("'max_turns' is deprecated; use 'max_steps'.")

        if "watch" in data:
            watch_data = data["watch"]
            if not isinstance(watch_data, dict):
                errors.append("'watch' must be a dictionary")
            else:
                if "batch" in watch_data:
                    if not isinstance(watch_data["batch"], int) or watch_data["batch"] < 1:
                        errors.append("watch.batch must be a positive integer")
                if "poll" in watch_data:
                    if not isinstance(watch_data["poll"], int) or watch_data["poll"] < 1:
                        errors.append("watch.poll must be a positive integer")
                if "no_activity_timeout" in watch_data:
                    if (
                        not isinstance(watch_data["no_activity_timeout"], int)
                        or watch_data["no_activity_timeout"] < 1
                    ):
                        errors.append("watch.no_activity_timeout must be a positive integer")
                if "max_idle" in watch_data and watch_data["max_idle"] is not None:
                    if not isinstance(watch_data["max_idle"], int) or watch_data["max_idle"] < 1:
                        errors.append("watch.max_idle must be null or a positive integer")
                if "max_iterations" in watch_data:
                    if not isinstance(watch_data["max_iterations"], int) or watch_data["max_iterations"] < 1:
                        errors.append("watch.max_iterations must be a positive integer")
                if "restart_failed_batch" in watch_data:
                    if (
                        not isinstance(watch_data["restart_failed_batch"], int)
                        or watch_data["restart_failed_batch"] < 1
                    ):
                        errors.append("watch.restart_failed_batch must be a positive integer")
                if "failure_backoff_initial" in watch_data:
                    if (
                        not isinstance(watch_data["failure_backoff_initial"], int)
                        or watch_data["failure_backoff_initial"] < 1
                    ):
                        errors.append("watch.failure_backoff_initial must be a positive integer")
                if "failure_backoff_max" in watch_data:
                    if (
                        not isinstance(watch_data["failure_backoff_max"], int)
                        or watch_data["failure_backoff_max"] < 1
                    ):
                        errors.append("watch.failure_backoff_max must be a positive integer")
                initial_raw = watch_data.get("failure_backoff_initial")
                max_raw = watch_data.get("failure_backoff_max")
                if (
                    isinstance(initial_raw, int)
                    and initial_raw >= 1
                    and isinstance(max_raw, int)
                    and max_raw >= 1
                    and max_raw < initial_raw
                ):
                    errors.append("watch.failure_backoff_max must be >= watch.failure_backoff_initial")
                if "failure_halt_after" in watch_data and watch_data["failure_halt_after"] is not None:
                    if (
                        not isinstance(watch_data["failure_halt_after"], int)
                        or watch_data["failure_halt_after"] < 1
                    ):
                        errors.append("watch.failure_halt_after must be null or a positive integer")

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
            elif data["provider"] not in KNOWN_PROVIDERS:
                provider_list = ", ".join("'" + p + "'" for p in KNOWN_PROVIDERS)
                errors.append(f"'provider' must be one of: {provider_list}")

        if "task_providers" in data:
            if not isinstance(data["task_providers"], dict):
                errors.append("'task_providers' must be a dictionary")
            else:
                for task_type, provider_name in data["task_providers"].items():
                    if not isinstance(provider_name, str):
                        errors.append(f"'task_providers.{task_type}' must be a string")
                    elif provider_name not in KNOWN_PROVIDERS:
                        provider_list = ", ".join("'" + p + "'" for p in KNOWN_PROVIDERS)
                        errors.append(
                            f"'task_providers.{task_type}' must be one of: {provider_list}"
                        )

        if "model" in data and not isinstance(data["model"], str):
            errors.append("'model' must be a string")
        if "reasoning_effort" in data and not isinstance(data["reasoning_effort"], str):
            errors.append("'reasoning_effort' must be a string")

        if "verify_command" in data and not isinstance(data["verify_command"], str):
            errors.append("'verify_command' must be a string")
        if "inner_verify_command" in data and not isinstance(data["inner_verify_command"], str):
            errors.append("'inner_verify_command' must be a string")

        if "interactive_worktree_dir" in data and not isinstance(data["interactive_worktree_dir"], str):
            errors.append("'interactive_worktree_dir' must be a string")
        if "main_checkout_isolate" in data and not isinstance(data["main_checkout_isolate"], bool):
            errors.append("'main_checkout_isolate' must be a boolean (true/false)")

        if "review_diff_small_threshold" in data:
            if not isinstance(data["review_diff_small_threshold"], int):
                errors.append("'review_diff_small_threshold' must be an integer")
            elif data["review_diff_small_threshold"] <= 0:
                errors.append("'review_diff_small_threshold' must be positive")

        if "review_diff_medium_threshold" in data:
            if not isinstance(data["review_diff_medium_threshold"], int):
                errors.append("'review_diff_medium_threshold' must be an integer")
            elif data["review_diff_medium_threshold"] <= 0:
                errors.append("'review_diff_medium_threshold' must be positive")

        if "review_context_file_limit" in data:
            if not isinstance(data["review_context_file_limit"], int):
                errors.append("'review_context_file_limit' must be an integer")
            elif data["review_context_file_limit"] <= 0:
                errors.append("'review_context_file_limit' must be positive")

        if "review_verify_timeout_seconds" in data:
            if not isinstance(data["review_verify_timeout_seconds"], int):
                errors.append("'review_verify_timeout_seconds' must be an integer")
            elif data["review_verify_timeout_seconds"] <= 0:
                errors.append("'review_verify_timeout_seconds' must be positive")

        _resolve_code_task_timeout_scaling_fields(data, errors=errors)

        if "recommend_rebase_behind_commits" in data:
            if not isinstance(data["recommend_rebase_behind_commits"], int):
                errors.append("'recommend_rebase_behind_commits' must be an integer")
            elif data["recommend_rebase_behind_commits"] < 0:
                errors.append("'recommend_rebase_behind_commits' must be non-negative")

        if (
            isinstance(data.get("review_diff_small_threshold"), int)
            and isinstance(data.get("review_diff_medium_threshold"), int)
            and data["review_diff_medium_threshold"] < data["review_diff_small_threshold"]
        ):
            errors.append(
                "'review_diff_medium_threshold' must be greater than or equal to 'review_diff_small_threshold'"
            )
        if "advance_create_reviews" in data and not isinstance(data["advance_create_reviews"], bool):
            errors.append("'advance_create_reviews' must be a boolean (true/false)")

        if "require_review_before_merge" in data and not isinstance(data["require_review_before_merge"], bool):
            errors.append("'require_review_before_merge' must be a boolean (true/false)")
        if "pr_integration" in data and not isinstance(data["pr_integration"], bool):
            errors.append("'pr_integration' must be a boolean (true/false)")
        if "max_resume_attempts" in data:
            if not _is_strict_int(data["max_resume_attempts"]):
                errors.append("'max_resume_attempts' must be an integer")
            elif data["max_resume_attempts"] < 0:
                errors.append("'max_resume_attempts' must be non-negative")
        if "max_review_cycles" in data:
            if not _is_strict_int(data["max_review_cycles"]):
                errors.append("'max_review_cycles' must be an integer")
            elif data["max_review_cycles"] <= 0:
                errors.append("'max_review_cycles' must be positive")
        if "max_noop_improve_cycles" in data:
            if not _is_strict_int(data["max_noop_improve_cycles"]):
                errors.append("'max_noop_improve_cycles' must be an integer")
            elif data["max_noop_improve_cycles"] <= 0:
                errors.append("'max_noop_improve_cycles' must be positive")
        if "iterate_max_iterations" in data:
            if not _is_strict_int(data["iterate_max_iterations"]):
                errors.append("'iterate_max_iterations' must be an integer")
            elif data["iterate_max_iterations"] <= 0:
                errors.append("'iterate_max_iterations' must be positive")

        # Validate defaults section
        if "defaults" in data:
            if not isinstance(data["defaults"], dict):
                errors.append("'defaults' must be a dictionary")
            else:
                defaults = data["defaults"]
                if "model" in defaults and not isinstance(defaults["model"], str):
                    errors.append("'defaults.model' must be a string")
                if "reasoning_effort" in defaults and not isinstance(defaults["reasoning_effort"], str):
                    errors.append("'defaults.reasoning_effort' must be a string")
                if "max_turns" in defaults:
                    if not isinstance(defaults["max_turns"], int):
                        errors.append("'defaults.max_turns' must be an integer")
                    elif defaults["max_turns"] <= 0:
                        errors.append("'defaults.max_turns' must be positive")
                if "max_steps" in defaults:
                    if not isinstance(defaults["max_steps"], int):
                        errors.append("'defaults.max_steps' must be an integer")
                    elif defaults["max_steps"] <= 0:
                        errors.append("'defaults.max_steps' must be positive")
                # Warn about unknown keys in defaults
                valid_defaults_keys = {"model", "reasoning_effort", "max_steps", "max_turns"}
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
                        if "reasoning_effort" in config and not isinstance(config["reasoning_effort"], str):
                            errors.append(f"'task_types.{task_type}.reasoning_effort' must be a string")
                        if "max_turns" in config:
                            if not isinstance(config["max_turns"], int):
                                errors.append(f"'task_types.{task_type}.max_turns' must be an integer")
                            elif config["max_turns"] <= 0:
                                errors.append(f"'task_types.{task_type}.max_turns' must be positive")
                        if "max_steps" in config:
                            if not isinstance(config["max_steps"], int):
                                errors.append(f"'task_types.{task_type}.max_steps' must be an integer")
                            elif config["max_steps"] <= 0:
                                errors.append(f"'task_types.{task_type}.max_steps' must be positive")
                        if "timeout_minutes" in config:
                            _validate_optional_positive_int_field(
                                config["timeout_minutes"],
                                f"task_types.{task_type}.timeout_minutes",
                                errors=errors,
                            )
                        # Warn about unknown keys
                        valid_task_type_keys = {"model", "reasoning_effort", "max_steps", "max_turns", "timeout_minutes"}
                        for key in config.keys():
                            if key not in valid_task_type_keys:
                                warnings.append(f"Unknown field in 'task_types.{task_type}': '{key}'")

        # Validate provider/model compatibility to fail early on mixed-provider configs.
        provider_for_models = data.get("provider", DEFAULT_PROVIDER)
        task_providers_for_models = data.get("task_providers", {})
        if not isinstance(task_providers_for_models, dict):
            task_providers_for_models = {}
        if isinstance(provider_for_models, str) and provider_for_models in ("claude", "codex", "gemini"):
            top_model = data.get("model")
            if isinstance(top_model, str) and top_model and not _is_model_compatible_with_provider(provider_for_models, top_model):
                errors.append(_provider_model_mismatch_error("model", provider_for_models, top_model))

            defaults_cfg = data.get("defaults")
            if isinstance(defaults_cfg, dict):
                defaults_model = defaults_cfg.get("model")
                if (
                    isinstance(defaults_model, str)
                    and defaults_model
                    and not _is_model_compatible_with_provider(provider_for_models, defaults_model)
                ):
                    errors.append(_provider_model_mismatch_error("defaults.model", provider_for_models, defaults_model))

            task_types_cfg = data.get("task_types")
            if isinstance(task_types_cfg, dict):
                for task_type, task_cfg in task_types_cfg.items():
                    if isinstance(task_cfg, dict):
                        task_provider = task_providers_for_models.get(task_type, provider_for_models)
                        if not isinstance(task_provider, str) or task_provider not in KNOWN_PROVIDERS:
                            task_provider = provider_for_models
                        task_model = task_cfg.get("model")
                        if (
                            isinstance(task_model, str)
                            and task_model
                            and not _is_model_compatible_with_provider(task_provider, task_model)
                        ):
                            errors.append(
                                _provider_model_mismatch_error(
                                    f"task_types.{task_type}.model",
                                    task_provider,
                                    task_model,
                                )
                            )

        # Validate provider-scoped configuration
        if "providers" in data:
            if not isinstance(data["providers"], dict):
                errors.append("'providers' must be a dictionary")
            else:
                for provider_name, provider_data in data["providers"].items():
                    if provider_name not in KNOWN_PROVIDERS:
                        errors.append(
                            f"'providers.{provider_name}' is invalid. "
                            f"Known providers: {', '.join(KNOWN_PROVIDERS)}"
                        )
                        continue
                    if provider_data is None:
                        continue  # Empty block, skip
                    if not isinstance(provider_data, dict):
                        errors.append(f"'providers.{provider_name}' must be a dictionary")
                        continue

                    if "model" in provider_data and not isinstance(provider_data["model"], str):
                        errors.append(f"'providers.{provider_name}.model' must be a string")
                    elif (
                        isinstance(provider_data.get("model"), str)
                        and provider_data["model"]
                        and not _is_model_compatible_with_provider(provider_name, provider_data["model"])
                    ):
                        errors.append(
                            _provider_model_mismatch_error(
                                f"providers.{provider_name}.model",
                                provider_name,
                                provider_data["model"],
                            )
                        )
                    if "reasoning_effort" in provider_data and not isinstance(provider_data["reasoning_effort"], str):
                        errors.append(f"'providers.{provider_name}.reasoning_effort' must be a string")

                    if "task_types" in provider_data and provider_data["task_types"] is not None:
                        if not isinstance(provider_data["task_types"], dict):
                            errors.append(f"'providers.{provider_name}.task_types' must be a dictionary")
                        else:
                            for task_type, task_type_config in provider_data["task_types"].items():
                                if task_type_config is None:
                                    continue  # Empty block, skip
                                if not isinstance(task_type_config, dict):
                                    errors.append(
                                        f"'providers.{provider_name}.task_types.{task_type}' must be a dictionary"
                                    )
                                    continue
                                if "model" in task_type_config and not isinstance(task_type_config["model"], str):
                                    errors.append(
                                        f"'providers.{provider_name}.task_types.{task_type}.model' must be a string"
                                    )
                                elif (
                                    isinstance(task_type_config.get("model"), str)
                                    and task_type_config["model"]
                                    and not _is_model_compatible_with_provider(provider_name, task_type_config["model"])
                                ):
                                    errors.append(
                                        _provider_model_mismatch_error(
                                            f"providers.{provider_name}.task_types.{task_type}.model",
                                            provider_name,
                                            task_type_config["model"],
                                        )
                                    )
                                if "reasoning_effort" in task_type_config and not isinstance(
                                    task_type_config["reasoning_effort"], str
                                ):
                                    errors.append(
                                        f"'providers.{provider_name}.task_types.{task_type}.reasoning_effort' must be a string"
                                    )
                                if "max_turns" in task_type_config:
                                    if not isinstance(task_type_config["max_turns"], int):
                                        errors.append(
                                            f"'providers.{provider_name}.task_types.{task_type}.max_turns' must be an integer"
                                        )
                                    elif task_type_config["max_turns"] <= 0:
                                        errors.append(
                                            f"'providers.{provider_name}.task_types.{task_type}.max_turns' must be positive"
                                        )
                                if "max_steps" in task_type_config:
                                    if not isinstance(task_type_config["max_steps"], int):
                                        errors.append(
                                            f"'providers.{provider_name}.task_types.{task_type}.max_steps' must be an integer"
                                        )
                                    elif task_type_config["max_steps"] <= 0:
                                        errors.append(
                                            f"'providers.{provider_name}.task_types.{task_type}.max_steps' must be positive"
                                        )
                                if "timeout_minutes" in task_type_config:
                                    _validate_optional_positive_int_field(
                                        task_type_config["timeout_minutes"],
                                        f"providers.{provider_name}.task_types.{task_type}.timeout_minutes",
                                        errors=errors,
                                    )
                                valid_provider_task_type_keys = {
                                    "model",
                                    "reasoning_effort",
                                    "max_steps",
                                    "max_turns",
                                    "timeout_minutes",
                                }
                                for key in task_type_config.keys():
                                    if key not in valid_provider_task_type_keys:
                                        warnings.append(
                                            f"Unknown field in 'providers.{provider_name}.task_types.{task_type}': '{key}'"
                                        )

                    valid_provider_keys = {"model", "reasoning_effort", "task_types"}
                    for key in provider_data.keys():
                        if key not in valid_provider_keys:
                            warnings.append(f"Unknown field in 'providers.{provider_name}': '{key}'")

        # Warn when provider-scoped and broader fallback fields are both set for the same semantic target.
        legacy_model_set = "model" in data or (
            "defaults" in data and isinstance(data.get("defaults"), dict) and "model" in data["defaults"]
        )
        if legacy_model_set and isinstance(data.get("providers"), dict):
            for provider_name, provider_data in data["providers"].items():
                if isinstance(provider_data, dict) and provider_data.get("model") is not None:
                    warnings.append(
                        f"Both provider-scoped model ('providers.{provider_name}.model') and default model "
                        f"('model'/'defaults.model') are set. Provider-scoped value takes precedence."
                    )
        legacy_reasoning_effort_set = "reasoning_effort" in data or (
            "defaults" in data and isinstance(data.get("defaults"), dict) and "reasoning_effort" in data["defaults"]
        )
        if legacy_reasoning_effort_set and isinstance(data.get("providers"), dict):
            for provider_name, provider_data in data["providers"].items():
                if isinstance(provider_data, dict) and provider_data.get("reasoning_effort") is not None:
                    warnings.append(
                        "Both provider-scoped reasoning_effort "
                        f"('providers.{provider_name}.reasoning_effort') and default reasoning_effort "
                        "('reasoning_effort'/'defaults.reasoning_effort') are set. Provider-scoped value takes precedence."
                    )

        if isinstance(data.get("task_types"), dict) and isinstance(data.get("providers"), dict):
            for provider_name, provider_data in data["providers"].items():
                if not isinstance(provider_data, dict):
                    continue
                provider_task_types = provider_data.get("task_types")
                if not isinstance(provider_task_types, dict):
                    continue
                for task_type, provider_task_type in provider_task_types.items():
                    legacy_task_type = data["task_types"].get(task_type)
                    if not isinstance(provider_task_type, dict) or not isinstance(legacy_task_type, dict):
                        continue
                    if "model" in provider_task_type and "model" in legacy_task_type:
                        warnings.append(
                            f"Both provider-scoped and task-type default model are set for task type '{task_type}' "
                            f"(providers.{provider_name}.task_types.{task_type}.model and task_types.{task_type}.model). "
                            f"Provider-scoped value takes precedence."
                        )
                    if "reasoning_effort" in provider_task_type and "reasoning_effort" in legacy_task_type:
                        warnings.append(
                            f"Both provider-scoped and task-type default reasoning_effort are set for task type '{task_type}' "
                            f"(providers.{provider_name}.task_types.{task_type}.reasoning_effort and "
                            f"task_types.{task_type}.reasoning_effort). "
                            f"Provider-scoped value takes precedence."
                        )
                    if "max_turns" in provider_task_type and "max_turns" in legacy_task_type:
                        warnings.append(
                            f"Both provider-scoped and task-type default max_turns are set for task type '{task_type}' "
                            f"(providers.{provider_name}.task_types.{task_type}.max_turns and task_types.{task_type}.max_turns). "
                            f"Provider-scoped value takes precedence."
                        )
                    if "max_steps" in provider_task_type and "max_steps" in legacy_task_type:
                        warnings.append(
                            f"Both provider-scoped and task-type default max_steps are set for task type '{task_type}' "
                            f"(providers.{provider_name}.task_types.{task_type}.max_steps and task_types.{task_type}.max_steps). "
                            f"Provider-scoped value takes precedence."
                        )

        # Validate branch_strategy section
        if "branch_strategy" in data:
            bs_data = data["branch_strategy"]
            if isinstance(bs_data, str):
                # Validate preset names
                valid_presets = {"conventional", "simple", "date_slug", "project_date_slug"}
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
