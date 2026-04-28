"""Discoverable configuration key registry for CLI/help/docs parity."""

from dataclasses import dataclass


@dataclass(frozen=True)
class ConfigKeySpec:
    """Machine-readable specification for a discoverable config key."""

    key: str
    value_type: str
    default: object | None
    description: str
    required: bool = False


# Runtime-only Config dataclass fields; not user-configurable keys.
RUNTIME_ONLY_CONFIG_FIELDS = {
    "project_dir",
    "db_path_value",
    "source_map",
    "local_override_path",
    "local_overrides_active",
}

# Extra accepted config roots that are not direct Config dataclass fields.
NON_CONFIG_ROOT_KEYS = {
    "defaults",
    "claude_args",
    "db_path",
}


CONFIG_KEY_REGISTRY: tuple[ConfigKeySpec, ...] = (
    ConfigKeySpec("advance_create_reviews", "bool", True, "Auto-create review tasks in lifecycle flows."),
    ConfigKeySpec("advance_mode", "str", "default", "Mode selector for `gza advance` behavior."),
    ConfigKeySpec("advance_requires_review", "bool", True, "Require review before merge in lifecycle flows."),
    ConfigKeySpec("branch_mode", "str", "multi", "Git branch mode: `single` or `multi`."),
    ConfigKeySpec("branch_strategy", "str | object", "{project}/{date}-{slug}", "Branch naming strategy preset or object."),
    ConfigKeySpec("branch_strategy.default_type", "str", "feature", "Default type token for branch strategy patterns."),
    ConfigKeySpec("branch_strategy.pattern", "str", "{project}/{date}-{slug}", "Branch naming format pattern."),
    ConfigKeySpec("chat_text_display_length", "int", 0, "Chat output truncation length (0 = unlimited)."),
    ConfigKeySpec("claude.args", "list[str]", ["--allowedTools", "Read", "Write", "Edit", "Glob", "Grep", "Bash"], "Claude CLI arguments."),
    ConfigKeySpec("claude.fetch_auth_token_from_keychain", "bool", False, "Fetch Claude auth token from macOS keychain for Docker."),
    ConfigKeySpec("claude_args", "list[str]", None, "Deprecated legacy alias for `claude.args`."),
    ConfigKeySpec("cleanup_days", "int", 30, "Default retention window for `gza clean`."),
    ConfigKeySpec("colors.*", "str", None, "Ad-hoc color override map keyed by output field."),
    ConfigKeySpec("defaults.max_steps", "int", 50, "Default step budget for task execution."),
    ConfigKeySpec("defaults.max_turns", "int", 50, "Deprecated legacy alias for defaults.max_steps."),
    ConfigKeySpec("defaults.model", "str", "", "Legacy global default model fallback."),
    ConfigKeySpec("defaults.reasoning_effort", "str", "", "Legacy global default reasoning effort fallback (Codex)."),
    ConfigKeySpec("docker_image", "str", "{project_name}-gza", "Docker image used for task execution."),
    ConfigKeySpec(
        "docker_setup_command",
        "str",
        "",
        "Pre-warm command run synchronously inside Docker before provider CLI starts.",
    ),
    ConfigKeySpec("docker_volumes", "list[str]", [], "Extra Docker volume mounts (`source:dest[:mode]`)."),
    ConfigKeySpec("db_path", "str", ".gza/gza.db", "SQLite database path."),
    ConfigKeySpec("interactive_worktree_dir", "str", "", "Base path for interactive worktree operations."),
    ConfigKeySpec("iterate_max_iterations", "int", 3, "Default iteration budget for `gza iterate`."),
    ConfigKeySpec("learnings_interval", "int", 5, "Auto-regenerate learnings every N completed tasks (0 disables)."),
    ConfigKeySpec("learnings_max_items", "int", 50, "Max number of items retained in `.gza/learnings.md`."),
    ConfigKeySpec("learnings_window", "int", 25, "Window size used when regenerating learnings."),
    ConfigKeySpec("log_dir", "str", ".gza/logs", "Directory for task and worker logs."),
    ConfigKeySpec("max_resume_attempts", "int", 1, "Retry cap for resume-based lifecycle automation."),
    ConfigKeySpec("max_review_cycles", "int", 3, "Cap for review/improve loops in lifecycle automation."),
    ConfigKeySpec("max_steps", "int", 50, "Global default step budget."),
    ConfigKeySpec("max_turns", "int", 50, "Deprecated global alias for `max_steps`."),
    ConfigKeySpec("merge_squash_threshold", "int", 0, "Auto-squash threshold for merge operations."),
    ConfigKeySpec("model", "str", "", "Legacy global model fallback."),
    ConfigKeySpec("reasoning_effort", "str", "", "Legacy global reasoning effort fallback (Codex)."),
    ConfigKeySpec(
        "project_id",
        "str",
        "derived from project path/name",
        "Stable project identity used for DB row scoping.",
    ),
    ConfigKeySpec("project_name", "str", None, "Project identifier used for naming and defaults.", required=True),
    ConfigKeySpec("project_prefix", "str", "derived from project_name", "Task-ID prefix (1-12 lowercase alphanumeric chars)."),
    ConfigKeySpec("provider", "str", "claude", "Default provider when task-specific routing is absent."),
    ConfigKeySpec("providers.*.model", "str", None, "Provider-scoped default model."),
    ConfigKeySpec("providers.*.reasoning_effort", "str", None, "Provider-scoped default reasoning effort (Codex)."),
    ConfigKeySpec("providers.*.task_types.*.max_steps", "int", None, "Provider/task-type step budget override."),
    ConfigKeySpec("providers.*.task_types.*.max_turns", "int", None, "Deprecated provider/task-type alias for max_steps."),
    ConfigKeySpec("providers.*.task_types.*.model", "str", None, "Provider/task-type model override."),
    ConfigKeySpec("providers.*.task_types.*.reasoning_effort", "str", None, "Provider/task-type reasoning effort override (Codex)."),
    ConfigKeySpec("review_context_file_limit", "int", 12, "Max changed files included in large review context excerpts."),
    ConfigKeySpec("review_diff_medium_threshold", "int", 2000, "Medium diff threshold for review prompt shaping."),
    ConfigKeySpec("review_diff_small_threshold", "int", 500, "Small diff threshold for full inline review diffs."),
    ConfigKeySpec("task_providers.*", "str", None, "Task-type to provider routing override."),
    ConfigKeySpec("task_types.*.max_steps", "int", None, "Legacy per-task-type step budget override."),
    ConfigKeySpec("task_types.*.max_turns", "int", None, "Deprecated legacy per-task-type alias for max_steps."),
    ConfigKeySpec("task_types.*.model", "str", None, "Legacy per-task-type model override."),
    ConfigKeySpec("task_types.*.reasoning_effort", "str", None, "Legacy per-task-type reasoning effort override (Codex)."),
    ConfigKeySpec("tasks_file", "str", "tasks.yaml", "Legacy task list file path."),
    ConfigKeySpec("theme", "str", "minimal", "Built-in color theme name."),
    ConfigKeySpec("timeout_minutes", "int", 10, "Max runtime minutes before a task times out."),
    ConfigKeySpec("tmux.auto_accept_timeout", "float", 10.0, "Seconds of quiescence before auto-accept in tmux."),
    ConfigKeySpec("tmux.detach_grace", "float", 5.0, "Grace period after manual detach before auto-accept resumes."),
    ConfigKeySpec("tmux.enabled", "bool", False, "Enable tmux-backed background sessions."),
    ConfigKeySpec("tmux.max_idle_timeout", "float", 300.0, "Max idle seconds before tmux session is considered stuck."),
    ConfigKeySpec("tmux.terminal_size", "list[int]", [200, 50], "Tmux terminal dimensions `[cols, rows]`."),
    ConfigKeySpec("use_docker", "bool", True, "Run providers in Docker."),
    ConfigKeySpec("verify_command", "str", "", "Project verification command used before completion."),
    ConfigKeySpec("watch.batch", "int", 5, "Default concurrent worker target for `gza watch`."),
    ConfigKeySpec("watch.failure_backoff_initial", "int", 60, "Initial cooldown after a non-auto-resumable watch failure."),
    ConfigKeySpec("watch.failure_backoff_max", "int", 3600, "Maximum cooldown after consecutive non-auto-resumable watch failures."),
    ConfigKeySpec("watch.failure_halt_after", "int | null", 10, "Exit `gza watch` after this many consecutive non-auto-resumable failures."),
    ConfigKeySpec("watch.max_idle", "int | null", None, "Idle timeout seconds for `gza watch` loop exit."),
    ConfigKeySpec("watch.max_iterations", "int", 10, "Default review/improve loop cap in `gza watch`."),
    ConfigKeySpec("watch.poll", "int", 300, "Polling interval seconds for `gza watch`."),
    ConfigKeySpec("work_count", "int", 1, "Default task count for each `gza work` run."),
    ConfigKeySpec("worktree_dir", "str", "/tmp/gza-worktrees", "Base directory for git worktrees."),
)
