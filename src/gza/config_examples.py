"""Generated config-example rendering from the discoverable config registry."""

from __future__ import annotations

import json
import textwrap
from dataclasses import dataclass, field
from pathlib import Path
from typing import cast

from .config import LOCAL_OVERRIDE_ALLOWED_SCHEMA
from .config_schema import CONFIG_KEY_REGISTRY, ConfigKeySpec

FULL_EXAMPLE_PATH = Path(__file__).with_name("gza.yaml.example")
LOCAL_EXAMPLE_PATH = Path(__file__).with_name("gza.local.yaml.example")


@dataclass(frozen=True)
class BranchStrategyRender:
    """How to render the branch_strategy setting in example output."""

    mode: str = "comment_default"
    preset: str | None = None
    pattern: str | None = None
    default_type: str = "feature"


@dataclass(frozen=True)
class ConfigExampleRenderOptions:
    """Rendering options for config-example generation."""

    flavor: str = "full"
    project_name: str = "my-project"
    project_name_enabled: bool = True
    project_id: str = "myproject01"
    project_id_enabled: bool = False
    db_path: str | None = None
    branch_strategy: BranchStrategyRender = field(default_factory=BranchStrategyRender)


@dataclass(frozen=True)
class _Section:
    title: str
    prefixes: tuple[str, ...]


@dataclass
class _Node:
    spec: ConfigKeySpec | None = None
    children: dict[str, _Node] = field(default_factory=dict)


_SECTIONS: tuple[_Section, ...] = (
    _Section("Project", ("project_name", "project_id", "project_prefix", "enforce_project_scope")),
    _Section(
        "Execution",
        (
            "use_docker",
            "docker_startup_timeout",
            "docker_image",
            "docker_volumes",
            "docker_setup_command",
            "timeout_minutes",
            "max_steps",
            "max_turns",
            "work_count",
            "verify_command",
            "unit_verify_command",
            "inner_verify_command",
            "code_task_diff_timeout_medium_threshold",
            "code_task_diff_timeout_large_threshold",
            "code_task_diff_timeout_medium_minutes",
            "code_task_diff_timeout_large_minutes",
            "code_task_diff_timeout_cap_minutes",
            "interactive_worktree_dir",
        ),
    ),
    _Section(
        "Branching",
        (
            "branch_mode",
            "branch_strategy",
            "worktree_dir",
            "merge_squash_threshold",
            "main_checkout_isolate",
        ),
    ),
    _Section("Storage", ("db_path", "log_dir", "tasks_file", "cleanup_days", "quiet_period_seconds")),
    _Section(
        "Provider",
        (
            "provider",
            "task_providers",
            "model",
            "reasoning_effort",
            "defaults",
            "task_types",
            "providers",
            "claude",
            "claude_args",
            "chat_text_display_length",
            "tmux",
        ),
    ),
    _Section(
        "Lifecycle",
        (
            "advance_create_reviews",
            "advance_create_plan_reviews",
            "advance_mode",
            "require_review_before_merge",
            "require_plan_review_before_implement",
            "pr_integration",
            "max_resume_attempts",
            "max_review_cycles",
            "max_plan_review_cycles",
            "max_failed_plan_review_retries",
            "max_noop_improve_cycles",
            "max_plan_slices",
            "plan_slice_target_timeout_minutes",
            "max_failed_closing_review_retries",
            "iterate_max_iterations",
            "watch",
            "behavior_monitor",
        ),
    ),
    _Section(
        "Review",
        (
            "review_diff_small_threshold",
            "review_diff_medium_threshold",
            "review_context_file_limit",
            "autonomous_verify_timeout_seconds",
            "review_verify_timeout_grace_seconds",
            "main_integration_verify_red_ttl_minutes",
            "advance_off_topic_verify_unblock",
            "recommend_rebase_behind_commits",
        ),
    ),
    _Section("Learnings", ("learnings_window", "learnings_interval", "learnings_max_items")),
    _Section("Output", ("theme", "no_color", "colors")),
)


def default_example_path(*, local: bool) -> Path:
    """Return the committed example path for the requested flavor."""
    return LOCAL_EXAMPLE_PATH if local else FULL_EXAMPLE_PATH


def render_config_example(
    *,
    local: bool = False,
    options: ConfigExampleRenderOptions | None = None,
    registry: tuple[ConfigKeySpec, ...] = CONFIG_KEY_REGISTRY,
) -> str:
    """Render the requested config example from the registry."""
    render_options = options or ConfigExampleRenderOptions(flavor="local" if local else "full")
    specs = _select_specs(local=local, registry=registry)
    sections = _group_specs_by_section(specs)
    lines = _render_header(local=local)
    for title, section_specs in sections:
        if not section_specs:
            continue
        lines.append("")
        lines.append(f"# --- {title} ---")
        lines.append("")
        tree = _build_tree(section_specs)
        for root_key in _ordered_root_keys(tree, section_specs):
            lines.extend(_render_node(root_key, tree[root_key], root_key, 0, render_options))
            lines.append("")
        while lines and lines[-1] == "":
            lines.pop()
    return "\n".join(lines) + "\n"


def write_config_example(path: Path, *, local: bool = False) -> None:
    """Write a generated example file to disk."""
    path.write_text(render_config_example(local=local), encoding="utf-8")


def committed_example_matches(*, local: bool = False) -> bool:
    """Return whether the committed example file matches generated output."""
    path = default_example_path(local=local)
    expected = render_config_example(local=local)
    if not path.exists():
        return False
    return path.read_text(encoding="utf-8") == expected


def _select_specs(*, local: bool, registry: tuple[ConfigKeySpec, ...]) -> list[ConfigKeySpec]:
    if not local:
        return list(registry)
    return [spec for spec in registry if _spec_allowed_in_local_override(spec)]


def _spec_allowed_in_local_override(spec: ConfigKeySpec) -> bool:
    schema: object = LOCAL_OVERRIDE_ALLOWED_SCHEMA
    for part in spec.key.split("."):
        if not isinstance(schema, dict):
            return False
        schema_dict = cast(dict[str, object], schema)
        if part in schema_dict:
            schema = schema_dict[part]
        elif "*" in schema_dict:
            schema = schema_dict["*"]
        else:
            return False
    return True


def _group_specs_by_section(specs: list[ConfigKeySpec]) -> list[tuple[str, list[ConfigKeySpec]]]:
    grouped: list[tuple[str, list[ConfigKeySpec]]] = []
    remaining = list(specs)
    for section in _SECTIONS:
        matched = [spec for spec in remaining if _matches_prefixes(spec.key, section.prefixes)]
        if matched:
            grouped.append((section.title, sorted(matched, key=lambda spec: spec.key)))
        remaining = [spec for spec in remaining if spec not in matched]
    if remaining:
        grouped.append(("Other", sorted(remaining, key=lambda spec: spec.key)))
    return grouped


def _matches_prefixes(key: str, prefixes: tuple[str, ...]) -> bool:
    return any(key == prefix or key.startswith(f"{prefix}.") for prefix in prefixes)


def _build_tree(specs: list[ConfigKeySpec]) -> dict[str, _Node]:
    roots: dict[str, _Node] = {}
    for spec in specs:
        parts = spec.key.split(".")
        node = roots.setdefault(parts[0], _Node())
        for part in parts[1:]:
            node = node.children.setdefault(part, _Node())
        node.spec = spec
    return roots


def _ordered_root_keys(tree: dict[str, _Node], specs: list[ConfigKeySpec]) -> list[str]:
    ordered: list[str] = []
    for spec in specs:
        root = spec.key.split(".", 1)[0]
        if root not in ordered:
            ordered.append(root)
    return [root for root in ordered if root in tree]


def _render_header(*, local: bool) -> list[str]:
    if local:
        return [
            "# Generated from CONFIG_KEY_REGISTRY via `uv run gza config example --local --write`.",
            "# Local machine-only overrides for gza.yaml.",
            "# This file should stay uncommitted (add gza.local.yaml to .gitignore).",
            "# Only settings allowed by LOCAL_OVERRIDE_ALLOWED_SCHEMA appear here.",
            "# Uncomment and modify any setting you want to override locally.",
        ]
    return [
        "# Generated from CONFIG_KEY_REGISTRY via `uv run gza config example --write`.",
        "# Uncomment and modify any setting you want to change.",
        "# `gza init` renders this same template engine with project-specific values.",
        "# Optional local overrides live in gza.local.yaml (gitignored).",
    ]


def _render_node(
    key: str,
    node: _Node,
    full_path: str,
    indent: int,
    options: ConfigExampleRenderOptions,
) -> list[str]:
    if full_path == "project_name":
        return _render_simple_setting(node.spec, key, indent, enabled=options.project_name_enabled, value=options.project_name)
    if full_path == "project_id":
        extra = [
            "gza init writes this automatically for new projects.",
            "In shared DB mode, persist an explicit project_id to keep task rows scoped correctly.",
        ]
        return _render_simple_setting(
            node.spec,
            key,
            indent,
            enabled=options.project_id_enabled,
            value=options.project_id,
            extra_comments=extra,
        )
    if full_path == "db_path":
        extra = [
            "Leave commented to use the default project-local DB path or an inherited user-level shared default.",
            "gza init activates this line when you explicitly choose local mode or a concrete shared DB path.",
        ]
        return _render_simple_setting(
            node.spec,
            key,
            indent,
            enabled=options.db_path is not None,
            value=options.db_path if options.db_path is not None else node.spec.default if node.spec else None,
            extra_comments=extra,
        )
    if full_path == "branch_strategy":
        return _render_branch_strategy(node, indent, options.branch_strategy)
    return _render_generic_node(key, node, full_path, indent)


def _render_generic_node(key: str, node: _Node, full_path: str, indent: int) -> list[str]:
    if node.spec and not node.children:
        return _render_simple_setting(node.spec, key, indent, enabled=False, value=_example_value(node.spec))

    lines: list[str] = []
    if node.spec:
        lines.extend(_render_simple_setting(node.spec, key, indent, enabled=False, value=_example_value(node.spec)))
        lines.append("")

    lines.extend(_render_container(key, indent, enabled=False))
    child_keys = sorted(node.children, key=_segment_sort_key)
    for child_key in child_keys:
        child = node.children[child_key]
        child_path = f"{full_path}.{child_key}"
        lines.extend(_render_node(_display_segment(child_key, child_path), child, child_path, indent + 2, ConfigExampleRenderOptions()))
    return lines


def _render_branch_strategy(node: _Node, indent: int, branch_strategy: BranchStrategyRender) -> list[str]:
    lines: list[str] = []
    if branch_strategy.mode == "comment_default":
        lines.extend(_render_simple_setting(node.spec, "branch_strategy", indent, enabled=False, value=node.spec.default if node.spec else None))
        lines.append("")
        lines.extend(_render_container("branch_strategy", indent, enabled=False))
        for child_key in sorted(node.children, key=_segment_sort_key):
            child = node.children[child_key]
            lines.extend(
                _render_simple_setting(
                    child.spec,
                    child_key,
                    indent + 2,
                    enabled=False,
                    value=_example_value(child.spec),
                )
            )
        return lines

    if branch_strategy.mode == "preset":
        return _render_simple_setting(node.spec, "branch_strategy", indent, enabled=True, value=branch_strategy.preset)

    if branch_strategy.mode == "custom":
        lines.extend(_render_spec_comments(node.spec, indent=indent))
        lines.extend(_render_container("branch_strategy", indent, enabled=True))
        for child_key in sorted(node.children, key=_segment_sort_key):
            child = node.children[child_key]
            value = branch_strategy.default_type if child_key == "default_type" else branch_strategy.pattern
            lines.extend(_render_simple_setting(child.spec, child_key, indent + 2, enabled=True, value=value))
        return lines

    raise ValueError(f"Unknown branch strategy render mode: {branch_strategy.mode}")


def _render_simple_setting(
    spec: ConfigKeySpec | None,
    key: str,
    indent: int,
    *,
    enabled: bool,
    value: object | None,
    extra_comments: list[str] | None = None,
) -> list[str]:
    lines: list[str] = []
    lines.extend(_render_spec_comments(spec, indent=indent, extra_comments=extra_comments))
    value_lines = _format_key_value_lines(key, value, indent)
    prefix = "" if enabled else "# "
    for line in value_lines:
        lines.append(f"{prefix}{line}")
    return lines


def _example_value(spec: ConfigKeySpec | None) -> object | None:
    """Return the sample value to render in generated example YAML."""
    if spec is None:
        return None
    if spec.example_value is not None:
        return spec.example_value
    return spec.default


def _render_container(key: str, indent: int, *, enabled: bool) -> list[str]:
    prefix = "" if enabled else "# "
    return [f"{prefix}{' ' * indent}{key}:"]


def _render_spec_comments(
    spec: ConfigKeySpec | None,
    *,
    indent: int = 0,
    extra_comments: list[str] | None = None,
) -> list[str]:
    lines: list[str] = []
    if spec is not None:
        lines.extend(_wrap_comment(spec.description, indent=indent))
        if spec.required:
            default_text = "(required)"
        elif spec.example_value is not None and isinstance(spec.default, str):
            default_text = spec.default
        else:
            default_text = _format_inline_scalar(spec.default)
        lines.append(f"# {' ' * indent}Default: {default_text}")
    if extra_comments:
        for extra in extra_comments:
            lines.extend(_wrap_comment(extra, indent=indent))
    return lines


def _wrap_comment(text: str, *, indent: int = 0) -> list[str]:
    wrapped = textwrap.wrap(text, width=96) or [text]
    return [f"# {' ' * indent}{line}" for line in wrapped]


def _format_key_value_lines(key: str, value: object | None, indent: int) -> list[str]:
    prefix = f"{' ' * indent}{key}:"
    if isinstance(value, list):
        if not value:
            return [f"{prefix} []"]
        lines = [prefix]
        for item in value:
            lines.append(f"{' ' * (indent + 2)}- {_format_inline_scalar(item)}")
        return lines
    return [f"{prefix} {_format_inline_scalar(value)}"]


def _format_inline_scalar(value: object | None) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if value is None:
        return "null"
    if isinstance(value, (int, float)):
        return json.dumps(value)
    if isinstance(value, str):
        if value == "":
            return '""'
        if _is_plain_string(value):
            return value
        return json.dumps(value)
    if isinstance(value, list):
        return json.dumps(value)
    return json.dumps(value)


def _is_plain_string(value: str) -> bool:
    if value[0] in "{[":
        return False
    allowed = set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._/-{}~")
    return bool(value) and all(ch in allowed for ch in value)


def _segment_sort_key(segment: str) -> tuple[int, str]:
    return (1 if segment == "*" else 0, segment)


def _display_segment(segment: str, full_path: str) -> str:
    if segment != "*":
        return segment
    if full_path.startswith("providers.*.task_types."):
        return "<task_type>"
    if full_path.startswith("providers."):
        return "<provider>"
    if full_path.startswith("task_types."):
        return "<task_type>"
    if full_path.startswith("task_providers."):
        return "<task_type>"
    if full_path.startswith("colors."):
        return "<field>"
    return "<item>"
