"""Helpers for split task conversation and ops log paths."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from .config import Config
from .db import Task

OPS_SUFFIX = ".ops.jsonl"
STARTUP_CONVERSATION_SUFFIX = ".startup.log"
STARTUP_OPS_SUFFIX = ".startup.ops.jsonl"
CONVERSATION_SUFFIX = ".log"


@dataclass(frozen=True)
class TaskLogPaths:
    """Resolved conversation and ops log paths for one task."""

    conversation: Path
    ops: Path
    startup_conversation: Path
    startup_ops: Path
    layout: str


def conversation_log_path_for(base_or_task_log: Path) -> Path:
    """Return the canonical conversation log path for a log-like path."""
    suffix = base_or_task_log.name
    if suffix.endswith(STARTUP_OPS_SUFFIX):
        return base_or_task_log.with_name(suffix[: -len(STARTUP_OPS_SUFFIX)] + STARTUP_CONVERSATION_SUFFIX)
    if suffix.endswith(OPS_SUFFIX):
        return base_or_task_log.with_name(suffix[: -len(OPS_SUFFIX)] + CONVERSATION_SUFFIX)
    return base_or_task_log


def ops_log_path_for(conversation_log: Path) -> Path:
    """Return the sibling ops log path for a conversation log path."""
    conversation_log = conversation_log_path_for(conversation_log)
    suffix = conversation_log.name
    if suffix.endswith(STARTUP_CONVERSATION_SUFFIX):
        return conversation_log.with_name(suffix[: -len(STARTUP_CONVERSATION_SUFFIX)] + STARTUP_OPS_SUFFIX)
    if suffix.endswith(CONVERSATION_SUFFIX):
        return conversation_log.with_name(suffix[: -len(CONVERSATION_SUFFIX)] + OPS_SUFFIX)
    return conversation_log.with_name(f"{conversation_log.name}{OPS_SUFFIX}")


def split_layout_for(conversation_log: Path) -> str:
    """Classify the on-disk task log layout."""
    conversation_log = conversation_log_path_for(conversation_log)
    ops_log = ops_log_path_for(conversation_log)
    if ops_log.exists():
        return "split"
    return "legacy"


def _resolve_task_conversation_path(config: Config, task: Task) -> Path:
    if task.log_file:
        stored_path = Path(task.log_file)
        if not stored_path.is_absolute():
            return config.project_dir / stored_path
        return stored_path
    if task.slug:
        return config.log_path / f"{task.slug}.log"
    suffix = task.id or "unknown-task"
    return config.log_path / f"{suffix}.startup.log"


def resolve_conversation_log_path(config: Config, task: Task) -> Path:
    """Resolve the canonical conversation log path for a task."""
    return conversation_log_path_for(_resolve_task_conversation_path(config, task))


def resolve_ops_log_path(config: Config, task_or_conversation_path: Task | Path) -> Path:
    """Resolve the canonical ops log path for a task or conversation log path."""
    if isinstance(task_or_conversation_path, Task):
        return ops_log_path_for(resolve_conversation_log_path(config, task_or_conversation_path))
    return ops_log_path_for(task_or_conversation_path)


def resolve_task_log_paths(config: Config, task: Task) -> TaskLogPaths:
    """Resolve the task's conversation, ops, and startup log siblings."""
    conversation = resolve_conversation_log_path(config, task)
    ops = resolve_ops_log_path(config, conversation)

    if task.id is None:
        startup_conversation = conversation
        startup_ops = ops
    else:
        startup_conversation = config.log_path / f"{task.id}.startup.log"
        startup_ops = config.log_path / f"{task.id}.startup.ops.jsonl"

    return TaskLogPaths(
        conversation=conversation,
        ops=ops,
        startup_conversation=startup_conversation,
        startup_ops=startup_ops,
        layout=split_layout_for(conversation),
    )


def paired_log_paths(path: Path) -> tuple[Path, Path]:
    """Return `(conversation, ops)` siblings for any task log-like path."""
    conversation = conversation_log_path_for(path)
    return conversation, ops_log_path_for(conversation)


def slug_from_log_path(path: Path) -> str:
    """Return the base slug/task stem for `.log` or `.ops.jsonl` log files."""
    name = conversation_log_path_for(path).name
    if name.endswith(STARTUP_CONVERSATION_SUFFIX):
        return name[: -len(STARTUP_CONVERSATION_SUFFIX)]
    if name.endswith(CONVERSATION_SUFFIX):
        return name[: -len(CONVERSATION_SUFFIX)]
    return Path(name).stem
