"""Shared cross-project task classification."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from gza.config import Config
    from gza.db import Task

CROSS_PROJECT_TAG = "cross-project"


def task_is_cross_project(task: Task, config: Config | None = None) -> bool:
    """Return whether a task should use cross-project scope semantics."""
    return CROSS_PROJECT_TAG in task.tags or (
        config is not None and getattr(config, "default_cross_project", False) is True
    )
