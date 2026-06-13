"""Shared derived-task tag inheritance helpers."""

from collections.abc import Iterable

from .db import Task


def resolve_derived_task_tags(
    parent_task: Task,
    explicit_tags: Iterable[str] | None = None,
) -> tuple[str, ...]:
    """Resolve tags for a newly created derived task.

    ``explicit_tags is None`` means "inherit the parent's current tags".
    Any non-``None`` explicit tag iterable wins exactly, including an empty tuple.
    Final normalization is handled by the store write path.
    """
    if explicit_tags is None:
        return parent_task.tags
    return tuple(str(tag) for tag in explicit_tags)
