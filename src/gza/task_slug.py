"""Helpers for extracting and normalizing task slugs from task IDs."""

from __future__ import annotations

import re

_TASK_ID_WITH_DATE_PREFIX_RE = re.compile(r"^\d{8}-(.+)$")
_TRAILING_REVISION_SUFFIX_RE = re.compile(r"-\d+$")


def get_task_slug(task_id: str | None) -> str | None:
    """Extract slug from task_id, preserving any trailing revision suffix.

    Example: ``20260305-my-feature-2`` -> ``my-feature-2``.
    """
    if not task_id:
        return None
    match = _TASK_ID_WITH_DATE_PREFIX_RE.match(task_id)
    return match.group(1) if match else task_id


def get_base_task_slug(task_id: str | None) -> str | None:
    """Extract slug from task_id and strip trailing numeric revision suffixes.

    Example: ``20260305-my-feature-2`` -> ``my-feature``.
    """
    slug = get_task_slug(task_id)
    if slug is None:
        return None
    return _TRAILING_REVISION_SUFFIX_RE.sub("", slug)
