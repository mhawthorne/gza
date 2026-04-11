"""Helpers for extracting and normalizing slugs from task slug strings."""

from __future__ import annotations

import re

_TASK_ID_WITH_DATE_PREFIX_RE = re.compile(r"^\d{8}-(.+)$")
_TRAILING_REVISION_SUFFIX_RE = re.compile(r"-\d+$")
_DERIVED_IMPLEMENT_PREFIX_RE = re.compile(r"^[a-z0-9]+-impl-")


def get_task_slug(slug: str | None) -> str | None:
    """Extract the slug portion from a full task slug, preserving any trailing revision suffix.

    The full task slug is in ``YYYYMMDD-<slug>`` format.  This function strips
    the date prefix and returns the remaining slug string.

    Example: ``20260305-my-feature-2`` -> ``my-feature-2``.
    """
    if not slug:
        return None
    match = _TASK_ID_WITH_DATE_PREFIX_RE.match(slug)
    return match.group(1) if match else slug


def get_base_task_slug(slug: str | None) -> str | None:
    """Extract the slug portion from a full task slug and strip trailing numeric revision suffixes.

    The full task slug is in ``YYYYMMDD-<slug>`` format.  This function strips
    the date prefix and any trailing ``-N`` retry suffix.

    Example: ``20260305-my-feature-2`` -> ``my-feature``.
    """
    extracted = get_task_slug(slug)
    if extracted is None:
        return None
    return _TRAILING_REVISION_SUFFIX_RE.sub("", extracted)


def strip_derived_implement_prefixes(slug: str | None) -> str | None:
    """Remove one or more leading ``<task_id_suffix>-impl-`` segments from a slug."""
    if slug is None:
        return None
    normalized = slug
    while True:
        stripped = _DERIVED_IMPLEMENT_PREFIX_RE.sub("", normalized, count=1)
        if stripped == normalized:
            return normalized
        normalized = stripped
