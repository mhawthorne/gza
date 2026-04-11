"""Helpers for extracting and normalizing slugs from task slug strings."""

from __future__ import annotations

import re

_TASK_ID_WITH_DATE_PREFIX_RE = re.compile(r"^\d{8}-(.+)$")
_TRAILING_REVISION_SUFFIX_RE = re.compile(r"-\d+$")
_DERIVED_IMPLEMENT_PREFIX_RE = re.compile(r"^([a-z0-9]+)-impl-(.+)$")


def _looks_like_task_id_suffix(token: str) -> bool:
    """Return True for task-id-like suffix tokens used in derived implement slugs.

    Derived prefixes are generated from task id suffixes. In practice these are
    fixed-width base36 values (often containing digits) and can also be short
    variable-width values in tests (e.g. ``aa``, ``mp``). Restricting removal to
    these shapes avoids stripping semantic slug segments such as
    ``add-impl-support``.
    """
    if not token:
        return False
    if any(ch.isdigit() for ch in token):
        return True
    return len(token) <= 2


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
        match = _DERIVED_IMPLEMENT_PREFIX_RE.match(normalized)
        if not match:
            return normalized
        prefix, remainder = match.groups()
        if not _looks_like_task_id_suffix(prefix):
            return normalized
        normalized = remainder
