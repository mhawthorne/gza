"""Helpers for extracting and normalizing slugs from task slug strings."""

from __future__ import annotations

import re

_TASK_ID_WITH_DATE_PREFIX_RE = re.compile(r"^\d{8}-(.+)$")
_TRAILING_REVISION_SUFFIX_RE = re.compile(r"-\d+$")
_DERIVED_IMPLEMENT_PREFIX_RE = re.compile(r"^([a-z0-9]+)-impl-(.+)$")


def extract_task_id_suffix(task_id: object | None) -> str:
    """Extract the suffix from a task id in ``prefix-suffix`` format.

    Task IDs look like ``gza-1234`` or ``gza-7`` (variable-width). This
    returns the portion after the first ``-``, or the full stripped id if
    there is no separator.
    """
    if task_id is None:
        return ""
    task_id_str = str(task_id).strip()
    if not task_id_str:
        return ""
    prefix, sep, suffix = task_id_str.partition("-")
    if sep and prefix and suffix:
        return suffix
    return task_id_str


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


def get_slug_display_text(slug: str | None, project_prefix: str | None = None) -> str | None:
    """Return a human-friendly slug body for operator-facing output.

    Supports both generated slug shapes:
    - ``YYYYMMDD-{project_prefix}-{slug}``
    - ``YYYYMMDD-{semantic-slug}``

    For the prefix-bearing shape, strips ``{project_prefix}-`` so displays show
    only the semantic body.
    """
    extracted = get_task_slug(slug)
    if extracted is None:
        return None

    if not project_prefix:
        return extracted

    if extracted.startswith(f"{project_prefix}-"):
        return extracted[len(project_prefix) + 1:]
    if extracted == project_prefix:
        return ""
    return extracted


def strip_derived_implement_prefixes(
    slug: str | None,
    known_task_id_suffixes: set[str] | None = None,
) -> str | None:
    """Remove one or more leading ``<task_id_suffix>-impl-`` segments from a slug.

    Only strips prefixes that exactly match ``known_task_id_suffixes`` (typically
    sourced from an implementation task's lineage). Semantic ``*-impl-*`` tokens
    like ``api-impl-migration`` or ``2fa-impl-login`` are preserved because they
    will not appear in the lineage suffix set. Variable-width task IDs are
    supported because matching is exact and does not depend on padding.
    """
    if slug is None:
        return None
    known_suffixes = known_task_id_suffixes or set()
    normalized = slug
    while True:
        match = _DERIVED_IMPLEMENT_PREFIX_RE.match(normalized)
        if not match:
            return normalized
        prefix, remainder = match.groups()
        if prefix not in known_suffixes:
            return normalized
        normalized = remainder
