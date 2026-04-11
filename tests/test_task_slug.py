"""Tests for shared task slug helpers."""

from gza.task_slug import (
    get_base_task_slug,
    get_task_slug,
    strip_derived_implement_prefixes,
)


def test_get_task_slug_strips_date_prefix_and_preserves_revision() -> None:
    """get_task_slug preserves retry suffixes for exact matching."""
    assert get_task_slug("20260305-plan-auth-migration-2") == "plan-auth-migration-2"


def test_get_base_task_slug_strips_revision_suffix() -> None:
    """get_base_task_slug removes trailing numeric retry suffixes."""
    assert get_base_task_slug("20260305-plan-auth-migration-2") == "plan-auth-migration"


def test_slug_helpers_return_none_for_missing_task_id() -> None:
    """Helpers return None when task_id is not present."""
    assert get_task_slug(None) is None
    assert get_base_task_slug(None) is None


def test_strip_derived_implement_prefixes_strips_nested_chain() -> None:
    """Nested derived implement prefixes are removed until semantic slug remains."""
    slug = "bb-impl-aa-impl-add-feature"
    assert strip_derived_implement_prefixes(slug) == "add-feature"


def test_strip_derived_implement_prefixes_preserves_non_derived_slug() -> None:
    """Slugs without a derived implement prefix are returned unchanged."""
    assert strip_derived_implement_prefixes("add-feature") == "add-feature"


def test_strip_derived_implement_prefixes_preserves_semantic_impl_token() -> None:
    """Only task-id-derived prefixes are removed; semantic ``*-impl-*`` remains."""
    slug = "0000ab-impl-add-impl-support"
    assert strip_derived_implement_prefixes(slug) == "add-impl-support"


def test_strip_derived_implement_prefixes_preserves_semantic_impl_token_api() -> None:
    """Semantic subjects like ``api-impl-*`` are not stripped as derived prefixes."""
    slug = "0000ab-impl-api-impl-migration"
    assert strip_derived_implement_prefixes(slug) == "api-impl-migration"
