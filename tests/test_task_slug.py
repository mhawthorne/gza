"""Tests for shared task slug helpers."""

from gza.task_slug import get_base_task_slug, get_task_slug


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
