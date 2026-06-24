from pathlib import Path

from gza.db import SqliteTaskStore
from gza.review_scope import (
    extract_review_scope_from_prompt,
    get_latest_review_scope_comment_for_impl,
    resolve_review_scope_for_impl,
)


def test_extract_review_scope_from_legacy_sliced_prompt() -> None:
    prompt = """Implement plan gza-4065, slice F-A1 + F-A2: introduce a first-class `empty` merge-unit state.

## Scope
1. Add the shared classifier.
2. Persist and present `empty`.

## Acceptance
- Add tests.

## Out of scope
- F-A3
- F-B1
"""

    result = extract_review_scope_from_prompt(prompt)

    assert result is not None
    assert "Slice F-A1 + F-A2" in result
    assert "Add the shared classifier." in result
    assert "Add tests." not in result


def test_resolve_review_scope_returns_structured_field_over_prompt(tmp_path: Path) -> None:
    store = SqliteTaskStore(tmp_path / "test.db")
    impl = store.add(
        "Implement plan gza-1, slice F-A1: old prompt scope",
        task_type="implement",
        review_scope="slice F-A1: authoritative scope from metadata",
    )

    resolved = resolve_review_scope_for_impl(store, impl)

    assert resolved is not None
    assert resolved.summary == "slice F-A1: authoritative scope from metadata"
    assert resolved.source == "task_field"


def test_resolve_review_scope_returns_none_for_unsliced_prompt(tmp_path: Path) -> None:
    store = SqliteTaskStore(tmp_path / "test.db")
    impl = store.add("Implement the full plan end to end", task_type="implement")

    resolved = resolve_review_scope_for_impl(store, impl)

    assert resolved is None


def test_get_latest_review_scope_comment_for_impl_ignores_pending_tasks(tmp_path: Path) -> None:
    store = SqliteTaskStore(tmp_path / "test.db")
    impl = store.add("Implement the full plan end to end", task_type="implement")
    assert impl.id is not None
    store.add_comment(impl.id, "Review only the parser slice.", kind="review_scope")

    assert get_latest_review_scope_comment_for_impl(store, impl) is None


def test_resolve_review_scope_uses_latest_scope_comment_after_task_field(tmp_path: Path) -> None:
    store = SqliteTaskStore(tmp_path / "test.db")
    impl = store.add("Implement the full plan end to end", task_type="implement")
    impl.status = "completed"
    store.update(impl)
    assert impl.id is not None

    first = store.add_comment(impl.id, "Review only the parser slice.", kind="review_scope")
    second = store.add_comment(impl.id, "Review only the executor slice.", kind="review_scope")

    latest = get_latest_review_scope_comment_for_impl(store, impl)
    resolved = resolve_review_scope_for_impl(store, impl)

    assert latest == second
    assert latest != first
    assert resolved is not None
    assert resolved.summary == "Review only the executor slice."
    assert resolved.source == f"comment:{second.id}"
