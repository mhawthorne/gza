from pathlib import Path

import pytest

from gza.db import SqliteTaskStore
from gza.review_scope import (
    build_spec_coherence_review_scope,
    build_resolution_review_scope,
    declares_spec_coherence_review_mode,
    declares_resolution_review_mode,
    extract_review_scope_from_prompt,
    get_latest_review_scope_comment_for_impl,
    parse_spec_coherence_review_scope,
    parse_resolution_review_scope,
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


def test_resolve_review_scope_derives_plan_backed_scope_for_unsliced_prompt(tmp_path: Path) -> None:
    store = SqliteTaskStore(tmp_path / "test.db")
    plan = store.add("Plan bridge slices", task_type="plan")
    impl = store.add(
        "Implement the bridge slices for the serial rerun path.",
        task_type="implement",
        based_on=plan.id,
    )

    resolved = resolve_review_scope_for_impl(store, impl)

    assert plan.id is not None
    assert resolved is not None
    assert resolved.summary.startswith(f"Plan-backed implementation scope from {plan.id}.")
    assert "Implementation request: Implement the bridge slices for the serial rerun path." in resolved.summary
    assert "Treat the linked plan as background context" in resolved.summary
    assert resolved.source == f"plan_fallback:{plan.id}"


def test_resolution_review_scope_round_trips() -> None:
    scope = build_resolution_review_scope(
        implementation_task_id="gza-10",
        rebase_task_id="gza-11",
        resolved_head_sha="head123",
        resolved_target_sha="target456",
        pre_rebase_head_sha="old123",
        pre_rebase_target_sha="target-start",
        pre_rebase_merge_base_sha="base789",
    )

    parsed = parse_resolution_review_scope(scope)

    assert parsed is not None
    assert parsed.implementation_task_id == "gza-10"
    assert parsed.rebase_task_id == "gza-11"
    assert parsed.pre_rebase_head_sha == "old123"
    assert parsed.pre_rebase_target_sha == "target-start"
    assert parsed.pre_rebase_merge_base_sha == "base789"
    assert parsed.resolved_head_sha == "head123"
    assert parsed.resolved_target_sha == "target456"


def test_declares_resolution_review_mode_detects_header_without_parsing() -> None:
    assert declares_resolution_review_mode(
        "Review mode: resolution\nImplementation task: gza-10\n"
    )
    assert not declares_resolution_review_mode("Review only the parser slice.")


def test_spec_coherence_review_scope_round_trips() -> None:
    scope = build_spec_coherence_review_scope(
        implementation_task_id="gza-7392",
        reviewed_head_sha="head123",
        changed_paths=(
            "specs/behavior/lifecycle-engine.md",
            "specs/behavior/watch.md",
        ),
    )

    parsed = parse_spec_coherence_review_scope(scope)

    assert parsed is not None
    assert parsed.implementation_task_id == "gza-7392"
    assert parsed.reviewed_head_sha == "head123"
    assert parsed.changed_paths == (
        "specs/behavior/lifecycle-engine.md",
        "specs/behavior/watch.md",
    )


def test_declares_spec_coherence_review_mode_detects_header_without_parsing() -> None:
    assert declares_spec_coherence_review_mode(
        "Review mode: spec-coherence\nImplementation task: gza-7392\n"
    )
    assert not declares_spec_coherence_review_mode("spec-coherence")


def test_spec_coherence_review_scope_parser_rejects_missing_required_fields() -> None:
    with pytest.raises(ValueError, match="missing required fields"):
        parse_spec_coherence_review_scope(
            "Review mode: spec-coherence\nImplementation task: gza-7392\n"
        )


def test_spec_coherence_review_scope_parser_rejects_malformed_paths_json() -> None:
    with pytest.raises(ValueError, match="paths JSON is malformed"):
        parse_spec_coherence_review_scope(
            "\n".join(
                (
                    "Review mode: spec-coherence",
                    "Implementation task: gza-7392",
                    "Reviewed head SHA: head123",
                    "Changed behavior-spec paths JSON: [oops]",
                )
            )
        )


def test_resolution_review_scope_parser_rejects_missing_required_fields() -> None:
    with pytest.raises(ValueError, match="missing required fields"):
        parse_resolution_review_scope(
            "Review mode: resolution\nImplementation task: gza-10\nRebase task: gza-11\n"
        )


def test_resolution_review_scope_parser_rejects_duplicate_fields() -> None:
    with pytest.raises(ValueError, match="duplicate resolution review metadata field"):
        parse_resolution_review_scope(
            "\n".join(
                (
                    "Review mode: resolution",
                    "Implementation task: gza-10",
                    "Implementation task: gza-10",
                    "Rebase task: gza-11",
                    "Resolved head SHA: head123",
                    "Resolved target SHA: target456",
                )
            )
        )
