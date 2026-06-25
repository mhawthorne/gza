from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import patch

from gza import recovery_engine
from gza.dispatch_preview import build_dispatch_preview
from gza.pickup import get_runnable_pending_tasks

from tests.cli.conftest import make_store, setup_config


def test_build_dispatch_preview_orders_recovery_then_pending_and_preserves_pending_pickup_order(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    store._default_merge_target_cache = "main"  # noqa: SLF001 - avoid real git in unit test
    store._project_root = None  # noqa: SLF001 - avoid real git fallback in unit test

    failed_retry = store.add("Older failed plan", task_type="plan")
    assert failed_retry.id is not None
    failed_retry.status = "failed"
    failed_retry.failure_reason = "INFRASTRUCTURE_ERROR"
    failed_retry.completed_at = datetime(2026, 6, 24, 10, 0, 0, tzinfo=UTC)
    store.update(failed_retry)

    failed_manual = store.add("Manual failed plan", task_type="plan")
    assert failed_manual.id is not None
    failed_manual.status = "failed"
    failed_manual.failure_reason = "TEST_FAILURE"
    failed_manual.completed_at = datetime(2026, 6, 24, 10, 5, 0, tzinfo=UTC)
    store.update(failed_manual)

    urgent = store.add("Urgent pending", urgent=True)
    ordered_two = store.add("Ordered two")
    ordered_one = store.add("Ordered one")
    normal = store.add("Normal pending")
    assert urgent.id is not None
    assert ordered_two.id is not None
    assert ordered_one.id is not None
    assert normal.id is not None

    store.set_queue_position(ordered_two.id, 2)
    store.set_queue_position(ordered_one.id, 1)

    with patch(
        "gza.recovery_engine._load_merge_context",
        return_value=recovery_engine._MergeContext(git=None, default_branch=None),
    ):
        preview = build_dispatch_preview(
            store,
            tags=None,
            any_tag=False,
            max_recovery_attempts=1,
        )

    recovery_entries = preview.recovery_entries
    pending_entries = preview.pending_entries

    assert [entry.task.id for entry in recovery_entries] == [failed_retry.id, failed_manual.id]
    assert [entry.task.id for entry in pending_entries] == [
        task.id for task in get_runnable_pending_tasks(store)
    ]
    assert [entry.lane for entry in preview.entries] == [
        "recovery",
        "recovery",
        "pending",
        "pending",
        "pending",
        "pending",
    ]
    assert recovery_entries[0].runnable is True
    assert recovery_entries[0].action == "retry"
    assert recovery_entries[1].runnable is False
    assert recovery_entries[1].manual_only is True
    assert recovery_entries[1].reason_code == "manual_failure_reason"


def test_build_dispatch_preview_keeps_manual_only_recovery_visible_but_non_runnable(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    store._default_merge_target_cache = "main"  # noqa: SLF001 - avoid real git in unit test
    store._project_root = None  # noqa: SLF001 - avoid real git fallback in unit test

    manual = store.add("Manual failed plan", task_type="plan")
    assert manual.id is not None
    manual.status = "failed"
    manual.failure_reason = "TEST_FAILURE"
    manual.completed_at = datetime(2026, 6, 24, 11, 0, 0, tzinfo=UTC)
    store.update(manual)

    with patch(
        "gza.recovery_engine._load_merge_context",
        return_value=recovery_engine._MergeContext(git=None, default_branch=None),
    ):
        preview = build_dispatch_preview(
            store,
            tags=None,
            any_tag=False,
            max_recovery_attempts=1,
            include_pending=False,
            selection_mode="recovery_only",
        )

    assert [entry.task.id for entry in preview.recovery_entries] == [manual.id]
    assert [entry.task.id for entry in preview.runnable_entries] == []
    assert [entry.task.id for entry in preview.needs_human_entries] == [manual.id]
    entry = preview.recovery_entries[0]
    assert entry.runnable is False
    assert entry.manual_only is True
    assert entry.action == "skip"
    assert entry.reason_code == "manual_failure_reason"


def test_build_dispatch_preview_recovery_first_explicit_filters_pending_to_explicit_positions(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    store._default_merge_target_cache = "main"  # noqa: SLF001 - avoid real git in unit test
    store._project_root = None  # noqa: SLF001 - avoid real git fallback in unit test

    failed_retry = store.add("Failed plan", task_type="plan")
    assert failed_retry.id is not None
    failed_retry.status = "failed"
    failed_retry.failure_reason = "INFRASTRUCTURE_ERROR"
    failed_retry.completed_at = datetime(2026, 6, 24, 12, 0, 0, tzinfo=UTC)
    store.update(failed_retry)

    urgent = store.add("Urgent fallback", urgent=True)
    ordered_two = store.add("Ordered two")
    ordered_one = store.add("Ordered one")
    normal = store.add("Normal fallback")
    assert urgent.id is not None
    assert ordered_two.id is not None
    assert ordered_one.id is not None
    assert normal.id is not None

    store.set_queue_position(ordered_two.id, 2)
    store.set_queue_position(ordered_one.id, 1)

    with patch(
        "gza.recovery_engine._load_merge_context",
        return_value=recovery_engine._MergeContext(git=None, default_branch=None),
    ):
        preview = build_dispatch_preview(
            store,
            tags=None,
            any_tag=False,
            max_recovery_attempts=1,
            selection_mode="recovery_first_explicit",
        )

    assert [entry.task.id for entry in preview.recovery_entries] == [failed_retry.id]
    assert [entry.task.id for entry in preview.pending_entries] == [ordered_one.id, ordered_two.id]
    assert all(entry.queue_position is not None for entry in preview.pending_entries)
    assert [entry.task.id for entry in preview.entries] == [
        failed_retry.id,
        ordered_one.id,
        ordered_two.id,
    ]
