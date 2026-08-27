from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import patch

import pytest

from gza import recovery_engine
from gza.cli._recovery_lane import collect_recovery_lane_entries
from gza.config import Config
from gza.dispatch_preview import build_dispatch_preview, plan_watch_dispatch_entries
from gza.pickup import get_runnable_pending_tasks
from tests.cli.conftest import make_store, setup_config


def _bulk_insert_completed_history(
    store,
    *,
    count: int,
    start: int,
    based_on: str | None = None,
    task_type: str = "implement",
) -> tuple[str, ...]:
    now = datetime(2026, 4, 4, 8, 0, tzinfo=UTC).isoformat()
    task_ids = tuple(f"gza-{start + idx}" for idx in range(count))
    with store._connect() as conn:  # noqa: SLF001
        conn.executemany(
            """
            INSERT INTO tasks (
                project_id, id, prompt, status, task_type, based_on,
                created_at, updated_at, completed_at
            )
            VALUES (?, ?, ?, 'completed', ?, ?, ?, ?, ?)
            """,
            [
                (
                    store._project_id,  # noqa: SLF001
                    task_id,
                    f"Historical completed {start} {idx}",
                    task_type,
                    based_on,
                    now,
                    now,
                    now,
                )
                for idx, task_id in enumerate(task_ids)
            ],
        )
    return task_ids


def _bulk_insert_completed_implement_siblings_with_distinct_slices(
    store,
    *,
    parent_id: str,
    count: int,
    start: int,
) -> tuple[str, ...]:
    now = datetime(2026, 4, 4, 8, 0, tzinfo=UTC).isoformat()
    task_ids = tuple(f"gza-{start + idx}" for idx in range(count))
    with store._connect() as conn:  # noqa: SLF001
        conn.executemany(
            """
            INSERT INTO tasks (
                project_id, id, prompt, status, task_type, based_on,
                review_scope, branch, has_commits, merge_status,
                created_at, updated_at, completed_at
            )
            VALUES (?, ?, ?, 'completed', 'implement', ?, ?, ?, 1, 'merged', ?, ?, ?)
            """,
            [
                (
                    store._project_id,  # noqa: SLF001
                    task_id,
                    f"Implement unrelated slice S{idx}",
                    parent_id,
                    f"Review unrelated slice {idx}.",
                    f"feature/unrelated-slice-{idx}",
                    now,
                    now,
                    now,
                )
                for idx, task_id in enumerate(task_ids)
            ],
        )
    return task_ids


def _bulk_insert_failed_tombstoned_merge_unit_tasks(
    store,
    *,
    count: int,
    start: int,
) -> tuple[str, ...]:
    now = datetime(2026, 4, 5, 8, 0, tzinfo=UTC).isoformat()
    task_ids = tuple(f"gza-{start + idx}" for idx in range(count))
    unit_ids = tuple(f"mu-tombstone-{start + idx}" for idx in range(count))
    with store._connect() as conn:  # noqa: SLF001
        conn.executemany(
            """
            INSERT INTO tasks (
                project_id, id, prompt, status, task_type, branch,
                failure_reason, created_at, updated_at, completed_at
            )
            VALUES (?, ?, ?, 'failed', 'implement', ?, 'INFRASTRUCTURE_ERROR', ?, ?, ?)
            """,
            [
                (
                    store._project_id,  # noqa: SLF001
                    task_id,
                    f"Tombstoned failed task {idx}",
                    f"feature/tombstoned-{idx}",
                    now,
                    now,
                    now,
                )
                for idx, task_id in enumerate(task_ids)
            ],
        )
        conn.executemany(
            """
            INSERT INTO merge_units (
                project_id, id, source_branch, target_branch, state,
                owner_task_id, created_at, updated_at, superseded_by_unit_id
            )
            VALUES (?, ?, ?, 'main', ?, ?, ?, ?, ?)
            """,
            [
                (
                    store._project_id,  # noqa: SLF001
                    unit_id,
                    f"feature/tombstoned-{idx}",
                    "dropped" if idx % 2 == 0 else "superseded",
                    task_id,
                    now,
                    now,
                    "mu-winner" if idx % 2 else None,
                )
                for idx, (task_id, unit_id) in enumerate(zip(task_ids, unit_ids, strict=True))
            ],
        )
        conn.executemany(
            """
            INSERT INTO merge_unit_tasks(project_id, merge_unit_id, task_id, role, attached_at)
            VALUES (?, ?, ?, 'owner', ?)
            """,
            [
                (store._project_id, unit_id, task_id, now)  # noqa: SLF001
                for task_id, unit_id in zip(task_ids, unit_ids, strict=True)
            ],
        )
    return task_ids


def _plan_review_slice_prompt(*, plan_id: str, review_id: str, slice_id: str, body: str) -> str:
    return "\n".join(
        (
            f"Implement approved plan-review slice {slice_id}: Dispatch preview regression",
            "",
            "Provenance:",
            f"- Plan source: {plan_id}",
            f"- Plan review: {review_id}",
            f"- Slice: {slice_id} (Dispatch preview regression)",
            "",
            "Slice prompt:",
            body,
        )
    )


def _recovery_entry_ids(preview) -> list[str | None]:
    return [entry.task.id for entry in preview.recovery_entries]


def _recovery_entry_keys(preview) -> list[tuple[str | None, str | None, str]]:
    return [(entry.owner_task.id, entry.task.id, entry.action) for entry in preview.recovery_entries]


def _build_recovery_only_preview(store, *, tags: tuple[str, ...] | None = None):
    with patch(
        "gza.recovery_engine._load_merge_context",
        return_value=recovery_engine._MergeContext(git=None, default_branch="main"),
    ):
        return build_dispatch_preview(
            store,
            tags=tags,
            any_tag=False,
            max_recovery_attempts=1,
            selection_mode="recovery_only",
            include_pending=False,
        )


def _build_forced_full_recovery_preview(store, *, tags: tuple[str, ...] | None = None):
    with patch("gza.lineage_query._load_recovery_unit_indexes", return_value=None):
        return _build_recovery_only_preview(store, tags=tags)


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
        return_value=recovery_engine._MergeContext(git=None, default_branch="main"),
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
    assert [entry.task.id for entry in pending_entries] == [task.id for task in get_runnable_pending_tasks(store)]
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
        return_value=recovery_engine._MergeContext(git=None, default_branch="main"),
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


def test_collect_recovery_lane_entries_matches_preview_for_branch_unpushable_reconcile(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    store._default_merge_target_cache = "main"  # noqa: SLF001 - avoid real git in unit test
    store._project_root = None  # noqa: SLF001 - avoid real git fallback in unit test

    failed = store.add("Branch publish failed", task_type="implement")
    assert failed.id is not None
    failed.status = "failed"
    failed.failure_reason = "BRANCH_UNPUSHABLE"
    failed.branch = "feature/reconcile-preview"
    failed.completed_at = datetime(2026, 6, 24, 11, 30, 0, tzinfo=UTC)
    store.update(failed)

    with patch(
        "gza.recovery_engine._load_merge_context",
        return_value=recovery_engine._MergeContext(git=None, default_branch="main"),
    ):
        preview = build_dispatch_preview(
            store,
            tags=None,
            any_tag=False,
            max_recovery_attempts=1,
            selection_mode="recovery_only",
            include_pending=False,
        )
        lane_entries = collect_recovery_lane_entries(
            store,
            tags=None,
            any_tag=False,
            max_recovery_attempts=1,
        )

    assert [entry.task.id for entry in preview.recovery_entries] == [failed.id]
    preview_entry = preview.recovery_entries[0]
    assert preview_entry.runnable is True
    assert preview_entry.decision is not None
    assert preview_entry.decision.action == "reconcile"

    assert [entry.task.id for entry in lane_entries] == [failed.id]
    lane_entry = lane_entries[0]
    assert lane_entry.decision.action == preview_entry.decision.action
    assert lane_entry.action is not None
    assert lane_entry.action["type"] == "reconcile_branch_divergence"
    assert lane_entry.attention_action is None


def test_recovery_preview_uses_actionable_merge_units_without_changing_entries(
    tmp_path: Path,
    monkeypatch,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    store._default_merge_target_cache = "main"  # noqa: SLF001 - avoid real git in unit test
    store._project_root = None  # noqa: SLF001 - avoid real git fallback in unit test

    old_root = store.add("Old merged root", task_type="implement")
    assert old_root.id is not None
    old_root.status = "completed"
    old_root.branch = "feature/old-root"
    old_root.has_commits = True
    old_root.completed_at = datetime(2026, 4, 1, 8, 0, tzinfo=UTC)
    store.update(old_root)
    old_root_unit = store.create_merge_unit(
        source_branch="feature/old-root",
        target_branch="main",
        owner_task_id=old_root.id,
        state="merged",
    )
    store.attach_task_to_merge_unit(old_root.id, old_root_unit.id, "owner")

    failed = store.add("Failed descendant", task_type="implement", based_on=old_root.id)
    assert failed.id is not None
    failed.status = "failed"
    failed.failure_reason = "INFRASTRUCTURE_ERROR"
    failed.branch = "feature/recovery-descendant"
    failed.has_commits = False
    failed.completed_at = datetime(2026, 4, 2, 8, 0, tzinfo=UTC)
    store.update(failed)
    failed_unit = store.create_merge_unit(
        source_branch="feature/recovery-descendant",
        target_branch="main",
        owner_task_id=failed.id,
        state="unmerged",
    )
    store.attach_task_to_merge_unit(failed.id, failed_unit.id, "owner")

    for idx in range(150):
        task = store.add(f"Background completed {idx}", task_type="implement")
        assert task.id is not None
        task.status = "completed"
        task.completed_at = datetime(2026, 4, 3, 8, 0, tzinfo=UTC)
        store.update(task)

    list_for_task_calls = 0
    original_list_for_task = store.list_merge_units_for_task

    def counted_list_for_task(*args, **kwargs):
        nonlocal list_for_task_calls
        list_for_task_calls += 1
        return original_list_for_task(*args, **kwargs)

    hydrated_ids: list[str] = []
    original_row_to_task = store._row_to_task  # noqa: SLF001

    def counted_row_to_task(row, *args, **kwargs):
        hydrated_ids.append(str(row["id"]))
        return original_row_to_task(row, *args, **kwargs)

    monkeypatch.setattr(store, "list_merge_units_for_task", counted_list_for_task)
    monkeypatch.setattr(store, "_row_to_task", counted_row_to_task)  # noqa: SLF001

    with patch(
        "gza.recovery_engine._load_merge_context",
        return_value=recovery_engine._MergeContext(git=None, default_branch="main"),
    ):
        scoped_preview = build_dispatch_preview(
            store,
            tags=None,
            any_tag=False,
            max_recovery_attempts=1,
            selection_mode="recovery_only",
            include_pending=False,
        )

    assert [entry.task.id for entry in scoped_preview.recovery_entries] == [failed.id]
    assert list_for_task_calls <= 2
    assert len(set(hydrated_ids)) < 25
    assert old_root.id in hydrated_ids
    assert failed.id in hydrated_ids

    monkeypatch.setattr("gza.lineage_query._load_recovery_unit_indexes", lambda *_args, **_kwargs: None)
    with patch(
        "gza.recovery_engine._load_merge_context",
        return_value=recovery_engine._MergeContext(git=None, default_branch="main"),
    ):
        full_preview = build_dispatch_preview(
            store,
            tags=None,
            any_tag=False,
            max_recovery_attempts=1,
            selection_mode="recovery_only",
            include_pending=False,
        )

    assert [(entry.owner_task.id, entry.task.id, entry.action) for entry in scoped_preview.recovery_entries] == [
        (entry.owner_task.id, entry.task.id, entry.action) for entry in full_preview.recovery_entries
    ]


def test_recovery_preview_zero_seed_scope_stays_empty_without_get_all(
    tmp_path: Path,
    monkeypatch,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    store._default_merge_target_cache = "main"  # noqa: SLF001 - avoid real git in unit test
    store._project_root = None  # noqa: SLF001 - avoid real git fallback in unit test
    _bulk_insert_completed_history(store, count=3000, start=100000)

    with patch(
        "gza.recovery_engine._load_merge_context",
        return_value=recovery_engine._MergeContext(git=None, default_branch="main"),
    ):
        full_preview = build_dispatch_preview(
            store,
            tags=None,
            any_tag=False,
            max_recovery_attempts=1,
            selection_mode="recovery_only",
            include_pending=False,
        )

    def fail_get_all():
        raise AssertionError("scoped recovery preview must not fall back to get_all()")

    hydrated_ids: list[str] = []
    original_row_to_task = store._row_to_task  # noqa: SLF001

    def counted_row_to_task(row, *args, **kwargs):
        hydrated_ids.append(str(row["id"]))
        return original_row_to_task(row, *args, **kwargs)

    monkeypatch.setattr(store, "get_all", fail_get_all)
    monkeypatch.setattr(store, "_row_to_task", counted_row_to_task)  # noqa: SLF001
    with patch(
        "gza.recovery_engine._load_merge_context",
        return_value=recovery_engine._MergeContext(git=None, default_branch="main"),
    ):
        scoped_preview = build_dispatch_preview(
            store,
            tags=None,
            any_tag=False,
            max_recovery_attempts=1,
            selection_mode="recovery_only",
            include_pending=False,
        )
        tagged_scoped_preview = build_dispatch_preview(
            store,
            tags=("missing",),
            any_tag=False,
            max_recovery_attempts=1,
            selection_mode="recovery_only",
            include_pending=False,
        )

    assert scoped_preview.recovery_entries == full_preview.recovery_entries == ()
    assert tagged_scoped_preview.recovery_entries == ()
    assert len(hydrated_ids) < 25


def test_recovery_preview_bulk_loads_terminal_no_work_merge_units(
    tmp_path: Path,
    monkeypatch,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    store._default_merge_target_cache = "main"  # noqa: SLF001 - avoid real git in unit test
    store._project_root = None  # noqa: SLF001 - avoid real git fallback in unit test

    failed_ids: list[str] = []
    for idx in range(60):
        failed = store.add(f"Failed empty {idx}", task_type="implement")
        assert failed.id is not None
        failed.status = "failed"
        failed.failure_reason = "INFRASTRUCTURE_ERROR"
        failed.session_id = f"sess-empty-{idx}"
        failed.num_steps_computed = 1
        failed.branch = f"feature/empty-{idx}"
        failed.has_commits = False
        failed.completed_at = datetime(2026, 6, 20, 8, idx % 60, tzinfo=UTC)
        store.update(failed)
        unit = store.create_merge_unit(
            source_branch=failed.branch,
            target_branch="main",
            owner_task_id=failed.id,
            state="empty",
        )
        store.attach_task_to_merge_unit(failed.id, unit.id, "owner")
        failed_ids.append(failed.id)

    list_calls: list[tuple[str, ...]] = []
    original_list = store.list_merge_units_for_tasks

    def counted_list_merge_units_for_tasks(task_ids, *args, **kwargs):
        list_calls.append(tuple(task_ids))
        return original_list(task_ids, *args, **kwargs)

    monkeypatch.setattr(store, "list_merge_units_for_tasks", counted_list_merge_units_for_tasks)

    preview = _build_recovery_only_preview(store)

    assert _recovery_entry_ids(preview) == failed_ids
    assert len(list_calls) <= 3
    assert any(len(call) == len(failed_ids) for call in list_calls)


# Review regression intentionally builds 3,000 unrelated rows to prove bounded hydration.
@pytest.mark.cpu_budget(ms=2000)
def test_recovery_preview_terminal_no_work_seed_bounds_hydration_with_large_descendants(
    tmp_path: Path,
    monkeypatch,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    store._default_merge_target_cache = "main"  # noqa: SLF001 - avoid real git in unit test
    store._project_root = None  # noqa: SLF001 - avoid real git fallback in unit test

    failed = store.add("Provider-backed empty seed", task_type="implement")
    assert failed.id is not None
    failed.status = "failed"
    failed.failure_reason = "INFRASTRUCTURE_ERROR"
    failed.session_id = "sess-provider-empty"
    failed.num_steps_reported = 1
    failed.branch = "feature/provider-empty"
    failed.has_commits = False
    failed.completed_at = datetime(2026, 6, 21, 8, 0, tzinfo=UTC)
    store.update(failed)
    unit = store.create_merge_unit(
        source_branch=failed.branch,
        target_branch="main",
        owner_task_id=failed.id,
        state="empty",
    )
    store.attach_task_to_merge_unit(failed.id, unit.id, "owner")
    _bulk_insert_completed_history(store, count=3000, start=300000, based_on=failed.id, task_type="internal")

    full_preview = _build_forced_full_recovery_preview(store)

    def fail_get_all():
        raise AssertionError("scoped recovery preview must not fall back to get_all()")

    hydrated_ids: list[str] = []
    original_row_to_task = store._row_to_task  # noqa: SLF001

    def counted_row_to_task(row, *args, **kwargs):
        hydrated_ids.append(str(row["id"]))
        return original_row_to_task(row, *args, **kwargs)

    monkeypatch.setattr(store, "get_all", fail_get_all)
    monkeypatch.setattr(store, "_row_to_task", counted_row_to_task)  # noqa: SLF001

    scoped_preview = _build_recovery_only_preview(store)

    assert _recovery_entry_keys(scoped_preview) == _recovery_entry_keys(full_preview)
    assert _recovery_entry_ids(scoped_preview) == [failed.id]
    assert failed.id in hydrated_ids
    assert len(set(hydrated_ids)) < 50


def test_recovery_preview_scoped_seed_ignores_large_non_recovery_descendant_history(
    tmp_path: Path,
    monkeypatch,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    store._default_merge_target_cache = "main"  # noqa: SLF001 - avoid real git in unit test
    store._project_root = None  # noqa: SLF001 - avoid real git fallback in unit test

    failed = store.add("Failed tagged seed", task_type="implement", tags=("alpha",))
    assert failed.id is not None
    failed.status = "failed"
    failed.failure_reason = "INFRASTRUCTURE_ERROR"
    failed.branch = "feature/scoped-seed"
    failed.completed_at = datetime(2026, 4, 2, 8, 0, tzinfo=UTC)
    store.update(failed)
    failed_unit = store.create_merge_unit(
        source_branch="feature/scoped-seed",
        target_branch="main",
        owner_task_id=failed.id,
        state="unmerged",
    )
    store.attach_task_to_merge_unit(failed.id, failed_unit.id, "owner")
    non_recovery_descendant_ids = _bulk_insert_completed_history(
        store,
        count=3000,
        start=200000,
        based_on=failed.id,
        task_type="internal",
    )

    def fail_get_all():
        raise AssertionError("scoped recovery preview must not fall back to get_all()")

    hydrated_ids: list[str] = []
    original_row_to_task = store._row_to_task  # noqa: SLF001

    def counted_row_to_task(row, *args, **kwargs):
        hydrated_ids.append(str(row["id"]))
        return original_row_to_task(row, *args, **kwargs)

    monkeypatch.setattr(store, "get_all", fail_get_all)
    monkeypatch.setattr(store, "_row_to_task", counted_row_to_task)  # noqa: SLF001
    with patch(
        "gza.recovery_engine._load_merge_context",
        return_value=recovery_engine._MergeContext(git=None, default_branch="main"),
    ):
        scoped_preview = build_dispatch_preview(
            store,
            tags=None,
            any_tag=False,
            max_recovery_attempts=1,
            selection_mode="recovery_only",
            include_pending=False,
        )
        tagged_scoped_preview = build_dispatch_preview(
            store,
            tags=("alpha",),
            any_tag=False,
            max_recovery_attempts=1,
            selection_mode="recovery_only",
            include_pending=False,
        )

    assert _recovery_entry_keys(scoped_preview) == [(failed.id, failed.id, "retry")]
    assert _recovery_entry_keys(tagged_scoped_preview) == [(failed.id, failed.id, "retry")]
    assert [entry.task.id for entry in scoped_preview.recovery_entries] == [failed.id]
    assert [entry.task.id for entry in tagged_scoped_preview.recovery_entries] == [failed.id]
    assert failed.id in hydrated_ids
    assert set(hydrated_ids).isdisjoint(non_recovery_descendant_ids)
    assert len(set(hydrated_ids)) < 50


def test_recovery_children_query_rejects_unrelated_same_type_completed_descendants(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    failed = store.add("Failed seed", task_type="implement", tags=("alpha",))
    assert failed.id is not None
    failed.status = "failed"
    failed.failure_reason = "INFRASTRUCTURE_ERROR"
    failed.branch = "feature/failed-seed"
    failed.completed_at = datetime(2026, 4, 2, 8, 0, tzinfo=UTC)
    store.update(failed)
    descendant_ids = _bulk_insert_completed_history(
        store,
        count=3000,
        start=210000,
        based_on=failed.id,
    )

    recovery_children = store.get_recovery_children_for_parents((failed.id,))

    assert recovery_children == []
    assert {
        task.id for task in recovery_children if task.id is not None
    }.isdisjoint(descendant_ids)


def test_recovery_preview_legacy_failed_recovery_child_uses_completed_sibling_evidence(
    tmp_path: Path,
    monkeypatch,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    store._default_merge_target_cache = "main"  # noqa: SLF001 - avoid real git in unit test
    store._project_root = None  # noqa: SLF001 - avoid real git fallback in unit test

    parent = store.add("Recovered parent", task_type="implement")
    assert parent.id is not None
    parent.status = "completed"
    parent.branch = "feature/recovered-parent"
    parent.completed_at = datetime(2026, 6, 22, 8, 0, tzinfo=UTC)
    store.update(parent)

    failed = store.add(parent.prompt, task_type="implement", based_on=parent.id, recovery_origin="resume")
    assert failed.id is not None
    failed.status = "failed"
    failed.failure_reason = "INFRASTRUCTURE_ERROR"
    failed.session_id = "sess-failed-resume"
    failed.branch = "feature/failed-resume"
    failed.completed_at = datetime(2026, 6, 22, 9, 0, tzinfo=UTC)
    store.update(failed)

    sibling = store.add(parent.prompt, task_type="implement", based_on=parent.id, recovery_origin="retry")
    assert sibling.id is not None
    sibling.status = "completed"
    sibling.session_id = "sess-sibling-retry"
    sibling.branch = "feature/sibling-retry"
    sibling.has_commits = False
    sibling.completed_at = datetime(2026, 6, 22, 10, 0, tzinfo=UTC)
    store.update(sibling)
    _bulk_insert_completed_history(store, count=150, start=310000)

    full_preview = _build_forced_full_recovery_preview(store)

    def fail_get_all():
        raise AssertionError("scoped recovery preview must not fall back to get_all()")

    hydrated_ids: list[str] = []
    original_row_to_task = store._row_to_task  # noqa: SLF001

    def counted_row_to_task(row, *args, **kwargs):
        hydrated_ids.append(str(row["id"]))
        return original_row_to_task(row, *args, **kwargs)

    monkeypatch.setattr(store, "get_all", fail_get_all)
    monkeypatch.setattr(store, "_row_to_task", counted_row_to_task)  # noqa: SLF001

    scoped_preview = _build_recovery_only_preview(store)

    assert failed.id not in _recovery_entry_ids(full_preview)
    assert failed.id not in _recovery_entry_ids(scoped_preview)
    assert _recovery_entry_keys(scoped_preview) == _recovery_entry_keys(full_preview)
    assert sibling.id in hydrated_ids
    assert len(set(hydrated_ids)) < 50


def test_recovery_preview_legacy_failed_slice_uses_same_slice_sibling_evidence(
    tmp_path: Path,
    monkeypatch,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    store._default_merge_target_cache = "main"  # noqa: SLF001 - avoid real git in unit test
    store._project_root = None  # noqa: SLF001 - avoid real git fallback in unit test

    plan = store.add("Plan source", task_type="plan")
    review = store.add("Plan review", task_type="plan_review", depends_on=plan.id)
    assert plan.id is not None
    assert review.id is not None

    failed = store.add(
        f"Implement slice S1 from plan {plan.id} review {review.id}",
        task_type="implement",
        based_on=plan.id,
        review_scope="Review only parser.",
    )
    assert failed.id is not None
    failed.status = "failed"
    failed.failure_reason = "INFRASTRUCTURE_ERROR"
    failed.completed_at = datetime(2026, 6, 23, 8, 0, tzinfo=UTC)
    store.update(failed)

    landed = store.add(
        failed.prompt,
        task_type="implement",
        based_on=plan.id,
        review_scope="Review only parser.",
    )
    assert landed.id is not None
    landed.status = "completed"
    landed.branch = "feature/same-slice-preview"
    landed.has_commits = True
    landed.completed_at = datetime(2026, 6, 23, 9, 0, tzinfo=UTC)
    store.update(landed)
    unit = store.create_merge_unit(
        source_branch=landed.branch,
        target_branch="main",
        owner_task_id=landed.id,
        state="merged",
    )
    store.attach_task_to_merge_unit(landed.id, unit.id, "owner")
    _bulk_insert_completed_history(store, count=150, start=320000)

    full_preview = _build_forced_full_recovery_preview(store)

    def fail_get_all():
        raise AssertionError("scoped recovery preview must not fall back to get_all()")

    hydrated_ids: list[str] = []
    original_row_to_task = store._row_to_task  # noqa: SLF001

    def counted_row_to_task(row, *args, **kwargs):
        hydrated_ids.append(str(row["id"]))
        return original_row_to_task(row, *args, **kwargs)

    monkeypatch.setattr(store, "get_all", fail_get_all)
    monkeypatch.setattr(store, "_row_to_task", counted_row_to_task)  # noqa: SLF001

    scoped_preview = _build_recovery_only_preview(store)

    assert failed.id not in _recovery_entry_ids(full_preview)
    assert failed.id not in _recovery_entry_ids(scoped_preview)
    assert _recovery_entry_keys(scoped_preview) == _recovery_entry_keys(full_preview)
    assert landed.id in hydrated_ids
    assert len(set(hydrated_ids)) < 50


def test_recovery_preview_scoped_same_slice_evidence_matches_full_with_normalized_scope_and_changed_prompt(
    tmp_path: Path,
    monkeypatch,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    store._default_merge_target_cache = "main"  # noqa: SLF001 - avoid real git in unit test
    store._project_root = None  # noqa: SLF001 - avoid real git fallback in unit test

    parent = store.add("Plan parent for normalized scope", task_type="plan")
    assert parent.id is not None
    parent.status = "completed"
    parent.completed_at = datetime(2026, 6, 30, 8, 0, tzinfo=UTC)
    store.update(parent)

    failed = store.add(
        "Implement parser attempt with original prompt",
        task_type="implement",
        based_on=parent.id,
        review_scope="  Review   only  parser   slice.  ",
    )
    assert failed.id is not None
    failed.status = "failed"
    failed.failure_reason = "INFRASTRUCTURE_ERROR"
    failed.completed_at = datetime(2026, 6, 30, 9, 0, tzinfo=UTC)
    store.update(failed)

    landed = store.add(
        "Implement parser attempt with rewritten prompt text",
        task_type="implement",
        based_on=parent.id,
        review_scope="Review only parser slice.",
    )
    assert landed.id is not None
    landed.status = "completed"
    landed.branch = "feature/normalized-scope-landed"
    landed.has_commits = True
    landed.completed_at = datetime(2026, 6, 30, 10, 0, tzinfo=UTC)
    store.update(landed)
    landed_unit = store.create_merge_unit(
        source_branch=landed.branch,
        target_branch="main",
        owner_task_id=landed.id,
        state="merged",
    )
    store.attach_task_to_merge_unit(landed.id, landed_unit.id, "owner")

    unrelated = store.add(
        "Implement unrelated normalized-scope control",
        task_type="implement",
        based_on=parent.id,
        review_scope="Review only a different parser slice.",
    )
    assert unrelated.id is not None
    unrelated.status = "completed"
    unrelated.branch = "feature/unrelated-normalized-scope"
    unrelated.has_commits = True
    unrelated.completed_at = datetime(2026, 6, 30, 11, 0, tzinfo=UTC)
    store.update(unrelated)
    unrelated_unit = store.create_merge_unit(
        source_branch=unrelated.branch,
        target_branch="main",
        owner_task_id=unrelated.id,
        state="merged",
    )
    store.attach_task_to_merge_unit(unrelated.id, unrelated_unit.id, "owner")
    _bulk_insert_completed_implement_siblings_with_distinct_slices(
        store,
        parent_id=parent.id,
        count=150,
        start=392000,
    )

    full_preview = _build_forced_full_recovery_preview(store)

    def fail_get_all():
        raise AssertionError("scoped recovery preview must not fall back to get_all()")

    hydrated_ids: list[str] = []
    original_row_to_task = store._row_to_task  # noqa: SLF001

    def counted_row_to_task(row, *args, **kwargs):
        hydrated_ids.append(str(row["id"]))
        return original_row_to_task(row, *args, **kwargs)

    monkeypatch.setattr(store, "get_all", fail_get_all)
    monkeypatch.setattr(store, "_row_to_task", counted_row_to_task)  # noqa: SLF001

    scoped_preview = _build_recovery_only_preview(store)

    assert failed.id not in _recovery_entry_ids(full_preview)
    assert _recovery_entry_keys(scoped_preview) == _recovery_entry_keys(full_preview)
    assert landed.id in hydrated_ids
    assert unrelated.id not in hydrated_ids
    assert len(set(hydrated_ids)) < 75


def test_recovery_preview_scoped_same_slice_evidence_matches_full_with_plan_review_provenance_and_changed_prompt(
    tmp_path: Path,
    monkeypatch,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    store._default_merge_target_cache = "main"  # noqa: SLF001 - avoid real git in unit test
    store._project_root = None  # noqa: SLF001 - avoid real git fallback in unit test

    plan = store.add("Plan source for provenance", task_type="plan")
    review = store.add("Plan review for provenance", task_type="plan_review", depends_on=plan.id)
    assert plan.id is not None
    assert review.id is not None

    failed = store.add(
        _plan_review_slice_prompt(
            plan_id=plan.id,
            review_id=review.id,
            slice_id="S1",
            body="Implement the original parser slice.",
        ),
        task_type="implement",
        based_on=plan.id,
        review_scope="  Review   only  parser   provenance.  ",
    )
    assert failed.id is not None
    failed.status = "failed"
    failed.failure_reason = "INFRASTRUCTURE_ERROR"
    failed.completed_at = datetime(2026, 7, 1, 9, 0, tzinfo=UTC)
    store.update(failed)

    landed = store.add(
        _plan_review_slice_prompt(
            plan_id=plan.id,
            review_id=review.id,
            slice_id="S1",
            body="Implement the rewritten parser slice text.",
        ),
        task_type="implement",
        based_on=plan.id,
        review_scope="Review only parser provenance.",
    )
    assert landed.id is not None
    landed.status = "completed"
    landed.branch = "feature/provenance-s1-landed"
    landed.has_commits = True
    landed.completed_at = datetime(2026, 7, 1, 10, 0, tzinfo=UTC)
    store.update(landed)
    landed_unit = store.create_merge_unit(
        source_branch=landed.branch,
        target_branch="main",
        owner_task_id=landed.id,
        state="merged",
    )
    store.attach_task_to_merge_unit(landed.id, landed_unit.id, "owner")

    unrelated = store.add(
        _plan_review_slice_prompt(
            plan_id=plan.id,
            review_id=review.id,
            slice_id="S2",
            body="Implement a different parser slice.",
        ),
        task_type="implement",
        based_on=plan.id,
        review_scope="Review only parser provenance.",
    )
    assert unrelated.id is not None
    unrelated.status = "completed"
    unrelated.branch = "feature/provenance-s2-unrelated"
    unrelated.has_commits = True
    unrelated.completed_at = datetime(2026, 7, 1, 11, 0, tzinfo=UTC)
    store.update(unrelated)
    unrelated_unit = store.create_merge_unit(
        source_branch=unrelated.branch,
        target_branch="main",
        owner_task_id=unrelated.id,
        state="merged",
    )
    store.attach_task_to_merge_unit(unrelated.id, unrelated_unit.id, "owner")
    _bulk_insert_completed_implement_siblings_with_distinct_slices(
        store,
        parent_id=plan.id,
        count=150,
        start=393000,
    )

    full_preview = _build_forced_full_recovery_preview(store)

    def fail_get_all():
        raise AssertionError("scoped recovery preview must not fall back to get_all()")

    hydrated_ids: list[str] = []
    original_row_to_task = store._row_to_task  # noqa: SLF001

    def counted_row_to_task(row, *args, **kwargs):
        hydrated_ids.append(str(row["id"]))
        return original_row_to_task(row, *args, **kwargs)

    monkeypatch.setattr(store, "get_all", fail_get_all)
    monkeypatch.setattr(store, "_row_to_task", counted_row_to_task)  # noqa: SLF001

    scoped_preview = _build_recovery_only_preview(store)

    assert failed.id not in _recovery_entry_ids(full_preview)
    assert _recovery_entry_keys(scoped_preview) == _recovery_entry_keys(full_preview)
    assert landed.id in hydrated_ids
    assert unrelated.id not in hydrated_ids
    assert len(set(hydrated_ids)) < 75


# Review regression intentionally runs the forced-full path over 9,000 same-parent
# siblings so scoped preview parity is proven against the historical broad loader.
@pytest.mark.cpu_budget(ms=15000)
def test_recovery_preview_scoped_same_parent_evidence_bounds_hydration_with_large_slice_siblings(
    tmp_path: Path,
    monkeypatch,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    store._default_merge_target_cache = "main"  # noqa: SLF001 - avoid real git in unit test
    store._project_root = None  # noqa: SLF001 - avoid real git fallback in unit test

    parent = store.add("Plan parent", task_type="plan")
    assert parent.id is not None
    parent.status = "completed"
    parent.completed_at = datetime(2026, 6, 25, 8, 0, tzinfo=UTC)
    store.update(parent)

    failed = store.add(
        "Implement slice S1 for bounded hydration",
        task_type="implement",
        based_on=parent.id,
        recovery_origin="resume",
        review_scope="Review only slice S1.",
    )
    assert failed.id is not None
    failed.status = "failed"
    failed.failure_reason = "INFRASTRUCTURE_ERROR"
    failed.session_id = "sess-failed-s1"
    failed.branch = "feature/failed-s1"
    failed.completed_at = datetime(2026, 6, 25, 9, 0, tzinfo=UTC)
    store.update(failed)
    recovery_sibling = store.add(
        parent.prompt,
        task_type="implement",
        based_on=parent.id,
        recovery_origin="retry",
    )
    assert recovery_sibling.id is not None
    recovery_sibling.status = "completed"
    recovery_sibling.branch = "feature/recovery-sibling"
    recovery_sibling.has_commits = True
    recovery_sibling.completed_at = datetime(2026, 6, 25, 10, 0, tzinfo=UTC)
    store.update(recovery_sibling)
    recovery_unit = store.create_merge_unit(
        source_branch=recovery_sibling.branch,
        target_branch="main",
        owner_task_id=recovery_sibling.id,
        state="merged",
    )
    store.attach_task_to_merge_unit(recovery_sibling.id, recovery_unit.id, "owner")

    landed_same_slice = store.add(
        failed.prompt,
        task_type="implement",
        based_on=parent.id,
        review_scope=failed.review_scope,
    )
    assert landed_same_slice.id is not None
    landed_same_slice.status = "completed"
    landed_same_slice.branch = "feature/landed-s1"
    landed_same_slice.has_commits = True
    landed_same_slice.completed_at = datetime(2026, 6, 25, 11, 0, tzinfo=UTC)
    store.update(landed_same_slice)
    landed_unit = store.create_merge_unit(
        source_branch=landed_same_slice.branch,
        target_branch="main",
        owner_task_id=landed_same_slice.id,
        state="merged",
    )
    store.attach_task_to_merge_unit(landed_same_slice.id, landed_unit.id, "owner")

    unrelated_ids = _bulk_insert_completed_implement_siblings_with_distinct_slices(
        store,
        parent_id=parent.id,
        count=9000,
        start=340000,
    )

    full_preview = _build_forced_full_recovery_preview(store)

    def fail_get_all():
        raise AssertionError("scoped recovery preview must not fall back to get_all()")

    hydrated_ids: list[str] = []
    original_row_to_task = store._row_to_task  # noqa: SLF001

    def counted_row_to_task(row, *args, **kwargs):
        hydrated_ids.append(str(row["id"]))
        return original_row_to_task(row, *args, **kwargs)

    monkeypatch.setattr(store, "get_all", fail_get_all)
    monkeypatch.setattr(store, "_row_to_task", counted_row_to_task)  # noqa: SLF001

    scoped_preview = _build_recovery_only_preview(store)

    assert failed.id not in _recovery_entry_ids(full_preview)
    assert _recovery_entry_keys(scoped_preview) == _recovery_entry_keys(full_preview)
    assert recovery_sibling.id in hydrated_ids
    assert landed_same_slice.id in hydrated_ids
    assert set(hydrated_ids).isdisjoint(unrelated_ids)
    assert len(set(hydrated_ids)) < 75


@pytest.mark.cpu_budget(ms=2500)
def test_recovery_preview_scoped_legacy_seed_excludes_large_inactive_tombstoned_units(
    tmp_path: Path,
    monkeypatch,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    store._default_merge_target_cache = "main"  # noqa: SLF001 - avoid real git in unit test
    store._project_root = None  # noqa: SLF001 - avoid real git fallback in unit test
    tombstoned_ids = _bulk_insert_failed_tombstoned_merge_unit_tasks(store, count=3000, start=360000)

    def fail_get_all():
        raise AssertionError("scoped recovery preview must not fall back to get_all()")

    hydrated_ids: list[str] = []
    original_row_to_task = store._row_to_task  # noqa: SLF001

    def counted_row_to_task(row, *args, **kwargs):
        hydrated_ids.append(str(row["id"]))
        return original_row_to_task(row, *args, **kwargs)

    monkeypatch.setattr(store, "get_all", fail_get_all)
    monkeypatch.setattr(store, "_row_to_task", counted_row_to_task)  # noqa: SLF001

    scoped_preview = _build_recovery_only_preview(store)

    assert scoped_preview.recovery_entries == ()
    assert set(hydrated_ids).isdisjoint(tombstoned_ids)
    assert len(set(hydrated_ids)) < 25


def test_recovery_preview_legacy_failed_task_uses_landed_lineage_evidence(
    tmp_path: Path,
    monkeypatch,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    store._default_merge_target_cache = "main"  # noqa: SLF001 - avoid real git in unit test
    store._project_root = None  # noqa: SLF001 - avoid real git fallback in unit test

    failed = store.add("Failed work with landed follow-up", task_type="implement")
    assert failed.id is not None
    failed.status = "failed"
    failed.failure_reason = "INFRASTRUCTURE_ERROR"
    failed.branch = "feature/landed-preview"
    failed.has_commits = False
    failed.completed_at = datetime(2026, 6, 24, 8, 0, tzinfo=UTC)
    store.update(failed)

    landed = store.add("Landed manual follow-up", task_type="implement", based_on=failed.id, recovery_origin="manual")
    assert landed.id is not None
    landed.status = "completed"
    landed.branch = failed.branch
    landed.has_commits = True
    landed.merge_status = "merged"
    landed.completed_at = datetime(2026, 6, 24, 9, 0, tzinfo=UTC)
    store.update(landed)
    _bulk_insert_completed_history(store, count=150, start=330000)

    full_preview = _build_forced_full_recovery_preview(store)

    def fail_get_all():
        raise AssertionError("scoped recovery preview must not fall back to get_all()")

    hydrated_ids: list[str] = []
    original_row_to_task = store._row_to_task  # noqa: SLF001

    def counted_row_to_task(row, *args, **kwargs):
        hydrated_ids.append(str(row["id"]))
        return original_row_to_task(row, *args, **kwargs)

    monkeypatch.setattr(store, "get_all", fail_get_all)
    monkeypatch.setattr(store, "_row_to_task", counted_row_to_task)  # noqa: SLF001

    scoped_preview = _build_recovery_only_preview(store)

    assert failed.id not in _recovery_entry_ids(full_preview)
    assert failed.id not in _recovery_entry_ids(scoped_preview)
    assert _recovery_entry_keys(scoped_preview) == _recovery_entry_keys(full_preview)
    assert landed.id in hydrated_ids
    assert len(set(hydrated_ids)) < 50


def test_recovery_preview_scoped_landed_evidence_matches_full_for_transitive_manual_followup(
    tmp_path: Path,
    monkeypatch,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    store._default_merge_target_cache = "main"  # noqa: SLF001 - avoid real git in unit test
    store._project_root = None  # noqa: SLF001 - avoid real git fallback in unit test

    failed = store.add("Failed root with landed grandchild", task_type="implement")
    assert failed.id is not None
    failed.status = "failed"
    failed.failure_reason = "INFRASTRUCTURE_ERROR"
    failed.branch = "feature/transitive-landed"
    failed.completed_at = datetime(2026, 6, 26, 8, 0, tzinfo=UTC)
    store.update(failed)

    intermediate = store.add("Manual intermediate", task_type="implement", based_on=failed.id, recovery_origin="manual")
    assert intermediate.id is not None
    intermediate.status = "completed"
    intermediate.branch = failed.branch
    intermediate.completed_at = datetime(2026, 6, 26, 9, 0, tzinfo=UTC)
    store.update(intermediate)

    landed = store.add("Merged same-branch grandchild", task_type="implement", based_on=intermediate.id, recovery_origin="manual")
    assert landed.id is not None
    landed.status = "completed"
    landed.branch = failed.branch
    landed.has_commits = True
    landed.completed_at = datetime(2026, 6, 26, 10, 0, tzinfo=UTC)
    store.update(landed)
    unit = store.create_merge_unit(
        source_branch=landed.branch,
        target_branch="main",
        owner_task_id=landed.id,
        state="merged",
    )
    store.attach_task_to_merge_unit(landed.id, unit.id, "owner")
    _bulk_insert_completed_history(store, count=500, start=370000)

    full_preview = _build_forced_full_recovery_preview(store)

    def fail_get_all():
        raise AssertionError("scoped recovery preview must not fall back to get_all()")

    hydrated_ids: list[str] = []
    original_row_to_task = store._row_to_task  # noqa: SLF001

    def counted_row_to_task(row, *args, **kwargs):
        hydrated_ids.append(str(row["id"]))
        return original_row_to_task(row, *args, **kwargs)

    monkeypatch.setattr(store, "get_all", fail_get_all)
    monkeypatch.setattr(store, "_row_to_task", counted_row_to_task)  # noqa: SLF001

    scoped_preview = _build_recovery_only_preview(store)

    assert failed.id not in _recovery_entry_ids(full_preview)
    assert _recovery_entry_keys(scoped_preview) == _recovery_entry_keys(full_preview)
    assert landed.id in hydrated_ids
    assert intermediate.id not in hydrated_ids
    assert len(set(hydrated_ids)) < 50


@pytest.mark.parametrize(
    ("intermediate_branch", "case_id"),
    (
        (None, "branchless"),
        ("feature/different-intermediate", "different-branch"),
    ),
)
def test_recovery_preview_scoped_landed_evidence_reaches_through_nonmatching_intermediate_branch(
    tmp_path: Path,
    monkeypatch,
    intermediate_branch: str | None,
    case_id: str,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    store._default_merge_target_cache = "main"  # noqa: SLF001 - avoid real git in unit test
    store._project_root = None  # noqa: SLF001 - avoid real git fallback in unit test

    failed = store.add(f"Failed root with {case_id} landed grandchild", task_type="implement")
    assert failed.id is not None
    failed.status = "failed"
    failed.failure_reason = "INFRASTRUCTURE_ERROR"
    failed.branch = f"feature/{case_id}-landed"
    failed.completed_at = datetime(2026, 6, 28, 8, 0, tzinfo=UTC)
    store.update(failed)

    intermediate = store.add(
        f"Manual {case_id} intermediate",
        task_type="implement",
        based_on=failed.id,
        recovery_origin="manual",
    )
    assert intermediate.id is not None
    intermediate.status = "completed"
    intermediate.branch = intermediate_branch
    intermediate.completed_at = datetime(2026, 6, 28, 9, 0, tzinfo=UTC)
    store.update(intermediate)

    landed = store.add(
        f"Merged same-branch grandchild behind {case_id} intermediate",
        task_type="implement",
        based_on=intermediate.id,
        recovery_origin="manual",
    )
    assert landed.id is not None
    landed.status = "completed"
    landed.branch = failed.branch
    landed.has_commits = True
    landed.completed_at = datetime(2026, 6, 28, 10, 0, tzinfo=UTC)
    store.update(landed)
    unit = store.create_merge_unit(
        source_branch=landed.branch,
        target_branch="main",
        owner_task_id=landed.id,
        state="merged",
    )
    store.attach_task_to_merge_unit(landed.id, unit.id, "owner")
    _bulk_insert_completed_history(store, count=150, start=390000)

    full_preview = _build_forced_full_recovery_preview(store)

    def fail_get_all():
        raise AssertionError("scoped recovery preview must not fall back to get_all()")

    hydrated_ids: list[str] = []
    original_row_to_task = store._row_to_task  # noqa: SLF001

    def counted_row_to_task(row, *args, **kwargs):
        hydrated_ids.append(str(row["id"]))
        return original_row_to_task(row, *args, **kwargs)

    monkeypatch.setattr(store, "get_all", fail_get_all)
    monkeypatch.setattr(store, "_row_to_task", counted_row_to_task)  # noqa: SLF001

    scoped_preview = _build_recovery_only_preview(store)

    assert failed.id not in _recovery_entry_ids(full_preview)
    assert _recovery_entry_keys(scoped_preview) == _recovery_entry_keys(full_preview)
    assert landed.id in hydrated_ids
    assert intermediate.id not in hydrated_ids
    assert len(set(hydrated_ids)) < 50


@pytest.mark.parametrize("tombstone_state", ("dropped", "superseded"))
def test_recovery_preview_scoped_landed_evidence_ignores_stale_legacy_merge_status_with_inactive_unit(
    tmp_path: Path,
    monkeypatch,
    tombstone_state: str,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    store._default_merge_target_cache = "main"  # noqa: SLF001 - avoid real git in unit test
    store._project_root = None  # noqa: SLF001 - avoid real git fallback in unit test

    failed = store.add(f"Failed root with stale {tombstone_state} descendant", task_type="implement")
    assert failed.id is not None
    failed.status = "failed"
    failed.failure_reason = "INFRASTRUCTURE_ERROR"
    failed.branch = f"feature/stale-{tombstone_state}-descendant"
    failed.has_commits = False
    failed.completed_at = datetime(2026, 6, 29, 8, 0, tzinfo=UTC)
    store.update(failed)

    stale = store.add("Completed stale legacy merged descendant", task_type="implement", based_on=failed.id)
    assert stale.id is not None
    stale.status = "completed"
    stale.branch = failed.branch
    stale.has_commits = True
    stale.merge_status = "merged"
    stale.completed_at = datetime(2026, 6, 29, 9, 0, tzinfo=UTC)
    store.update(stale)
    unit = store.create_merge_unit(
        source_branch=stale.branch,
        target_branch="main",
        owner_task_id=stale.id,
        state=tombstone_state,
    )
    store.attach_task_to_merge_unit(stale.id, unit.id, "owner")
    if tombstone_state == "superseded":
        with store._connect() as conn:  # noqa: SLF001 - fixture-only stale legacy setup
            conn.execute(
                """
                UPDATE merge_units
                SET superseded_by_unit_id = ?, updated_at = ?
                WHERE project_id = ? AND id = ?
                """,
                ("mu-superseding-winner", "2026-06-29 09:30:00", store._project_id, unit.id),
            )
    _bulk_insert_completed_history(store, count=150, start=391000)

    def fail_get_all():
        raise AssertionError("scoped recovery preview must not fall back to get_all()")

    hydrated_ids: list[str] = []
    original_row_to_task = store._row_to_task  # noqa: SLF001

    def counted_row_to_task(row, *args, **kwargs):
        hydrated_ids.append(str(row["id"]))
        return original_row_to_task(row, *args, **kwargs)

    monkeypatch.setattr(store, "get_all", fail_get_all)
    monkeypatch.setattr(store, "_row_to_task", counted_row_to_task)  # noqa: SLF001

    scoped_preview = _build_recovery_only_preview(store)
    with patch(
        "gza.recovery_engine._load_merge_context",
        return_value=recovery_engine._MergeContext(git=None, default_branch="main"),
    ):
        decision = recovery_engine.decide_failed_task_recovery(store, failed, max_recovery_attempts=1)

    assert decision.action == "retry"
    assert _recovery_entry_keys(scoped_preview) == [(failed.id, failed.id, "retry")]
    landed_evidence = store.list_landed_lineage_tasks_for_roots(
        (failed.id,),
        branch_keys_by_root_id={failed.id: (failed.branch,)},
    )
    assert landed_evidence == {failed.id: []}
    assert len(set(hydrated_ids)) < 50


def test_recovery_preview_scoped_landed_evidence_uses_depends_on_canonical_root(
    tmp_path: Path,
    monkeypatch,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    store._default_merge_target_cache = "main"  # noqa: SLF001 - avoid real git in unit test
    store._project_root = None  # noqa: SLF001 - avoid real git fallback in unit test

    depends_root = store.add("Earlier dependency root", task_type="plan")
    based_root = store.add("Later based-on root", task_type="plan")
    assert depends_root.id is not None
    assert based_root.id is not None
    depends_root.status = "completed"
    depends_root.completed_at = datetime(2026, 6, 27, 7, 0, tzinfo=UTC)
    based_root.status = "completed"
    based_root.completed_at = datetime(2026, 6, 27, 8, 0, tzinfo=UTC)
    store.update(depends_root)
    store.update(based_root)

    failed = store.add(
        "Failed dual-parent seed",
        task_type="implement",
        based_on=based_root.id,
        depends_on=depends_root.id,
    )
    assert failed.id is not None
    failed.status = "failed"
    failed.failure_reason = "INFRASTRUCTURE_ERROR"
    failed.branch = "feature/dual-parent-landed"
    failed.completed_at = datetime(2026, 6, 27, 9, 0, tzinfo=UTC)
    store.update(failed)

    landed = store.add("Merged descendant through failed seed", task_type="implement", based_on=failed.id)
    assert landed.id is not None
    landed.status = "completed"
    landed.branch = failed.branch
    landed.has_commits = True
    landed.completed_at = datetime(2026, 6, 27, 10, 0, tzinfo=UTC)
    store.update(landed)
    unit = store.create_merge_unit(
        source_branch=landed.branch,
        target_branch="main",
        owner_task_id=landed.id,
        state="merged",
    )
    store.attach_task_to_merge_unit(landed.id, unit.id, "owner")
    _bulk_insert_completed_history(store, count=500, start=380000)

    full_preview = _build_forced_full_recovery_preview(store)

    def fail_get_all():
        raise AssertionError("scoped recovery preview must not fall back to get_all()")

    hydrated_ids: list[str] = []
    original_row_to_task = store._row_to_task  # noqa: SLF001

    def counted_row_to_task(row, *args, **kwargs):
        hydrated_ids.append(str(row["id"]))
        return original_row_to_task(row, *args, **kwargs)

    monkeypatch.setattr(store, "get_all", fail_get_all)
    monkeypatch.setattr(store, "_row_to_task", counted_row_to_task)  # noqa: SLF001

    scoped_preview = _build_recovery_only_preview(store)

    assert failed.id not in _recovery_entry_ids(full_preview)
    assert _recovery_entry_keys(scoped_preview) == _recovery_entry_keys(full_preview)
    assert landed.id in hydrated_ids
    assert based_root.id in hydrated_ids
    assert depends_root.id in hydrated_ids
    assert len(set(hydrated_ids)) < 50


def test_recovery_preview_tag_scope_does_not_hydrate_out_of_scope_merge_units(
    tmp_path: Path,
    monkeypatch,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    store._default_merge_target_cache = "main"  # noqa: SLF001 - avoid real git in unit test
    store._project_root = None  # noqa: SLF001 - avoid real git fallback in unit test

    tagged = store.add("Tagged failed", task_type="implement", tags=("v0.5.1",))
    other = store.add("Other failed", task_type="implement", tags=("v0.6.0",))
    for task, branch in ((tagged, "feature/tagged"), (other, "feature/other")):
        assert task.id is not None
        task.status = "failed"
        task.failure_reason = "INFRASTRUCTURE_ERROR"
        task.branch = branch
        task.has_commits = False
        task.completed_at = datetime(2026, 5, 1, 8, 0, tzinfo=UTC)
        store.update(task)
        unit = store.create_merge_unit(
            source_branch=branch,
            target_branch="main",
            owner_task_id=task.id,
            state="unmerged",
        )
        store.attach_task_to_merge_unit(task.id, unit.id, "owner")

    hydrated_ids: list[str] = []
    original_row_to_task = store._row_to_task  # noqa: SLF001

    def counted_row_to_task(row, *args, **kwargs):
        hydrated_ids.append(str(row["id"]))
        return original_row_to_task(row, *args, **kwargs)

    monkeypatch.setattr(store, "_row_to_task", counted_row_to_task)  # noqa: SLF001

    with patch(
        "gza.recovery_engine._load_merge_context",
        return_value=recovery_engine._MergeContext(git=None, default_branch="main"),
    ):
        preview = build_dispatch_preview(
            store,
            tags=("v0.5.1",),
            any_tag=False,
            max_recovery_attempts=1,
            selection_mode="recovery_only",
            include_pending=False,
        )

    assert [entry.task.id for entry in preview.recovery_entries] == [tagged.id]
    assert tagged.id in hydrated_ids
    assert other.id not in hydrated_ids


def test_recovery_preview_tag_intersection_requires_one_matching_unit_member(
    tmp_path: Path,
    monkeypatch,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    store._default_merge_target_cache = "main"  # noqa: SLF001 - avoid real git in unit test
    store._project_root = None  # noqa: SLF001 - avoid real git fallback in unit test

    split_alpha = store.add("Split alpha failed", task_type="implement", tags=("alpha",))
    split_beta = store.add("Split beta failed", task_type="implement", tags=("beta",))
    control = store.add("Full tag failed", task_type="implement", tags=("alpha", "beta"))
    for task, branch in (
        (split_alpha, "feature/split-alpha"),
        (split_beta, "feature/split-beta"),
        (control, "feature/full-tags"),
    ):
        assert task.id is not None
        task.status = "failed"
        task.failure_reason = "INFRASTRUCTURE_ERROR"
        task.branch = branch
        task.completed_at = datetime(2026, 5, 2, 8, 0, tzinfo=UTC)
        store.update(task)

    split_unit = store.create_merge_unit(
        source_branch="feature/split",
        target_branch="main",
        owner_task_id=split_alpha.id,
        state="unmerged",
    )
    assert split_alpha.id is not None
    assert split_beta.id is not None
    store.attach_task_to_merge_unit(split_alpha.id, split_unit.id, "owner")
    store.attach_task_to_merge_unit(split_beta.id, split_unit.id, "implement")
    control_unit = store.create_merge_unit(
        source_branch="feature/full-tags",
        target_branch="main",
        owner_task_id=control.id,
        state="unmerged",
    )
    assert control.id is not None
    store.attach_task_to_merge_unit(control.id, control_unit.id, "owner")

    hydrated_ids: list[str] = []
    original_row_to_task = store._row_to_task  # noqa: SLF001

    def counted_row_to_task(row, *args, **kwargs):
        hydrated_ids.append(str(row["id"]))
        return original_row_to_task(row, *args, **kwargs)

    monkeypatch.setattr(store, "_row_to_task", counted_row_to_task)  # noqa: SLF001
    with patch(
        "gza.recovery_engine._load_merge_context",
        return_value=recovery_engine._MergeContext(git=None, default_branch="main"),
    ):
        preview = build_dispatch_preview(
            store,
            tags=("alpha", "beta"),
            any_tag=False,
            max_recovery_attempts=1,
            selection_mode="recovery_only",
            include_pending=False,
        )

    assert [entry.task.id for entry in preview.recovery_entries] == [control.id]
    assert control.id in hydrated_ids
    assert split_alpha.id not in hydrated_ids
    assert split_beta.id not in hydrated_ids


def test_recovery_preview_keeps_provider_backed_terminal_no_work_failures_with_actionable_unit(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    store._default_merge_target_cache = "main"  # noqa: SLF001 - avoid real git in unit test
    store._project_root = None  # noqa: SLF001 - avoid real git fallback in unit test

    actionable = store.add("Actionable failed", task_type="implement")
    assert actionable.id is not None
    actionable.status = "failed"
    actionable.failure_reason = "INFRASTRUCTURE_ERROR"
    actionable.branch = "feature/actionable"
    actionable.completed_at = datetime(2026, 5, 3, 8, 0, tzinfo=UTC)
    store.update(actionable)
    actionable_unit = store.create_merge_unit(
        source_branch=actionable.branch,
        target_branch="main",
        owner_task_id=actionable.id,
        state="unmerged",
    )
    store.attach_task_to_merge_unit(actionable.id, actionable_unit.id, "owner")

    terminal_tasks = []
    for state in ("empty", "redundant"):
        failed = store.add(f"Provider {state} failed", task_type="implement", tags=("recoverable",))
        assert failed.id is not None
        failed.status = "failed"
        failed.failure_reason = "MAX_TURNS"
        failed.session_id = f"sess-{state}"
        failed.num_steps_computed = 2
        failed.branch = f"feature/{state}-provider"
        failed.completed_at = datetime(2026, 5, 3, 8, 5, 0, tzinfo=UTC)
        store.update(failed)
        unit = store.create_merge_unit(
            source_branch=failed.branch,
            target_branch="main",
            owner_task_id=failed.id,
            state=state,
        )
        store.attach_task_to_merge_unit(failed.id, unit.id, "owner")
        terminal_tasks.append(failed)

    with patch(
        "gza.recovery_engine._load_merge_context",
        return_value=recovery_engine._MergeContext(git=None, default_branch="main"),
    ):
        scoped_preview = build_dispatch_preview(
            store,
            tags=None,
            any_tag=False,
            max_recovery_attempts=1,
            selection_mode="recovery_only",
            include_pending=False,
        )
        tagged_scoped_preview = build_dispatch_preview(
            store,
            tags=("recoverable",),
            any_tag=False,
            max_recovery_attempts=1,
            selection_mode="recovery_only",
            include_pending=False,
        )

    terminal_ids = {task.id for task in terminal_tasks}
    assert terminal_ids <= {entry.task.id for entry in scoped_preview.recovery_entries}
    assert {entry.task.id for entry in tagged_scoped_preview.recovery_entries} == terminal_ids
    for entry in scoped_preview.recovery_entries:
        if entry.task.id in terminal_ids:
            assert entry.action == "resume"
            assert entry.lineage_row is not None
            assert entry.lineage_row.recovery_action_task is not None
            assert entry.lineage_row.recovery_action_task.id == entry.task.id


def test_recovery_preview_hides_explicit_zero_terminal_no_work_failures_with_actionable_unit(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    store._default_merge_target_cache = "main"  # noqa: SLF001 - avoid real git in unit test
    store._project_root = None  # noqa: SLF001 - avoid real git fallback in unit test

    actionable = store.add("Actionable failed", task_type="implement")
    assert actionable.id is not None
    actionable.status = "failed"
    actionable.failure_reason = "INFRASTRUCTURE_ERROR"
    actionable.branch = "feature/actionable-zero-control"
    actionable.completed_at = datetime(2026, 5, 4, 8, 0, tzinfo=UTC)
    store.update(actionable)
    actionable_unit = store.create_merge_unit(
        source_branch=actionable.branch,
        target_branch="main",
        owner_task_id=actionable.id,
        state="unmerged",
    )
    store.attach_task_to_merge_unit(actionable.id, actionable_unit.id, "owner")

    moot = store.add("Moot terminal no-work failed", task_type="implement")
    assert moot.id is not None
    moot.status = "failed"
    moot.failure_reason = "MAX_TURNS"
    moot.session_id = "sess-zero"
    moot.num_steps_computed = 0
    moot.num_steps_reported = 0
    moot.output_tokens = 0
    moot.branch = "feature/moot-zero"
    moot.completed_at = datetime(2026, 5, 4, 8, 5, 0, tzinfo=UTC)
    store.update(moot)
    moot_unit = store.create_merge_unit(
        source_branch=moot.branch,
        target_branch="main",
        owner_task_id=moot.id,
        state="empty",
    )
    store.attach_task_to_merge_unit(moot.id, moot_unit.id, "owner")

    with patch(
        "gza.recovery_engine._load_merge_context",
        return_value=recovery_engine._MergeContext(git=None, default_branch="main"),
    ):
        preview = build_dispatch_preview(
            store,
            tags=None,
            any_tag=False,
            max_recovery_attempts=1,
            selection_mode="recovery_only",
            include_pending=False,
        )

    assert actionable.id in {entry.task.id for entry in preview.recovery_entries}
    assert moot.id not in {entry.task.id for entry in preview.recovery_entries}


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


def test_build_dispatch_preview_filters_quiet_pending_but_keeps_exempt_tasks(tmp_path: Path) -> None:
    setup_config(tmp_path)
    config_path = tmp_path / "gza.yaml"
    config_path.write_text(config_path.read_text() + "quiet_period_seconds: 300\n")
    config = Config.load(tmp_path)
    store = make_store(tmp_path)

    quiet = store.add("Fresh quiet pending", task_type="plan")
    expired = store.add("Expired pending", task_type="plan")
    urgent = store.add("Urgent fresh pending", task_type="plan", urgent=True)
    explicit = store.add("Explicit fresh pending", task_type="plan")
    assert quiet.id is not None
    assert expired.id is not None
    assert urgent.id is not None
    assert explicit.id is not None

    now = datetime.now(UTC)
    quiet.last_edited_at = now - timedelta(seconds=30)
    expired.last_edited_at = now - timedelta(seconds=300)
    urgent.last_edited_at = now - timedelta(seconds=30)
    explicit.last_edited_at = now - timedelta(seconds=30)
    store.update(quiet)
    store.update(expired)
    store.update(urgent)
    store.update(explicit)
    store.set_queue_position(explicit.id, 1)

    preview = build_dispatch_preview(
        store,
        config=config,
        tags=None,
        any_tag=False,
        max_recovery_attempts=1,
        include_recovery=False,
    )

    pending_ids = [entry.task.id for entry in preview.pending_entries]
    assert quiet.id not in pending_ids
    assert explicit.id in pending_ids
    assert urgent.id in pending_ids
    assert expired.id in pending_ids


def test_plan_watch_dispatch_entries_caps_worker_recovery_and_preserves_preview_order(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    store._default_merge_target_cache = "main"  # noqa: SLF001 - avoid real git in unit test
    store._project_root = None  # noqa: SLF001 - avoid real git fallback in unit test

    recovery_one = store.add("Failed implement one", task_type="implement")
    assert recovery_one.id is not None
    recovery_one.status = "failed"
    recovery_one.failure_reason = "MAX_TURNS"
    recovery_one.session_id = "sess-one"
    recovery_one.completed_at = datetime(2026, 6, 24, 13, 0, 0, tzinfo=UTC)
    store.update(recovery_one)

    recovery_two = store.add("Failed implement two", task_type="implement")
    assert recovery_two.id is not None
    recovery_two.status = "failed"
    recovery_two.failure_reason = "MAX_TURNS"
    recovery_two.session_id = "sess-two"
    recovery_two.completed_at = datetime(2026, 6, 24, 13, 5, 0, tzinfo=UTC)
    store.update(recovery_two)

    pending_one = store.add("Pending one", task_type="plan")
    pending_two = store.add("Pending two", task_type="plan")
    pending_three = store.add("Pending three", task_type="plan")
    assert pending_one.id is not None
    assert pending_two.id is not None
    assert pending_three.id is not None

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

    plan = plan_watch_dispatch_entries(
        preview.runnable_entries,
        slots=3,
        recovery_slot_cap=1,
        selection_mode="default",
    )

    assert plan.recovery_worker_slots == 1
    assert plan.pending_slots == 2
    assert [entry.task.id for entry in plan.entries] == [
        recovery_one.id,
        pending_one.id,
        pending_two.id,
    ]
