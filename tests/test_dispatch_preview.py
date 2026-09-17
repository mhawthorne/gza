from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import patch

import pytest

from gza import recovery_engine
from gza.cli._recovery_lane import collect_recovery_lane_entries
from gza.config import Config
from gza.dispatch_preview import DispatchPreviewEntry, build_dispatch_preview, plan_watch_dispatch_entries
from gza.main_integration_verify import (
    MAIN_INTEGRATION_VERIFY_REMEDIATION_TRIGGER_SOURCE,
    MAIN_INTEGRATION_VERIFY_TAG,
)
from gza.pickup import get_runnable_pending_tasks
from tests.cli.conftest import make_store, setup_config

# Row count for the scale-guard fixtures below. These tests prove the scoped
# recovery preview never falls back to `get_all()` and never hydrates unrelated
# rows. Every one of them asserts that via a raising `get_all`, an `isdisjoint`
# check against the bulk ids, or a fixed hydration bound (the largest is 75) --
# all of which are independent of how many rows exist, so long as the count
# exceeds the bound. 400 clears the largest bound by more than 5x while keeping
# fixture setup cheap; the previous 3000/9000 only inflated insert time.
_SCALE_GUARD_ROWS = 400


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


def _bulk_insert_failed_connector_history(
    store,
    *,
    count: int,
    start: int,
) -> tuple[str, ...]:
    now = datetime(2026, 4, 4, 8, 0, tzinfo=UTC).isoformat()
    task_ids = tuple(f"gza-{start + idx}" for idx in range(count))
    with store._connect() as conn:  # noqa: SLF001 - fast large-store fixture setup
        conn.executemany(
            """
            INSERT INTO tasks (
                project_id, id, prompt, status, task_type, based_on, depends_on,
                branch, failure_reason, created_at, updated_at, completed_at
            )
            VALUES (?, ?, ?, 'failed', 'implement', ?, ?, ?, 'INFRASTRUCTURE_ERROR', ?, ?, ?)
            """,
            [
                (
                    store._project_id,  # noqa: SLF001
                    task_id,
                    f"Unrelated failed connector-bearing task {idx}",
                    f"gza-unrelated-parent-{idx}",
                    f"gza-unrelated-dependency-{idx}",
                    f"feature/unrelated-connector-{idx}",
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




def test_plan_watch_dispatch_entries_under_main_verify_hold_keeps_only_direct_recovery_and_exact_remediation(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    direct_recovery = store.add("Direct recovery reconcile", task_type="implement")
    worker_recovery = store.add("Worker recovery retry", task_type="implement")
    remediation = store.add(
        "Main verify remediation",
        task_type="implement",
        tags=(MAIN_INTEGRATION_VERIFY_TAG,),
        trigger_source=MAIN_INTEGRATION_VERIFY_REMEDIATION_TRIGGER_SOURCE,
    )
    ordinary_pending = store.add("Ordinary pending", task_type="plan")
    assert direct_recovery.id is not None
    assert worker_recovery.id is not None
    assert remediation.id is not None
    assert ordinary_pending.id is not None
    entries = (
        DispatchPreviewEntry(
            lane="recovery",
            task=direct_recovery,
            runnable=True,
            worker_consuming=False,
            advance_action={"type": "reconcile"},
        ),
        DispatchPreviewEntry(
            lane="recovery",
            task=worker_recovery,
            runnable=True,
            worker_consuming=True,
            advance_action={"type": "retry"},
        ),
        DispatchPreviewEntry(
            lane="pending",
            task=remediation,
            runnable=True,
            worker_consuming=True,
        ),
        DispatchPreviewEntry(
            lane="pending",
            task=ordinary_pending,
            runnable=True,
            worker_consuming=True,
        ),
    )

    plan = plan_watch_dispatch_entries(
        entries,
        slots=0,
        recovery_slot_cap=2,
        selection_mode="recovery_only",
        main_verify_remediation_task_id=remediation.id,
    )

    assert [entry.task.id for entry in plan.entries] == [direct_recovery.id, remediation.id]
    assert plan.recovery_worker_slots == 0
    assert plan.pending_slots == 0
    assert plan.main_verify_remediation_slots == 1
    assert plan.main_verify_remediation_entry is not None
    assert plan.main_verify_remediation_entry.task.id == remediation.id


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
    _bulk_insert_completed_history(store, count=_SCALE_GUARD_ROWS, start=300000, based_on=failed.id, task_type="internal")

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
        count=_SCALE_GUARD_ROWS,
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


def test_build_dispatch_preview_recovery_only_does_not_admit_ordinary_positioned_pending(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    store._default_merge_target_cache = "main"  # noqa: SLF001 - avoid real git in unit test
    store._project_root = None  # noqa: SLF001 - avoid real git fallback in unit test

    positioned = store.add("Positioned ordinary pending", task_type="implement")
    urgent = store.add("Urgent ordinary pending", task_type="implement", urgent=True)
    assert positioned.id is not None
    assert urgent.id is not None
    store.set_queue_position(positioned.id, 1)

    with patch(
        "gza.recovery_engine._load_merge_context",
        return_value=recovery_engine._MergeContext(git=None, default_branch=None),
    ):
        preview = build_dispatch_preview(
            store,
            tags=None,
            any_tag=False,
            max_recovery_attempts=1,
            selection_mode="recovery_only",
        )

    assert preview.recovery_entries == ()
    assert preview.pending_entries == ()
    assert positioned.id not in {entry.task.id for entry in preview.entries}
    assert urgent.id not in {entry.task.id for entry in preview.entries}


def test_build_dispatch_preview_recovery_only_admits_unpositioned_watch_main_verify_remediation_only(
    tmp_path: Path,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    store._default_merge_target_cache = "main"  # noqa: SLF001 - avoid real git in unit test
    store._project_root = None  # noqa: SLF001 - avoid real git fallback in unit test

    failed_retry = store.add("Failed recovery", task_type="plan")
    assert failed_retry.id is not None
    failed_retry.status = "failed"
    failed_retry.failure_reason = "INFRASTRUCTURE_ERROR"
    failed_retry.completed_at = datetime(2026, 6, 24, 12, 0, 0, tzinfo=UTC)
    store.update(failed_retry)

    remediation = store.add(
        "Watch main verify remediation",
        task_type="implement",
        tags=("system", MAIN_INTEGRATION_VERIFY_TAG),
        trigger_source=MAIN_INTEGRATION_VERIFY_REMEDIATION_TRIGGER_SOURCE,
    )
    ordinary_positioned = store.add("Positioned ordinary pending", task_type="implement")
    assert remediation.id is not None
    assert ordinary_positioned.id is not None
    store.set_queue_position(ordinary_positioned.id, 1)

    with patch(
        "gza.recovery_engine._load_merge_context",
        return_value=recovery_engine._MergeContext(git=None, default_branch=None),
    ):
        preview = build_dispatch_preview(
            store,
            tags=None,
            any_tag=False,
            max_recovery_attempts=1,
            selection_mode="recovery_only",
        )
    plan = plan_watch_dispatch_entries(
        preview.runnable_entries,
        slots=0,
        recovery_slot_cap=0,
        selection_mode="recovery_only",
        main_verify_remediation_task_id=remediation.id,
    )

    assert [entry.task.id for entry in preview.recovery_entries] == [failed_retry.id]
    assert [entry.task.id for entry in preview.pending_entries] == [remediation.id]
    assert ordinary_positioned.id not in {entry.task.id for entry in preview.pending_entries}
    assert plan.entries == (preview.pending_entries[0],)
    assert plan.main_verify_remediation_slots == 1
    assert plan.pending_slots == 0


@pytest.mark.parametrize(
    ("task_status", "tags", "trigger_source", "selected_id"),
    [
        ("completed", ("system", MAIN_INTEGRATION_VERIFY_TAG), MAIN_INTEGRATION_VERIFY_REMEDIATION_TRIGGER_SOURCE, None),
        ("pending", ("system",), MAIN_INTEGRATION_VERIFY_REMEDIATION_TRIGGER_SOURCE, None),
        ("pending", ("system", MAIN_INTEGRATION_VERIFY_TAG), "manual", None),
        ("pending", ("system", MAIN_INTEGRATION_VERIFY_TAG), MAIN_INTEGRATION_VERIFY_REMEDIATION_TRIGGER_SOURCE, "other"),
    ],
)
def test_plan_watch_dispatch_entries_exact_main_verify_remediation_fails_closed_for_stale_rows(
    tmp_path: Path,
    task_status: str,
    tags: tuple[str, ...],
    trigger_source: str,
    selected_id: str | None,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)
    remediation = store.add(
        "Maybe remediation",
        task_type="implement",
        tags=tags,
        trigger_source=trigger_source,
    )
    ordinary = store.add("Ordinary pending", task_type="implement")
    assert remediation.id is not None
    assert ordinary.id is not None
    remediation.status = task_status
    store.update(remediation)
    entry = DispatchPreviewEntry(
        lane="pending",
        task=remediation,
        runnable=task_status == "pending",
        worker_consuming=True,
    )
    ordinary_entry = DispatchPreviewEntry(
        lane="pending",
        task=ordinary,
        runnable=True,
        worker_consuming=True,
    )

    plan = plan_watch_dispatch_entries(
        (entry, ordinary_entry),
        slots=1,
        recovery_slot_cap=0,
        selection_mode="recovery_only",
        main_verify_remediation_task_id=selected_id or remediation.id,
    )

    assert plan.entries == ()
    assert plan.main_verify_remediation_slots == 0
    assert plan.pending_slots == 0


def test_build_dispatch_preview_filters_quiet_pending_but_keeps_exempt_tasks(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    setup_config(tmp_path)
    config_path = tmp_path / "gza.yaml"
    config_path.write_text(config_path.read_text() + "quiet_period_seconds: 300\n")
    config = Config.load(tmp_path)
    store = make_store(tmp_path)

    class FrozenDateTime(datetime):
        current = datetime(2026, 6, 24, 12, 13, tzinfo=UTC)

        @classmethod
        def now(cls, tz=None):
            assert tz is not None
            return cls.current.astimezone(tz)

    monkeypatch.setattr("gza.db.datetime", FrozenDateTime)

    quiet = store.add("Fresh quiet pending", task_type="plan")
    expired = store.add("Expired pending", task_type="plan")
    urgent = store.add("Urgent fresh pending", task_type="plan", urgent=True)
    explicit = store.add("Explicit fresh pending", task_type="plan")
    assert quiet.id is not None
    assert expired.id is not None
    assert urgent.id is not None
    assert explicit.id is not None

    now = FrozenDateTime.current
    quiet.last_edited_at = now - timedelta(seconds=30)
    expired.last_edited_at = now - timedelta(seconds=600)
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
