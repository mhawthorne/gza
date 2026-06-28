from pathlib import Path

from gza import dependency_preconditions as dependency_preconditions_module
from gza.dependency_preconditions import (
    dependency_readiness,
    empty_prereq_satisfies_dependency,
    get_unmerged_dependency_precondition,
)
from gza.cli._common import release_held_plan_source
from gza.db import SqliteTaskStore
from gza.lineage_query import _load_indexes
from gza.recovery_read_context import RecoveryReadContext


def _read_context_for_store(store: SqliteTaskStore) -> RecoveryReadContext:
    indexes = _load_indexes(store)
    return RecoveryReadContext(
        tasks=indexes.tasks,
        task_by_id=indexes.task_by_id,
        based_on_children=indexes.based_on_children,
        depends_on_children=indexes.depends_on_children,
        root_by_task_id=indexes.root_by_task_id,
        merge_units_by_task_id=indexes.merge_units_by_task_id,
        historical_merge_units_by_task_id=indexes.historical_merge_units_by_task_id,
        allow_reconcile_mutation=False,
    )


def test_dependency_precondition_reads_merge_unit_state(tmp_path: Path) -> None:
    store = SqliteTaskStore(tmp_path / "test.db")

    dependency = store.add("Dependency", task_type="implement")
    store.mark_completed(dependency, has_commits=True, branch="feature/dependency")
    assert dependency.id is not None
    store.set_merge_status(dependency.id, "merged")

    refreshed_dependency = store.get(dependency.id)
    assert refreshed_dependency is not None
    refreshed_dependency.merge_status = None
    store.update(refreshed_dependency)

    downstream = store.add("Downstream", task_type="implement", depends_on=dependency.id)

    assert get_unmerged_dependency_precondition(store, downstream) is None


def test_dependency_precondition_completed_empty_unit_unblocks(tmp_path: Path) -> None:
    store = SqliteTaskStore(tmp_path / "test.db")

    dependency = store.add("Dependency", task_type="implement")
    store.mark_completed(dependency, has_commits=True, branch="feature/dependency-empty")
    assert dependency.id is not None
    unit = store.resolve_merge_unit_for_task(dependency.id)
    assert unit is not None
    store.set_merge_unit_state(unit.id, "empty")

    downstream = store.add("Downstream", task_type="implement", depends_on=dependency.id)

    assert callable(empty_prereq_satisfies_dependency)
    assert get_unmerged_dependency_precondition(store, downstream) is None


def test_dependency_precondition_failed_empty_unit_stays_blocked(tmp_path: Path) -> None:
    store = SqliteTaskStore(tmp_path / "test.db")

    dependency = store.add("Dependency", task_type="implement")
    store.mark_completed(dependency, has_commits=True, branch="feature/dependency-empty-toggle")
    assert dependency.id is not None
    unit = store.resolve_merge_unit_for_task(dependency.id)
    assert unit is not None
    store.set_merge_unit_state(unit.id, "empty")
    dependency = store.get(dependency.id)
    assert dependency is not None
    store.mark_failed(dependency, failure_reason="UNKNOWN")

    downstream = store.add("Downstream", task_type="implement", depends_on=dependency.id)

    readiness = dependency_readiness(store, downstream)
    assert readiness.ready is False
    assert readiness.reason == "failed"
    assert get_unmerged_dependency_precondition(store, downstream) is None


def test_dependency_precondition_failed_merged_unit_unblocks(tmp_path: Path) -> None:
    store = SqliteTaskStore(tmp_path / "test.db")

    dependency = store.add("Dependency", task_type="implement")
    store.mark_completed(dependency, has_commits=True, branch="feature/dependency-merged-toggle")
    assert dependency.id is not None
    unit = store.resolve_merge_unit_for_task(dependency.id)
    assert unit is not None
    store.set_merge_unit_state(unit.id, "merged")
    dependency = store.get(dependency.id)
    assert dependency is not None
    store.mark_failed(dependency, failure_reason="UNKNOWN")

    downstream = store.add("Downstream", task_type="implement", depends_on=dependency.id)

    readiness = dependency_readiness(store, downstream)
    assert readiness.ready is True
    assert readiness.reason == "ready"
    assert readiness.direct_dependency is not None
    assert readiness.direct_dependency.id == dependency.id
    assert readiness.resolved_dependency is not None
    assert readiness.resolved_dependency.id == dependency.id
    assert get_unmerged_dependency_precondition(store, downstream) is None


def test_dependency_precondition_redundant_unit_uses_completed_empty_policy(tmp_path: Path) -> None:
    store = SqliteTaskStore(tmp_path / "test.db")

    dependency = store.add("Dependency", task_type="implement")
    store.mark_completed(dependency, has_commits=True, branch="feature/dependency-redundant")
    assert dependency.id is not None
    unit = store.resolve_merge_unit_for_task(dependency.id)
    assert unit is not None
    store.set_merge_unit_state(unit.id, "redundant")

    downstream = store.add("Downstream", task_type="implement", depends_on=dependency.id)

    assert get_unmerged_dependency_precondition(store, downstream) is None


def test_dependency_readiness_redundant_direct_failed_dependency_without_resolved_descendant_stays_blocked(
    tmp_path: Path,
) -> None:
    store = SqliteTaskStore(tmp_path / "test.db")

    dependency = store.add("Dependency", task_type="implement")
    store.mark_completed(dependency, has_commits=True, branch="feature/dependency-redundant-failed")
    assert dependency.id is not None
    unit = store.resolve_merge_unit_for_task(dependency.id)
    assert unit is not None
    store.set_merge_unit_state(unit.id, "redundant")

    dependency = store.get(dependency.id)
    assert dependency is not None
    store.mark_failed(dependency, failure_reason="UNKNOWN")

    downstream = store.add("Downstream", task_type="implement", depends_on=dependency.id)

    readiness = dependency_readiness(store, downstream)

    assert readiness.ready is False
    assert readiness.reason == "failed"
    assert readiness.direct_dependency is not None
    assert readiness.direct_dependency.id == dependency.id
    assert get_unmerged_dependency_precondition(store, downstream) is None


def test_dependency_precondition_blocks_when_unit_is_unmerged_but_legacy_row_says_merged(tmp_path: Path) -> None:
    store = SqliteTaskStore(tmp_path / "test.db")

    dependency = store.add("Dependency", task_type="implement")
    store.mark_completed(dependency, has_commits=True, branch="feature/dependency")
    assert dependency.id is not None

    refreshed_dependency = store.get(dependency.id)
    assert refreshed_dependency is not None
    refreshed_dependency.merge_status = "merged"
    store.update(refreshed_dependency)

    downstream = store.add("Downstream", task_type="implement", depends_on=dependency.id)

    assert get_unmerged_dependency_precondition(store, downstream).id == dependency.id


def test_dependency_precondition_blocks_when_unit_is_blocked_even_if_legacy_row_says_merged(tmp_path: Path) -> None:
    store = SqliteTaskStore(tmp_path / "test.db")

    dependency = store.add("Dependency", task_type="implement")
    store.mark_completed(dependency, has_commits=True, branch="feature/dependency")
    assert dependency.id is not None
    unit = store.resolve_merge_unit_for_task(dependency.id)
    assert unit is not None
    store.set_merge_unit_state(unit.id, "blocked")

    refreshed_dependency = store.get(dependency.id)
    assert refreshed_dependency is not None
    refreshed_dependency.merge_status = "merged"
    store.update(refreshed_dependency)

    downstream = store.add("Downstream", task_type="implement", depends_on=dependency.id)

    assert get_unmerged_dependency_precondition(store, downstream).id == dependency.id


def test_dependency_precondition_held_plan_unblocks_after_release_helper(tmp_path: Path) -> None:
    store = SqliteTaskStore(tmp_path / "test.db")

    plan = store.add("Held plan", task_type="plan", auto_implement=False)
    assert plan.id is not None
    store.mark_completed(plan)

    downstream = store.add("Downstream", task_type="implement", depends_on=plan.id)

    blocked = dependency_readiness(store, downstream)
    assert blocked.ready is False
    assert blocked.reason == "plan_awaiting_review"

    refreshed_plan = store.get(plan.id)
    assert refreshed_plan is not None
    assert release_held_plan_source(store, refreshed_plan) is True

    ready = dependency_readiness(store, downstream)
    assert ready.ready is True
    assert ready.reason == "ready"


def test_dependency_precondition_uses_canonical_dependency_lineage_merge_unit(tmp_path: Path) -> None:
    store = SqliteTaskStore(tmp_path / "test.db")

    dependency = store.add("Dependency", task_type="implement")
    store.mark_completed(dependency, has_commits=True, branch="feature/dependency-lineage")
    assert dependency.id is not None
    unit = store.resolve_merge_unit_for_task(dependency.id)
    assert unit is not None
    store.set_merge_unit_state(unit.id, "unmerged")

    dependency = store.get(dependency.id)
    assert dependency is not None
    store.mark_failed(dependency, failure_reason="UNKNOWN")

    recovered = store.add("Recovered dependency", task_type="implement", based_on=dependency.id)
    store.mark_completed(recovered, has_commits=True, branch="feature/dependency-lineage-recovered")
    assert recovered.id is not None
    recovered_unit = store.resolve_merge_unit_for_task(recovered.id)
    assert recovered_unit is not None

    downstream = store.add("Downstream", task_type="implement", depends_on=dependency.id)

    blocking_dep = get_unmerged_dependency_precondition(store, downstream)
    assert blocking_dep is not None
    assert blocking_dep.id == recovered.id

    readiness = store.get_dependency_readiness(downstream)
    assert readiness.blocking_merge_unit_id == recovered_unit.id
    assert readiness.blocking_merge_unit_owner_task_id == recovered.id
    assert readiness.blocking_source_branch == "feature/dependency-lineage-recovered"

    store.set_merge_unit_state(recovered_unit.id, "merged")
    assert get_unmerged_dependency_precondition(store, downstream) is None


def test_dependency_readiness_uses_read_context_for_merge_unit_owner(tmp_path: Path, monkeypatch) -> None:
    store = SqliteTaskStore(tmp_path / "test.db")

    dependency = store.add("Dependency", task_type="implement")
    store.mark_completed(dependency, has_commits=True, branch="feature/dependency-read-context")
    assert dependency.id is not None
    unit = store.resolve_merge_unit_for_task(dependency.id)
    assert unit is not None
    store.set_merge_unit_state(unit.id, "unmerged")

    dependency = store.get(dependency.id)
    assert dependency is not None
    store.mark_failed(dependency, failure_reason="UNKNOWN")

    recovered = store.add("Recovered dependency", task_type="implement", based_on=dependency.id)
    store.mark_completed(recovered, has_commits=True, branch="feature/dependency-read-context-recovered")

    downstream = store.add("Downstream", task_type="implement", depends_on=dependency.id)

    expected = store.get_dependency_readiness(downstream)
    read_context = _read_context_for_store(store)

    def _unexpected_store_lookup(*_args, **_kwargs):
        raise AssertionError("store owner lookup should not run when read context is available")

    monkeypatch.setattr(store, "resolve_merge_unit_owner_task", _unexpected_store_lookup)

    actual = dependency_preconditions_module.dependency_readiness(store, downstream, read_context=read_context)

    assert actual == expected


def test_failed_held_plan_dependency_stays_blocked_after_completed_retry_descendant(
    tmp_path: Path,
) -> None:
    store = SqliteTaskStore(tmp_path / "test.db")

    plan = store.add("Held plan", task_type="plan", auto_implement=False)
    assert plan.id is not None
    store.mark_failed(plan, failure_reason="UNKNOWN")

    retry = store.add("Completed retry", task_type="plan", based_on=plan.id)
    assert retry.id is not None
    store.mark_completed(retry, has_commits=False)

    downstream = store.add("Downstream", task_type="implement", depends_on=plan.id)
    assert downstream.id is not None

    readiness = store.get_dependency_readiness(downstream)
    assert readiness.ready is False
    assert readiness.reason == "plan_awaiting_review"
    assert readiness.direct_dependency is not None
    assert readiness.direct_dependency.id == plan.id
    assert readiness.resolved_dependency is not None
    assert readiness.resolved_dependency.id == retry.id
    assert readiness.blocking_task_id == plan.id
    assert readiness.blocking_task_status == "failed"
    assert store.get_pending_pickup() == []

    refreshed_plan = store.get(plan.id)
    assert refreshed_plan is not None
    refreshed_plan.auto_implement = True
    store.update(refreshed_plan)

    released_readiness = store.get_dependency_readiness(downstream)
    assert released_readiness.ready is True
    assert [task.id for task in store.get_pending_pickup()] == [downstream.id]


def test_completed_retry_descendant_with_empty_merge_unit_unblocks_dependency(tmp_path: Path) -> None:
    store = SqliteTaskStore(tmp_path / "test.db")

    dependency = store.add("Failed dependency", task_type="implement")
    assert dependency.id is not None
    store.mark_failed(dependency, failure_reason="UNKNOWN")

    retry = store.add("Completed retry", task_type="implement", based_on=dependency.id, recovery_origin="retry")
    assert retry.id is not None
    store.mark_completed(retry, has_commits=True, branch="feature/completed-empty-retry")
    retry_unit = store.resolve_merge_unit_for_task(retry.id)
    assert retry_unit is not None
    store.set_merge_unit_state(retry_unit.id, "empty")

    downstream = store.add("Downstream", task_type="implement", depends_on=dependency.id)
    assert downstream.id is not None

    readiness = store.get_dependency_readiness(downstream)
    assert readiness.ready is True
    assert readiness.direct_dependency is not None
    assert readiness.direct_dependency.id == dependency.id
    assert readiness.resolved_dependency is not None
    assert readiness.resolved_dependency.id == retry.id
    assert get_unmerged_dependency_precondition(store, downstream) is None


def test_failed_empty_direct_dependency_does_not_use_unmerged_retry_descendant_as_ready(tmp_path: Path) -> None:
    store = SqliteTaskStore(tmp_path / "test.db")

    dependency = store.add("Failed dependency", task_type="implement")
    assert dependency.id is not None
    store.mark_completed(dependency, has_commits=True, branch="feature/direct-empty-blocked")
    dependency_unit = store.resolve_merge_unit_for_task(dependency.id)
    assert dependency_unit is not None
    store.set_merge_unit_state(dependency_unit.id, "empty")
    dependency = store.get(dependency.id)
    assert dependency is not None
    store.mark_failed(dependency, failure_reason="UNKNOWN")

    retry = store.add("Completed retry", task_type="implement", based_on=dependency.id, recovery_origin="retry")
    assert retry.id is not None
    store.mark_completed(retry, has_commits=True, branch="feature/retry-unmerged-blocked")
    retry_unit = store.resolve_merge_unit_for_task(retry.id)
    assert retry_unit is not None

    downstream = store.add("Downstream", task_type="implement", depends_on=dependency.id)
    assert downstream.id is not None

    readiness = dependency_readiness(store, downstream)
    assert readiness.ready is False
    assert readiness.reason == "unmerged"
    assert readiness.resolved_dependency is not None
    assert readiness.resolved_dependency.id == retry.id
    assert readiness.blocking_merge_unit_id == retry_unit.id
    assert readiness.blocking_merge_state == "unmerged"

    blocked = get_unmerged_dependency_precondition(store, downstream)
    assert blocked is not None
    assert blocked.id == retry.id

    indexed_readiness = dependency_preconditions_module.dependency_readiness(
        store,
        downstream,
        read_context=_read_context_for_store(store),
    )
    assert indexed_readiness == readiness


def test_same_branch_dependency_waits_for_completed_predecessor_before_pickup(tmp_path: Path) -> None:
    store = SqliteTaskStore(tmp_path / "test.db")

    first_slice = store.add("First slice", task_type="implement")
    assert first_slice.id is not None
    second_slice = store.add(
        "Second slice",
        task_type="implement",
        depends_on=first_slice.id,
        based_on=first_slice.id,
        same_branch=True,
    )
    assert second_slice.id is not None

    blocked = store.get_dependency_readiness(second_slice)
    assert blocked.ready is False
    assert blocked.reason == "pending"
    assert [task.id for task in store.get_pending_pickup()] == [first_slice.id]

    claim = store.try_mark_in_progress(second_slice.id, 12345)
    assert claim.task is None
    assert claim.refusal_reason == "blocked"
    assert claim.readiness_reason == "pending"
    assert claim.blocking_task_id == first_slice.id

    store.mark_completed(first_slice, has_commits=True, branch="feature/plan-slice-1")

    released = store.get_dependency_readiness(second_slice)
    assert released.ready is True
    assert [task.id for task in store.get_pending_pickup()] == [second_slice.id]
