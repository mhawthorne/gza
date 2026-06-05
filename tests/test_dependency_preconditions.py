from pathlib import Path

from gza import dependency_preconditions as dependency_preconditions_module
from gza.dependency_preconditions import (
    empty_prereq_satisfies_dependency,
    get_unmerged_dependency_precondition,
)
from gza.db import SqliteTaskStore


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


def test_dependency_precondition_empty_unit_blocks_by_default(tmp_path: Path) -> None:
    store = SqliteTaskStore(tmp_path / "test.db")

    dependency = store.add("Dependency", task_type="implement")
    store.mark_completed(dependency, has_commits=True, branch="feature/dependency-empty")
    assert dependency.id is not None
    unit = store.resolve_merge_unit_for_task(dependency.id)
    assert unit is not None
    store.set_merge_unit_state(unit.id, "empty")

    downstream = store.add("Downstream", task_type="implement", depends_on=dependency.id)

    assert callable(empty_prereq_satisfies_dependency)
    assert get_unmerged_dependency_precondition(store, downstream).id == dependency.id


def test_dependency_precondition_empty_policy_can_unblock_dependency(
    tmp_path: Path, monkeypatch
) -> None:
    store = SqliteTaskStore(tmp_path / "test.db")

    dependency = store.add("Dependency", task_type="implement")
    store.mark_completed(dependency, has_commits=True, branch="feature/dependency-empty-toggle")
    assert dependency.id is not None
    unit = store.resolve_merge_unit_for_task(dependency.id)
    assert unit is not None
    store.set_merge_unit_state(unit.id, "empty")

    downstream = store.add("Downstream", task_type="implement", depends_on=dependency.id)

    monkeypatch.setattr(
        dependency_preconditions_module,
        "empty_prereq_satisfies_dependency",
        lambda _store, _prereq, _dependent: True,
    )

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

    downstream = store.add("Downstream", task_type="implement", depends_on=dependency.id)

    blocking_dep = get_unmerged_dependency_precondition(store, downstream)
    assert blocking_dep is not None
    assert blocking_dep.id == recovered.id

    readiness = store.get_dependency_readiness(downstream)
    assert readiness.blocking_merge_unit_id == unit.id
    assert readiness.blocking_merge_unit_owner_task_id == dependency.id
    assert readiness.blocking_source_branch == "feature/dependency-lineage"

    store.set_merge_unit_state(unit.id, "merged")
    assert get_unmerged_dependency_precondition(store, downstream) is None


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
