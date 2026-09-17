"""Tests for gza.lineage helpers."""
from datetime import UTC, datetime
from pathlib import Path

from gza.db import SqliteTaskStore
from gza.lineage import get_plan_for_task, get_root_impl, resolve_impl_task, walk_lineage_descendants
from gza.lineage_view import LineageView


def test_get_plan_for_task_finds_plan_through_retry_chain(tmp_path: Path) -> None:
    store = SqliteTaskStore(tmp_path / "test.db")
    plan = store.add("Plan feature", task_type="plan")
    impl = store.add("Implement feature", task_type="implement", depends_on=plan.id)
    retry = store.add("Retry implementation", task_type="implement", based_on=impl.id)

    found = get_plan_for_task(store, retry)
    assert found is not None
    assert found.id == plan.id


def test_get_plan_for_task_finds_direct_depends_on_plan(tmp_path: Path) -> None:
    store = SqliteTaskStore(tmp_path / "test.db")
    plan = store.add("Plan feature", task_type="plan")
    impl = store.add("Implement feature", task_type="implement", depends_on=plan.id)

    found = get_plan_for_task(store, impl)
    assert found is not None
    assert found.id == plan.id


def test_get_plan_for_task_finds_direct_based_on_plan_for_transition(tmp_path: Path) -> None:
    store = SqliteTaskStore(tmp_path / "test.db")
    plan = store.add("Plan feature", task_type="plan")
    impl = store.add("Implement feature", task_type="implement", based_on=plan.id)

    found = get_plan_for_task(store, impl)
    assert found is not None
    assert found.id == plan.id


def test_get_plan_for_task_prefers_nested_depends_on_plan_before_direct_based_on_plan(tmp_path: Path) -> None:
    store = SqliteTaskStore(tmp_path / "test.db")
    nested_plan = store.add("Nested plan", task_type="plan")
    upstream_impl = store.add("Upstream implementation", task_type="implement", depends_on=nested_plan.id)
    direct_plan = store.add("Direct transition plan", task_type="plan")
    impl = store.add(
        "Implementation with competing plan ancestors",
        task_type="implement",
        based_on=direct_plan.id,
        depends_on=upstream_impl.id,
    )

    found = get_plan_for_task(store, impl)

    assert found is not None
    assert found.id == nested_plan.id


def test_get_plan_for_task_cycle_guard(tmp_path: Path) -> None:
    store = SqliteTaskStore(tmp_path / "test.db")
    first = store.add("First implementation", task_type="implement")
    second = store.add("Second implementation", task_type="implement", based_on=first.id)
    first.based_on = second.id
    store.update(first)

    assert get_plan_for_task(store, first) is None


def test_get_root_impl_returns_oldest_retry_ancestor(tmp_path: Path) -> None:
    store = SqliteTaskStore(tmp_path / "test.db")
    root = store.add("Initial implementation", task_type="implement")
    retry1 = store.add("Retry 1", task_type="implement", based_on=root.id)
    retry2 = store.add("Retry 2", task_type="implement", based_on=retry1.id)
    retry3 = store.add("Retry 3", task_type="implement", based_on=retry2.id)

    assert get_root_impl(store, retry3).id == root.id


def test_walk_lineage_descendants_follows_both_based_on_and_depends_on_links(tmp_path: Path) -> None:
    store = SqliteTaskStore(tmp_path / "test.db")
    root = store.add("Root plan", task_type="plan")
    based_child = store.add("Based child", task_type="implement", based_on=root.id)
    depends_child = store.add("Depends child", task_type="implement", depends_on=root.id)
    grandchild = store.add("Grandchild", task_type="review", depends_on=depends_child.id)

    descendants = list(walk_lineage_descendants(store, root))
    descendant_ids = {task.id for task in descendants}

    assert based_child.id in descendant_ids
    assert depends_child.id in descendant_ids
    assert grandchild.id in descendant_ids




def test_resolve_impl_task_review_error_paths(tmp_path: Path) -> None:
    store = SqliteTaskStore(tmp_path / "test.db")
    review_no_dep = store.add("Review no dep", task_type="review")
    plan = store.add("Plan", task_type="plan")
    review_wrong_parent = store.add("Review wrong parent", task_type="review", depends_on=plan.id)

    resolved, err = resolve_impl_task(store, review_no_dep.id)
    assert resolved is None
    assert err == f"Review task {review_no_dep.id} has no depends_on implementation task"

    resolved, err = resolve_impl_task(store, review_wrong_parent.id)
    assert resolved is None
    assert err == (
        f"Review task {review_wrong_parent.id} points to task {plan.id}, "
        "which is not an implementation task"
    )


def test_resolve_impl_task_improve_error_paths(tmp_path: Path) -> None:
    store = SqliteTaskStore(tmp_path / "test.db")
    improve_no_parent = store.add("Improve no parent", task_type="improve")
    plan = store.add("Plan", task_type="plan")
    improve_wrong_parent = store.add("Improve wrong parent", task_type="improve", based_on=plan.id)

    resolved, err = resolve_impl_task(store, improve_no_parent.id)
    assert resolved is None
    assert err == f"Improve task {improve_no_parent.id} has no based_on implementation task"

    resolved, err = resolve_impl_task(store, improve_wrong_parent.id)
    assert resolved is None
    assert err == (
        f"Improve task {improve_wrong_parent.id} points to task {plan.id}, "
        "which is not an implementation task"
    )


def test_lineage_view_owner_prefers_completed_reattempt_over_failed_original(tmp_path: Path) -> None:
    store = SqliteTaskStore(tmp_path / "test.db")

    original = store.add("Original implementation", task_type="implement")
    assert original.id is not None
    original.branch = "feature/lineage-owner"
    original.status = "failed"
    original.completed_at = datetime(2026, 7, 5, 10, 0, tzinfo=UTC)
    store.update(original)

    reattempt = store.add("Completed reattempt", task_type="implement", based_on=original.id)
    assert reattempt.id is not None
    reattempt.branch = original.branch
    reattempt.status = "completed"
    reattempt.completed_at = datetime(2026, 7, 5, 11, 0, tzinfo=UTC)
    store.update(reattempt)

    owner = LineageView(store, original).owner()

    assert owner is not None
    assert owner.id == reattempt.id




def test_lineage_view_original_latest_and_all_sort_by_event_time(tmp_path: Path) -> None:
    store = SqliteTaskStore(tmp_path / "test.db")

    original = store.add("Original implementation", task_type="implement")
    assert original.id is not None
    original.branch = "feature/lineage-order"
    original.status = "failed"
    original.completed_at = datetime(2026, 7, 5, 10, 0, tzinfo=UTC)
    store.update(original)

    review = store.add("First review", task_type="review", based_on=original.id, depends_on=original.id)
    assert review.id is not None
    review.status = "completed"
    review.completed_at = datetime(2026, 7, 5, 10, 30, tzinfo=UTC)
    store.update(review)

    reattempt = store.add("Completed reattempt", task_type="implement", based_on=original.id)
    assert reattempt.id is not None
    reattempt.branch = original.branch
    reattempt.status = "completed"
    reattempt.completed_at = datetime(2026, 7, 5, 11, 0, tzinfo=UTC)
    store.update(reattempt)

    followup_review = store.add("Follow-up review", task_type="review", based_on=reattempt.id, depends_on=reattempt.id)
    assert followup_review.id is not None
    followup_review.status = "completed"
    followup_review.completed_at = datetime(2026, 7, 5, 11, 30, tzinfo=UTC)
    store.update(followup_review)

    view = LineageView(store, reattempt)

    assert view.original() is not None
    assert view.original().id == original.id
    assert view.latest() is not None
    assert view.latest().id == followup_review.id
    assert [task.id for task in view.all("review")] == [review.id, followup_review.id]
