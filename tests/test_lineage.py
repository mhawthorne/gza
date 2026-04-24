"""Tests for gza.lineage helpers."""

from pathlib import Path

from gza.db import SqliteTaskStore
from gza.lineage import get_plan_for_task, get_root_impl, resolve_impl_task


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


def test_resolve_impl_task_for_implement_review_improve_and_fix(tmp_path: Path) -> None:
    store = SqliteTaskStore(tmp_path / "test.db")
    impl = store.add("Implementation", task_type="implement")
    review = store.add("Review", task_type="review", depends_on=impl.id)
    improve1 = store.add("Improve 1", task_type="improve", based_on=impl.id, depends_on=review.id)
    improve2 = store.add("Improve 2", task_type="improve", based_on=improve1.id, depends_on=review.id)
    fix = store.add("Fix", task_type="fix", based_on=improve2.id, depends_on=review.id)

    resolved_impl, err = resolve_impl_task(store, impl.id)
    assert err is None
    assert resolved_impl is not None
    assert resolved_impl.id == impl.id

    resolved_review, err = resolve_impl_task(store, review.id)
    assert err is None
    assert resolved_review is not None
    assert resolved_review.id == impl.id

    resolved_improve, err = resolve_impl_task(store, improve2.id)
    assert err is None
    assert resolved_improve is not None
    assert resolved_improve.id == impl.id

    resolved_fix, err = resolve_impl_task(store, fix.id)
    assert err is None
    assert resolved_fix is not None
    assert resolved_fix.id == impl.id


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
