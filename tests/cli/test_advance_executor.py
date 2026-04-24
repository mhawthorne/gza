"""Tests for shared advance action execution."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest

from gza.cli.advance_executor import AdvanceActionExecutionContext, execute_advance_action
from gza.db import Task as DbTask

from .conftest import make_store, setup_config


def _mark_completed(task: DbTask, *, branch: str | None = None) -> None:
    task.status = "completed"
    task.completed_at = datetime.now(UTC)
    if branch is not None:
        task.branch = branch


@pytest.mark.parametrize(
    ("failure_reason", "session_id", "expected_mode"),
    [
        (None, None, "new"),
        ("MAX_STEPS", "sess-1", "resume"),
        ("TEST_FAILURE", None, "retry"),
    ],
)
def test_improve_dry_run_modes_do_not_mutate_db(
    tmp_path: Path,
    failure_reason: str | None,
    session_id: str | None,
    expected_mode: str,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    impl = store.add("Implement feature", task_type="implement")
    assert impl.id is not None
    _mark_completed(impl, branch="feature/improve-dry-run")
    store.update(impl)
    store.set_merge_status(impl.id, "unmerged")

    review = store.add("Review feature", task_type="review", depends_on=impl.id)
    assert review.id is not None
    _mark_completed(review)
    store.update(review)

    if failure_reason is not None:
        failed = store.add(
            "Improve attempt",
            task_type="improve",
            depends_on=review.id,
            based_on=impl.id,
            same_branch=True,
        )
        assert failed.id is not None
        failed.status = "failed"
        failed.failure_reason = failure_reason
        failed.session_id = session_id
        failed.completed_at = datetime.now(UTC)
        store.update(failed)

    before_count = len(store.get_all())
    context = AdvanceActionExecutionContext(
        store=store,
        dry_run=True,
        max_resume_attempts=3,
        use_iterate_for_create_implement=False,
        use_iterate_for_needs_rebase=False,
        prepare_create_review=lambda _task: pytest.fail("create_review should not run in dry-run"),
        create_resume_task=lambda _task: pytest.fail("create_resume should not run in dry-run"),
        create_rebase_task=lambda _task: pytest.fail("create_rebase should not run in dry-run"),
        create_implement_task=lambda _task: pytest.fail("create_implement should not run in dry-run"),
        spawn_worker=lambda _task_id, _kind: pytest.fail("spawn_worker should not run in dry-run"),
        spawn_resume_worker=lambda _task_id, _kind: pytest.fail("spawn_resume should not run in dry-run"),
        spawn_iterate_worker=lambda _task, _kind: pytest.fail("spawn_iterate should not run in dry-run"),
    )

    result = execute_advance_action(
        task=impl,
        action={"type": "improve", "review_task": review, "description": "Create improve"},
        context=context,
    )

    assert result.status == "dry_run"
    assert result.improve_mode == expected_mode
    assert result.worker_consuming is True
    assert result.work_done is True
    assert len(store.get_all()) == before_count


def test_improve_give_up_returns_skip_without_mutation(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    impl = store.add("Implement feature", task_type="implement")
    assert impl.id is not None
    _mark_completed(impl, branch="feature/improve-cap")
    store.update(impl)
    store.set_merge_status(impl.id, "unmerged")

    review = store.add("Review feature", task_type="review", depends_on=impl.id)
    assert review.id is not None
    _mark_completed(review)
    store.update(review)

    prev_id = impl.id
    for idx in range(2):
        improve = store.add(
            f"Improve {idx}",
            task_type="improve",
            depends_on=review.id,
            based_on=prev_id,
            same_branch=True,
        )
        assert improve.id is not None
        improve.status = "failed"
        improve.failure_reason = "MAX_STEPS"
        improve.session_id = f"sess-{idx}"
        improve.completed_at = datetime.now(UTC)
        store.update(improve)
        prev_id = improve.id

    before_count = len(store.get_all())
    context = AdvanceActionExecutionContext(
        store=store,
        dry_run=False,
        max_resume_attempts=1,
        use_iterate_for_create_implement=False,
        use_iterate_for_needs_rebase=False,
        prepare_create_review=lambda _task: pytest.fail("unused"),
        create_resume_task=lambda _task: pytest.fail("unused"),
        create_rebase_task=lambda _task: pytest.fail("unused"),
        create_implement_task=lambda _task: pytest.fail("unused"),
        spawn_worker=lambda _task_id, _kind: pytest.fail("unused"),
        spawn_resume_worker=lambda _task_id, _kind: pytest.fail("unused"),
        spawn_iterate_worker=lambda _task, _kind: pytest.fail("unused"),
    )

    result = execute_advance_action(
        task=impl,
        action={"type": "improve", "review_task": review},
        context=context,
    )

    assert result.status == "skip"
    assert result.attention_type == "max_improve_attempts"
    assert "max improve attempts" in result.message
    assert len(store.get_all()) == before_count


def test_create_review_skip_propagates_message_without_spawning(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    task = store.add("Implement feature", task_type="implement")
    assert task.id is not None
    _mark_completed(task, branch="feature/create-review-skip")
    store.update(task)

    context = AdvanceActionExecutionContext(
        store=store,
        dry_run=False,
        max_resume_attempts=1,
        use_iterate_for_create_implement=False,
        use_iterate_for_needs_rebase=False,
        prepare_create_review=lambda _task: type(
            "_R",
            (),
            {"status": "skip", "review_task": None, "message": "SKIP: review already pending"},
        )(),
        create_resume_task=lambda _task: pytest.fail("unused"),
        create_rebase_task=lambda _task: pytest.fail("unused"),
        create_implement_task=lambda _task: pytest.fail("unused"),
        spawn_worker=lambda _task_id, _kind: pytest.fail("spawn should not run"),
        spawn_resume_worker=lambda _task_id, _kind: pytest.fail("unused"),
        spawn_iterate_worker=lambda _task, _kind: pytest.fail("unused"),
    )

    result = execute_advance_action(task=task, action={"type": "create_review"}, context=context)

    assert result.status == "skip"
    assert result.message == "SKIP: review already pending"


def test_create_implement_uses_shared_lineage_and_selected_spawn_path(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    plan = store.add("Plan feature", task_type="plan")
    assert plan.id is not None
    _mark_completed(plan)
    store.update(plan)

    spawned: dict[str, int] = {"worker": 0, "iterate": 0}

    def _create_implement(parent: DbTask) -> DbTask:
        assert parent.id is not None
        return store.add(
            prompt=f"Implement plan {parent.id}",
            task_type="implement",
            depends_on=parent.id,
            group=parent.group,
        )

    context = AdvanceActionExecutionContext(
        store=store,
        dry_run=False,
        max_resume_attempts=1,
        use_iterate_for_create_implement=True,
        use_iterate_for_needs_rebase=False,
        prepare_create_review=lambda _task: pytest.fail("unused"),
        create_resume_task=lambda _task: pytest.fail("unused"),
        create_rebase_task=lambda _task: pytest.fail("unused"),
        create_implement_task=_create_implement,
        spawn_worker=lambda _task_id, _kind: spawned.__setitem__("worker", spawned["worker"] + 1) or 0,
        spawn_resume_worker=lambda _task_id, _kind: pytest.fail("unused"),
        spawn_iterate_worker=lambda _task, _kind: spawned.__setitem__("iterate", spawned["iterate"] + 1) or 0,
    )

    result = execute_advance_action(task=plan, action={"type": "create_implement"}, context=context)

    assert result.status == "success"
    assert result.created_task is not None
    assert result.created_task.depends_on == plan.id
    assert spawned["iterate"] == 1
    assert spawned["worker"] == 0


def test_needs_rebase_dry_run_does_not_create_task(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    task = store.add("Implement feature", task_type="implement")
    assert task.id is not None
    _mark_completed(task, branch="feature/rebase-dry-run")
    store.update(task)

    before_count = len(store.get_all())
    context = AdvanceActionExecutionContext(
        store=store,
        dry_run=True,
        max_resume_attempts=1,
        use_iterate_for_create_implement=False,
        use_iterate_for_needs_rebase=False,
        prepare_create_review=lambda _task: pytest.fail("unused"),
        create_resume_task=lambda _task: pytest.fail("unused"),
        create_rebase_task=lambda _task: pytest.fail("should not create rebase task in dry-run"),
        create_implement_task=lambda _task: pytest.fail("unused"),
        spawn_worker=lambda _task_id, _kind: pytest.fail("unused"),
        spawn_resume_worker=lambda _task_id, _kind: pytest.fail("unused"),
        spawn_iterate_worker=lambda _task, _kind: pytest.fail("unused"),
    )

    result = execute_advance_action(task=task, action={"type": "needs_rebase"}, context=context)

    assert result.status == "dry_run"
    assert result.worker_consuming is True
    assert len(store.get_all()) == before_count
