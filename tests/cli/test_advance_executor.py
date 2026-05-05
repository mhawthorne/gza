"""Tests for shared advance action execution."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest

from gza.cli._common import resolve_improve_action
from gza.cli.advance_executor import (
    AdvanceActionExecutionContext,
    build_improve_needs_attention_result,
    execute_advance_action,
    resolve_execution_needs_attention,
)
from gza.db import Task as DbTask
from gza.recovery_engine import decide_failed_task_recovery

from .conftest import make_store, setup_config


def _mark_completed(task: DbTask, *, branch: str | None = None) -> None:
    task.status = "completed"
    task.completed_at = datetime.now(UTC)
    if branch is not None:
        task.branch = branch


@pytest.mark.parametrize(
    ("failure_reason", "session_id", "expected_mode", "expected_status"),
    [
        (None, None, "new", "dry_run"),
        ("MAX_STEPS", "sess-1", "resume", "dry_run"),
        ("TEST_FAILURE", None, "manual_review", "skip"),
    ],
)
def test_improve_dry_run_modes_do_not_mutate_db(
    tmp_path: Path,
    failure_reason: str | None,
    session_id: str | None,
    expected_mode: str,
    expected_status: str,
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

    assert result.status == expected_status
    assert result.improve_mode == expected_mode
    if expected_status == "dry_run":
        assert result.worker_consuming is True
        assert result.work_done is True
    else:
        assert result.attention_type == "manual_review_required"
    assert len(store.get_all()) == before_count


def test_improve_manual_review_returns_skip_without_mutation(tmp_path: Path) -> None:
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

    first = store.add(
        "Improve 0",
        task_type="improve",
        depends_on=review.id,
        based_on=impl.id,
        same_branch=True,
    )
    assert first.id is not None
    first.status = "failed"
    first.failure_reason = "MAX_STEPS"
    first.session_id = "sess-0"
    first.completed_at = datetime.now(UTC)
    store.update(first)

    second = store.add(
        first.prompt,
        task_type="improve",
        depends_on=review.id,
        based_on=first.id,
        same_branch=True,
    )
    assert second.id is not None
    second.status = "failed"
    second.failure_reason = "INFRASTRUCTURE_ERROR"
    second.session_id = first.session_id
    second.completed_at = datetime.now(UTC)
    store.update(second)

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
    improve_mode, failed_improve, improve_decision = resolve_improve_action(
        store,
        impl.id,
        review.id,
        max_resume_attempts=1,
    )
    expected = build_improve_needs_attention_result(
        store=store,
        impl_task=impl,
        review_task=review,
        improve_mode=improve_mode,
        failed_improve=failed_improve,
        improve_decision=improve_decision,
        max_resume_attempts=1,
    )

    assert expected is not None
    assert result == expected
    assert len(store.get_all()) == before_count


@pytest.mark.parametrize(
    ("reason_code", "reason_text"),
    [
        ("dependency_not_ready", "dependency precondition not satisfied"),
        ("recovery_already_running", "recovery child already in progress"),
    ],
)
def test_improve_skip_without_attention_for_shared_non_attention_recovery_reasons(
    tmp_path: Path,
    reason_code: str,
    reason_text: str,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    impl = store.add("Implement feature", task_type="implement")
    assert impl.id is not None
    _mark_completed(impl, branch="feature/improve-shared-skip")
    store.update(impl)
    store.set_merge_status(impl.id, "unmerged")

    review = store.add("Review feature", task_type="review", depends_on=impl.id)
    assert review.id is not None
    _mark_completed(review)
    store.update(review)

    if reason_code == "dependency_not_ready":
        dependency = store.add("Dependency", task_type="implement")
        assert dependency.id is not None
        _mark_completed(dependency, branch="feature/dependency")
        dependency.merge_status = "unmerged"
        store.update(dependency)

        failed_improve = store.add(
            "Improve attempt",
            task_type="improve",
            depends_on=dependency.id,
            based_on=impl.id,
        )
        assert failed_improve.id is not None
        failed_improve.status = "failed"
        failed_improve.failure_reason = "PREREQUISITE_UNMERGED"
        failed_improve.completed_at = datetime.now(UTC)
        store.update(failed_improve)
    else:
        failed_improve = store.add(
            "Improve attempt",
            task_type="improve",
            depends_on=review.id,
            based_on=impl.id,
            same_branch=True,
        )
        assert failed_improve.id is not None
        failed_improve.status = "failed"
        failed_improve.failure_reason = "MAX_TURNS"
        failed_improve.session_id = "sess-improve"
        failed_improve.completed_at = datetime.now(UTC)
        store.update(failed_improve)

        running_child = store.add(
            failed_improve.prompt,
            task_type="improve",
            based_on=failed_improve.id,
            depends_on=failed_improve.depends_on,
            same_branch=failed_improve.same_branch,
        )
        assert running_child.id is not None
        running_child.status = "in_progress"
        running_child.session_id = failed_improve.session_id
        store.update(running_child)

    improve_decision = decide_failed_task_recovery(
        store,
        failed_improve,
        max_recovery_attempts=1,
    )
    assert improve_decision.reason_code == reason_code

    result = build_improve_needs_attention_result(
        store=store,
        impl_task=impl,
        review_task=review,
        improve_mode="manual_review",
        failed_improve=failed_improve,
        improve_decision=improve_decision,
        max_resume_attempts=1,
    )

    assert result is not None
    assert result.status == "skip"
    assert result.attention_type is None
    assert result.attention_reason is None
    assert reason_text in result.message
    assert resolve_execution_needs_attention(impl, result) is None


def test_improve_give_up_reports_automatic_recovery_disabled(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    impl = store.add("Implement feature", task_type="implement")
    assert impl.id is not None
    _mark_completed(impl, branch="feature/improve-disabled")
    store.update(impl)
    store.set_merge_status(impl.id, "unmerged")

    review = store.add("Review feature", task_type="review", depends_on=impl.id)
    assert review.id is not None
    _mark_completed(review)
    store.update(review)

    failed = store.add(
        "Improve 0",
        task_type="improve",
        depends_on=review.id,
        based_on=impl.id,
        same_branch=True,
    )
    assert failed.id is not None
    failed.status = "failed"
    failed.failure_reason = "MAX_TURNS"
    failed.session_id = "sess-0"
    failed.completed_at = datetime.now(UTC)
    store.update(failed)

    before_count = len(store.get_all())
    context = AdvanceActionExecutionContext(
        store=store,
        dry_run=False,
        max_resume_attempts=0,
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
    expected = build_improve_needs_attention_result(
        store=store,
        impl_task=impl,
        review_task=review,
        improve_mode="give_up",
        failed_improve=failed,
        improve_decision=None,
        max_resume_attempts=0,
    )

    assert expected is not None
    assert result.status == "skip"
    assert result.attention_type == "automatic_recovery_disabled"
    assert result == expected
    assert len(store.get_all()) == before_count


def test_improve_retry_preserves_review_backed_execution_settings(tmp_path: Path) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    impl = store.add("Implement feature", task_type="implement")
    assert impl.id is not None
    _mark_completed(impl, branch="feature/improve-retry-preserve")
    store.update(impl)
    store.set_merge_status(impl.id, "unmerged")

    review = store.add("Review feature", task_type="review", depends_on=impl.id)
    assert review.id is not None
    _mark_completed(review)
    review.output_content = "**Verdict: CHANGES_REQUESTED**"
    store.update(review)

    failed = store.add(
        "Improve attempt",
        task_type="improve",
        depends_on=review.id,
        based_on=impl.id,
        same_branch=True,
    )
    assert failed.id is not None
    failed.status = "failed"
    failed.failure_reason = "INFRASTRUCTURE_ERROR"
    failed.create_review = True
    failed.create_pr = True
    failed.model = "gpt-5.4"
    failed.provider = "codex"
    failed.provider_is_explicit = True
    failed.completed_at = datetime.now(UTC)
    store.update(failed)

    spawned: list[tuple[str, str]] = []
    context = AdvanceActionExecutionContext(
        store=store,
        dry_run=False,
        max_resume_attempts=3,
        use_iterate_for_create_implement=False,
        use_iterate_for_needs_rebase=False,
        prepare_create_review=lambda _task: pytest.fail("unused"),
        create_resume_task=lambda _task: pytest.fail("unused"),
        create_rebase_task=lambda _task: pytest.fail("unused"),
        create_implement_task=lambda _task: pytest.fail("unused"),
        spawn_worker=lambda task_id, kind: spawned.append((task_id, kind)) or 0,
        spawn_resume_worker=lambda _task_id, _kind: pytest.fail("unused"),
        spawn_iterate_worker=lambda _task, _kind: pytest.fail("unused"),
    )

    result = execute_advance_action(
        task=impl,
        action={"type": "improve", "review_task": review},
        context=context,
    )

    assert result.status == "success"
    assert result.improve_mode == "retry"
    assert result.created_task is not None
    assert result.created_task.id is not None
    assert result.created_task.id != failed.id
    assert result.created_task.based_on == failed.id
    assert result.created_task.create_review is True
    assert result.created_task.create_pr is True
    assert result.created_task.model == "gpt-5.4"
    assert result.created_task.provider == "codex"
    assert result.created_task.provider_is_explicit is True
    assert spawned == [(result.created_task.id, "improve")]


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


@pytest.mark.parametrize(
    ("action_type", "expected_message"),
    [
        ("resume", "Reused pending resume task"),
        ("retry", "Reused pending retry task"),
    ],
)
def test_reused_failed_task_recovery_reports_reuse_message(
    tmp_path: Path,
    action_type: str,
    expected_message: str,
) -> None:
    setup_config(tmp_path)
    store = make_store(tmp_path)

    failed = store.add("Failed task", task_type="plan")
    assert failed.id is not None
    failed.status = "failed"
    failed.failure_reason = "MAX_TURNS" if action_type == "resume" else "INFRASTRUCTURE_ERROR"
    failed.session_id = "sess-1" if action_type == "resume" else None
    failed.completed_at = datetime.now(UTC)
    store.update(failed)

    reused = store.add("Pending recovery task", task_type=failed.task_type, based_on=failed.id)
    assert reused.id is not None
    reused.status = "pending"
    if action_type == "resume":
        reused.depends_on = failed.depends_on
        reused.session_id = failed.session_id
        reused.spec = failed.spec
        reused.branch = failed.branch
    store.update(reused)

    spawned: list[tuple[str, str]] = []
    context = AdvanceActionExecutionContext(
        store=store,
        dry_run=False,
        max_resume_attempts=1,
        use_iterate_for_create_implement=False,
        use_iterate_for_needs_rebase=False,
        prepare_create_review=lambda _task: pytest.fail("unused"),
        create_resume_task=lambda _task: pytest.fail("should reuse existing task"),
        create_rebase_task=lambda _task: pytest.fail("unused"),
        create_implement_task=lambda _task: pytest.fail("unused"),
        spawn_worker=lambda task_id, kind: spawned.append((task_id, kind)) or 0,
        spawn_resume_worker=lambda task_id, kind: spawned.append((task_id, kind)) or 0,
        spawn_iterate_worker=lambda _task, _kind: pytest.fail("unused"),
        create_retry_task=lambda _task: pytest.fail("should reuse existing task"),
    )

    result = execute_advance_action(
        task=failed,
        action={
            "type": action_type,
            "launch_mode": "worker",
            "recovery_task_id": reused.id,
            "reuse_existing": True,
        },
        context=context,
    )

    assert result.status == "success"
    assert result.success_message == f"{expected_message} {reused.id}"
    assert result.created_task is not None
    assert result.created_task.id == reused.id
    expected_kind = failed.task_type or "task"
    assert spawned == [(reused.id, expected_kind)]


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
