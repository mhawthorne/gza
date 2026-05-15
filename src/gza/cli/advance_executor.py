"""Shared per-action execution for advance-style lifecycle commands."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Literal, Protocol

from ..db import SqliteTaskStore, Task as DbTask
from ..recovery_engine import FailedRecoveryDecision, get_failed_recovery_needs_attention_reason
from ._common import _create_improve_task, _create_retry_task, resolve_improve_action
from .advance_engine import (
    classify_advance_action,
    failed_recovery_decision_to_attention_action,
)


class CreateReviewActionResult(Protocol):
    """Duck type returned by create-review preparation helpers."""

    status: str
    review_task: DbTask | None
    message: str


@dataclass
class AdvanceActionExecutionContext:
    """Execution dependencies for a single advance action."""

    store: SqliteTaskStore
    dry_run: bool
    max_resume_attempts: int
    use_iterate_for_create_implement: bool
    use_iterate_for_needs_rebase: bool
    prepare_task_for_background_start: Callable[[DbTask, bool], DbTask | None]
    prepare_create_review: Callable[[DbTask], CreateReviewActionResult]
    create_resume_task: Callable[[DbTask], DbTask]
    create_rebase_task: Callable[[DbTask], DbTask]
    create_implement_task: Callable[[DbTask], DbTask]
    spawn_worker: Callable[[DbTask, str], int]
    spawn_resume_worker: Callable[[DbTask, str], int]
    # Signature: (task, kind, *, prepared_task=None, prepared_phase=None,
    # prepared_action_type=None) -> int. Callers that route through iterate without
    # a parent-prepared child (e.g. _maybe_route_action_through_iterate,
    # create_implement) omit the kwargs; the needs_rebase iterate path passes
    # prepared_task=<rebase child> so worker metadata points at the prepared row.
    spawn_iterate_worker: Callable[..., int]
    is_rebase_target_already_merged: Callable[[DbTask], bool] | None = None
    prefer_iterate_for_action: Callable[
        [DbTask, dict[str, Any]],
        DbTask | AdvanceActionExecutionResult | None,
    ] | None = None
    spawn_iterate_recovery: Callable[[DbTask, Literal["resume", "retry"], DbTask], int] | None = None
    create_retry_task: Callable[[DbTask], DbTask] | None = None


@dataclass
class AdvanceActionExecutionResult:
    """Structured outcome for one advance action execution attempt."""

    action_type: str
    status: Literal["success", "skip", "error", "dry_run", "unsupported"]
    message: str = ""
    success_message: str = ""
    error_message: str = ""
    worker_consuming: bool = False
    attempted_spawn: bool = False
    worker_started: bool = False
    work_done: bool = False
    handled_task_id: str | None = None
    created_task: DbTask | None = None
    improve_mode: str | None = None
    failed_improve: DbTask | None = None
    attention_type: str | None = None
    attention_reason: str | None = None
    worker_label: str | None = None
    guarded_pending_task_id: str | None = None


@dataclass(frozen=True)
class AdvanceExecutionNeedsAttention:
    """Normalized execution-time needs-attention payload for shared renderers."""

    task: DbTask
    action: dict[str, Any]


_WORKER_ACTIONS = frozenset(
    {
        "create_review",
        "run_review",
        "improve",
        "run_improve",
        "resume",
        "retry",
        "create_implement",
        "needs_rebase",
    }
)


def build_improve_needs_attention_result(
    *,
    store: SqliteTaskStore,
    impl_task: DbTask,
    review_task: DbTask,
    improve_mode: str,
    failed_improve: DbTask | None,
    improve_decision: FailedRecoveryDecision | None,
    max_resume_attempts: int,
) -> AdvanceActionExecutionResult | None:
    """Build the shared improve-stop outcome used by advance/watch/iterate."""
    if failed_improve is None:
        return None

    attention_reason = get_failed_recovery_needs_attention_reason(
        store,
        failed_improve,
        decision=improve_decision,
        max_recovery_attempts=max_resume_attempts,
    )
    attention_type: str | None = None
    message = ""
    if improve_mode == "give_up":
        attention_type = "automatic_recovery_disabled"
        message = (
            "SKIP: automatic improve recovery is disabled "
            f"(max_resume_attempts={max_resume_attempts}) for "
            f"{impl_task.id} + {review_task.id}; latest failed improve: {failed_improve.id}. "
            f"Run uv run gza fix {impl_task.id}"
        )
    elif improve_mode == "manual_review":
        if improve_decision is None:
            return None
        if attention_reason is None:
            message = (
                f"SKIP: latest failed improve {failed_improve.id}: "
                f"{improve_decision.reason_text}"
            )
        else:
            attention_type = "manual_review_required"
            message = (
                f"SKIP: latest failed improve {failed_improve.id} requires manual review "
                f"({improve_decision.reason_text})"
            )
    else:
        return None

    return AdvanceActionExecutionResult(
        action_type="improve",
        status="skip",
        message=message,
        improve_mode=improve_mode,
        failed_improve=failed_improve,
        attention_type=attention_type,
        attention_reason=attention_reason,
    )


def build_failed_recovery_needs_attention_result(
    *,
    store: SqliteTaskStore,
    failed_task: DbTask,
    recovery_decision: FailedRecoveryDecision,
    max_resume_attempts: int,
) -> AdvanceActionExecutionResult | None:
    """Build shared execution-time attention output for terminal recovery stops."""
    attention_action = failed_recovery_decision_to_attention_action(
        store,
        failed_task,
        recovery_decision,
        max_recovery_attempts=max_resume_attempts,
    )
    if attention_action is None:
        return None

    if recovery_decision.reason_code == "automatic_recovery_disabled":
        attention_type = "automatic_recovery_disabled"
    else:
        attention_type = "manual_review_required"
    attention_reason = attention_action.get("needs_attention_reason")
    if not isinstance(attention_reason, str) or not attention_reason:
        return None
    task_type = failed_task.task_type or "task"

    return AdvanceActionExecutionResult(
        action_type=task_type,
        status="skip",
        message=str(attention_action.get("description", "")),
        failed_improve=failed_task if failed_task.task_type == "improve" else None,
        attention_type=attention_type,
        attention_reason=attention_reason,
    )


def resolve_execution_needs_attention(
    task: DbTask,
    result: AdvanceActionExecutionResult,
) -> AdvanceExecutionNeedsAttention | None:
    """Convert execution-time skip outcomes into shared needs-attention rows."""
    action = {
        "type": result.attention_type or "skip",
        "description": result.message,
        "needs_attention_reason": result.attention_reason,
    }
    if classify_advance_action(action) != "needs_attention":
        return None
    return AdvanceExecutionNeedsAttention(
        task=result.failed_improve or task,
        action=action,
    )


def _spawn_result(
    *,
    action_type: str,
    rc: int,
    handled_task_id: str,
    worker_label: str,
    created_task: DbTask | None = None,
    improve_mode: str | None = None,
    failed_improve: DbTask | None = None,
) -> AdvanceActionExecutionResult:
    return AdvanceActionExecutionResult(
        action_type=action_type,
        status="success" if rc == 0 else "error",
        error_message="" if rc == 0 else f"Failed to start {worker_label} worker for task {handled_task_id}",
        worker_consuming=True,
        attempted_spawn=True,
        worker_started=rc == 0,
        work_done=rc == 0,
        handled_task_id=handled_task_id,
        created_task=created_task,
        improve_mode=improve_mode,
        failed_improve=failed_improve,
        worker_label=worker_label,
    )


_ITERATE_ROUTABLE_ACTIONS = frozenset({"create_review", "run_review", "improve", "run_improve"})


def _startup_preparation_failed_result(
    *,
    action_type: str,
    task: DbTask,
    worker_label: str,
) -> AdvanceActionExecutionResult:
    task_id = str(task.id) if task.id is not None else "<unknown>"
    return AdvanceActionExecutionResult(
        action_type=action_type,
        status="error",
        message=f"startup preparation failed for task {task_id}",
        error_message=f"Failed to start {worker_label} worker for task {task_id}",
        handled_task_id=task_id,
        created_task=task,
        worker_label=worker_label,
    )


def _prepare_background_start(
    *,
    context: AdvanceActionExecutionContext,
    action_type: str,
    task: DbTask,
    worker_label: str,
    rollback_on_failure: bool,
) -> tuple[DbTask | None, AdvanceActionExecutionResult | None]:
    prepared_task = context.prepare_task_for_background_start(task, rollback_on_failure)
    if prepared_task is None:
        return None, _startup_preparation_failed_result(
            action_type=action_type,
            task=task,
            worker_label=worker_label,
        )
    return prepared_task, None


def _maybe_route_action_through_iterate(
    *,
    task: DbTask,
    action: dict[str, Any],
    action_type: str,
    context: AdvanceActionExecutionContext,
) -> AdvanceActionExecutionResult | None:
    if action_type not in _ITERATE_ROUTABLE_ACTIONS or context.prefer_iterate_for_action is None:
        return None

    preferred = context.prefer_iterate_for_action(task, action)
    if preferred is None:
        return None
    if isinstance(preferred, AdvanceActionExecutionResult):
        return preferred
    if preferred.id is None:
        return AdvanceActionExecutionResult(
            action_type=action_type,
            status="error",
            message="iterate routing selected implementation with no task id",
        )

    guarded_pending_task_id: str | None = None
    if action_type == "run_review":
        review_task = action.get("review_task")
        if isinstance(review_task, DbTask):
            guarded_pending_task_id = review_task.id
    elif action_type == "run_improve":
        improve_task = action.get("improve_task")
        if isinstance(improve_task, DbTask):
            guarded_pending_task_id = improve_task.id

    if context.dry_run:
        return AdvanceActionExecutionResult(
            action_type=action_type,
            status="dry_run",
            message=action.get("description", "Run iterate"),
            worker_consuming=True,
            work_done=True,
            handled_task_id=preferred.id,
            created_task=preferred,
            worker_label="iterate",
            guarded_pending_task_id=guarded_pending_task_id,
        )

    rc = context.spawn_iterate_worker(preferred, "iterate")
    result = _spawn_result(
        action_type=action_type,
        rc=rc,
        handled_task_id=preferred.id,
        worker_label="iterate",
        created_task=preferred,
    )
    result.guarded_pending_task_id = guarded_pending_task_id
    result.success_message = f"Started iterate for {preferred.id}"
    return result


def execute_advance_action(
    *,
    task: DbTask,
    action: dict,
    context: AdvanceActionExecutionContext,
) -> AdvanceActionExecutionResult:
    """Execute one worker-style advance action with shared side-effect logic."""
    action_type = str(action.get("type", "skip"))

    if action_type not in _WORKER_ACTIONS:
        return AdvanceActionExecutionResult(
            action_type=action_type,
            status="unsupported",
            message=f"unsupported action: {action_type}",
        )

    iterate_routed_result = _maybe_route_action_through_iterate(
        task=task,
        action=action,
        action_type=action_type,
        context=context,
    )
    if iterate_routed_result is not None:
        return iterate_routed_result

    if action_type == "create_review":
        if task.id is None:
            return AdvanceActionExecutionResult(action_type=action_type, status="skip", message="missing task id")
        if context.dry_run:
            return AdvanceActionExecutionResult(
                action_type=action_type,
                status="dry_run",
                message=action.get("description", "Create review"),
                worker_consuming=True,
                work_done=True,
            )

        create_result = context.prepare_create_review(task)
        if create_result.status == "skip":
            return AdvanceActionExecutionResult(
                action_type=action_type,
                status="skip",
                message=create_result.message,
            )

        review_task = create_result.review_task
        if review_task is None or review_task.id is None:
            return AdvanceActionExecutionResult(
                action_type=action_type,
                status="error",
                message="review creation returned no task",
            )

        prepared_review_task, prepare_error = _prepare_background_start(
            context=context,
            action_type=action_type,
            task=review_task,
            worker_label="review",
            rollback_on_failure=True,
        )
        if prepared_review_task is None:
            assert prepare_error is not None
            return prepare_error
        assert prepared_review_task.id is not None

        rc = context.spawn_worker(prepared_review_task, "review")
        result = _spawn_result(
            action_type=action_type,
            rc=rc,
            handled_task_id=prepared_review_task.id,
            worker_label="review",
            created_task=prepared_review_task,
        )
        result.success_message = create_result.message
        return result

    if action_type == "run_review":
        review_task = action.get("review_task")
        if not isinstance(review_task, DbTask) or review_task.id is None:
            return AdvanceActionExecutionResult(action_type=action_type, status="skip", message="missing review task")

        if context.dry_run:
            return AdvanceActionExecutionResult(
                action_type=action_type,
                status="dry_run",
                message=action.get("description", "Run review"),
                worker_consuming=True,
                work_done=True,
                handled_task_id=review_task.id,
                created_task=review_task,
            )

        prepared_review_task, prepare_error = _prepare_background_start(
            context=context,
            action_type=action_type,
            task=review_task,
            worker_label="review",
            rollback_on_failure=False,
        )
        if prepared_review_task is None:
            assert prepare_error is not None
            return prepare_error
        assert prepared_review_task.id is not None

        rc = context.spawn_worker(prepared_review_task, "review")
        return _spawn_result(
            action_type=action_type,
            rc=rc,
            handled_task_id=prepared_review_task.id,
            worker_label="review",
            created_task=prepared_review_task,
        )

    if action_type == "improve":
        review_task = action.get("review_task")
        if not isinstance(review_task, DbTask) or review_task.id is None or task.id is None:
            return AdvanceActionExecutionResult(action_type=action_type, status="skip", message="missing improve inputs")

        improve_mode, failed_improve, improve_decision = resolve_improve_action(
            context.store,
            task.id,
            review_task.id,
            max_resume_attempts=context.max_resume_attempts,
        )
        attention_result = build_improve_needs_attention_result(
            store=context.store,
            impl_task=task,
            review_task=review_task,
            improve_mode=improve_mode,
            failed_improve=failed_improve,
            improve_decision=improve_decision,
            max_resume_attempts=context.max_resume_attempts,
        )
        if attention_result is not None:
            return attention_result

        if context.dry_run:
            dry_msg = action.get("description", "Create improve")
            if improve_mode == "resume" and failed_improve is not None:
                dry_msg = (
                    f"Resume improve {failed_improve.id} "
                    f"(failed: {failed_improve.failure_reason or 'UNKNOWN'})"
                )
            elif improve_mode == "retry" and failed_improve is not None:
                dry_msg = (
                    f"Retry improve {failed_improve.id} "
                    f"(failed: {failed_improve.failure_reason or 'UNKNOWN'})"
                )
            return AdvanceActionExecutionResult(
                action_type=action_type,
                status="dry_run",
                message=dry_msg,
                worker_consuming=True,
                work_done=True,
                improve_mode=improve_mode,
                failed_improve=failed_improve,
            )

        if improve_mode == "resume" and failed_improve is not None:
            assert failed_improve.id is not None
            improve_task = context.create_resume_task(failed_improve)
        elif improve_mode == "retry" and failed_improve is not None:
            assert failed_improve.id is not None
            if context.create_retry_task is not None:
                improve_task = context.create_retry_task(failed_improve)
            else:
                improve_task = _create_retry_task(context.store, failed_improve)
        else:
            try:
                improve_task = _create_improve_task(context.store, task, review_task)
            except ValueError as exc:
                return AdvanceActionExecutionResult(
                    action_type=action_type,
                    status="error",
                    message=str(exc),
                    improve_mode=improve_mode,
                    failed_improve=failed_improve,
                )

        prepared_improve_task, prepare_error = _prepare_background_start(
            context=context,
            action_type=action_type,
            task=improve_task,
            worker_label="improve",
            rollback_on_failure=True,
        )
        if prepared_improve_task is None:
            assert prepare_error is not None
            return prepare_error
        assert prepared_improve_task.id is not None
        if prepared_improve_task.session_id is not None:
            rc = context.spawn_resume_worker(prepared_improve_task, "improve")
        else:
            rc = context.spawn_worker(prepared_improve_task, "improve")

        return _spawn_result(
            action_type=action_type,
            rc=rc,
            handled_task_id=prepared_improve_task.id,
            worker_label="improve",
            created_task=prepared_improve_task,
            improve_mode=improve_mode,
            failed_improve=failed_improve,
        )

    if action_type == "run_improve":
        run_improve_task = action.get("improve_task")
        if not isinstance(run_improve_task, DbTask) or run_improve_task.id is None:
            return AdvanceActionExecutionResult(action_type=action_type, status="skip", message="missing improve task")

        if context.dry_run:
            return AdvanceActionExecutionResult(
                action_type=action_type,
                status="dry_run",
                message=action.get("description", "Run improve"),
                worker_consuming=True,
                work_done=True,
                handled_task_id=run_improve_task.id,
                created_task=run_improve_task,
            )

        prepared_improve_task, prepare_error = _prepare_background_start(
            context=context,
            action_type=action_type,
            task=run_improve_task,
            worker_label="improve",
            rollback_on_failure=False,
        )
        if prepared_improve_task is None:
            assert prepare_error is not None
            return prepare_error
        assert prepared_improve_task.id is not None

        rc = context.spawn_worker(prepared_improve_task, "improve")
        return _spawn_result(
            action_type=action_type,
            rc=rc,
            handled_task_id=prepared_improve_task.id,
            worker_label="improve",
            created_task=prepared_improve_task,
        )

    if action_type == "resume":
        if task.id is None:
            return AdvanceActionExecutionResult(action_type=action_type, status="skip", message="missing task id")
        if context.dry_run:
            return AdvanceActionExecutionResult(
                action_type=action_type,
                status="dry_run",
                message=action.get("description", "Resume failed task"),
                worker_consuming=True,
                work_done=True,
            )

        launch_mode = str(action.get("launch_mode") or ("iterate" if task.task_type == "implement" else "worker"))
        resume_task_id = action.get("recovery_task_id")
        reuse_existing = bool(action.get("reuse_existing", False))
        if launch_mode == "iterate":
            if context.spawn_iterate_recovery is None:
                return AdvanceActionExecutionResult(
                    action_type=action_type,
                    status="error",
                    message="missing iterate recovery launcher",
                )
            if reuse_existing and isinstance(resume_task_id, str):
                resume_task = context.store.get(resume_task_id)
                if resume_task is None:
                    return AdvanceActionExecutionResult(
                        action_type=action_type,
                        status="error",
                        message=f"missing existing resume task {resume_task_id}",
                    )
            else:
                resume_task = context.create_resume_task(task)
            prepared_resume_task, prepare_error = _prepare_background_start(
                context=context,
                action_type=action_type,
                task=resume_task,
                worker_label="iterate",
                rollback_on_failure=not reuse_existing,
            )
            if prepared_resume_task is None:
                assert prepare_error is not None
                return prepare_error
            assert prepared_resume_task.id is not None

            rc = context.spawn_iterate_recovery(task, "resume", prepared_resume_task)
            result = _spawn_result(
                action_type=action_type,
                rc=rc,
                handled_task_id=prepared_resume_task.id,
                worker_label="iterate",
                created_task=prepared_resume_task,
            )
            if reuse_existing:
                result.success_message = f"Reused pending resume task {prepared_resume_task.id}"
            else:
                result.success_message = f"Created resume task {prepared_resume_task.id}"
            return result

        if reuse_existing and isinstance(resume_task_id, str):
            resume_task = context.store.get(resume_task_id)
            if resume_task is None:
                return AdvanceActionExecutionResult(
                    action_type=action_type,
                    status="error",
                    message=f"missing existing resume task {resume_task_id}",
                )
        else:
            resume_task = context.create_resume_task(task)
        prepared_resume_task, prepare_error = _prepare_background_start(
            context=context,
            action_type=action_type,
            task=resume_task,
            worker_label="resume",
            rollback_on_failure=not reuse_existing,
        )
        if prepared_resume_task is None:
            assert prepare_error is not None
            return prepare_error

        assert prepared_resume_task.id is not None
        rc = context.spawn_resume_worker(prepared_resume_task, task.task_type or "task")
        result = _spawn_result(
            action_type=action_type,
            rc=rc,
            handled_task_id=prepared_resume_task.id,
            worker_label="resume",
            created_task=prepared_resume_task,
        )
        if reuse_existing:
            result.success_message = f"Reused pending resume task {prepared_resume_task.id}"
        else:
            result.success_message = f"Created resume task {prepared_resume_task.id}"
        return result

    if action_type == "retry":
        if task.id is None:
            return AdvanceActionExecutionResult(action_type=action_type, status="skip", message="missing task id")
        if context.dry_run:
            return AdvanceActionExecutionResult(
                action_type=action_type,
                status="dry_run",
                message=action.get("description", "Retry failed task"),
                worker_consuming=True,
                work_done=True,
            )

        launch_mode = str(action.get("launch_mode") or ("iterate" if task.task_type == "implement" else "worker"))
        retry_task_id = action.get("recovery_task_id")
        reuse_existing = bool(action.get("reuse_existing", False))
        if reuse_existing and isinstance(retry_task_id, str):
            retry_task = context.store.get(retry_task_id)
            if retry_task is None:
                return AdvanceActionExecutionResult(
                    action_type=action_type,
                    status="error",
                    message=f"missing existing retry task {retry_task_id}",
                )
        else:
            if context.create_retry_task is None:
                return AdvanceActionExecutionResult(
                    action_type=action_type,
                    status="error",
                    message="missing retry task factory",
                )
            retry_task = context.create_retry_task(task)
        if launch_mode == "iterate":
            if context.spawn_iterate_recovery is None:
                return AdvanceActionExecutionResult(
                    action_type=action_type,
                    status="error",
                    message="missing iterate recovery launcher",
                )
            prepared_retry_task, prepare_error = _prepare_background_start(
                context=context,
                action_type=action_type,
                task=retry_task,
                worker_label="iterate",
                rollback_on_failure=not reuse_existing,
            )
            if prepared_retry_task is None:
                assert prepare_error is not None
                return prepare_error
            assert prepared_retry_task.id is not None
            rc = context.spawn_iterate_recovery(task, "retry", prepared_retry_task)
            worker_label = "iterate"
            handled_task = prepared_retry_task
        else:
            prepared_retry_task, prepare_error = _prepare_background_start(
                context=context,
                action_type=action_type,
                task=retry_task,
                worker_label="retry",
                rollback_on_failure=not reuse_existing,
            )
            if prepared_retry_task is None:
                assert prepare_error is not None
                return prepare_error
            assert prepared_retry_task.id is not None
            rc = context.spawn_worker(prepared_retry_task, task.task_type or "task")
            worker_label = "retry"
            handled_task = prepared_retry_task
        assert handled_task.id is not None
        result = _spawn_result(
            action_type=action_type,
            rc=rc,
            handled_task_id=handled_task.id,
            worker_label=worker_label,
            created_task=handled_task,
        )
        if reuse_existing:
            result.success_message = f"Reused pending retry task {handled_task.id}"
        else:
            result.success_message = f"Created retry task {handled_task.id}"
        return result

    if action_type == "create_implement":
        if task.id is None:
            return AdvanceActionExecutionResult(action_type=action_type, status="skip", message="missing task id")
        if context.dry_run:
            return AdvanceActionExecutionResult(
                action_type=action_type,
                status="dry_run",
                message=action.get("description", "Create implement"),
                worker_consuming=True,
                work_done=True,
            )

        impl_task = context.create_implement_task(task)
        prepared_impl_task, prepare_error = _prepare_background_start(
            context=context,
            action_type=action_type,
            task=impl_task,
            worker_label="implement" if not context.use_iterate_for_create_implement else "iterate",
            rollback_on_failure=True,
        )
        if prepared_impl_task is None:
            assert prepare_error is not None
            return prepare_error

        assert prepared_impl_task.id is not None
        if context.use_iterate_for_create_implement:
            rc = context.spawn_iterate_worker(prepared_impl_task, "implement")
            worker_label = "iterate"
        else:
            rc = context.spawn_worker(prepared_impl_task, "implement")
            worker_label = "implement"

        result = _spawn_result(
            action_type=action_type,
            rc=rc,
            handled_task_id=prepared_impl_task.id,
            worker_label=worker_label,
            created_task=prepared_impl_task,
        )
        result.success_message = f"Created implement task {prepared_impl_task.id}"
        return result

    if action_type == "needs_rebase":
        if task.id is None or not task.branch:
            return AdvanceActionExecutionResult(
                action_type=action_type,
                status="error",
                message=f"Cannot rebase: task {task.id} has no branch",
            )
        if (
            context.is_rebase_target_already_merged is not None
            and context.is_rebase_target_already_merged(task)
        ):
            return AdvanceActionExecutionResult(
                action_type=action_type,
                status="skip",
                message="target implementation already merged",
                worker_consuming=False,
                work_done=False,
            )
        if context.dry_run:
            return AdvanceActionExecutionResult(
                action_type=action_type,
                status="dry_run",
                message=action.get("description", "Create rebase"),
                worker_consuming=True,
                work_done=True,
            )

        rebase_task = context.create_rebase_task(task)
        prepared_rebase_task, prepare_error = _prepare_background_start(
            context=context,
            action_type=action_type,
            task=rebase_task,
            worker_label="rebase",
            rollback_on_failure=True,
        )
        if prepared_rebase_task is None:
            assert prepare_error is not None
            return prepare_error

        assert prepared_rebase_task.id is not None
        if context.use_iterate_for_needs_rebase:
            rc = context.spawn_iterate_worker(
                task,
                "rebase",
                prepared_task=prepared_rebase_task,
                prepared_phase="iteration",
                prepared_action_type="needs_rebase",
            )
            worker_label = "iterate"
        else:
            rc = context.spawn_worker(prepared_rebase_task, "rebase")
            worker_label = "rebase"

        result = _spawn_result(
            action_type=action_type,
            rc=rc,
            handled_task_id=prepared_rebase_task.id,
            worker_label=worker_label,
            created_task=prepared_rebase_task,
        )
        result.success_message = f"Created rebase task {prepared_rebase_task.id}"
        return result

    return AdvanceActionExecutionResult(
        action_type=action_type,
        status="unsupported",
        message=f"unsupported action: {action_type}",
    )
