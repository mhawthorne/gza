"""Shared per-action execution for advance-style lifecycle commands."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Literal, Protocol

from ..db import SqliteTaskStore, Task as DbTask
from ._common import _create_improve_task, resolve_improve_action


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
    prepare_create_review: Callable[[DbTask], CreateReviewActionResult]
    create_resume_task: Callable[[DbTask], DbTask]
    create_rebase_task: Callable[[DbTask], DbTask]
    create_implement_task: Callable[[DbTask], DbTask]
    spawn_worker: Callable[[str, str], int]
    spawn_resume_worker: Callable[[str, str], int]
    spawn_iterate_worker: Callable[[DbTask, str], int]


@dataclass
class AdvanceActionExecutionResult:
    """Structured outcome for one advance action execution attempt."""

    action_type: str
    status: Literal["success", "skip", "error", "dry_run", "unsupported"]
    message: str = ""
    worker_consuming: bool = False
    attempted_spawn: bool = False
    worker_started: bool = False
    work_done: bool = False
    handled_task_id: str | None = None
    created_task: DbTask | None = None
    improve_mode: str | None = None
    failed_improve: DbTask | None = None
    attention_type: str | None = None


_WORKER_ACTIONS = frozenset(
    {
        "create_review",
        "run_review",
        "improve",
        "run_improve",
        "resume",
        "create_implement",
        "needs_rebase",
    }
)


def _spawn_result(
    *,
    action_type: str,
    rc: int,
    handled_task_id: str,
    created_task: DbTask | None = None,
    improve_mode: str | None = None,
    failed_improve: DbTask | None = None,
) -> AdvanceActionExecutionResult:
    return AdvanceActionExecutionResult(
        action_type=action_type,
        status="success" if rc == 0 else "error",
        worker_consuming=True,
        attempted_spawn=True,
        worker_started=rc == 0,
        work_done=rc == 0,
        handled_task_id=handled_task_id,
        created_task=created_task,
        improve_mode=improve_mode,
        failed_improve=failed_improve,
    )


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

        rc = context.spawn_worker(review_task.id, "review")
        result = _spawn_result(
            action_type=action_type,
            rc=rc,
            handled_task_id=review_task.id,
            created_task=review_task,
        )
        result.message = create_result.message
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

        rc = context.spawn_worker(review_task.id, "review")
        return _spawn_result(
            action_type=action_type,
            rc=rc,
            handled_task_id=review_task.id,
            created_task=review_task,
        )

    if action_type == "improve":
        review_task = action.get("review_task")
        if not isinstance(review_task, DbTask) or review_task.id is None or task.id is None:
            return AdvanceActionExecutionResult(action_type=action_type, status="skip", message="missing improve inputs")

        improve_mode, failed_improve = resolve_improve_action(
            context.store,
            task.id,
            review_task.id,
            max_resume_attempts=context.max_resume_attempts,
        )

        if improve_mode == "give_up" and failed_improve is not None:
            assert failed_improve.id is not None
            msg = (
                f"SKIP: max improve attempts ({context.max_resume_attempts}) reached for "
                f"{task.id} + {review_task.id}; latest failed improve: {failed_improve.id}. "
                f"Run uv run gza fix {task.id}"
            )
            return AdvanceActionExecutionResult(
                action_type=action_type,
                status="skip",
                message=msg,
                improve_mode=improve_mode,
                failed_improve=failed_improve,
                attention_type="max_improve_attempts",
            )

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
            retry_same_branch = failed_improve.same_branch
            retry_base_branch: str | None = None
            if failed_improve.same_branch and failed_improve.branch:
                retry_same_branch = False
                retry_base_branch = failed_improve.branch
            improve_task = context.store.add(
                prompt=failed_improve.prompt,
                task_type="improve",
                depends_on=failed_improve.depends_on,
                based_on=failed_improve.id,
                same_branch=retry_same_branch,
                tags=failed_improve.tags,
                base_branch=retry_base_branch,
            )
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

        assert improve_task.id is not None
        if improve_task.session_id is not None:
            rc = context.spawn_resume_worker(improve_task.id, "improve")
        else:
            rc = context.spawn_worker(improve_task.id, "improve")

        return _spawn_result(
            action_type=action_type,
            rc=rc,
            handled_task_id=improve_task.id,
            created_task=improve_task,
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

        rc = context.spawn_worker(run_improve_task.id, "improve")
        return _spawn_result(
            action_type=action_type,
            rc=rc,
            handled_task_id=run_improve_task.id,
            created_task=run_improve_task,
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

        resume_task = context.create_resume_task(task)
        assert resume_task.id is not None
        rc = context.spawn_resume_worker(resume_task.id, task.task_type or "task")
        result = _spawn_result(
            action_type=action_type,
            rc=rc,
            handled_task_id=resume_task.id,
            created_task=resume_task,
        )
        result.message = f"Created resume task {resume_task.id}"
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
        assert impl_task.id is not None
        if context.use_iterate_for_create_implement:
            rc = context.spawn_iterate_worker(impl_task, "implement")
        else:
            rc = context.spawn_worker(impl_task.id, "implement")

        result = _spawn_result(
            action_type=action_type,
            rc=rc,
            handled_task_id=impl_task.id,
            created_task=impl_task,
        )
        result.message = f"Created implement task {impl_task.id}"
        return result

    if action_type == "needs_rebase":
        if task.id is None or not task.branch:
            return AdvanceActionExecutionResult(
                action_type=action_type,
                status="error",
                message=f"Cannot rebase: task {task.id} has no branch",
            )
        if context.dry_run:
            return AdvanceActionExecutionResult(
                action_type=action_type,
                status="dry_run",
                message=action.get("description", "Create rebase"),
                worker_consuming=True,
                work_done=True,
            )

        if context.use_iterate_for_needs_rebase:
            rc = context.spawn_iterate_worker(task, "rebase")
            return _spawn_result(
                action_type=action_type,
                rc=rc,
                handled_task_id=task.id,
                created_task=task,
            )

        rebase_task = context.create_rebase_task(task)
        assert rebase_task.id is not None
        rc = context.spawn_worker(rebase_task.id, "rebase")
        result = _spawn_result(
            action_type=action_type,
            rc=rc,
            handled_task_id=rebase_task.id,
            created_task=rebase_task,
        )
        result.message = f"Created rebase task {rebase_task.id}"
        return result

    return AdvanceActionExecutionResult(
        action_type=action_type,
        status="unsupported",
        message=f"unsupported action: {action_type}",
    )
