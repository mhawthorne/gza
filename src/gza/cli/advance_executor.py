"""Shared per-action execution for advance-style lifecycle commands."""

from __future__ import annotations

import logging
import shutil
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal, Protocol

from ..concurrency import (
    LaunchPermit,
    MaxConcurrentTasksError,
    launch_permit,
    release_task_launch_permit,
    reserve_task_launch_permit,
)
from ..db import SqliteTaskStore, Task as DbTask
from ..git import Git, GitError
from ..plan_review_verdict import PlanReviewManifest
from ..recovery_engine import FailedRecoveryDecision, get_failed_recovery_needs_attention_reason
from ..review_verdict import is_verify_blocked_only_review
from ..runner import (
    ProjectReviewVerifyResult,
    _capture_review_verify_result,
    _create_detached_review_worktree,
    _format_review_verify_result,
    _get_task_output,
    _project_boundary,
    _resolve_review_verify_base_sha,
    _run_review_verify_command,
    _run_review_verify_commands_for_projects,
    _task_has_current_passing_review_verify_evidence,
    _task_is_cross_project,
    _worktree_execution_dir,
)
from ._common import (
    PlanReviewMaterializationResult,
    _create_improve_task,
    _create_retry_task,
    _prepare_task_for_reserved_launch,
    resolve_improve_action,
)
from .advance_engine import (
    classify_advance_action,
    failed_recovery_decision_to_attention_action,
)

logger = logging.getLogger(__name__)


class CreateReviewActionResult(Protocol):
    """Duck type returned by create-review preparation helpers."""

    status: str
    review_task: DbTask | None
    message: str


@dataclass
class PreparedCreateReviewActionResult:
    """Concrete create-review preparation result for typed wrappers."""

    status: str
    review_task: DbTask | None
    message: str


@dataclass
class AdvanceActionExecutionContext:
    """Execution dependencies for a single advance action."""

    store: SqliteTaskStore
    trigger_source: str
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
    can_spawn_worker: Callable[[str], bool] | None = None
    no_worker_capacity_message: Callable[[str], str] | None = None
    is_rebase_target_already_merged: Callable[[DbTask], bool] | None = None
    prefer_iterate_for_action: Callable[
        [DbTask, dict[str, Any]],
        DbTask | AdvanceActionExecutionResult | None,
    ] | None = None
    spawn_iterate_recovery: Callable[[DbTask, Literal["resume", "retry"], DbTask], int] | None = None
    create_retry_task: Callable[[DbTask], DbTask] | None = None
    create_plan_review_task: Callable[[DbTask], DbTask] | None = None
    create_plan_improve_task: Callable[[DbTask, DbTask], DbTask] | None = None
    materialize_plan_slices: Callable[[DbTask, DbTask, PlanReviewManifest], PlanReviewMaterializationResult] | None = None
    create_targeted_rebase_task: Callable[[DbTask, str], DbTask] | None = None
    reconcile_diverged_branch: Callable[[DbTask], BranchDivergenceReconcileResult] | None = None
    config: Any | None = None
    git: Any | None = None


@dataclass(frozen=True)
class BranchDivergenceReconcileResult:
    """Outcome of a direct local/origin branch-divergence reconciliation attempt."""

    status: Literal["reconciled", "needs_rebase", "needs_attention", "error"]
    message: str
    rebase_target: str | None = None
    attention_reason: str | None = None


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


@dataclass(frozen=True)
class NoopVerifyThenReviewOutcome:
    """Shared execution outcome for verify-then-rereview no-op recovery."""

    status: Literal["create_review", "review_cleared", "needs_attention", "skip", "error"]
    message: str
    verify_markdown: str | None = None
    review_task: DbTask | None = None


_NOOP_REVERIFY_RECOVERABLE_EXCEPTIONS = (GitError, OSError, RuntimeError, ValueError)


def _fresh_verify_resolves_verify_only_review(
    *,
    task: DbTask,
    review_task: DbTask,
    current_branch: str | None,
    current_head_sha: str | None,
    context: AdvanceActionExecutionContext,
) -> bool:
    if context.config is None or task.id is None:
        return False
    review_content = _get_task_output(review_task, Path(context.config.project_dir))
    if not is_verify_blocked_only_review(review_content):
        return False
    if not _task_has_current_passing_review_verify_evidence(
        task=task,
        review_task=review_task,
        current_branch=current_branch,
        current_head_sha=current_head_sha,
    ):
        return False
    context.store.clear_review_state(task.id)
    return True


_WORKER_ACTIONS = frozenset(
    {
        "create_plan_review",
        "run_plan_review",
        "create_plan_improve",
        "run_plan_improve",
        "materialize_plan_slices",
        "create_review",
        "run_review",
        "verify_noop_improve_then_review",
        "improve",
        "run_improve",
        "resume",
        "retry",
        "create_implement",
        "needs_rebase",
        "reconcile_branch_divergence",
    }
)


def _should_continue_branch_publication_after_reconcile(
    *,
    task: DbTask,
    action: dict[str, Any],
) -> bool:
    """Return whether a successful reconcile should resume failed PR publication."""
    decision = action.get("decision")
    if not isinstance(decision, FailedRecoveryDecision):
        return False
    if decision.action != "reconcile":
        return False
    return task.status == "failed" and task.failure_reason in {"BRANCH_UNPUSHABLE", "PR_REQUIRED"}


def _build_noop_verify_attention_result(
    *,
    action_type: str,
    description: str,
    verify_result: Any,
) -> AdvanceActionExecutionResult:
    detail = verify_result.failure or verify_result.exit_status
    return AdvanceActionExecutionResult(
        action_type=action_type,
        status="skip",
        message=f"SKIP: {description}. Fresh verify_command {verify_result.status} ({detail}).",
        attention_type="needs_discussion",
        attention_reason="improve-no-op",
    )


def run_noop_improve_verify_then_review(
    *,
    task: DbTask,
    action: dict[str, Any],
    context: AdvanceActionExecutionContext,
) -> NoopVerifyThenReviewOutcome:
    """Shared no-op improve escape hatch: fresh verify on tip, then create a new review."""
    description = str(action.get("description", "Re-run verify_command before re-review"))

    review_task = action.get("review_task")
    if not isinstance(review_task, DbTask) or review_task.id is None or task.id is None:
        return NoopVerifyThenReviewOutcome(status="skip", message="missing verify/review inputs")
    if context.config is None or context.git is None:
        return NoopVerifyThenReviewOutcome(status="error", message="missing verify execution context")
    verify_git = context.git

    verify_command = getattr(context.config, "verify_command", None)
    if (
        not _task_is_cross_project(task)
        and (not isinstance(verify_command, str) or not verify_command.strip())
    ):
        return NoopVerifyThenReviewOutcome(
            status="needs_attention",
            message=f"SKIP: {description}. verify_command is unavailable.",
        )
    if not task.branch:
        return NoopVerifyThenReviewOutcome(
            status="needs_attention",
            message=f"SKIP: {description}. implementation branch is unavailable.",
        )

    worktree_label = task.slug or task.id or "review-verify"
    worktree_path = Path(context.config.worktree_path) / f"{worktree_label}-noop-review-verify"
    timeout_seconds = getattr(context.config, "review_verify_timeout_seconds", 120)
    if not isinstance(timeout_seconds, int) or timeout_seconds < 1:
        timeout_seconds = 120

    def _append_cleanup_failure(message: str, cleanup_failure: str | None) -> str:
        if cleanup_failure is None:
            return message
        return f"{message} Cleanup also failed: {cleanup_failure}."

    def _cleanup_worktree() -> str | None:
        try:
            remove_result = verify_git.worktree_remove(worktree_path, force=True)
        except _NOOP_REVERIFY_RECOVERABLE_EXCEPTIONS as exc:
            logger.warning("Failed to remove noop review verify worktree %s", worktree_path, exc_info=True)
            if worktree_path.exists():
                shutil.rmtree(worktree_path, ignore_errors=True)
            return str(exc)
        if getattr(remove_result, "returncode", 0) not in (0, None):
            logger.warning(
                "git worktree remove failed for noop review verify worktree %s: %s",
                worktree_path,
                getattr(remove_result, "stderr", "") or getattr(remove_result, "stdout", "") or remove_result.returncode,
            )
            if worktree_path.exists():
                shutil.rmtree(worktree_path, ignore_errors=True)
            if worktree_path.exists():
                return (
                    getattr(remove_result, "stderr", "") or getattr(remove_result, "stdout", "") or f"return code {remove_result.returncode}"
                ).strip()
            return None
        if worktree_path.exists():
            shutil.rmtree(worktree_path, ignore_errors=True)
        if worktree_path.exists():
            return f"{worktree_path} still exists after cleanup"
        return None

    verify_result = None
    verify_markdown = None
    reviewed_head_sha: str | None = None
    reviewed_base_sha: str | None = None
    project_results: tuple[ProjectReviewVerifyResult, ...] = ()
    lifecycle_failure: Exception | None = None
    try:
        default_branch = verify_git.default_branch()
        reviewed_base_sha = _resolve_review_verify_base_sha(verify_git, default_branch)
        _create_detached_review_worktree(verify_git, worktree_path, task.branch)
        worktree_git = Git(worktree_path)
        reviewed_head_sha = worktree_git.rev_parse_if_exists("HEAD")
        if reviewed_head_sha is not None:
            if _task_is_cross_project(task):
                cross_project_verify = _run_review_verify_commands_for_projects(
                    config=context.config,
                    task=task,
                    worktree_git=worktree_git,
                    worktree_path=worktree_path,
                    timeout_seconds=timeout_seconds,
                    reviewed_branch=task.branch,
                    reviewed_head_sha=reviewed_head_sha,
                    reviewed_base_sha=reviewed_base_sha,
                )
                if cross_project_verify is not None:
                    verify_result = cross_project_verify.aggregate_result
                    verify_markdown = cross_project_verify.markdown
                    project_results = cross_project_verify.project_results
            elif isinstance(verify_command, str) and verify_command.strip():
                provider_cwd = _worktree_execution_dir(
                    worktree_path,
                    _project_boundary(context.config),
                )
                verify_result = _run_review_verify_command(
                    verify_command,
                    cwd=provider_cwd,
                    reviewed_branch=task.branch,
                    reviewed_head_sha=reviewed_head_sha,
                    reviewed_base_sha=reviewed_base_sha,
                    timeout_seconds=timeout_seconds,
                )
                verify_markdown = _format_review_verify_result(verify_result)
    except _NOOP_REVERIFY_RECOVERABLE_EXCEPTIONS as exc:
        lifecycle_failure = exc
    cleanup_failure = _cleanup_worktree()

    if lifecycle_failure is not None:
        return NoopVerifyThenReviewOutcome(
            status="needs_attention",
            message=_append_cleanup_failure(
                f"SKIP: {description}. unable to prepare or run fresh verify_command: {lifecycle_failure}.",
                cleanup_failure,
            ),
        )
    if reviewed_head_sha is None:
        return NoopVerifyThenReviewOutcome(
            status="needs_attention",
            message=_append_cleanup_failure(
                f"SKIP: {description}. unable to resolve review worktree HEAD before verify_command ran.",
                cleanup_failure,
            ),
        )
    if verify_result is None or verify_markdown is None:
        return NoopVerifyThenReviewOutcome(
            status="needs_attention",
            message=_append_cleanup_failure(
                f"SKIP: {description}. verify_command is unavailable.",
                cleanup_failure,
            ),
        )
    _capture_review_verify_result(
        context.config,
        context.store,
        task,
        verify_result,
        markdown=verify_markdown,
        project_results=project_results,
        producer="noop_review_verify",
        metadata={"triggering_review_task_id": review_task.id},
    )
    if cleanup_failure is not None:
        return NoopVerifyThenReviewOutcome(
            status="needs_attention",
            message=(
                f"SKIP: {description}. fresh verify_command completed but temporary review worktree cleanup failed: "
                f"{cleanup_failure}."
            ),
            verify_markdown=verify_markdown,
        )

    if verify_result.status != "passed":
        detail = verify_result.failure or verify_result.exit_status
        return NoopVerifyThenReviewOutcome(
            status="needs_attention",
            message=f"SKIP: {description}. Fresh verify_command {verify_result.status} ({detail}).",
            verify_markdown=verify_markdown,
        )

    expected_head_sha = action.get("current_branch_head_sha")
    current_head_sha = expected_head_sha if isinstance(expected_head_sha, str) and expected_head_sha else None
    persisted_task = context.store.get(task.id) if task.id is not None else None
    task_with_evidence = persisted_task if persisted_task is not None else task
    if _fresh_verify_resolves_verify_only_review(
        task=task_with_evidence,
        review_task=review_task,
        current_branch=task.branch,
        current_head_sha=current_head_sha or reviewed_head_sha,
        context=context,
    ):
        return NoopVerifyThenReviewOutcome(
            status="review_cleared",
            message=(
                f"Fresh verify passed for {task.branch} at {verify_result.reviewed_head_sha}; "
                f"cleared verify-only review block on {task.id}"
            ),
            verify_markdown=verify_markdown,
        )

    create_result = context.prepare_create_review(task)
    if create_result.status == "skip":
        return NoopVerifyThenReviewOutcome(status="skip", message=create_result.message, verify_markdown=verify_markdown)
    if create_result.status != "created":
        return NoopVerifyThenReviewOutcome(
            status="error",
            message=create_result.message,
            verify_markdown=verify_markdown,
        )
    new_review = create_result.review_task
    if new_review is None or new_review.id is None:
        return NoopVerifyThenReviewOutcome(
            status="error",
            message="review creation returned no task after fresh verify",
            verify_markdown=verify_markdown,
        )
    return NoopVerifyThenReviewOutcome(
        status="create_review",
        message=(
            f"Fresh verify passed for {task.branch} at {verify_result.reviewed_head_sha}; "
            f"created review {new_review.id} to re-grade current tip"
        ),
        verify_markdown=verify_markdown,
        review_task=new_review,
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
    subject_task = result.failed_improve or task
    if result.action_type == "improve" and result.attention_type in {"automatic_recovery_disabled", "manual_review_required"}:
        subject_task = task
    if subject_task.id is not None:
        action["subject_task_id"] = subject_task.id
    if classify_advance_action(action) != "needs_attention":
        return None
    return AdvanceExecutionNeedsAttention(
        task=subject_task,
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


ITERATE_ROUTABLE_ACTIONS = frozenset(
    {"create_review", "run_review", "verify_noop_improve_then_review", "improve", "run_improve"}
)


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
    permit: LaunchPermit | None = None,
) -> tuple[DbTask | None, AdvanceActionExecutionResult | None]:
    if permit is not None and context.config is not None:
        prepared_task = _prepare_task_for_reserved_launch(
            context.config,
            task,
            permit=permit,
            rollback_on_failure=rollback_on_failure,
        )
        if prepared_task is not None and prepared_task.id is not None:
            reserve_task_launch_permit(str(prepared_task.id), permit)
    else:
        prepared_task = context.prepare_task_for_background_start(task, rollback_on_failure)
        if prepared_task is None and permit is not None:
            permit.release()
    if prepared_task is None:
        return None, _startup_preparation_failed_result(
            action_type=action_type,
            task=task,
            worker_label=worker_label,
        )
    return prepared_task, None


def _reserve_background_launch(
    *,
    action_type: str,
    context: AdvanceActionExecutionContext,
    worker_label: str,
) -> tuple[LaunchPermit | None, AdvanceActionExecutionResult | None]:
    blocked = _worker_capacity_blocked_result(
        action_type=action_type,
        context=context,
        worker_label=worker_label,
    )
    if blocked is not None:
        return None, blocked
    if context.config is None:
        return None, None
    try:
        return launch_permit(context.config, context.store), None
    except MaxConcurrentTasksError as exc:
        return None, AdvanceActionExecutionResult(
            action_type=action_type,
            status="skip",
            message=f"SKIP: {exc}",
            worker_label=worker_label,
        )


def _release_reserved_launch_if_left(task: DbTask | None) -> None:
    if task is not None and task.id is not None:
        release_task_launch_permit(str(task.id))


def _worker_capacity_blocked_result(
    *,
    action_type: str,
    context: AdvanceActionExecutionContext,
    worker_label: str,
) -> AdvanceActionExecutionResult | None:
    if context.can_spawn_worker is None or context.can_spawn_worker(worker_label):
        return None
    message = (
        context.no_worker_capacity_message(worker_label)
        if context.no_worker_capacity_message is not None
        else f"SKIP: no worker capacity available for {worker_label}"
    )
    return AdvanceActionExecutionResult(
        action_type=action_type,
        status="skip",
        message=message,
        worker_label=worker_label,
    )


def _maybe_route_action_through_iterate(
    *,
    task: DbTask,
    action: dict[str, Any],
    action_type: str,
    context: AdvanceActionExecutionContext,
) -> AdvanceActionExecutionResult | None:
    if action_type not in ITERATE_ROUTABLE_ACTIONS or context.prefer_iterate_for_action is None:
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

    if action_type == "create_plan_review":
        if task.id is None:
            return AdvanceActionExecutionResult(action_type=action_type, status="skip", message="missing task id")
        if context.dry_run:
            return AdvanceActionExecutionResult(
                action_type=action_type,
                status="dry_run",
                message=action.get("description", "Create plan review"),
                worker_consuming=True,
                work_done=True,
            )

        permit, blocked = _reserve_background_launch(
            action_type=action_type,
            context=context,
            worker_label="plan_review",
        )
        if blocked is not None:
            return blocked
        if context.create_plan_review_task is None:
            if permit is not None:
                permit.release()
            return AdvanceActionExecutionResult(action_type=action_type, status="error", message="plan review creation is unavailable")
        plan_review_task = context.create_plan_review_task(task)
        prepared_plan_review_task, prepare_error = _prepare_background_start(
            context=context,
            action_type=action_type,
            task=plan_review_task,
            worker_label="plan_review",
            rollback_on_failure=True,
            permit=permit,
        )
        if prepared_plan_review_task is None:
            assert prepare_error is not None
            return prepare_error
        assert prepared_plan_review_task.id is not None

        rc = context.spawn_worker(prepared_plan_review_task, "plan_review")
        _release_reserved_launch_if_left(prepared_plan_review_task)
        result = _spawn_result(
            action_type=action_type,
            rc=rc,
            handled_task_id=prepared_plan_review_task.id,
            worker_label="plan_review",
            created_task=prepared_plan_review_task,
        )
        result.success_message = f"Created plan review task {prepared_plan_review_task.id}"
        return result

    if action_type == "run_plan_review":
        pending_plan_review_task = action.get("plan_review_task")
        if not isinstance(pending_plan_review_task, DbTask) or pending_plan_review_task.id is None:
            return AdvanceActionExecutionResult(action_type=action_type, status="skip", message="missing plan review task")
        plan_review_task = pending_plan_review_task
        if context.dry_run:
            return AdvanceActionExecutionResult(
                action_type=action_type,
                status="dry_run",
                message=action.get("description", "Run plan review"),
                worker_consuming=True,
                work_done=True,
                handled_task_id=plan_review_task.id,
                created_task=plan_review_task,
            )

        permit, blocked = _reserve_background_launch(
            action_type=action_type,
            context=context,
            worker_label="plan_review",
        )
        if blocked is not None:
            return blocked
        prepared_plan_review_task, prepare_error = _prepare_background_start(
            context=context,
            action_type=action_type,
            task=plan_review_task,
            worker_label="plan_review",
            rollback_on_failure=False,
            permit=permit,
        )
        if prepared_plan_review_task is None:
            assert prepare_error is not None
            return prepare_error
        assert prepared_plan_review_task.id is not None

        rc = context.spawn_worker(prepared_plan_review_task, "plan_review")
        _release_reserved_launch_if_left(prepared_plan_review_task)
        return _spawn_result(
            action_type=action_type,
            rc=rc,
            handled_task_id=prepared_plan_review_task.id,
            worker_label="plan_review",
            created_task=prepared_plan_review_task,
        )

    if action_type == "create_plan_improve":
        review_task = action.get("plan_review_task")
        plan_source_task = action.get("plan_source_task")
        if (
            not isinstance(review_task, DbTask)
            or review_task.id is None
            or not isinstance(plan_source_task, DbTask)
            or plan_source_task.id is None
        ):
            return AdvanceActionExecutionResult(action_type=action_type, status="skip", message="missing plan improve inputs")
        if context.dry_run:
            return AdvanceActionExecutionResult(
                action_type=action_type,
                status="dry_run",
                message=action.get("description", "Create plan improve"),
                worker_consuming=True,
                work_done=True,
            )

        permit, blocked = _reserve_background_launch(
            action_type=action_type,
            context=context,
            worker_label="plan_improve",
        )
        if blocked is not None:
            return blocked
        if context.create_plan_improve_task is None:
            if permit is not None:
                permit.release()
            return AdvanceActionExecutionResult(action_type=action_type, status="error", message="plan improve creation is unavailable")
        plan_improve_task = context.create_plan_improve_task(plan_source_task, review_task)
        prepared_plan_improve_task, prepare_error = _prepare_background_start(
            context=context,
            action_type=action_type,
            task=plan_improve_task,
            worker_label="plan_improve",
            rollback_on_failure=True,
            permit=permit,
        )
        if prepared_plan_improve_task is None:
            assert prepare_error is not None
            return prepare_error
        assert prepared_plan_improve_task.id is not None

        rc = context.spawn_worker(prepared_plan_improve_task, "plan_improve")
        _release_reserved_launch_if_left(prepared_plan_improve_task)
        result = _spawn_result(
            action_type=action_type,
            rc=rc,
            handled_task_id=prepared_plan_improve_task.id,
            worker_label="plan_improve",
            created_task=prepared_plan_improve_task,
        )
        result.success_message = f"Created plan improve task {prepared_plan_improve_task.id}"
        return result

    if action_type == "run_plan_improve":
        pending_plan_improve_task = action.get("plan_improve_task")
        if not isinstance(pending_plan_improve_task, DbTask) or pending_plan_improve_task.id is None:
            return AdvanceActionExecutionResult(action_type=action_type, status="skip", message="missing plan improve task")
        plan_improve_task = pending_plan_improve_task
        if context.dry_run:
            return AdvanceActionExecutionResult(
                action_type=action_type,
                status="dry_run",
                message=action.get("description", "Run plan improve"),
                worker_consuming=True,
                work_done=True,
                handled_task_id=plan_improve_task.id,
                created_task=plan_improve_task,
            )

        permit, blocked = _reserve_background_launch(
            action_type=action_type,
            context=context,
            worker_label="plan_improve",
        )
        if blocked is not None:
            return blocked
        prepared_plan_improve_task, prepare_error = _prepare_background_start(
            context=context,
            action_type=action_type,
            task=plan_improve_task,
            worker_label="plan_improve",
            rollback_on_failure=False,
            permit=permit,
        )
        if prepared_plan_improve_task is None:
            assert prepare_error is not None
            return prepare_error
        assert prepared_plan_improve_task.id is not None

        rc = context.spawn_worker(prepared_plan_improve_task, "plan_improve")
        _release_reserved_launch_if_left(prepared_plan_improve_task)
        return _spawn_result(
            action_type=action_type,
            rc=rc,
            handled_task_id=prepared_plan_improve_task.id,
            worker_label="plan_improve",
            created_task=prepared_plan_improve_task,
        )

    if action_type == "materialize_plan_slices":
        review_task = action.get("plan_review_task")
        plan_source_task = action.get("plan_source_task")
        manifest = action.get("manifest")
        if (
            not isinstance(review_task, DbTask)
            or review_task.id is None
            or not isinstance(plan_source_task, DbTask)
            or plan_source_task.id is None
            or not isinstance(manifest, PlanReviewManifest)
        ):
            return AdvanceActionExecutionResult(action_type=action_type, status="skip", message="missing plan materialization inputs")
        if context.dry_run:
            return AdvanceActionExecutionResult(
                action_type=action_type,
                status="dry_run",
                message=action.get("description", "Materialize plan slices"),
                work_done=True,
            )

        if context.materialize_plan_slices is None:
            return AdvanceActionExecutionResult(action_type=action_type, status="error", message="plan slice materialization is unavailable")
        materialization = context.materialize_plan_slices(plan_source_task, review_task, manifest)
        created_tasks = materialization.tasks
        created_ids = ", ".join(task.id or "unknown" for task in created_tasks)
        return AdvanceActionExecutionResult(
            action_type=action_type,
            status="success",
            message=f"Materialized implementation slices: {created_ids}",
            success_message=f"Materialized implementation slices: {created_ids}",
            work_done=bool(created_tasks),
            handled_task_id=review_task.id,
            created_task=created_tasks[0] if created_tasks else None,
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

        permit, blocked = _reserve_background_launch(
            action_type=action_type,
            context=context,
            worker_label="review",
        )
        if blocked is not None:
            return blocked
        create_result = context.prepare_create_review(task)
        if create_result.status == "skip":
            if permit is not None:
                permit.release()
            return AdvanceActionExecutionResult(
                action_type=action_type,
                status="skip",
                message=create_result.message,
            )

        review_task = create_result.review_task
        if review_task is None or review_task.id is None:
            if permit is not None:
                permit.release()
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
            permit=permit,
        )
        if prepared_review_task is None:
            assert prepare_error is not None
            return prepare_error
        assert prepared_review_task.id is not None

        rc = context.spawn_worker(prepared_review_task, "review")
        _release_reserved_launch_if_left(prepared_review_task)
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

        permit, blocked = _reserve_background_launch(
            action_type=action_type,
            context=context,
            worker_label="review",
        )
        if blocked is not None:
            return blocked
        prepared_review_task, prepare_error = _prepare_background_start(
            context=context,
            action_type=action_type,
            task=review_task,
            worker_label="review",
            rollback_on_failure=False,
            permit=permit,
        )
        if prepared_review_task is None:
            assert prepare_error is not None
            return prepare_error
        assert prepared_review_task.id is not None

        rc = context.spawn_worker(prepared_review_task, "review")
        _release_reserved_launch_if_left(prepared_review_task)
        return _spawn_result(
            action_type=action_type,
            rc=rc,
            handled_task_id=prepared_review_task.id,
            worker_label="review",
            created_task=prepared_review_task,
        )

    if action_type == "verify_noop_improve_then_review":
        if context.dry_run:
            return AdvanceActionExecutionResult(
                action_type=action_type,
                status="dry_run",
                message=str(action.get("description", "Re-run verify_command before re-review")),
                worker_consuming=True,
                work_done=True,
            )
        reserved_verify_permit: LaunchPermit | None = None

        def _prepare_review_after_verify(parent_task: DbTask) -> CreateReviewActionResult:
            nonlocal reserved_verify_permit
            permit, blocked = _reserve_background_launch(
                action_type=action_type,
                context=context,
                worker_label="review",
            )
            if blocked is not None:
                return PreparedCreateReviewActionResult(
                    status="error",
                    review_task=None,
                    message=blocked.message,
                )
            reserved_verify_permit = permit
            create_result = context.prepare_create_review(parent_task)
            if create_result.status != "created":
                if reserved_verify_permit is not None:
                    reserved_verify_permit.release()
                    reserved_verify_permit = None
                return create_result
            review_task = create_result.review_task
            if review_task is None or review_task.id is None:
                if reserved_verify_permit is not None:
                    reserved_verify_permit.release()
                    reserved_verify_permit = None
                return PreparedCreateReviewActionResult(
                    status="error",
                    review_task=None,
                    message="review creation returned no task",
                )
            prepared_review_task, prepare_error = _prepare_background_start(
                context=context,
                action_type=action_type,
                task=review_task,
                worker_label="review",
                rollback_on_failure=True,
                permit=reserved_verify_permit,
            )
            reserved_verify_permit = None
            if prepared_review_task is None:
                assert prepare_error is not None
                return PreparedCreateReviewActionResult(
                    status="error",
                    review_task=None,
                    message=prepare_error.message or "startup preparation failed for review",
                )
            return PreparedCreateReviewActionResult(
                status="created",
                review_task=prepared_review_task,
                message=create_result.message,
            )

        verify_context = AdvanceActionExecutionContext(
            store=context.store,
            trigger_source=context.trigger_source,
            dry_run=context.dry_run,
            max_resume_attempts=context.max_resume_attempts,
            use_iterate_for_create_implement=context.use_iterate_for_create_implement,
            use_iterate_for_needs_rebase=context.use_iterate_for_needs_rebase,
            prepare_task_for_background_start=context.prepare_task_for_background_start,
            prepare_create_review=_prepare_review_after_verify,
            create_resume_task=context.create_resume_task,
            create_rebase_task=context.create_rebase_task,
            create_implement_task=context.create_implement_task,
            spawn_worker=context.spawn_worker,
            spawn_resume_worker=context.spawn_resume_worker,
            spawn_iterate_worker=context.spawn_iterate_worker,
            can_spawn_worker=context.can_spawn_worker,
            no_worker_capacity_message=context.no_worker_capacity_message,
            is_rebase_target_already_merged=context.is_rebase_target_already_merged,
            prefer_iterate_for_action=context.prefer_iterate_for_action,
            spawn_iterate_recovery=context.spawn_iterate_recovery,
            create_retry_task=context.create_retry_task,
            create_targeted_rebase_task=context.create_targeted_rebase_task,
            reconcile_diverged_branch=context.reconcile_diverged_branch,
            config=context.config,
            git=context.git,
        )
        outcome = run_noop_improve_verify_then_review(task=task, action=action, context=verify_context)
        if outcome.status == "needs_attention":
            return AdvanceActionExecutionResult(
                action_type=action_type,
                status="skip",
                message=outcome.message,
                attention_type="needs_discussion",
                attention_reason="improve-no-op",
            )
        if outcome.status == "skip":
            return AdvanceActionExecutionResult(action_type=action_type, status="skip", message=outcome.message)
        if outcome.status == "error":
            return AdvanceActionExecutionResult(action_type=action_type, status="error", message=outcome.message)
        if outcome.status == "review_cleared":
            return AdvanceActionExecutionResult(
                action_type=action_type,
                status="success",
                message=outcome.verify_markdown or outcome.message,
                success_message=outcome.message,
                work_done=True,
                handled_task_id=task.id,
            )
        new_review = outcome.review_task
        assert new_review is not None and new_review.id is not None
        rc = context.spawn_worker(new_review, "review")
        _release_reserved_launch_if_left(new_review)
        result = _spawn_result(
            action_type=action_type,
            rc=rc,
            handled_task_id=new_review.id,
            worker_label="review",
            created_task=new_review,
        )
        result.success_message = outcome.message
        result.message = outcome.verify_markdown or outcome.message
        return result

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

        permit, blocked = _reserve_background_launch(
            action_type=action_type,
            context=context,
            worker_label="improve",
        )
        if blocked is not None:
            return blocked
        if improve_mode == "resume" and failed_improve is not None:
            assert failed_improve.id is not None
            improve_task = context.create_resume_task(failed_improve)
        elif improve_mode == "retry" and failed_improve is not None:
            assert failed_improve.id is not None
            if context.create_retry_task is not None:
                improve_task = context.create_retry_task(failed_improve)
            else:
                improve_task = _create_retry_task(
                    context.store,
                    failed_improve,
                    trigger_source=context.trigger_source,
                )
        else:
            try:
                improve_task = _create_improve_task(
                    context.store,
                    task,
                    review_task,
                    trigger_source=context.trigger_source,
                )
            except ValueError as exc:
                if permit is not None:
                    permit.release()
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
            permit=permit,
        )
        if prepared_improve_task is None:
            assert prepare_error is not None
            return prepare_error
        assert prepared_improve_task.id is not None
        if prepared_improve_task.session_id is not None:
            rc = context.spawn_resume_worker(prepared_improve_task, "improve")
        else:
            rc = context.spawn_worker(prepared_improve_task, "improve")
        _release_reserved_launch_if_left(prepared_improve_task)

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

        permit, blocked = _reserve_background_launch(
            action_type=action_type,
            context=context,
            worker_label="improve",
        )
        if blocked is not None:
            return blocked
        prepared_improve_task, prepare_error = _prepare_background_start(
            context=context,
            action_type=action_type,
            task=run_improve_task,
            worker_label="improve",
            rollback_on_failure=False,
            permit=permit,
        )
        if prepared_improve_task is None:
            assert prepare_error is not None
            return prepare_error
        assert prepared_improve_task.id is not None

        rc = context.spawn_worker(prepared_improve_task, "improve")
        _release_reserved_launch_if_left(prepared_improve_task)
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
            permit, blocked = _reserve_background_launch(
                action_type=action_type,
                context=context,
                worker_label="iterate",
            )
            if blocked is not None:
                return blocked
            if context.spawn_iterate_recovery is None:
                if permit is not None:
                    permit.release()
                return AdvanceActionExecutionResult(
                    action_type=action_type,
                    status="error",
                    message="missing iterate recovery launcher",
                )
            if reuse_existing and isinstance(resume_task_id, str):
                resume_task = context.store.get(resume_task_id)
                if resume_task is None:
                    if permit is not None:
                        permit.release()
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
                permit=permit,
            )
            if prepared_resume_task is None:
                assert prepare_error is not None
                return prepare_error
            assert prepared_resume_task.id is not None

            rc = context.spawn_iterate_recovery(task, "resume", prepared_resume_task)
            _release_reserved_launch_if_left(prepared_resume_task)
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

        permit, blocked = _reserve_background_launch(
            action_type=action_type,
            context=context,
            worker_label="resume",
        )
        if blocked is not None:
            return blocked
        if reuse_existing and isinstance(resume_task_id, str):
            resume_task = context.store.get(resume_task_id)
            if resume_task is None:
                if permit is not None:
                    permit.release()
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
            permit=permit,
        )
        if prepared_resume_task is None:
            assert prepare_error is not None
            return prepare_error

        assert prepared_resume_task.id is not None
        rc = context.spawn_resume_worker(prepared_resume_task, task.task_type or "task")
        _release_reserved_launch_if_left(prepared_resume_task)
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
        permit, blocked = _reserve_background_launch(
            action_type=action_type,
            context=context,
            worker_label="iterate" if launch_mode == "iterate" else "retry",
        )
        if blocked is not None:
            return blocked
        if reuse_existing and isinstance(retry_task_id, str):
            retry_task = context.store.get(retry_task_id)
            if retry_task is None:
                if permit is not None:
                    permit.release()
                return AdvanceActionExecutionResult(
                    action_type=action_type,
                    status="error",
                    message=f"missing existing retry task {retry_task_id}",
                )
        else:
            if context.create_retry_task is None:
                if permit is not None:
                    permit.release()
                return AdvanceActionExecutionResult(
                    action_type=action_type,
                    status="error",
                    message="missing retry task factory",
                )
            retry_task = context.create_retry_task(task)
        if launch_mode == "iterate":
            if context.spawn_iterate_recovery is None:
                if permit is not None:
                    permit.release()
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
                permit=permit,
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
                permit=permit,
            )
            if prepared_retry_task is None:
                assert prepare_error is not None
                return prepare_error
            assert prepared_retry_task.id is not None
            rc = context.spawn_worker(prepared_retry_task, task.task_type or "task")
            worker_label = "retry"
            handled_task = prepared_retry_task
        _release_reserved_launch_if_left(handled_task)
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

        permit, blocked = _reserve_background_launch(
            action_type=action_type,
            context=context,
            worker_label="iterate" if context.use_iterate_for_create_implement else "implement",
        )
        if blocked is not None:
            return blocked
        impl_task = context.create_implement_task(task)
        prepared_impl_task, prepare_error = _prepare_background_start(
            context=context,
            action_type=action_type,
            task=impl_task,
            worker_label="implement" if not context.use_iterate_for_create_implement else "iterate",
            rollback_on_failure=True,
            permit=permit,
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
        _release_reserved_launch_if_left(prepared_impl_task)

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

        permit, blocked = _reserve_background_launch(
            action_type=action_type,
            context=context,
            worker_label="iterate" if context.use_iterate_for_needs_rebase else "rebase",
        )
        if blocked is not None:
            return blocked
        rebase_task = context.create_rebase_task(task)
        prepared_rebase_task, prepare_error = _prepare_background_start(
            context=context,
            action_type=action_type,
            task=rebase_task,
            worker_label="rebase",
            rollback_on_failure=True,
            permit=permit,
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
        _release_reserved_launch_if_left(prepared_rebase_task)

        result = _spawn_result(
            action_type=action_type,
            rc=rc,
            handled_task_id=prepared_rebase_task.id,
            worker_label=worker_label,
            created_task=prepared_rebase_task,
        )
        result.success_message = f"Created rebase task {prepared_rebase_task.id}"
        return result

    if action_type == "reconcile_branch_divergence":
        if task.id is None or not task.branch:
            return AdvanceActionExecutionResult(
                action_type=action_type,
                status="error",
                message=f"Cannot reconcile divergence: task {task.id} has no branch",
            )
        if context.dry_run:
            return AdvanceActionExecutionResult(
                action_type=action_type,
                status="dry_run",
                message=action.get("description", "Reconcile diverged local/origin refs"),
                worker_consuming=False,
                work_done=True,
            )
        if context.reconcile_diverged_branch is None:
            return AdvanceActionExecutionResult(
                action_type=action_type,
                status="error",
                message="missing branch reconciliation helper",
            )

        reconcile_outcome = context.reconcile_diverged_branch(task)
        if reconcile_outcome.status == "reconciled":
            if _should_continue_branch_publication_after_reconcile(task=task, action=action):
                if context.config is None or context.git is None:
                    return AdvanceActionExecutionResult(
                        action_type=action_type,
                        status="error",
                        message="missing config/git context for branch publication continuation",
                    )
                from .git_ops import complete_branch_unpushable_after_reconcile

                completion_rc = complete_branch_unpushable_after_reconcile(
                    config=context.config,
                    store=context.store,
                    git=context.git,
                    task=task,
                )
                if completion_rc != 0:
                    refreshed = context.store.get(task.id) if task.id is not None else None
                    failure_reason = refreshed.failure_reason if refreshed is not None else task.failure_reason
                    return AdvanceActionExecutionResult(
                        action_type=action_type,
                        status="error",
                        message=(
                            reconcile_outcome.message
                            if failure_reason is None
                            else f"{reconcile_outcome.message}; completion retry ended in {failure_reason}"
                        ),
                    )
            return AdvanceActionExecutionResult(
                action_type=action_type,
                status="success",
                message=reconcile_outcome.message,
                work_done=True,
            )
        if reconcile_outcome.status == "error":
            return AdvanceActionExecutionResult(
                action_type=action_type,
                status="error",
                message=reconcile_outcome.message,
            )
        if reconcile_outcome.status == "needs_attention":
            return AdvanceActionExecutionResult(
                action_type=action_type,
                status="skip",
                message=reconcile_outcome.message,
                attention_type="needs_discussion",
                attention_reason=reconcile_outcome.attention_reason or "reconcile-needs-manual-resolution",
            )

        rebase_target = reconcile_outcome.rebase_target or f"origin/{task.branch}"
        capacity_blocked = _worker_capacity_blocked_result(
            action_type=action_type,
            context=context,
            worker_label="rebase",
        )
        if capacity_blocked is not None:
            return capacity_blocked
        permit, blocked = _reserve_background_launch(
            action_type=action_type,
            context=context,
            worker_label="rebase",
        )
        if blocked is not None:
            return blocked
        create_rebase_task = context.create_targeted_rebase_task
        rebase_task = (
            create_rebase_task(task, rebase_target)
            if create_rebase_task is not None
            else context.create_rebase_task(task)
        )
        prepared_rebase_task, prepare_error = _prepare_background_start(
            context=context,
            action_type=action_type,
            task=rebase_task,
            worker_label="rebase",
            rollback_on_failure=True,
            permit=permit,
        )
        if prepared_rebase_task is None:
            assert prepare_error is not None
            return prepare_error

        assert prepared_rebase_task.id is not None
        rc = context.spawn_worker(prepared_rebase_task, "rebase")
        _release_reserved_launch_if_left(prepared_rebase_task)
        result = _spawn_result(
            action_type=action_type,
            rc=rc,
            handled_task_id=prepared_rebase_task.id,
            worker_label="rebase",
            created_task=prepared_rebase_task,
        )
        result.success_message = f"Created rebase task {prepared_rebase_task.id}"
        return result

    return AdvanceActionExecutionResult(
        action_type=action_type,
        status="unsupported",
        message=f"unsupported action: {action_type}",
    )
