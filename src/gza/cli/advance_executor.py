"""Shared per-action execution for advance-style lifecycle commands."""

from __future__ import annotations

import shutil
import tempfile
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal, Protocol

from ..advance_engine import (
    NOOP_IMPROVE_KIND_VERIFY_ONLY,
    PARK_REASON_IMPROVE_NO_OP,
    REVIEW_CLEARANCE_ARTIFACT_KIND,
    VERIFY_ONLY_NOOP_RECOVERY_ATTENTION_ARTIFACT_KIND,
    VERIFY_ONLY_NOOP_RECOVERY_ATTENTION_STATUS,
    VERIFY_ONLY_NOOP_REVIEW_CLEARANCE_KIND,
    VERIFY_ONLY_NOOP_REVIEW_CLEARANCE_STATUS,
)
from ..artifacts import store_command_output_artifact
from ..concurrency import (
    LaunchPermit,
    MaxConcurrentTasksError,
    launch_permit,
    release_task_launch_permit,
    reserve_task_launch_permit,
)
from ..db import DuplicateActiveChildError, SqliteTaskStore, Task as DbTask
from ..flaky_investigations import create_or_reuse_flaky_investigations
from ..git import Git, GitError
from ..plan_review_verdict import PlanReviewManifest
from ..recovery_engine import FailedRecoveryDecision, get_failed_recovery_needs_attention_reason
from ..review_tasks import (
    DuplicateReviewError,
    OffTopicVerifyPersistenceError,
    build_review_blocker_dispute_metadata,
    create_resolution_review_task,
    create_spec_coherence_review_task,
    persist_review_clearance_artifact,
)
from ..runner import (
    ProjectReviewVerifyResult,
    _capture_review_verify_result,
    _format_review_verify_result,
    _make_review_verify_result,
    _project_boundary,
    _resolve_review_verify_base_sha,
    _resolve_review_verify_timeout_settings,
    _run_review_verify_command,
    _run_review_verify_commands_for_projects,
    _task_is_cross_project,
    _worktree_execution_dir,
)
from ._common import (
    PlanReviewMaterializationResult,
    _create_improve_task,
    _create_retry_task,
    _prepare_task_for_reserved_launch,
    format_duplicate_active_child_message,
    format_duplicate_rebase_message,
    release_held_plan_source,
    resolve_improve_action,
)
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
    create_review_adjudication_task: Callable[[DbTask, DbTask, Any, dict[str, Any]], DbTask] | None = None
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
    created_investigations: tuple[DbTask, ...] = ()
    reused_investigations: tuple[DbTask, ...] = ()
    improve_mode: str | None = None
    failed_improve: DbTask | None = None
    attention_type: str | None = None
    attention_reason: str | None = None
    worker_label: str | None = None
    guarded_pending_task_id: str | None = None
    noop_improve_kind: str | None = None


@dataclass(frozen=True)
class AdvanceExecutionNeedsAttention:
    """Normalized execution-time needs-attention payload for shared renderers."""

    task: DbTask
    action: dict[str, Any]


@dataclass
class _InlineCreateReviewActionResult:
    status: str
    review_task: DbTask | None
    message: str


_WORKER_ACTIONS = frozenset(
    {
        "create_plan_review",
        "run_plan_review",
        "create_plan_improve",
        "run_plan_improve",
        "materialize_plan_slices",
        "clear_off_topic_verify_blocker",
        "recover_verify_only_noop_review",
        "create_review",
        "run_review",
        "create_review_adjudication",
        "run_review_adjudication",
        "improve",
        "run_improve",
        "resume",
        "retry",
        "create_implement",
        "needs_rebase",
        "reconcile_branch_divergence",
    }
)

_DIRECT_ACTIONS = frozenset({"release_approved_plan_review"})


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


def _prepare_resolution_review_action(
    store: SqliteTaskStore,
    task: DbTask,
    action: dict[str, Any],
    *,
    trigger_source: str,
) -> CreateReviewActionResult:
    rebase_task_id = action.get("resolution_rebase_task_id")
    resolved_head_sha = str(action.get("resolution_head_sha") or "").strip()
    resolved_target_sha = str(action.get("resolution_target_sha") or "").strip()
    if not isinstance(rebase_task_id, str) or not rebase_task_id.strip():
        return _InlineCreateReviewActionResult(status="skip", review_task=None, message="SKIP: missing resolution rebase task")
    rebase_task = store.get(rebase_task_id)
    if rebase_task is None:
        return _InlineCreateReviewActionResult(
            status="skip",
            review_task=None,
            message=f"SKIP: missing rebase task {rebase_task_id}",
        )
    try:
        review_task = create_resolution_review_task(
            store,
            task,
            rebase_task=rebase_task,
            resolved_head_sha=resolved_head_sha,
            resolved_target_sha=resolved_target_sha,
            trigger_source=trigger_source,
        )
    except DuplicateReviewError as exc:
        return _InlineCreateReviewActionResult(
            status="skip",
            review_task=exc.active_review,
            message=f"SKIP: review {exc.active_review.id} is already {exc.active_review.status}",
        )
    except ValueError as exc:
        return _InlineCreateReviewActionResult(status="skip", review_task=None, message=f"SKIP: {exc}")
    return _InlineCreateReviewActionResult(
        status="created",
        review_task=review_task,
        message=f"Created resolution review task {review_task.id}",
    )


def _prepare_spec_coherence_review_action(
    store: SqliteTaskStore,
    task: DbTask,
    action: dict[str, Any],
    *,
    trigger_source: str,
) -> CreateReviewActionResult:
    raw_head_sha = action.get("review_head_sha")
    if not isinstance(raw_head_sha, str) or not raw_head_sha.strip():
        return _InlineCreateReviewActionResult(
            status="skip",
            review_task=None,
            message="SKIP: missing behavior-spec coherence reviewed head SHA",
        )
    raw_paths = action.get("review_changed_paths")
    if not isinstance(raw_paths, (tuple, list)):
        return _InlineCreateReviewActionResult(
            status="skip",
            review_task=None,
            message="SKIP: missing behavior-spec coherence changed paths",
        )
    changed_paths = tuple(str(path).strip() for path in raw_paths if str(path).strip())
    if not changed_paths:
        return _InlineCreateReviewActionResult(
            status="skip",
            review_task=None,
            message="SKIP: missing behavior-spec coherence changed paths",
        )
    try:
        review_task = create_spec_coherence_review_task(
            store,
            task,
            reviewed_head_sha=raw_head_sha.strip(),
            changed_paths=changed_paths,
            trigger_source=trigger_source,
        )
    except DuplicateReviewError as exc:
        return _InlineCreateReviewActionResult(
            status="skip",
            review_task=exc.active_review,
            message=f"SKIP: review {exc.active_review.id} is already {exc.active_review.status}",
        )
    except ValueError as exc:
        return _InlineCreateReviewActionResult(
            status="skip",
            review_task=None,
            message=f"SKIP: {exc}",
        )
    return _InlineCreateReviewActionResult(
        status="created",
        review_task=review_task,
        message=f"Created behavior-spec coherence review task {review_task.id}",
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
    if result.noop_improve_kind is not None:
        action["noop_improve_kind"] = result.noop_improve_kind
    subject_task = result.failed_improve or task
    display_task = subject_task
    if result.action_type == "improve" and result.attention_type in {"automatic_recovery_disabled", "manual_review_required"}:
        display_task = task
    if subject_task.id is not None:
        action["subject_task_id"] = subject_task.id
    if classify_advance_action(action) != "needs_attention":
        return None
    return AdvanceExecutionNeedsAttention(
        task=display_task,
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


ITERATE_ROUTABLE_ACTIONS = frozenset({"create_review", "run_review", "improve", "run_improve"})


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


def _skip_duplicate_rebase_creation(
    *,
    action_type: str,
    permit: LaunchPermit | None,
    exc: DuplicateActiveChildError,
    parent_task_id: str | None,
) -> AdvanceActionExecutionResult:
    if permit is not None:
        permit.release()
    return AdvanceActionExecutionResult(
        action_type=action_type,
        status="skip",
        message=f"SKIP: {format_duplicate_rebase_message(exc, parent_task_id=parent_task_id)}",
        worker_consuming=False,
        work_done=False,
    )


def _skip_duplicate_recovery_creation(
    *,
    action_type: str,
    permit: LaunchPermit | None,
    exc: DuplicateActiveChildError,
    task: DbTask,
) -> AdvanceActionExecutionResult:
    if permit is not None:
        permit.release()
    return AdvanceActionExecutionResult(
        action_type=action_type,
        status="skip",
        message=f"SKIP: {format_duplicate_active_child_message(exc, parent_task_id=task.id, task=task)}",
        worker_consuming=False,
        work_done=False,
    )


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


def _persist_verify_only_noop_clearance(
    *,
    context: AdvanceActionExecutionContext,
    task: DbTask,
    review_task: DbTask,
    noop_improve_task: DbTask,
    reviewed_head_sha: str,
    captured_at: datetime,
) -> datetime | None:
    if context.config is None or task.id is None or review_task.id is None or noop_improve_task.id is None:
        return None
    persisted_at = _verify_only_noop_recorded_at(
        review_task=review_task,
        noop_improve_task=noop_improve_task,
        captured_at=captured_at,
    )
    clearance_payload = {
        "schema_version": 1,
        "clearance_kind": VERIFY_ONLY_NOOP_REVIEW_CLEARANCE_KIND,
        "clearance_status": VERIFY_ONLY_NOOP_REVIEW_CLEARANCE_STATUS,
        "implementation_task_id": task.id,
        "review_task_id": review_task.id,
        "source_task_id": noop_improve_task.id,
        "noop_improve_kind": NOOP_IMPROVE_KIND_VERIFY_ONLY,
        "reviewed_head_sha": reviewed_head_sha,
        "captured_at": captured_at.isoformat(),
    }
    persisted = persist_review_clearance_artifact(
        context.store,
        config=context.config,
        impl_task=task,
        clearance_payload=clearance_payload,
        created_at=persisted_at,
        review_clearance_artifact_kind=REVIEW_CLEARANCE_ARTIFACT_KIND,
        review_clearance_artifact_label="review_clearance",
        review_clearance_artifact_producer="advance_verify_only_noop_recovered",
        status=VERIFY_ONLY_NOOP_REVIEW_CLEARANCE_STATUS,
        head_sha=reviewed_head_sha,
        metadata={
            "clearance_kind": VERIFY_ONLY_NOOP_REVIEW_CLEARANCE_KIND,
            "clearance_status": VERIFY_ONLY_NOOP_REVIEW_CLEARANCE_STATUS,
            "review_task_id": review_task.id,
            "source_task_id": noop_improve_task.id,
            "noop_improve_kind": NOOP_IMPROVE_KIND_VERIFY_ONLY,
            "reviewed_head_sha": reviewed_head_sha,
        },
    )
    return persisted.review_cleared_at


def _verify_only_noop_recorded_at(
    *,
    review_task: DbTask,
    noop_improve_task: DbTask,
    captured_at: datetime | None,
) -> datetime:
    """Clamp verify-only noop persistence to the latest known lineage event."""
    recorded_at = captured_at or datetime.now(UTC)
    if review_task.completed_at is not None and review_task.completed_at > recorded_at:
        recorded_at = review_task.completed_at
    if noop_improve_task.completed_at is not None and noop_improve_task.completed_at > recorded_at:
        recorded_at = noop_improve_task.completed_at
    return recorded_at


def _verify_only_noop_attention_result(
    *,
    action_type: str,
    message: str,
) -> AdvanceActionExecutionResult:
    return AdvanceActionExecutionResult(
        action_type=action_type,
        status="skip",
        message=message,
        attention_type="needs_discussion",
        attention_reason=PARK_REASON_IMPROVE_NO_OP,
        noop_improve_kind=NOOP_IMPROVE_KIND_VERIFY_ONLY,
    )


def _persist_verify_only_noop_attention(
    *,
    context: AdvanceActionExecutionContext,
    task: DbTask,
    review_task: DbTask,
    noop_improve_task: DbTask,
    reviewed_head_sha: str,
    message: str,
    outcome_kind: str,
    verify_status: str | None = None,
    captured_at: datetime | None = None,
) -> None:
    if context.config is None:
        raise ValueError("config is required to persist verify-only no-op recovery attention")
    persisted_at = _verify_only_noop_recorded_at(
        review_task=review_task,
        noop_improve_task=noop_improve_task,
        captured_at=captured_at,
    )
    store_command_output_artifact(
        context.store,
        noop_improve_task,
        context.config,
        kind=VERIFY_ONLY_NOOP_RECOVERY_ATTENTION_ARTIFACT_KIND,
        producer="advance_verify_only_noop_recovery",
        label="verify_only_noop_recovery_attention",
        output=message,
        status=VERIFY_ONLY_NOOP_RECOVERY_ATTENTION_STATUS,
        head_sha=reviewed_head_sha,
        metadata={
            "schema_version": 1,
            "attention_reason": PARK_REASON_IMPROVE_NO_OP,
            "implementation_task_id": task.id,
            "review_task_id": review_task.id,
            "source_task_id": noop_improve_task.id,
            "noop_improve_kind": NOOP_IMPROVE_KIND_VERIFY_ONLY,
            "reviewed_head_sha": reviewed_head_sha,
            "message": message,
            "outcome_kind": outcome_kind,
            "verify_status": verify_status,
        },
        created_at=persisted_at,
    )


def _verify_only_noop_error_result(
    *,
    action_type: str,
    message: str,
) -> AdvanceActionExecutionResult:
    return AdvanceActionExecutionResult(
        action_type=action_type,
        status="error",
        message=message,
        noop_improve_kind=NOOP_IMPROVE_KIND_VERIFY_ONLY,
    )


def _cleanup_verify_only_noop_worktree(
    *,
    context: AdvanceActionExecutionContext,
    worktree_path: Path | None,
    added_worktree: bool,
) -> str | None:
    if worktree_path is None:
        return None
    if context.git is None:
        return "missing git context for isolated worktree cleanup"

    cleanup_failures: list[str] = []
    if added_worktree:
        try:
            context.git.worktree_remove(worktree_path, force=True)
        except (GitError, OSError, RuntimeError, ValueError) as exc:
            cleanup_failures.append(f"worktree removal failed: {exc}")
    try:
        shutil.rmtree(worktree_path)
    except OSError as exc:
        if worktree_path.exists():
            cleanup_failures.append(f"temporary directory cleanup failed: {exc}")
    if cleanup_failures:
        return "; ".join(cleanup_failures)
    return None


def _execute_recover_verify_only_noop_review(
    *,
    task: DbTask,
    action_type: str,
    action: dict[str, Any],
    context: AdvanceActionExecutionContext,
) -> AdvanceActionExecutionResult:
    review_task = action.get("review_task")
    noop_improve_task = action.get("latest_noop_improve_task")
    current_branch_head_sha = action.get("current_branch_head_sha")
    if (
        not isinstance(review_task, DbTask)
        or review_task.id is None
        or not isinstance(noop_improve_task, DbTask)
        or noop_improve_task.id is None
        or task.id is None
        or context.config is None
        or context.git is None
        or task.branch is None
        or not isinstance(current_branch_head_sha, str)
        or not current_branch_head_sha
    ):
        return AdvanceActionExecutionResult(
            action_type=action_type,
            status="skip",
            message="missing verify-only no-op recovery inputs",
        )
    if context.dry_run:
        return AdvanceActionExecutionResult(
            action_type=action_type,
            status="dry_run",
            message=action.get("description", "Fresh verify on current tip"),
            work_done=True,
            handled_task_id=task.id,
        )

    def _persist_attention(
        message: str,
        *,
        outcome_kind: str,
        verify_status: str | None = None,
        captured_at: datetime | None = None,
    ) -> AdvanceActionExecutionResult:
        try:
            _persist_verify_only_noop_attention(
                context=context,
                task=task,
                review_task=review_task,
                noop_improve_task=noop_improve_task,
                reviewed_head_sha=current_branch_head_sha,
                message=message,
                outcome_kind=outcome_kind,
                verify_status=verify_status,
                captured_at=captured_at,
            )
        except (OSError, ValueError, RuntimeError) as exc:
            return _verify_only_noop_error_result(
                action_type=action_type,
                message=f"failed to persist verify-only no-op recovery attention: {exc}",
            )
        return _verify_only_noop_attention_result(
            action_type=action_type,
            message=message,
        )

    live_head_before = context.git.rev_parse_if_exists(task.branch)
    if live_head_before != current_branch_head_sha:
        return _persist_attention(
            "SKIP: verify-only no-op recovery no longer matches the evaluated branch tip; "
            "rerun lifecycle on the current head.",
            outcome_kind="head_drift_before_verify",
        )

    worktree_path: Path | None = None
    added_worktree = False
    try:
        tmp_root = Path(context.config.project_dir) / "tmp"
        tmp_root.mkdir(parents=True, exist_ok=True)
        worktree_path = Path(
            tempfile.mkdtemp(
                prefix=f"verify-only-noop-{task.id or 'task'}-",
                dir=tmp_root,
            )
        )
        context.git.worktree_add_existing(worktree_path, current_branch_head_sha, detach=True)
        added_worktree = True
        worktree_git = Git(worktree_path)
    except (GitError, OSError, RuntimeError, ValueError) as exc:
        cleanup_failure = _cleanup_verify_only_noop_worktree(
            context=context,
            worktree_path=worktree_path,
            added_worktree=added_worktree,
        )
        message = (
            "SKIP: verify-only no-op recovery could not prepare its isolated worktree; "
            f"manual attention is required. Setup failure: {exc}"
        )
        if cleanup_failure:
            message = f"{message}. Cleanup failure: {cleanup_failure}"
        return _persist_attention(message, outcome_kind="setup_failure")

    try:
        timeout_seconds, timeout_grace_seconds = _resolve_review_verify_timeout_settings(context.config)
        provider_cwd = _worktree_execution_dir(worktree_git.repo_dir, _project_boundary(context.config))
        verify_command = (
            str(context.config.verify_command).strip()
            if isinstance(getattr(context.config, "verify_command", None), str)
            else ""
        )
        reviewed_base_sha: str | None = None
        reviewed_head_sha: str | None = None
        project_results: tuple[ProjectReviewVerifyResult, ...] = ()
        deferred_attention_message: str | None = None
        deferred_attention_outcome_kind: str | None = None
        deferred_attention_verify_status: str | None = None
        deferred_attention_captured_at: datetime | None = None
        command_label = verify_command or "(review verify unavailable)"
        result = None
        try:
            default_branch = worktree_git.default_branch()
            reviewed_base_sha = _resolve_review_verify_base_sha(worktree_git, default_branch)
            reviewed_head_sha = worktree_git.rev_parse_if_exists("HEAD")
            if reviewed_head_sha is None:
                result = _make_review_verify_result(
                    command_label,
                    status="unavailable",
                    exit_status="unresolved head",
                    captured_at=datetime.now(UTC),
                    reviewed_branch=task.branch,
                    reviewed_head_sha=None,
                    reviewed_base_sha=reviewed_base_sha,
                    working_directory=str(provider_cwd),
                    failure="unable to resolve detached review-verify HEAD",
                )
            elif _task_is_cross_project(noop_improve_task):
                cross_project_verify = _run_review_verify_commands_for_projects(
                    config=context.config,
                    task=noop_improve_task,
                    worktree_git=worktree_git,
                    worktree_path=worktree_git.repo_dir,
                    timeout_seconds=timeout_seconds,
                    timeout_grace_seconds=timeout_grace_seconds,
                    reviewed_branch=task.branch,
                    reviewed_head_sha=reviewed_head_sha,
                    reviewed_base_sha=reviewed_base_sha,
                )
                if cross_project_verify is None:
                    deferred_attention_message = (
                        "SKIP: verify-only no-op recovery could not run cross-project verify."
                    )
                    deferred_attention_outcome_kind = "cross_project_verify_unavailable"
                else:
                    result = cross_project_verify.aggregate_result
                    project_results = cross_project_verify.project_results
            elif not verify_command:
                result = _make_review_verify_result(
                    command_label,
                    status="unavailable",
                    exit_status="not configured",
                    captured_at=datetime.now(UTC),
                    reviewed_branch=task.branch,
                    reviewed_head_sha=reviewed_head_sha,
                    reviewed_base_sha=reviewed_base_sha,
                    working_directory=str(provider_cwd),
                    failure="verify_command is not configured for verify-only no-op recovery",
                )
            else:
                result = _run_review_verify_command(
                    verify_command,
                    cwd=provider_cwd,
                    reviewed_branch=task.branch,
                    reviewed_head_sha=reviewed_head_sha,
                    reviewed_base_sha=reviewed_base_sha,
                    timeout_seconds=timeout_seconds,
                    timeout_grace_seconds=timeout_grace_seconds,
                )
        except (GitError, OSError, RuntimeError, ValueError) as exc:
            result = _make_review_verify_result(
                command_label,
                status="unavailable",
                exit_status="launch failed",
                captured_at=datetime.now(UTC),
                reviewed_branch=task.branch,
                reviewed_head_sha=reviewed_head_sha,
                reviewed_base_sha=reviewed_base_sha,
                working_directory=str(provider_cwd),
                failure=f"unable to prepare or run verify_command for verify-only no-op recovery: {exc}",
            )

        if result is not None:
            markdown = _format_review_verify_result(result)
            _capture_review_verify_result(
                context.config,
                context.store,
                noop_improve_task,
                result,
                markdown=markdown,
                project_results=project_results,
                producer="advance_verify_only_noop_recovery",
            )

        live_head_after = context.git.rev_parse_if_exists(task.branch)
        cleanup_failure = _cleanup_verify_only_noop_worktree(
            context=context,
            worktree_path=worktree_path,
            added_worktree=added_worktree,
        )
        added_worktree = False
        worktree_path = None
        if cleanup_failure:
            return _persist_attention(
                "SKIP: verify-only no-op recovery could not clean up its isolated worktree; "
                f"manual attention is required. Cleanup failure: {cleanup_failure}",
                outcome_kind="cleanup_failure",
                verify_status=None if result is None else result.status,
                captured_at=None if result is None else result.captured_at,
            )
        if deferred_attention_message is not None:
            assert deferred_attention_outcome_kind is not None
            return _persist_attention(
                deferred_attention_message,
                outcome_kind=deferred_attention_outcome_kind,
                verify_status=deferred_attention_verify_status,
                captured_at=deferred_attention_captured_at,
            )
        assert result is not None
        if reviewed_head_sha != current_branch_head_sha or live_head_after != current_branch_head_sha:
            return _persist_attention(
                "SKIP: verify-only no-op recovery finished on a stale tip; "
                "the implementation branch moved during verification.",
                outcome_kind="head_drift_after_verify",
                verify_status=result.status,
                captured_at=result.captured_at,
            )
        if result.status != "passed":
            return _persist_attention(
                "SKIP: fresh verify did not clear the verify-only no-op review blocker; "
                "manual attention is required.",
                outcome_kind="verify_not_cleared",
                verify_status=result.status,
                captured_at=result.captured_at,
            )

        try:
            persisted_clearance = _persist_verify_only_noop_clearance(
                context=context,
                task=task,
                review_task=review_task,
                noop_improve_task=noop_improve_task,
                reviewed_head_sha=current_branch_head_sha,
                captured_at=result.captured_at,
            )
        except OffTopicVerifyPersistenceError as exc:
            message = (
                "failed to persist verify-only no-op clearance: "
                f"{exc}"
            )
            _persist_verify_only_noop_attention(
                context=context,
                task=task,
                review_task=review_task,
                noop_improve_task=noop_improve_task,
                reviewed_head_sha=current_branch_head_sha,
                message=(
                    "SKIP: verify-only no-op recovery captured a passing verify result for the current tip, "
                    "but the required structured review_clearance could not be persisted. "
                    "Manual attention is required before merge. "
                    f"Persistence failure: {exc}"
                ),
                outcome_kind="clearance_persistence_failure",
                verify_status=result.status,
                captured_at=result.captured_at,
            )
            return _verify_only_noop_error_result(
                action_type=action_type,
                message=message,
            )
        if persisted_clearance is None:
            _persist_verify_only_noop_attention(
                context=context,
                task=task,
                review_task=review_task,
                noop_improve_task=noop_improve_task,
                reviewed_head_sha=current_branch_head_sha,
                message=(
                    "SKIP: verify-only no-op recovery captured a passing verify result for the current tip, "
                    "but the required structured review_clearance could not be persisted. "
                    "Manual attention is required before merge."
                ),
                outcome_kind="clearance_persistence_missing",
                verify_status=result.status,
                captured_at=result.captured_at,
            )
            return _verify_only_noop_error_result(
                action_type=action_type,
                message="failed to persist verify-only no-op clearance",
            )
        refreshed_task = context.store.get(task.id) or task
        refreshed_task.review_cleared_at = persisted_clearance
        context.store.update(refreshed_task)
        return AdvanceActionExecutionResult(
            action_type=action_type,
            status="success",
            success_message="Fresh verify passed; verify-only no-op review blocker cleared for the current tip.",
            work_done=True,
            handled_task_id=task.id,
            created_task=refreshed_task,
        )
    finally:
        _cleanup_verify_only_noop_worktree(
            context=context,
            worktree_path=worktree_path,
            added_worktree=added_worktree,
        )


def execute_advance_action(
    *,
    task: DbTask,
    action: dict,
    context: AdvanceActionExecutionContext,
) -> AdvanceActionExecutionResult:
    """Execute one worker-style advance action with shared side-effect logic."""
    action_type = str(action.get("type", "skip"))

    if action_type not in _WORKER_ACTIONS and action_type not in _DIRECT_ACTIONS:
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

    if action_type == "release_approved_plan_review":
        review_task = action.get("plan_review_task")
        plan_source_task = action.get("plan_source_task")
        if (
            not isinstance(review_task, DbTask)
            or review_task.id is None
            or not isinstance(plan_source_task, DbTask)
            or plan_source_task.id is None
        ):
            return AdvanceActionExecutionResult(
                action_type=action_type,
                status="skip",
                message="missing held-plan release inputs",
            )
        message = (
            f"Released held plan {plan_source_task.id} after approved plan review {review_task.id}"
        )
        if context.dry_run:
            return AdvanceActionExecutionResult(
                action_type=action_type,
                status="dry_run",
                message=message,
                work_done=True,
                handled_task_id=plan_source_task.id,
            )

        changed = release_held_plan_source(context.store, plan_source_task)
        return AdvanceActionExecutionResult(
            action_type=action_type,
            status="success",
            message=message,
            success_message=message,
            work_done=changed,
            handled_task_id=plan_source_task.id,
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
        if action.get("review_mode") == "resolution":
            create_result = _prepare_resolution_review_action(
                context.store,
                task,
                action,
                trigger_source=context.trigger_source,
            )
        elif action.get("review_mode") == "spec_coherence":
            create_result = _prepare_spec_coherence_review_action(
                context.store,
                task,
                action,
                trigger_source=context.trigger_source,
            )
        else:
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

    if action_type == "create_review_adjudication":
        review_task = action.get("review_task")
        candidate = action.get("review_blocker_adjudication_candidate")
        if (
            not isinstance(review_task, DbTask)
            or review_task.id is None
            or task.id is None
            or candidate is None
        ):
            return AdvanceActionExecutionResult(action_type=action_type, status="skip", message="missing adjudication inputs")

        if context.dry_run:
            return AdvanceActionExecutionResult(
                action_type=action_type,
                status="dry_run",
                message=action.get("description", "Create adjudication"),
                worker_consuming=True,
                work_done=True,
            )

        permit, blocked = _reserve_background_launch(
            action_type=action_type,
            context=context,
            worker_label="review_adjudication",
        )
        if blocked is not None:
            return blocked
        if context.create_review_adjudication_task is None:
            if permit is not None:
                permit.release()
            return AdvanceActionExecutionResult(
                action_type=action_type,
                status="error",
                message="review adjudication creation is unavailable",
            )

        dispute_metadata = (
            dict(candidate.dispute_metadata)
            if hasattr(candidate, "dispute_metadata")
            else build_review_blocker_dispute_metadata(candidate.dispute_artifact)
        )
        adjudication_task = context.create_review_adjudication_task(
            task,
            review_task,
            candidate.finding,
            dispute_metadata,
        )
        prepared_adjudication_task, prepare_error = _prepare_background_start(
            context=context,
            action_type=action_type,
            task=adjudication_task,
            worker_label="review_adjudication",
            rollback_on_failure=True,
            permit=permit,
        )
        if prepared_adjudication_task is None:
            assert prepare_error is not None
            return prepare_error
        assert prepared_adjudication_task.id is not None

        rc = context.spawn_worker(prepared_adjudication_task, "review_adjudication")
        _release_reserved_launch_if_left(prepared_adjudication_task)
        result = _spawn_result(
            action_type=action_type,
            rc=rc,
            handled_task_id=prepared_adjudication_task.id,
            worker_label="review_adjudication",
            created_task=prepared_adjudication_task,
        )
        result.success_message = f"Created adjudication task {prepared_adjudication_task.id}"
        return result

    if action_type == "run_review_adjudication":
        review_adjudication_task = action.get("review_adjudication_task")
        if not isinstance(review_adjudication_task, DbTask) or review_adjudication_task.id is None:
            return AdvanceActionExecutionResult(action_type=action_type, status="skip", message="missing adjudication task")

        if context.dry_run:
            return AdvanceActionExecutionResult(
                action_type=action_type,
                status="dry_run",
                message=action.get("description", "Run adjudication"),
                worker_consuming=True,
                work_done=True,
                handled_task_id=review_adjudication_task.id,
                created_task=review_adjudication_task,
            )

        permit, blocked = _reserve_background_launch(
            action_type=action_type,
            context=context,
            worker_label="review_adjudication",
        )
        if blocked is not None:
            return blocked
        prepared_adjudication_task, prepare_error = _prepare_background_start(
            context=context,
            action_type=action_type,
            task=review_adjudication_task,
            worker_label="review_adjudication",
            rollback_on_failure=False,
            permit=permit,
        )
        if prepared_adjudication_task is None:
            assert prepare_error is not None
            return prepare_error
        assert prepared_adjudication_task.id is not None

        rc = context.spawn_worker(prepared_adjudication_task, "review_adjudication")
        _release_reserved_launch_if_left(prepared_adjudication_task)
        return _spawn_result(
            action_type=action_type,
            rc=rc,
            handled_task_id=prepared_adjudication_task.id,
            worker_label="review_adjudication",
            created_task=prepared_adjudication_task,
        )

    if action_type == "recover_verify_only_noop_review":
        return _execute_recover_verify_only_noop_review(
            task=task,
            action_type=action_type,
            action=action,
            context=context,
        )

    if action_type == "clear_off_topic_verify_blocker":
        review_task = action.get("review_task")
        clearance = action.get("off_topic_verify_clearance_candidate")
        if (
            not isinstance(review_task, DbTask)
            or review_task.id is None
            or task.id is None
            or context.config is None
        ):
            return AdvanceActionExecutionResult(
                action_type=action_type,
                status="skip",
                message="missing off-topic clearance inputs",
            )
        if context.dry_run:
            return AdvanceActionExecutionResult(
                action_type=action_type,
                status="dry_run",
                message=action.get("description", "Clear off-topic verify blocker"),
                work_done=True,
            )
        if clearance is None:
            return AdvanceActionExecutionResult(
                action_type=action_type,
                status="error",
                message="missing off-topic clearance candidate",
            )

        try:
            upsert = create_or_reuse_flaky_investigations(
                context.store,
                config=context.config,
                review_task=review_task,
                impl_task=task,
                evidences=clearance.evidences,
                trigger_source=context.trigger_source,
            )
            context.store.clear_review_state(task.id)
            persisted_task = context.store.get(task.id)
            if persisted_task is None or persisted_task.review_cleared_at is None:
                raise RuntimeError("review clearance was not persisted")
            task.review_cleared_at = persisted_task.review_cleared_at
        except Exception as exc:
            return AdvanceActionExecutionResult(
                action_type=action_type,
                status="error",
                message=f"failed to persist off-topic clearance: {exc}",
            )

        fragments = ["Cleared verify-only review blocker as off-topic"]
        if upsert.created:
            fragments.append(
                "created investigation task(s): "
                + ", ".join(str(t.id) for t in upsert.created if t.id is not None)
            )
        if upsert.reused:
            fragments.append(
                "reused investigation task(s): "
                + ", ".join(str(t.id) for t in upsert.reused if t.id is not None)
            )
        return AdvanceActionExecutionResult(
            action_type=action_type,
            status="success",
            success_message="; ".join(fragments),
            work_done=True,
            handled_task_id=task.id,
            created_investigations=upsert.created,
            reused_investigations=upsert.reused,
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

        permit, blocked = _reserve_background_launch(
            action_type=action_type,
            context=context,
            worker_label="improve",
        )
        if blocked is not None:
            return blocked
        if improve_mode == "resume" and failed_improve is not None:
            assert failed_improve.id is not None
            try:
                improve_task = context.create_resume_task(failed_improve)
            except DuplicateActiveChildError as exc:
                return _skip_duplicate_recovery_creation(
                    action_type=action_type,
                    permit=permit,
                    exc=exc,
                    task=failed_improve,
                )
        elif improve_mode == "retry" and failed_improve is not None:
            assert failed_improve.id is not None
            try:
                if context.create_retry_task is not None:
                    improve_task = context.create_retry_task(failed_improve)
                else:
                    improve_task = _create_retry_task(
                        context.store,
                        failed_improve,
                        trigger_source=context.trigger_source,
                    )
            except DuplicateActiveChildError as exc:
                return _skip_duplicate_recovery_creation(
                    action_type=action_type,
                    permit=permit,
                    exc=exc,
                    task=failed_improve,
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
                try:
                    resume_task = context.create_resume_task(task)
                except DuplicateActiveChildError as exc:
                    return _skip_duplicate_recovery_creation(
                        action_type=action_type,
                        permit=permit,
                        exc=exc,
                        task=task,
                    )
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
            try:
                resume_task = context.create_resume_task(task)
            except DuplicateActiveChildError as exc:
                return _skip_duplicate_recovery_creation(
                    action_type=action_type,
                    permit=permit,
                    exc=exc,
                    task=task,
                )
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
            try:
                retry_task = context.create_retry_task(task)
            except DuplicateActiveChildError as exc:
                return _skip_duplicate_recovery_creation(
                    action_type=action_type,
                    permit=permit,
                    exc=exc,
                    task=task,
                )
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
        rebase_parent_task = task
        rebase_parent_task_id = action.get("rebase_parent_task_id")
        if isinstance(rebase_parent_task_id, str) and rebase_parent_task_id:
            resolved_parent_task = context.store.get(rebase_parent_task_id)
            if resolved_parent_task is None:
                return AdvanceActionExecutionResult(
                    action_type=action_type,
                    status="error",
                    message=f"Cannot rebase: recovery preflight parent task {rebase_parent_task_id} is missing",
                )
            if not resolved_parent_task.branch:
                return AdvanceActionExecutionResult(
                    action_type=action_type,
                    status="error",
                    message=(
                        f"Cannot rebase: recovery preflight parent task {rebase_parent_task_id} has no branch"
                    ),
                )
            rebase_parent_task = resolved_parent_task
        try:
            rebase_task = context.create_rebase_task(rebase_parent_task)
        except DuplicateActiveChildError as exc:
            return _skip_duplicate_rebase_creation(
                action_type=action_type,
                permit=permit,
                exc=exc,
                parent_task_id=rebase_parent_task.id,
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
        if context.use_iterate_for_needs_rebase:
            rc = context.spawn_iterate_worker(
                rebase_parent_task,
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

        rebase_target = reconcile_outcome.rebase_target
        if not rebase_target:
            return AdvanceActionExecutionResult(
                action_type=action_type,
                status="error",
                message="branch reconciliation returned needs_rebase without a rebase_target",
            )
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
        try:
            rebase_task = (
                create_rebase_task(task, rebase_target)
                if create_rebase_task is not None
                else context.create_rebase_task(task)
            )
        except DuplicateActiveChildError as exc:
            return _skip_duplicate_rebase_creation(
                action_type=action_type,
                permit=permit,
                exc=exc,
                parent_task_id=task.id,
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
