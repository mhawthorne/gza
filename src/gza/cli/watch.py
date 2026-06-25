"""Continuous watch loop and queue management commands."""

import argparse
import contextlib
import hashlib
import io
import os
import re
import signal
import sys
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Literal, TypeVar, cast

from rich.text import Text

from .. import colors as _colors, lineage
from ..advance_engine import _resolve_and_persist_post_merge_rebase_state, _resolve_current_merge_source
from ..concurrency import (
    LaunchPermit,
    MaxConcurrentTasksError,
    get_concurrency_snapshot,
    launch_permit,
    release_task_launch_permit,
    reserve_task_launch_permit,
)
from ..config import Config
from ..console import console, prompt_available_width, shorten_prompt
from ..db import (
    MERGE_SOURCE_WATCH,
    SqliteTaskStore,
    Task as DbTask,
    WatchProgressObservation,
    WatchRecoveryBackoff,
    task_id_numeric_key,
)
from ..git import Git, GitError
from ..lifecycle_completion import merge_state_is_terminal_for_lifecycle
from ..lineage_query import (
    LineageOwnerQuery,
    LineageOwnerRow,
    query_lineage_owner_rows_in_read_session,
)
from ..main_integration_verify import (
    MAIN_INTEGRATION_VERIFY_REASON,
    MAIN_INTEGRATION_VERIFY_REMEDIATION_TRIGGER_SOURCE,
    MainIntegrationVerifyCheck,
    MainIntegrationVerifyRemediation,
    check_main_integration_verify,
)
from ..merge_state import effective_no_work_merge_state, resolve_task_merge_state_for_target
from ..operator_state import blocked_dependency_label
from ..pickup import get_runnable_pending_tasks, is_worker_consuming_advance_action
from ..providers.base import wait_for_docker_ready
from ..recovery_engine import (
    FailedRecoveryDecision,
    decide_failed_task_recovery,
    resolve_pending_recovery_execution_mode,
    should_hide_failed_recovery_decision,
)
from ..recovery_read_context import RecoveryReadContext
from ..recovery_transients import (
    classify_transient_recovery_terminal,
    compute_transient_recovery_backoff_seconds,
)
from ..source_followup import collect_non_dropped_implement_source_ids
from ..sync_ops import reconcile_task_branch_merge_truth
from ..task_query import (
    ScopedTagScopeGap,
    TaskQueryPresets,
    TaskQueryService,
    TaskRow,
    collect_scoped_tag_scope_gaps,
    normalize_tag_filters,
    task_matches_tag_filters,
)
from ..watch_progress import (
    WATCH_NO_PROGRESS_BACKSTOP_REASON,
    WatchProgressCandidate,
    build_watch_progress_candidate,
    finalize_background_watch_execution,
    finalize_watch_progress_after_execution,
    get_active_watch_no_progress_attention,
    reconcile_stale_watch_no_progress_parks,
    record_background_watch_execution_start,
)
from ..workers import WorkerRegistry
from ._common import (
    _TASK_ID_RE,
    _create_implementation_task_from_source,
    _create_plan_improve_task,
    _create_plan_review_task,
    _create_rebase_task,
    _create_resume_task,
    _create_retry_task,
    _create_review_adjudication_task,
    _materialize_plan_review_slices,
    _precondition_blocking_dependency_id,
    _prepare_task_for_immediate_execution,
    _spawn_background_resume_worker,
    _spawn_background_worker,
    clear_task_queue_position_scoped,
    format_review_outcome,
    get_store,
    parse_cli_tag_filters,
    resolve_id,
    resolve_improve_action,
    set_task_queue_position_scoped,
    set_task_urgency,
)
from ._lifecycle_actions import (
    LifecycleActionEntry,
    collect_lifecycle_action_entries,
    format_cycle_lifecycle_action_summary,
    lifecycle_action_execution_sort_key,
    plan_lifecycle_execution,
    print_lifecycle_action_entries,
)
from ._queue_render import (
    QueueRenderRow,
    build_queue_summary,
    print_queue_rows,
    queue_render_widths,
)
from ._recovery_lane import RecoveryLaneEntry, collect_recovery_lane_entries
from .advance_engine import (
    NEEDS_ATTENTION_LABEL,
    classify_advance_action,
    determine_next_action,
    failed_recovery_decision_to_action,
    failed_recovery_decision_to_attention_action,
    format_needs_attention_entry_for_display,
    get_action_subject_task_id,
    get_needs_attention_reason,
    resolve_subject_task,
    with_needs_attention,
)
from .advance_executor import (
    ITERATE_ROUTABLE_ACTIONS,
    AdvanceActionExecutionContext,
    AdvanceActionExecutionResult,
    build_improve_needs_attention_result,
    execute_advance_action,
    resolve_execution_needs_attention,
)
from .execution import _spawn_background_iterate
from .git_ops import (
    _collect_advance_completed_tasks as _git_ops_collect_advance_completed_tasks,
    _execute_merge_action,
    _merge_single_task as _git_ops_merge_single_task,
    _prepare_create_review_action,
    _reconcile_diverged_branch_with_origin,
    _require_default_branch,
    _unimplemented_implement_prompt,
    cleanup_failed_merge_checkout,
    ensure_watch_main_checkout,
)
from .query import _resolve_incomplete_owner_task

_WATCH_EVENT_LABEL_WIDTH = len("ATTENTION")
_WATCH_PARKED_LINEAGE_POLICY: Literal["skip"] = "skip"
_WATCH_TRANSIENT_RECOVERY_BACKOFF_DEFER_REASON = "transient-recovery-backoff"
_WATCH_PARKED_NEEDS_ATTENTION_REASONS = frozenset(
    {"retry-limit-reached", "retryable-provider-error", WATCH_NO_PROGRESS_BACKSTOP_REASON}
)
_WATCH_TASK_ID_TOKEN_RE = re.compile(
    rf"(?<![a-z0-9]){_TASK_ID_RE.pattern.removeprefix('^').removesuffix('$')}(?![a-z0-9])"
)
T = TypeVar("T")


def _render_watch_stdout(line: str) -> Text:
    """Return watch stdout content with themed task IDs highlighted."""
    # TODO(gza-4221): if watch stdout gains more themed spans, keep routing
    # them through Rich Text on the shared console so `no_color` stays global.
    rendered = Text(line)
    for match in _WATCH_TASK_ID_TOKEN_RE.finditer(line):
        rendered.stylize(_colors.TASK_COLORS.task_id, match.start(), match.end())
    return rendered


def _resolve_watch_iterate_impl_for_task(store: SqliteTaskStore, task: DbTask) -> DbTask | None:
    """Resolve the implementation iterate target for a same-branch lifecycle member."""
    if task.task_type == "implement":
        return task if task.id is not None else None

    current: DbTask | None = task
    visited: set[str] = set()
    while current is not None and current.id is not None:
        if current.id in visited:
            return None
        visited.add(current.id)
        if current.task_type == "implement":
            return current
        if current.based_on is None:
            return None
        current = store.get(current.based_on)
    return None


def _merge_single_task(
    task_id: str,
    config: Config,
    store: SqliteTaskStore,
    git: Git,
    args: argparse.Namespace,
    current_branch: str,
) -> int:
    """Compatibility shim for tests patching watch-local merge execution."""
    return _git_ops_merge_single_task(task_id, config, store, git, args, current_branch).rc


def _collect_advance_completed_tasks(
    store: SqliteTaskStore,
    *,
    advance_type: str | None = None,
    target_branch: str | None = None,
) -> tuple[list[DbTask], set[str]]:
    """Compatibility shim for tests patching watch-local task collection."""
    return _git_ops_collect_advance_completed_tasks(
        store,
        advance_type=advance_type,
        target_branch=target_branch,
    )


def _watch_skip_message(task: DbTask, action: dict) -> str:
    """Build a stable skip message for non-executed advance actions."""
    action_type = str(action.get("type", "skip"))
    description = str(action.get("description", "")).strip()
    if description.startswith("SKIP: "):
        description = description[len("SKIP: ") :]
    if not description:
        description = action_type.replace("_", " ")
    return f"{task.id}: {description}"


def _main_verify_remediation_tags(tags: tuple[str, ...] | None) -> tuple[str, ...]:
    scope_tags = tuple(tags or ())
    return tuple(dict.fromkeys(("system", *scope_tags)))


def _merge_main_verify_remediation_tags(
    existing_tags: tuple[str, ...] | None,
    scope_tags: tuple[str, ...] | None,
) -> tuple[str, ...]:
    return tuple(dict.fromkeys((*_main_verify_remediation_tags(scope_tags), *(existing_tags or ()))))


def _main_verify_remediation_prompt(
    remediation: MainIntegrationVerifyRemediation,
    *,
    head_sha: str | None,
) -> str:
    phase = remediation.failing_phase or remediation.signature
    short_sha = (head_sha or "unknown")[:12]
    if remediation.kind == "deflake":
        heading = f"De-flake local main integration verify phase `{phase}`"
        outcome = "The verify gate went red once, passed on rerun, and should be stabilized so watch does not keep rediscovering the flake."
    else:
        heading = f"Fix local main integration verify phase `{phase}`"
        outcome = "The verify gate stayed red across bounded reruns and is currently halting merges onto local main."
    body = [
        heading,
        "",
        outcome,
        "",
        f"Remediation kind: {remediation.kind}",
        f"Failure signature: {remediation.signature}",
        f"Tree fingerprint: {remediation.tree_fingerprint or 'unavailable'}",
        f"Observed main HEAD: {short_sha}",
    ]
    if remediation.failure:
        body.append(f"Verify failure: {remediation.failure}")
    body.extend(
        [
            "",
            "Required outcome:",
            "- restore a stable green local main integration verify result for this failure signature",
            "- add targeted regression coverage for the failing phase or flake mode",
            "- rerun the project verify gate after the fix",
        ]
    )
    return "\n".join(body)


def _find_open_main_verify_remediation_task(
    store: SqliteTaskStore,
    *,
    signature: str,
    tree_fingerprint: str | None,
) -> DbTask | None:
    for task in store.get_all():
        if task.task_type != "implement":
            continue
        if task.trigger_source != MAIN_INTEGRATION_VERIFY_REMEDIATION_TRIGGER_SOURCE:
            continue
        if task.status in {"completed", "dropped"}:
            continue
        if _main_verify_remediation_signature_from_prompt(task.prompt) != signature:
            continue
        existing_tree_fingerprint = _main_verify_remediation_tree_fingerprint_from_prompt(task.prompt)
        if tree_fingerprint is None:
            return task
        if existing_tree_fingerprint == tree_fingerprint:
            return task
    return None


def _main_verify_remediation_kind_from_prompt(prompt: str) -> str | None:
    for line in prompt.splitlines():
        if line.startswith("Remediation kind: "):
            kind = line.removeprefix("Remediation kind: ").strip()
            if kind in {"deflake", "fix"}:
                return kind
    if prompt.startswith("De-flake local main integration verify phase `"):
        return "deflake"
    if prompt.startswith("Fix local main integration verify phase `"):
        return "fix"
    return None


def _main_verify_remediation_signature_from_prompt(prompt: str) -> str | None:
    for line in prompt.splitlines():
        if line.startswith("Failure signature: "):
            signature = line.removeprefix("Failure signature: ").strip()
            return signature or None
    return None


def _main_verify_remediation_tree_fingerprint_from_prompt(prompt: str) -> str | None:
    for line in prompt.splitlines():
        if line.startswith("Tree fingerprint: "):
            fingerprint = line.removeprefix("Tree fingerprint: ").strip()
            return None if fingerprint == "unavailable" else fingerprint or None
    return None


def _queue_main_verify_remediation_task(
    *,
    store: SqliteTaskStore,
    task: DbTask,
    tags: tuple[str, ...] | None,
    any_tag: bool,
) -> None:
    if task.id is None:
        return
    if task.status == "failed":
        task.status = "pending"
        task.started_at = None
        task.running_pid = None
        task.completed_at = None
        task.failure_reason = None
        task.completion_reason = None
        task.execution_mode = None
        store.update(task)
    set_task_urgency(store, task.id, urgent=True)
    set_task_queue_position_scoped(
        store,
        task.id,
        position=1,
        tags=tags,
        any_tag=any_tag,
    )


def _ensure_main_verify_remediation_task(
    *,
    store: SqliteTaskStore,
    remediation: MainIntegrationVerifyRemediation,
    state,
    tags: tuple[str, ...] | None,
    any_tag: bool,
) -> tuple[DbTask, bool]:
    existing = _find_open_main_verify_remediation_task(
        store,
        signature=remediation.signature,
        tree_fingerprint=remediation.tree_fingerprint,
    )
    if existing is not None:
        desired_prompt = _main_verify_remediation_prompt(remediation, head_sha=state.head_sha)
        desired_tags = _merge_main_verify_remediation_tags(existing.tags, tags)
        if (
            _main_verify_remediation_kind_from_prompt(existing.prompt) != remediation.kind
            or existing.prompt != desired_prompt
            or tuple(existing.tags or ()) != desired_tags
        ):
            existing.prompt = desired_prompt
            existing.tags = desired_tags
            store.update(existing)
        _queue_main_verify_remediation_task(
            store=store,
            task=existing,
            tags=tags,
            any_tag=any_tag,
        )
        return existing, False

    task = store.add(
        _main_verify_remediation_prompt(remediation, head_sha=state.head_sha),
        task_type="implement",
        tags=_main_verify_remediation_tags(tags),
        trigger_source=MAIN_INTEGRATION_VERIFY_REMEDIATION_TRIGGER_SOURCE,
        urgent=True,
    )
    _queue_main_verify_remediation_task(
        store=store,
        task=task,
        tags=tags,
        any_tag=any_tag,
    )
    return task, True


def _maybe_file_main_verify_remediation(
    *,
    dry_run: bool,
    store: SqliteTaskStore,
    tags: tuple[str, ...] | None,
    any_tag: bool,
    log: "_WatchLog",
    check: MainIntegrationVerifyCheck,
) -> None:
    remediation = getattr(check, "remediation", None)
    if remediation is None or dry_run:
        return
    task, created_now = _ensure_main_verify_remediation_task(
        store=store,
        remediation=remediation,
        state=check.state,
        tags=tags,
        any_tag=any_tag,
    )
    phase = remediation.failing_phase or remediation.signature
    verb = "created" if created_now else "reused"
    log.emit(
        "REMEDY",
        f"{task.id}: {verb} {remediation.kind} remediation for {phase}; moved to queue position 1",
    )


def _maybe_repair_target_already_merged_skip(
    *,
    store: SqliteTaskStore,
    git: Git,
    task: DbTask,
    display_task: DbTask,
    action: Mapping[str, Any],
    target_branch: str,
    dry_run: bool,
    log: "_WatchLog",
) -> bool:
    if dry_run or action.get("advance_reason") != "target-already-merged" or task.id is None:
        return False

    merge_unit = store.resolve_merge_unit_for_task(task.id)
    if merge_unit is None:
        return False

    owner_task = store.resolve_merge_unit_owner_task(merge_unit) or display_task
    owner_effective_state = effective_no_work_merge_state(owner_task, merge_unit.state)
    if merge_state_is_terminal_for_lifecycle(owner_effective_state):
        return False

    repaired = reconcile_task_branch_merge_truth(
        store,
        git,
        str(task.id),
        target_branch=target_branch,
        include_diff_stats=True,
        persist=True,
    )
    if repaired.ok and merge_state_is_terminal_for_lifecycle(repaired.merge_status):
        log.emit(
            "REPAIR",
            f"{display_task.id}: marked {repaired.merge_status} after shared reconciliation against {target_branch}",
        )
        return True
    return False


def _watch_needs_attention_message(task: DbTask, action: dict) -> str:
    return format_needs_attention_entry_for_display(task, action=action)


def _build_guarded_pending_skip_attention(
    pending_task: DbTask,
    *,
    guard_message: str,
) -> dict[str, Any]:
    return with_needs_attention(
        {
            "type": "skip",
            "description": f"SKIP: {guard_message}; will not run automatically",
        },
        reason="guarded-pending-skip",
        subject_task_id=pending_task.id,
    )


def _maybe_emit_recurring_guarded_pending_skip_attention(
    *,
    store: SqliteTaskStore,
    log: "_WatchLog",
    guarded_pending_task_id: str | None,
    guard_message: str,
) -> None:
    if guarded_pending_task_id is None:
        return
    pending_task = store.get(str(guarded_pending_task_id))
    if pending_task is None or pending_task.id is None:
        return
    attention = _build_guarded_pending_skip_attention(
        pending_task,
        guard_message=guard_message,
    )
    attention_key = f"guarded-pending-skip:{pending_task.id}"
    log.emit_attention(
        attention_key=attention_key,
        message=_watch_needs_attention_message(pending_task, attention),
    )


def _watch_parked_lineage_action(row: LineageOwnerRow) -> dict[str, Any] | None:
    """Return the row's already-parked recovery action when watch should not respawn work."""
    action = row.next_action
    if action is None:
        return None
    reason = get_needs_attention_reason(action)
    if reason not in _WATCH_PARKED_NEEDS_ATTENTION_REASONS:
        return None
    if _WATCH_PARKED_LINEAGE_POLICY == "skip":
        return action
    return None


def _watch_parked_iterate_result(
    *,
    store: SqliteTaskStore,
    impl_task: DbTask,
    action: dict[str, object],
    action_type: str,
    max_recovery_attempts: int,
) -> AdvanceActionExecutionResult | None:
    """Preflight iterate-routed improve actions that would only re-park immediately."""
    if _WATCH_PARKED_LINEAGE_POLICY != "skip" or action_type != "improve":
        return None

    review_task: DbTask | None
    review = action.get("review_task")
    review_task = review if isinstance(review, DbTask) else None

    if review_task is None or review_task.id is None or impl_task.id is None:
        return None

    improve_mode, failed_improve, improve_decision = resolve_improve_action(
        store,
        impl_task.id,
        review_task.id,
        max_resume_attempts=max_recovery_attempts,
    )
    result = build_improve_needs_attention_result(
        store=store,
        impl_task=impl_task,
        review_task=review_task,
        improve_mode=improve_mode,
        failed_improve=failed_improve,
        improve_decision=improve_decision,
        max_resume_attempts=max_recovery_attempts,
    )
    if result is None or result.attention_reason not in _WATCH_PARKED_NEEDS_ATTENTION_REASONS:
        return None
    return result


def _resolve_watch_attention_display_task(store: SqliteTaskStore, row: LineageOwnerRow) -> DbTask:
    """Resolve the declared attention subject, falling back to incomplete-owner behavior."""
    action = row.next_action or {}
    return resolve_subject_task(
        store,
        action,
        row,
        fallback_task=_resolve_incomplete_owner_task(store, cast(Any, row)),
    )


def _failed_recovery_attention_action(
    *,
    store: SqliteTaskStore,
    task: DbTask,
    decision: FailedRecoveryDecision,
    max_recovery_attempts: int,
) -> dict[str, object] | None:
    return failed_recovery_decision_to_attention_action(
        store,
        task,
        decision,
        max_recovery_attempts=max_recovery_attempts,
    )


def _reroot_failed_recovery_attention_action(
    *,
    owner_task: DbTask,
    failed_task: DbTask,
    attention_action: Mapping[str, object],
) -> dict[str, object]:
    if owner_task.id is None or failed_task.id is None:
        return dict(attention_action)
    annotated = dict(attention_action)
    annotated["subject_task_id"] = owner_task.id
    if owner_task.id != failed_task.id:
        description = str(annotated.get("description", "")).strip()
        leaf_detail = f"failed leaf {failed_task.id}"
        if leaf_detail not in description:
            annotated["description"] = f"{description}; {leaf_detail}" if description else leaf_detail
    return annotated


def _owner_failed_recovery_attention_action(
    *,
    store: SqliteTaskStore,
    owner_task: DbTask,
    failed_task: DbTask,
    decision: FailedRecoveryDecision,
    max_recovery_attempts: int,
) -> dict[str, object] | None:
    attention_action = _failed_recovery_attention_action(
        store=store,
        task=failed_task,
        decision=decision,
        max_recovery_attempts=max_recovery_attempts,
    )
    if attention_action is None:
        return None
    return _reroot_failed_recovery_attention_action(
        owner_task=owner_task,
        failed_task=failed_task,
        attention_action=attention_action,
    )


def _format_recovery_report_subject(row: LineageOwnerRow, task: DbTask) -> str:
    owner_id = row.owner_task.id or "unknown"
    task_id = task.id or "unknown"
    subject_ids: list[str] = [owner_id]
    if task_id != owner_id:
        subject_ids.append(task_id)
    subject_ids.extend(
        failed_task.id
        for failed_task in row.unresolved_tasks
        if failed_task.id is not None and failed_task.status == "failed" and failed_task.id not in set(subject_ids)
    )
    return " ".join(subject_ids)


def _query_owner_rows_with_context(
    *,
    store: SqliteTaskStore,
    config: Config | None = None,
    git: Git | None = None,
    target_branch: str | None = None,
    tags: tuple[str, ...] | None = None,
    any_tag: bool = False,
    max_recovery_attempts: int,
    include_skipped: bool,
) -> tuple[list[LineageOwnerRow], RecoveryReadContext]:
    rows, read_context = query_lineage_owner_rows_in_read_session(
        store,
        LineageOwnerQuery(
            limit=None,
            tags=tags,
            any_tag=any_tag,
            include_skipped=include_skipped,
            exclude_dropped_from_planning=True,
            max_recovery_attempts=max_recovery_attempts,
        ),
        config=config,
        git=git,
        target_branch=target_branch,
    )
    return list(rows), read_context


def _watch_iterate_result(
    *,
    action_type: str,
    status: Literal["skip", "error"],
    message: str,
    guarded_pending_task_id: str | None = None,
) -> AdvanceActionExecutionResult:
    return AdvanceActionExecutionResult(
        action_type=action_type,
        status=status,
        message=message,
        guarded_pending_task_id=guarded_pending_task_id,
        worker_label="iterate",
    )


def _watch_iterate_impl_target(
    *,
    store: SqliteTaskStore,
    git: Git,
    task: DbTask,
    action: dict[str, object],
    running_task_ids: set[str],
    target_branch: str,
    max_recovery_attempts: int,
) -> DbTask | AdvanceActionExecutionResult | None:
    action_type = str(action.get("type", "skip"))
    if action_type not in ITERATE_ROUTABLE_ACTIONS:
        return None

    guarded_pending_task_id: str | None = None
    impl_task: DbTask | None = None

    if action_type in {"create_review", "improve"}:
        impl_task = _resolve_watch_iterate_impl_for_task(store, task)
        if impl_task is None or impl_task.id is None:
            return None
    elif action_type == "run_review":
        review_task = action.get("review_task")
        if not isinstance(review_task, DbTask) or review_task.id is None:
            return _watch_iterate_result(
                action_type=action_type,
                status="skip",
                message="missing review task",
            )
        guarded_pending_task_id = review_task.id
        if review_task.depends_on is None:
            return None
        impl_task = store.get(review_task.depends_on)
        if impl_task is None or impl_task.id is None:
            return _watch_iterate_result(
                action_type=action_type,
                status="error",
                message=f"review task {review_task.id} points to missing implementation {review_task.depends_on}",
                guarded_pending_task_id=guarded_pending_task_id,
            )
        if impl_task.task_type != "implement":
            return _watch_iterate_result(
                action_type=action_type,
                status="skip",
                message=(
                    f"review task {review_task.id} points to non-implementation task "
                    f"{review_task.depends_on}"
                ),
                guarded_pending_task_id=guarded_pending_task_id,
            )
        anchor_impl = impl_task if task.id == review_task.id else _resolve_watch_iterate_impl_for_task(store, task)
        if anchor_impl is None or anchor_impl.id != impl_task.id:
            return _watch_iterate_result(
                action_type=action_type,
                status="skip",
                message=(
                    f"review task {review_task.id} resolves to {impl_task.id}, "
                    f"not completed task {task.id}"
                ),
                guarded_pending_task_id=guarded_pending_task_id,
            )
    else:
        improve_task = action.get("improve_task")
        if not isinstance(improve_task, DbTask) or improve_task.id is None:
            return _watch_iterate_result(
                action_type=action_type,
                status="skip",
                message="missing improve task",
            )
        guarded_pending_task_id = improve_task.id
        impl_task, resolve_error = lineage.resolve_impl_task(store, improve_task.id)
        if impl_task is None:
            if "has no based_on implementation task" in str(resolve_error):
                return None
            return _watch_iterate_result(
                action_type=action_type,
                status="skip",
                message=resolve_error or f"unable to resolve implementation for {improve_task.id}",
                guarded_pending_task_id=guarded_pending_task_id,
            )
        if task.id is not None and impl_task.id != task.id:
            return _watch_iterate_result(
                action_type=action_type,
                status="skip",
                message=(
                    f"improve task {improve_task.id} resolves to {impl_task.id}, "
                    f"not completed task {task.id}"
                ),
                guarded_pending_task_id=guarded_pending_task_id,
            )

    if impl_task is None or impl_task.id is None:
        return None
    if impl_task.task_type != "implement":
        return None
    if impl_task.status not in {"completed", "pending"}:
        return _watch_iterate_result(
            action_type=action_type,
            status="skip",
            message=(
                f"{impl_task.id}: iterate routing requires implementation status "
                f"completed or pending (found {impl_task.status})"
            ),
            guarded_pending_task_id=guarded_pending_task_id,
        )
    if (
        resolve_task_merge_state_for_target(
            store=store,
            task=impl_task,
            git=git,
            target_branch=target_branch,
        )
        == "merged"
    ):
        return _watch_iterate_result(
            action_type=action_type,
            status="skip",
            message=f"{impl_task.id}: implementation chain already merged; not starting iterate",
            guarded_pending_task_id=guarded_pending_task_id,
        )
    if impl_task.id in running_task_ids:
        return _watch_iterate_result(
            action_type=action_type,
            status="skip",
            message=f"{impl_task.id}: iterate already running for implementation chain",
            guarded_pending_task_id=guarded_pending_task_id,
        )
    parked_result = _watch_parked_iterate_result(
        store=store,
        impl_task=impl_task,
        action=action,
        action_type=action_type,
        max_recovery_attempts=max_recovery_attempts,
    )
    if parked_result is not None:
        parked_result.guarded_pending_task_id = guarded_pending_task_id
        return parked_result
    return impl_task


def _maybe_park_watch_no_progress(
    *,
    config: Config | None = None,
    store: SqliteTaskStore,
    subject_task: DbTask,
    action: dict[str, Any],
    action_task: DbTask | None,
    failed_task: DbTask | None,
    no_progress_cycles: int | None = None,
) -> dict[str, Any] | None:
    """Return an existing parked no-progress attention action for the current evidence."""
    if subject_task.id is None:
        return None
    if no_progress_cycles is not None:
        deferred_attention = _maybe_finalize_deferred_watch_no_progress(
            config=config,
            store=store,
            subject_task=subject_task,
            action=action,
            action_task=action_task,
            failed_task=failed_task,
            no_progress_cycles=no_progress_cycles,
        )
        if deferred_attention is not None:
            return deferred_attention
    candidate = build_watch_progress_candidate(
        store,
        subject_task=subject_task,
        action=action,
        action_task=action_task,
        failed_task=failed_task,
    )
    active_attention = get_active_watch_no_progress_attention(store, candidate=candidate)
    if active_attention is not None:
        return active_attention
    if _get_active_watch_recovery_backoff(store=store, candidate=candidate) is not None:
        return {"defer_launch_reason": _WATCH_TRANSIENT_RECOVERY_BACKOFF_DEFER_REASON}
    if _watch_action_uses_transient_recovery_backoff(
        subject_task=subject_task,
        action=action,
    ) and _maybe_skip_watch_no_progress_for_transient_terminal(
        config=config,
        store=store,
        subject_task=subject_task,
        action=action,
        action_task=action_task,
        failed_task=failed_task,
        candidate=candidate,
    ):
        if _get_active_watch_recovery_backoff(store=store, candidate=candidate) is not None:
            return {"defer_launch_reason": _WATCH_TRANSIENT_RECOVERY_BACKOFF_DEFER_REASON}
    return None


def _watch_no_progress_result_deferred_for_transient_backoff(result: dict[str, Any] | None) -> bool:
    return result is not None and result.get("defer_launch_reason") == _WATCH_TRANSIENT_RECOVERY_BACKOFF_DEFER_REASON


def _maybe_finalize_deferred_watch_no_progress(
    *,
    config: Config | None,
    store: SqliteTaskStore,
    subject_task: DbTask,
    action: dict[str, Any],
    action_task: DbTask | None,
    failed_task: DbTask | None,
    no_progress_cycles: int,
) -> dict[str, Any] | None:
    """Finalize a previously launched detached action once watch can observe its terminal outcome."""
    candidate = build_watch_progress_candidate(
        store,
        subject_task=subject_task,
        action=action,
        action_task=action_task,
        failed_task=failed_task,
    )
    observation = store.get_watch_progress_observation(
        subject_kind=candidate.subject_kind,
        subject_id=candidate.subject_id,
        action_type=candidate.action_type,
        action_reason=candidate.action_reason,
    )
    if observation is None or observation.launch_evidence_fingerprint is None:
        return None
    if observation.action_task_id is None:
        return None
    observed_action_task = store.get(observation.action_task_id)
    if not _watch_background_execution_completed(observed_action_task):
        return None
    terminal_candidate = build_watch_progress_candidate(
        store,
        subject_task=subject_task,
        action=action,
        action_task=observed_action_task,
        failed_task=failed_task,
    )
    if _maybe_skip_watch_no_progress_for_transient_terminal(
        config=config,
        store=store,
        subject_task=subject_task,
        action=action,
        action_task=observed_action_task,
        failed_task=failed_task,
        candidate=terminal_candidate,
    ):
        return None
    return finalize_background_watch_execution(
        store,
        candidate=terminal_candidate,
        no_progress_cycles=no_progress_cycles,
    )


def _finalize_watch_no_progress_after_execution(
    *,
    config: Config | None = None,
    store: SqliteTaskStore,
    subject_task: DbTask,
    action: dict[str, Any],
    action_task_before: DbTask | None,
    action_task_after: DbTask | None,
    failed_task: DbTask | None,
    no_progress_cycles: int,
) -> dict[str, Any] | None:
    """Record one executed watch action after it finishes."""
    if subject_task.id is None:
        return None
    previous_candidate = build_watch_progress_candidate(
        store,
        subject_task=subject_task,
        action=action,
        action_task=action_task_before,
        failed_task=failed_task,
    )
    refreshed_candidate = build_watch_progress_candidate(
        store,
        subject_task=subject_task,
        action=action,
        action_task=action_task_after,
        failed_task=failed_task,
    )
    if _maybe_skip_watch_no_progress_for_transient_terminal(
        config=config,
        store=store,
        subject_task=subject_task,
        action=action,
        action_task=action_task_after,
        failed_task=failed_task,
        candidate=refreshed_candidate,
    ):
        return None
    return finalize_watch_progress_after_execution(
        store,
        before=previous_candidate,
        after=refreshed_candidate,
        no_progress_cycles=no_progress_cycles,
    )


def _watch_background_execution_completed(action_task: DbTask | None) -> bool:
    """Return whether watch can already observe a detached action has finished."""
    return action_task is not None and action_task.status not in {"pending", "in_progress"}


def _maybe_finalize_watch_no_progress_for_background_action(
    *,
    config: Config | None = None,
    store: SqliteTaskStore,
    subject_task: DbTask,
    action: dict[str, Any],
    action_task_before: DbTask | None,
    action_task_after: DbTask | None,
    failed_task: DbTask | None,
    no_progress_cycles: int,
) -> dict[str, Any] | None:
    """Finalize no-progress only when the detached action already reached an observed outcome."""
    if action_task_after is not None and not _watch_background_execution_completed(action_task_after):
        before_candidate = build_watch_progress_candidate(
            store,
            subject_task=subject_task,
            action=action,
            action_task=action_task_before,
            failed_task=failed_task,
        )
        after_candidate = build_watch_progress_candidate(
            store,
            subject_task=subject_task,
            action=action,
            action_task=action_task_after,
            failed_task=failed_task,
        )
        record_background_watch_execution_start(
            store,
            before=before_candidate,
            after=after_candidate,
        )
        return None
    if not _watch_background_execution_completed(action_task_after):
        return None
    return _finalize_watch_no_progress_after_execution(
        config=config,
        store=store,
        subject_task=subject_task,
        action=action,
        action_task_before=action_task_before,
        action_task_after=action_task_after,
        failed_task=failed_task,
        no_progress_cycles=no_progress_cycles,
    )


def _watch_task_sort_key(task: DbTask) -> tuple[datetime, int]:
    when = task.completed_at or task.created_at or datetime.min
    if when.tzinfo is not None:
        when = when.astimezone(UTC).replace(tzinfo=None)
    return (when, task_id_numeric_key(task.id))


def _collect_recovery_descendants(store: SqliteTaskStore, root_task_id: str) -> list[DbTask]:
    descendants: list[DbTask] = []
    stack = list(store.get_based_on_children(root_task_id))
    while stack:
        current = stack.pop()
        descendants.append(current)
        if current.id is not None:
            stack.extend(store.get_based_on_children(current.id))
    return descendants


def _resolve_latest_failed_recovery_attempt(store: SqliteTaskStore, failed_task: DbTask) -> DbTask | None:
    if failed_task.id is None:
        return None
    candidates = [
        descendant
        for descendant in _collect_recovery_descendants(store, failed_task.id)
        if descendant.status == "failed"
        and descendant.task_type == failed_task.task_type
        and descendant.recovery_origin in {"resume", "retry"}
        and descendant.id is not None
    ]
    if not candidates:
        return None
    return max(candidates, key=_watch_task_sort_key)


def _resolve_improve_subject_and_review(
    *,
    store: SqliteTaskStore,
    subject_task: DbTask,
    action: Mapping[str, Any],
) -> tuple[DbTask | None, DbTask | None]:
    review = action.get("review_task")
    review_task = review if isinstance(review, DbTask) else None
    impl_task: DbTask | None = None
    if subject_task.task_type == "implement":
        impl_task = subject_task
    elif subject_task.task_type == "review" and subject_task.depends_on:
        candidate = store.get(subject_task.depends_on)
        if candidate is not None and candidate.task_type == "implement":
            impl_task = candidate
    if review_task is None and subject_task.task_type == "review":
        review_task = subject_task
    return impl_task, review_task


def _resolve_latest_failed_watch_attempt(
    *,
    store: SqliteTaskStore,
    subject_task: DbTask,
    action: Mapping[str, Any],
    action_task: DbTask | None,
    failed_task: DbTask | None,
    max_resume_attempts: int | None = None,
) -> DbTask | None:
    action_type = str(action.get("type", "")).strip()
    if action_type in {"resume", "retry"} and failed_task is not None:
        return _resolve_latest_failed_recovery_attempt(store, failed_task)
    if action_task is not None and action_task.status == "failed":
        return action_task
    if action_type in {"improve", "run_improve"}:
        impl_task, review_task = _resolve_improve_subject_and_review(
            store=store,
            subject_task=subject_task,
            action=action,
        )
        if impl_task is None or impl_task.id is None or review_task is None or review_task.id is None:
            return None
        improve_mode, failed_improve, _improve_decision = resolve_improve_action(
            store,
            impl_task.id,
            review_task.id,
            max_resume_attempts=max_resume_attempts,
        )
        if improve_mode in {"resume", "retry"} and failed_improve is not None:
            return failed_improve
    return None


def _persist_transient_watch_recovery_backoff(
    *,
    config: Config | None,
    store: SqliteTaskStore,
    candidate,
    failed_attempt: DbTask,
    transient_code: str,
    transient_fingerprint: str,
) -> None:
    now = datetime.now(UTC)
    existing = store.get_watch_recovery_backoff(
        subject_kind=candidate.subject_kind,
        subject_id=candidate.subject_id,
        action_type=candidate.action_type,
        action_reason=candidate.action_reason,
    )
    streak = 1
    next_retry_at = None
    if existing is not None:
        if existing.last_failure_task_id == failed_attempt.id:
            streak = existing.streak
            next_retry_at = existing.next_retry_at
        else:
            streak = existing.streak + 1
    delay_seconds = compute_transient_recovery_backoff_seconds(config, streak) if config is not None else 0
    if next_retry_at is None:
        next_retry_at = now + timedelta(seconds=delay_seconds)
    store.upsert_watch_recovery_backoff(
        WatchRecoveryBackoff(
            subject_kind=candidate.subject_kind,
            subject_id=candidate.subject_id,
            action_type=candidate.action_type,
            action_reason=candidate.action_reason,
            subject_task_id=candidate.subject_task_id,
            last_failure_task_id=failed_attempt.id,
            last_failure_reason=failed_attempt.failure_reason or transient_code,
            last_failure_fingerprint=transient_fingerprint,
            streak=streak,
            next_retry_at=next_retry_at,
            updated_at=now,
        )
    )


def _preserve_watch_no_progress_observation_after_transient_terminal(
    *,
    store: SqliteTaskStore,
    candidate,
    failed_attempt: DbTask,
) -> None:
    observation = store.get_watch_progress_observation(
        subject_kind=candidate.subject_kind,
        subject_id=candidate.subject_id,
        action_type=candidate.action_type,
        action_reason=candidate.action_reason,
    )
    if observation is None:
        return
    store.upsert_watch_progress_observation(
        WatchProgressObservation(
            subject_kind=candidate.subject_kind,
            subject_id=candidate.subject_id,
            action_type=candidate.action_type,
            action_reason=candidate.action_reason,
            subject_task_id=candidate.subject_task_id,
            action_task_id=candidate.action_task_id,
            action_task_status=candidate.action_task_status,
            action_task_started_at=candidate.action_task_started_at,
            action_task_running_pid=candidate.action_task_running_pid,
            failed_task_id=failed_attempt.id or candidate.failed_task_id,
            recovery_task_id=candidate.recovery_task_id,
            merge_unit_id=candidate.merge_unit_id,
            merge_unit_state=candidate.merge_unit_state,
            merge_unit_head_sha=candidate.merge_unit_head_sha,
            evidence_fingerprint=observation.evidence_fingerprint,
            launch_evidence_fingerprint=None,
            streak=observation.streak,
            parked_reason=observation.parked_reason,
            observed_at=datetime.now(UTC),
        )
    )


def _maybe_skip_watch_no_progress_for_transient_terminal(
    *,
    config: Config | None,
    store: SqliteTaskStore,
    subject_task: DbTask,
    action: Mapping[str, Any],
    action_task: DbTask | None,
    failed_task: DbTask | None,
    candidate,
) -> bool:
    failed_attempt = _resolve_latest_failed_watch_attempt(
        store=store,
        subject_task=subject_task,
        action=action,
        action_task=action_task,
        failed_task=failed_task,
        max_resume_attempts=config.max_resume_attempts if config is not None else None,
    )
    transient = classify_transient_recovery_terminal(
        failed_attempt,
        project_dir=config.project_dir if config is not None else None,
    )
    if transient is None or failed_attempt is None:
        return False
    _persist_transient_watch_recovery_backoff(
        config=config,
        store=store,
        candidate=candidate,
        failed_attempt=failed_attempt,
        transient_code=transient.code,
        transient_fingerprint=transient.fingerprint,
    )
    _preserve_watch_no_progress_observation_after_transient_terminal(
        store=store,
        candidate=candidate,
        failed_attempt=failed_attempt,
    )
    return True


def _watch_transient_backoff_remaining_seconds(backoff: WatchRecoveryBackoff, *, now: datetime) -> int:
    if backoff.next_retry_at is None:
        return 0
    return max(0, int((backoff.next_retry_at - now).total_seconds()))


def _get_active_watch_recovery_backoff(
    *,
    store: SqliteTaskStore,
    candidate: WatchProgressCandidate,
    now: datetime | None = None,
) -> WatchRecoveryBackoff | None:
    now = now or datetime.now(UTC)
    backoff = store.get_watch_recovery_backoff(
        subject_kind=candidate.subject_kind,
        subject_id=candidate.subject_id,
        action_type=candidate.action_type,
        action_reason=candidate.action_reason,
    )
    if backoff is None:
        return None
    if _watch_transient_backoff_remaining_seconds(backoff, now=now) <= 0:
        return None
    return backoff


def _watch_action_uses_transient_recovery_backoff(
    *,
    subject_task: DbTask,
    action: Mapping[str, Any],
) -> bool:
    action_type = str(action.get("type", "")).strip()
    if action_type in {"improve", "run_improve", "resume", "retry"}:
        return True
    if action_type != "iterate":
        return False
    return resolve_pending_recovery_execution_mode(subject_task) in {"resume", "retry"}


def _resolve_pending_improve_root_task(store: SqliteTaskStore, task: DbTask) -> DbTask | None:
    current = task
    visited: set[str] = set()
    while True:
        if current.task_type == "implement":
            return current
        current_id = current.id
        if current_id is not None:
            if current_id in visited:
                return None
            visited.add(current_id)
        if current.depends_on:
            dependency = store.get(current.depends_on)
            if dependency is not None and dependency.task_type == "review" and dependency.depends_on:
                impl_task = store.get(dependency.depends_on)
                if impl_task is not None and impl_task.task_type == "implement":
                    return impl_task
        if not current.based_on:
            return None
        parent = store.get(current.based_on)
        if parent is None:
            return None
        current = parent


def _resolve_pending_improve_backoff_candidate_from_rows(
    *,
    store: SqliteTaskStore,
    impl_task: DbTask,
    pending_improve: DbTask,
    failed_improve: DbTask | None,
) -> WatchProgressCandidate | None:
    if impl_task.id is None:
        return None
    subject_candidate = build_watch_progress_candidate(
        store,
        subject_task=impl_task,
        action={"type": "", "description": ""},
        action_task=pending_improve,
        failed_task=failed_improve if failed_improve is not None and failed_improve.status == "failed" else None,
    )
    matching_backoffs = [
        backoff
        for backoff in store.list_watch_recovery_backoffs(
            subject_kind=subject_candidate.subject_kind,
            subject_id=subject_candidate.subject_id,
        )
        if backoff.action_type in {"improve", "run_improve"}
        and (
            failed_improve is None
            or failed_improve.id is None
            or backoff.last_failure_task_id == failed_improve.id
        )
    ]
    if not matching_backoffs:
        return None
    backoff = matching_backoffs[0]
    return build_watch_progress_candidate(
        store,
        subject_task=impl_task,
        action={
            "type": backoff.action_type,
            "description": backoff.action_reason,
        },
        action_task=pending_improve,
        failed_task=failed_improve if failed_improve is not None and failed_improve.status == "failed" else None,
    )


def _resolve_pending_recovery_backoff_candidate(
    *,
    store: SqliteTaskStore,
    subject_task: DbTask,
    action: Mapping[str, Any],
) -> tuple[DbTask, WatchProgressCandidate] | None:
    pending_recovery_mode = resolve_pending_recovery_execution_mode(subject_task)
    if subject_task.status == "pending" and pending_recovery_mode in {"resume", "retry"}:
        parent = store.get(subject_task.based_on) if subject_task.based_on else None
        if subject_task.task_type == "improve":
            impl_task = _resolve_pending_improve_root_task(store, subject_task)
            if impl_task is not None and impl_task.id is not None:
                observations = store.list_watch_progress_observations(subject_kind="lineage", subject_id=impl_task.id)
                parent_id = parent.id if parent is not None else None
                improve_observation = next(
                    (
                        observation
                        for observation in observations
                        if observation.action_type in {"improve", "run_improve"}
                        and parent_id is not None
                        and (observation.action_task_id == parent_id or observation.failed_task_id == parent_id)
                    ),
                    None,
                )
                if improve_observation is None:
                    improve_observation = next(
                        (
                            observation
                            for observation in observations
                            if observation.action_type in {"improve", "run_improve"}
                        ),
                        None,
                    )
                if improve_observation is not None:
                    candidate = build_watch_progress_candidate(
                        store,
                        subject_task=impl_task,
                        action={
                            "type": improve_observation.action_type,
                            "description": improve_observation.action_reason,
                        },
                        action_task=subject_task,
                        failed_task=parent if parent is not None and parent.status == "failed" else None,
                    )
                    return impl_task, candidate
                row_candidate = _resolve_pending_improve_backoff_candidate_from_rows(
                    store=store,
                    impl_task=impl_task,
                    pending_improve=subject_task,
                    failed_improve=parent if parent is not None and parent.status == "failed" else None,
                )
                if row_candidate is not None:
                    return impl_task, row_candidate

        if parent is None or parent.status != "failed":
            return None
        recovery_description = (
            f"Resume failed task ({parent.failure_reason or 'UNKNOWN'})"
            if pending_recovery_mode == "resume"
            else f"Retry failed task ({parent.failure_reason or 'UNKNOWN'})"
        )
        candidate = build_watch_progress_candidate(
            store,
            subject_task=parent,
            action={
                "type": pending_recovery_mode,
                "description": recovery_description,
            },
            action_task=subject_task,
            failed_task=parent,
        )
        return parent, candidate

    if _watch_action_uses_transient_recovery_backoff(subject_task=subject_task, action=action):
        candidate = build_watch_progress_candidate(
            store,
            subject_task=subject_task,
            action=dict(action),
            action_task=subject_task,
            failed_task=subject_task if subject_task.status == "failed" else None,
        )
        return subject_task, candidate
    return None


def _maybe_emit_active_watch_recovery_backoff(
    *,
    store: SqliteTaskStore,
    log: "_WatchLog",
    subject_task: DbTask,
    action: Mapping[str, Any],
) -> bool:
    resolved = _resolve_active_watch_recovery_backoff(
        store=store,
        subject_task=subject_task,
        action=action,
    )
    if resolved is None:
        return False
    log_subject_task, candidate, backoff = resolved
    now = datetime.now(UTC)
    remaining_seconds = _watch_transient_backoff_remaining_seconds(backoff, now=now)
    if remaining_seconds <= 0:
        return False
    action_label = candidate.action_type or str(action.get("type", "action")).strip() or "action"
    last_failure_task_id = backoff.last_failure_task_id or "unknown"
    last_failure_reason = backoff.last_failure_reason or "UNKNOWN"
    log.emit(
        "BACKOFF",
        (
            f"{log_subject_task.id} {action_label} delayed {remaining_seconds}s after transient failure "
            f"(last={last_failure_task_id} {last_failure_reason})"
        ),
        dedupe_key=(
            "transient-recovery-backoff:"
            f"{candidate.subject_kind}:{candidate.subject_id}:{candidate.action_type}:{candidate.action_reason}:"
            f"{last_failure_task_id}:{remaining_seconds}"
        ),
    )
    return True


def _resolve_active_watch_recovery_backoff(
    *,
    store: SqliteTaskStore,
    subject_task: DbTask,
    action: Mapping[str, Any],
) -> tuple[DbTask, WatchProgressCandidate, WatchRecoveryBackoff] | None:
    resolved = _resolve_pending_recovery_backoff_candidate(
        store=store,
        subject_task=subject_task,
        action=action,
    )
    if resolved is None:
        return None
    log_subject_task, candidate = resolved
    now = datetime.now(UTC)
    backoff = _get_active_watch_recovery_backoff(store=store, candidate=candidate, now=now)
    if backoff is None:
        return None
    return log_subject_task, candidate, backoff


def _resolve_recovery_action_task(
    store: SqliteTaskStore,
    *,
    failed_task: DbTask,
    recovery_task_id: str | None,
) -> DbTask:
    if recovery_task_id is None:
        return failed_task
    action_task = store.get(recovery_task_id)
    return action_task or failed_task


def _is_watch_observable_recovery_skip(decision: FailedRecoveryDecision) -> bool:
    return decision.action == "skip" and decision.reason_code in {
        "recovery_already_pending",
        "recovery_already_running",
    }


def _pending_queue_dispatch_action(task: DbTask) -> dict[str, Any]:
    """Build the stable watch action used for pending-queue no-progress tracking."""
    if task.task_type == "implement":
        pending_recovery_mode = resolve_pending_recovery_execution_mode(task)
        if pending_recovery_mode == "resume":
            return {"type": "iterate", "description": "pending queue iterate resume"}
        if pending_recovery_mode == "retry":
            return {"type": "iterate", "description": "pending queue iterate retry"}
        return {"type": "iterate", "description": "pending queue iterate"}
    task_kind = task.task_type or "task"
    return {"type": "worker", "description": f"pending queue {task_kind} worker"}


@dataclass(frozen=True)
class _IsolatedMergeFailureAssessment:
    is_conflict: bool
    reason: str | None = None


@dataclass
class _InstalledPackageDriftState:
    startup_fingerprint: str
    warned_fingerprint: str | None = None
    pending_restart_fingerprint: str | None = None


def _assess_isolated_merge_failure(
    merge_git: Git,
    branch: str,
    target_branch: str,
) -> _IsolatedMergeFailureAssessment:
    """Classify whether an isolated merge failure is a real merge conflict."""
    if not merge_git.branch_exists(branch):
        return _IsolatedMergeFailureAssessment(False, "branch missing")
    if merge_git.is_merged(branch, target_branch):
        return _IsolatedMergeFailureAssessment(False, "branch already merged")
    if merge_git.can_merge(branch, target_branch):
        return _IsolatedMergeFailureAssessment(False, "no merge conflict detected")
    return _IsolatedMergeFailureAssessment(True)


def _format_prompt_for_width(prompt: str, *, prefix: int = 0, suffix: int = 0) -> str:
    available = prompt_available_width(prefix=prefix, suffix=suffix)
    return shorten_prompt(prompt, available)


def _format_hms() -> str:
    return datetime.now(UTC).strftime("%H:%M:%S")


def _installed_gza_package_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _installed_gza_package_fingerprint(package_root: Path | None = None) -> str:
    root = package_root or _installed_gza_package_root()
    hasher = hashlib.sha256()
    for path in sorted(root.rglob("*.py")):
        if not path.is_file():
            continue
        relative_path = path.relative_to(root).as_posix()
        hasher.update(relative_path.encode("utf-8"))
        hasher.update(b"\0")
        hasher.update(path.read_bytes())
        hasher.update(b"\0")
    return hasher.hexdigest()


def _warn_if_installed_gza_changed(
    log: "_WatchLog",
    drift_state: _InstalledPackageDriftState | None,
    *,
    auto_restart_on_drift: bool,
) -> None:
    if drift_state is None:
        return
    current_fingerprint = _installed_gza_package_fingerprint()
    if current_fingerprint == drift_state.startup_fingerprint:
        drift_state.pending_restart_fingerprint = None
        return
    drift_state.pending_restart_fingerprint = current_fingerprint
    if current_fingerprint == drift_state.warned_fingerprint:
        return
    drift_state.warned_fingerprint = current_fingerprint
    if auto_restart_on_drift:
        message = (
            "installed gza changed since watch started -- watch will re-exec "
            "on the next watch pass to load new code"
        )
    else:
        message = "installed gza changed since watch started -- restart watch to pick up new code"
    log.emit(
        "WARNING",
        message,
    )


def _should_reexec_watch(
    *,
    auto_restart_on_drift: bool,
    dry_run: bool,
    stop_requested: bool,
    cycle_result: "_CycleResult",
    drift_state: _InstalledPackageDriftState | None,
) -> bool:
    if not auto_restart_on_drift or dry_run or stop_requested or drift_state is None:
        return False
    if drift_state.pending_restart_fingerprint is None:
        return False
    return True


def _watch_reexec_argv(args: argparse.Namespace) -> list[str]:
    argv = [sys.executable, "-m", "gza", "watch", "--project", str(args.project_dir)]
    if getattr(args, "batch", None) is not None:
        argv.extend(["--batch", str(args.batch)])
    if getattr(args, "poll", None) is not None:
        argv.extend(["--poll", str(args.poll)])
    if getattr(args, "max_idle", None) is not None:
        argv.extend(["--max-idle", str(args.max_idle)])
    if getattr(args, "max_iterations", None) is not None:
        argv.extend(["--max-iterations", str(args.max_iterations)])
    recovery_mode = getattr(args, "recovery_mode", None)
    if recovery_mode is None and getattr(args, "restart_failed", False):
        recovery_mode = "recovery-only"
    if recovery_mode == "recovery-only":
        argv.append("--recovery-only")
    elif recovery_mode == "pending-only":
        argv.append("--pending-only")
    if getattr(args, "recovery_slots", None) is not None:
        argv.extend(["--recovery-slots", str(args.recovery_slots)])
    if getattr(args, "max_resume_attempts", None) is not None:
        argv.extend(["--max-resume-attempts", str(args.max_resume_attempts)])
    if getattr(args, "dry_run", False):
        argv.append("--dry-run")
    if getattr(args, "show_skipped", False):
        argv.append("--show-skipped")
    if getattr(args, "quiet", False):
        argv.append("--quiet")
    if getattr(args, "yes", False):
        argv.append("--yes")
    argv.append("--resumed-reexec")
    for tag in getattr(args, "tags", None) or ():
        argv.extend(["--tag", tag])
    if getattr(args, "all_tags", False):
        argv.append("--all-tags")
    if not getattr(args, "auto_restart_on_drift", True):
        argv.append("--no-auto-restart-on-drift")
    return argv


def _format_scope_message(tags: tuple[str, ...] | None, *, any_tag: bool) -> str | None:
    """Return a stable watch-scope message when tag filtering is active."""
    if not tags:
        return None
    mode = "any" if any_tag else "all"
    return f"scope: tags={','.join(tags)} mode={mode}"


def _format_queue_scope_error(task_id: str, tags: tuple[str, ...], *, any_tag: bool) -> str:
    """Return consistent fail-closed messaging for queue ordering scope mismatch."""
    mode = "any" if any_tag else "all"
    return (
        f"Error: Task {task_id} does not match tag scope ({mode}: {', '.join(tags)}); "
        "queue ordering was not changed"
    )


def _parse_dt(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _format_elapsed(started_at: str | None, completed_at: str | None) -> str | None:
    start_dt = _parse_dt(started_at)
    end_dt = _parse_dt(completed_at)
    if start_dt is None or end_dt is None:
        return None
    elapsed = max(0.0, (end_dt - start_dt).total_seconds())
    mins = int(elapsed // 60)
    secs = int(elapsed % 60)
    if mins > 0:
        return f"{mins}m{secs:02d}s"
    return f"{secs}s"


def _sleep_interruptibly(seconds: int, stop_requested: Callable[[], bool], *, quantum: float = 1.0) -> None:
    """Sleep for up to `seconds`, exiting early if stop was requested."""
    remaining = float(seconds)
    while remaining > 0:
        if stop_requested():
            return
        step = min(quantum, remaining)
        time.sleep(step)
        remaining -= step


def _pid_alive(pid: int | None) -> bool:
    if pid is None or pid <= 0:
        return False
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False


def _task_snapshot(store: SqliteTaskStore) -> dict[str, dict[str, str | None]]:
    snap: dict[str, dict[str, str | None]] = {}
    with store._connect() as conn:  # noqa: SLF001 - CLI internal polling helper
        cur = conn.execute(
            """
            SELECT
                id,
                status,
                task_type,
                started_at,
                completed_at,
                failure_reason,
                completion_reason,
                depends_on,
                COALESCE(
                    (
                        SELECT mu.state
                        FROM merge_unit_tasks mut
                        JOIN merge_units mu
                          ON mu.project_id = mut.project_id
                         AND mu.id = mut.merge_unit_id
                        WHERE mut.project_id = tasks.project_id
                          AND mut.task_id = tasks.id
                          AND mu.superseded_by_unit_id IS NULL
                        ORDER BY mu.updated_at DESC, mu.id DESC
                        LIMIT 1
                    ),
                    merge_status
                ) AS merge_status,
                (
                    SELECT mu.target_branch
                    FROM merge_unit_tasks mut
                    JOIN merge_units mu
                      ON mu.project_id = mut.project_id
                     AND mu.id = mut.merge_unit_id
                    WHERE mut.project_id = tasks.project_id
                      AND mut.task_id = tasks.id
                      AND mu.superseded_by_unit_id IS NULL
                    ORDER BY mu.updated_at DESC, mu.id DESC
                    LIMIT 1
                ) AS merge_target_branch
            FROM tasks
            """
        )
        for row in cur.fetchall():
            task_id = str(row["id"])
            snap[task_id] = {
                "status": row["status"],
                "task_type": row["task_type"],
                "started_at": row["started_at"],
                "completed_at": row["completed_at"],
                "failure_reason": row["failure_reason"],
                "completion_reason": row["completion_reason"],
                "depends_on": row["depends_on"],
                "merge_status": row["merge_status"],
                "merge_target_branch": row["merge_target_branch"],
            }
    return snap


class _WatchLog:
    def __init__(self, path: Path, *, quiet: bool = False) -> None:
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.quiet = quiet
        self._has_emitted_cycle = False
        self._skip_keys_prev_cycle: set[str] = set()
        self._skip_keys_this_cycle: set[str] = set()
        self._merge_logged_this_cycle: set[str] = set()
        self._sticky_attention_prev_cycle: dict[str, str] = {}
        self._sticky_attention_this_cycle: dict[str, str] = {}
        self._visible_attention_this_cycle: dict[str, str] = {}

    def begin_cycle(self) -> None:
        if self._has_emitted_cycle:
            with open(self.path, "a") as f:
                f.write("\n")
            if not self.quiet:
                console.print()
        self._skip_keys_this_cycle.clear()
        self._merge_logged_this_cycle.clear()
        self._sticky_attention_this_cycle.clear()
        self._visible_attention_this_cycle.clear()
        self._has_emitted_cycle = True

    def end_cycle(self) -> None:
        self._skip_keys_prev_cycle = set(self._skip_keys_this_cycle)
        self._sticky_attention_prev_cycle = dict(self._sticky_attention_this_cycle)

    def emit_attention(self, *, attention_key: str, message: str) -> None:
        self._visible_attention_this_cycle[attention_key] = message
        previous_message = self._sticky_attention_this_cycle.get(attention_key)
        if previous_message == message:
            return
        self._sticky_attention_this_cycle[attention_key] = message
        if self._sticky_attention_prev_cycle.get(attention_key) == message:
            return
        self.emit("ATTENTION", message)

    def visible_attention_messages(self) -> tuple[str, ...]:
        return tuple(self._visible_attention_this_cycle.values())

    def note_merge_logged(self, merge_key: str) -> None:
        self._merge_logged_this_cycle.add(merge_key)

    def was_merge_logged(self, merge_key: str) -> bool:
        return merge_key in self._merge_logged_this_cycle

    def emit(self, event: str, message: str, *, dedupe_key: str | None = None) -> None:
        if event == "SKIP" and dedupe_key is not None:
            self._skip_keys_this_cycle.add(dedupe_key)
            if dedupe_key in self._skip_keys_prev_cycle:
                return
        prefix = f"{_format_hms()} {event:<{_WATCH_EVENT_LABEL_WIDTH}} "
        continuation_prefix = " " * len(prefix)
        parts = message.splitlines() or [""]
        line = "\n".join(
            (prefix if idx == 0 else continuation_prefix) + part.rstrip()
            for idx, part in enumerate(parts)
        ).rstrip()
        with open(self.path, "a") as f:
            f.write(line + "\n")
        if not self.quiet:
            console.print(_render_watch_stdout(line), soft_wrap=True, highlight=False)


def _emit_transition_events(
    old: dict[str, dict[str, str | None]],
    new: dict[str, dict[str, str | None]],
    *,
    store: SqliteTaskStore,
    config: Config,
    log: _WatchLog,
    restart_failed_mode: bool = False,
    max_recovery_attempts: int = 1,
) -> None:
    # Detector-owned event tags come from snapshot diffs, regardless of which
    # process caused the state change: REVIEW, DONE, FAIL, and MERGE. Inline
    # emits remain watch's own action/decision events such as START, SKIP,
    # ATTENTION, INFO, PHASE, WAKE, and IDLE.
    for task_id in sorted(new.keys()):
        old_row = old.get(task_id) or {}
        old_status = old_row.get("status")
        new_row = new[task_id]
        new_status = new_row.get("status")

        task_type = new_row.get("task_type") or "implement"
        elapsed = _format_elapsed(new_row.get("started_at"), new_row.get("completed_at"))
        elapsed_suffix = f" ({elapsed})" if elapsed else ""
        if old_status != new_status:
            if new_status == "completed":
                completion_reason = new_row.get("completion_reason")
                reason_suffix = f": {completion_reason}" if completion_reason else ""
                if task_type == "review":
                    task = store.get(task_id)
                    impl_id = new_row.get("depends_on") or "unknown"
                    verdict = (
                        format_review_outcome(config, task)
                        if task is not None
                        else "UNKNOWN"
                    )
                    log.emit("REVIEW", f"{task_id} for {impl_id}: {verdict}{reason_suffix}")
                else:
                    log.emit("DONE", f"{task_id} {task_type}{reason_suffix}{elapsed_suffix}")
            elif new_status == "failed":
                reason = new_row.get("failure_reason") or "UNKNOWN"
                task = store.get(task_id)
                if restart_failed_mode and task is not None:
                    decision = decide_failed_task_recovery(
                        store,
                        task,
                        max_recovery_attempts=max_recovery_attempts,
                    )
                    if should_hide_failed_recovery_decision(decision):
                        continue
                log.emit("FAIL", f"{task_id} {task_type}: {reason}{elapsed_suffix}")

        if task_id in old and old_row.get("merge_status") != "merged" and new_row.get("merge_status") == "merged":
            merge_event = _resolve_watch_merge_log_event(
                store,
                task_id=task_id,
                target_branch=new_row.get("merge_target_branch"),
            )
            if merge_event is not None and not log.was_merge_logged(merge_event.merge_key):
                log.emit("MERGE", f"{merge_event.display_task_id} -> {merge_event.target_branch}")
                log.note_merge_logged(merge_event.merge_key)


@dataclass(frozen=True)
class _WatchMergeLogEvent:
    merge_key: str
    display_task_id: str
    target_branch: str


def _resolve_watch_merge_log_event(
    store: SqliteTaskStore,
    *,
    task_id: str,
    target_branch: str | None = None,
) -> _WatchMergeLogEvent | None:
    task = store.get(task_id)
    if task is None or task.id is None:
        return None

    merge_unit = store.resolve_merge_unit_for_task(task.id)
    if merge_unit is not None:
        owner = store.resolve_merge_unit_owner_task(merge_unit)
        owner_task_id = owner.id if owner is not None and owner.id is not None else merge_unit.owner_task_id
        if owner_task_id is not None:
            return _WatchMergeLogEvent(
                merge_key=merge_unit.id,
                display_task_id=owner_task_id,
                target_branch=target_branch or merge_unit.target_branch or store.default_merge_target(),
            )

    return _WatchMergeLogEvent(
        merge_key=task.id,
        display_task_id=task.id,
        target_branch=target_branch or store.default_merge_target(),
    )


def _count_live_workers(config: Config, store: SqliteTaskStore) -> int:
    live_pids, _, _ = _collect_live_running_state(config, store)
    return len(live_pids)


def _collect_live_running_state(config: Config, store: SqliteTaskStore) -> tuple[set[int], list[str], int]:
    registry = WorkerRegistry(config.workers_path)
    live_pids: set[int] = set()
    live_task_ids: set[str] = set()
    active_task_statuses = {
        str(task.id): task.status
        for task in store.get_in_progress()
        if task.id is not None
    }

    for worker in registry.list_all(include_completed=False):
        if worker.status != "running":
            continue
        if not registry.is_running(worker.worker_id):
            continue
        if worker.task_id is not None:
            task_id = str(worker.task_id)
            task_status = active_task_statuses.get(task_id)
            if task_status is None:
                task = store.get(task_id)
                task_status = task.status if task is not None else None
                if task_status is not None:
                    active_task_statuses[task_id] = task_status
            if task_status not in {"pending", "in_progress"}:
                continue
            if worker.pid > 0:
                live_pids.add(worker.pid)
        elif worker.pid > 0:
            live_pids.add(worker.pid)
        if worker.task_id is not None:
            live_task_ids.add(str(worker.task_id))

    for task in store.get_in_progress():
        pid = task.running_pid
        if not _pid_alive(pid):
            continue
        assert pid is not None
        live_pids.add(pid)
        if task.id is not None:
            live_task_ids.add(str(task.id))

    running_task_ids = sorted(live_task_ids, key=lambda task_id: task_id_numeric_key(task_id))
    anonymous_worker_count = max(0, len(live_pids) - len(running_task_ids))
    return live_pids, running_task_ids, anonymous_worker_count


def _format_wake_message(
    *,
    running: int,
    runnable_pending: int,
    blocked_pending: int,
    slots: int,
    running_task_ids: list[str],
    anonymous_worker_count: int = 0,
) -> str:
    message = (
        f"checking... ({running} running, pending={runnable_pending} runnable, "
        f"blocked={blocked_pending}, {slots} slots)"
    )
    if running_task_ids or anonymous_worker_count > 0:
        worker_lines = ["live workers:"]
        worker_lines.extend(f"- {task_id}" for task_id in running_task_ids)
        if anonymous_worker_count == 1:
            worker_lines.append("- 1 worker without an active task id")
        elif anonymous_worker_count > 1:
            worker_lines.append(f"- {anonymous_worker_count} workers without active task ids")
        message += "\n" + "\n".join(worker_lines)
    return message


def _pending_runnable_tasks(
    store: SqliteTaskStore,
    *,
    tags: tuple[str, ...] | None = None,
    any_tag: bool = False,
) -> list[DbTask]:
    return get_runnable_pending_tasks(store, tags=tags, any_tag=any_tag)


def _run_with_optional_stdout_suppressed(quiet: bool, fn: Callable[[], T]) -> T:
    if not quiet:
        return fn()
    with contextlib.redirect_stdout(io.StringIO()):
        return fn()


def _spawn_worker_with_failure_log(
    *,
    quiet: bool,
    log: _WatchLog,
    failure_message: str,
    spawn_fn: Callable[[], int],
    dedupe_key: str,
) -> int:
    rc = _run_with_optional_stdout_suppressed(quiet, spawn_fn)
    if rc != 0:
        log.emit("START_FAILED", failure_message, dedupe_key=dedupe_key)
    return rc


@dataclass
class _CycleResult:
    work_done: bool
    running: int
    pending: int


@dataclass(frozen=True)
class WatchSlotAllocation:
    recovery_slots: int
    pending_slots: int


@dataclass(frozen=True)
class _ObservedFailure:
    task_id: str
    task_type: str
    reason: str


@dataclass
class _RecoveryReport:
    actionable_count: int
    resume_count: int
    retry_count: int


@dataclass(frozen=True)
class _WatchCycleAnalysis:
    target_branch: str
    scope_gaps: tuple[ScopedTagScopeGap, ...]
    owner_rows: tuple[LineageOwnerRow, ...]
    watch_read_context: RecoveryReadContext
    lifecycle_rows: tuple[LineageOwnerRow, ...]
    recovery_rows: tuple[LineageOwnerRow, ...]
    recovery_lane_entry_by_failed_id: dict[str, RecoveryLaneEntry]
    action_plan: tuple[tuple[LineageOwnerRow, DbTask, dict[str, Any]], ...]
    recovery_attention_rows: tuple[tuple[DbTask, FailedRecoveryDecision, dict[str, Any]], ...]
    recovery_visible_skips: tuple[tuple[DbTask, DbTask, FailedRecoveryDecision], ...]
    actionable_failed: tuple[
        tuple[LineageOwnerRow, DbTask, FailedRecoveryDecision, dict[str, Any], bool, DbTask],
        ...,
    ]
    pending_recovery_task_ids: frozenset[str]


def allocate_watch_slots(
    *,
    slots: int,
    recovery_slots_config: int,
    actionable_recovery_count: int,
    worker_consuming_recovery_count: int,
    pending_count: int,
    gate_pending_on_actionable_recovery: bool = False,
) -> WatchSlotAllocation:
    del pending_count
    recovery_slots = min(slots, recovery_slots_config, worker_consuming_recovery_count)
    pending_slots = max(0, slots - recovery_slots)
    if (
        gate_pending_on_actionable_recovery
        and recovery_slots_config > 0
        and actionable_recovery_count > 0
    ):
        pending_slots = 0
    return WatchSlotAllocation(
        recovery_slots=recovery_slots,
        pending_slots=pending_slots,
    )


def _iter_status_transitions(
    old: dict[str, dict[str, str | None]],
    new: dict[str, dict[str, str | None]],
) -> list[tuple[str, str | None, dict[str, str | None]]]:
    transitions: list[tuple[str, str | None, dict[str, str | None]]] = []
    for task_id in sorted(new.keys()):
        old_status = (old.get(task_id) or {}).get("status")
        new_row = new[task_id]
        new_status = new_row.get("status")
        if old_status == new_status:
            continue
        transitions.append((task_id, old_status, new_row))
    return transitions


def _task_matches_tags(
    store: SqliteTaskStore,
    task_id: str,
    tags: tuple[str, ...] | None,
    any_tag: bool,
) -> bool:
    normalized_tags = normalize_tag_filters(tags)
    if not normalized_tags:
        return True
    task = store.get(task_id)
    if task is None:
        return False
    return task_matches_tag_filters(task_tags=task.tags, tag_filters=normalized_tags, any_tag=any_tag)


def _format_scope_gap_message(
    gap: ScopedTagScopeGap,
    *,
    tags: tuple[str, ...] | None,
    any_tag: bool,
) -> str:
    mode = "any" if any_tag else "all"
    owner_id = getattr(gap, "owner_id", "unknown")
    blocker_id = getattr(gap, "blocking_child_id", "unknown")
    blocker_tags_value = getattr(gap, "child_tags", ())
    blocker_tags = ",".join(blocker_tags_value) if blocker_tags_value else "-"
    missing_tags = getattr(gap, "missing_filter_tags", ())
    missing = ",".join(missing_tags) if missing_tags else "-"
    suggested_next_command = getattr(gap, "suggested_next_command", "uv run gza watch")
    if missing_tags:
        hint = f"hint: `{suggested_next_command}` or broaden the scope"
    else:
        hint = "hint: broaden the scope"
    return (
        f"{owner_id} is blocked by out-of-scope child {blocker_id} "
        f"({getattr(gap, 'blocking_state', 'blocked')} {getattr(gap, 'child_task_type', 'task')}, "
        f"status={getattr(gap, 'child_status', 'unknown')}, tags={blocker_tags}, "
        f"scope_mode={mode}, missing_scope={missing}); watch/queue will not start it. {hint}"
    )


def _print_queue_scope_gaps(
    *,
    gaps: Sequence[ScopedTagScopeGap],
    tags: tuple[str, ...] | None,
    any_tag: bool,
) -> None:
    for gap in gaps:
        console.print(
            f"Scope gap: {_format_scope_gap_message(gap, tags=tags, any_tag=any_tag)}"
        )


def _collect_completed_transition_ids(
    old: dict[str, dict[str, str | None]],
    new: dict[str, dict[str, str | None]],
    *,
    store: SqliteTaskStore,
    tags: tuple[str, ...] | None = None,
    any_tag: bool = False,
) -> list[str]:
    completed_ids: list[str] = []
    for task_id, _old_status, new_row in _iter_status_transitions(old, new):
        if new_row.get("status") != "completed":
            continue
        if not _task_matches_tags(store, task_id, tags, any_tag):
            continue
        completed_ids.append(task_id)
    return completed_ids


def _collect_unhandled_failures(
    old: dict[str, dict[str, str | None]],
    new: dict[str, dict[str, str | None]],
    *,
    store: SqliteTaskStore,
    config: Config | None = None,
    max_recovery_attempts: int = 1,
    restart_failed_mode: bool = False,
    tags: tuple[str, ...] | None = None,
    any_tag: bool = False,
) -> list[_ObservedFailure]:
    failures: list[_ObservedFailure] = []
    for task_id, _old_status, new_row in _iter_status_transitions(old, new):
        if new_row.get("status") != "failed":
            continue
        if not _task_matches_tags(store, task_id, tags, any_tag):
            continue
        reason = new_row.get("failure_reason") or "UNKNOWN"
        task = store.get(task_id)
        if task is not None:
            decision = decide_failed_task_recovery(
                store,
                task,
                max_recovery_attempts=max_recovery_attempts,
            )
            if decision.action in {"resume", "retry"} or should_hide_failed_recovery_decision(decision):
                continue
        failures.append(
            _ObservedFailure(
                task_id=task_id,
                task_type=new_row.get("task_type") or "implement",
                reason=reason,
            )
        )
    return failures


def _emit_cycle_attention_summary(log: _WatchLog) -> None:
    messages = log.visible_attention_messages()
    if not messages:
        return
    plural = "s" if len(messages) != 1 else ""
    lines = [f"{NEEDS_ATTENTION_LABEL} ({len(messages)} task{plural}):"]
    lines.extend(f"  {message}" for message in messages)
    log.emit("INFO", "\n".join(lines))


def _emit_recovery_dry_run_report(
    *,
    store: SqliteTaskStore,
    tags: tuple[str, ...] | None,
    any_tag: bool,
    max_recovery_attempts: int,
    show_skipped: bool = False,
    git: Git | None = None,
    target_branch: str | None = None,
) -> _RecoveryReport:
    entries = collect_recovery_lane_entries(
        store,
        tags=tags,
        any_tag=any_tag,
        max_recovery_attempts=max_recovery_attempts,
        git=git,
        target_branch=target_branch,
    )
    scope = ",".join(tags) if tags else "*"
    print(f"Failed recovery plan (tags={scope}, mode=recovery-only)")
    print()
    actionable = resume = retry = 0
    attention_rows: list[tuple[DbTask, dict[str, object]]] = []
    skipped = 0
    hidden_skipped = 0
    visible_task_ids = {entry.task.id for entry in entries if entry.task.id is not None}
    all_owner_rows, report_read_context = _query_owner_rows_with_context(
        store=store,
        tags=tags,
        any_tag=any_tag,
        max_recovery_attempts=max_recovery_attempts,
        include_skipped=True,
    )
    failed_rows = [
        row
        for row in all_owner_rows
        if (
            row.recovery_leaf_task is not None
            and row.recovery_action_task is not None
            and row.recovery_action_task.id == row.recovery_leaf_task.id
        )
    ]
    failed_rows.sort(
        key=lambda row: (
            row.recovery_leaf_task.created_at if row.recovery_leaf_task and row.recovery_leaf_task.created_at else datetime.min.replace(tzinfo=UTC),
            task_id_numeric_key(row.owner_task.id),
        )
    )
    visible_entry_by_task_id = {entry.task.id: entry for entry in entries if entry.task.id is not None}
    reconcile = 0
    for row in failed_rows:
        task = row.recovery_leaf_task
        assert task is not None
        if task.id is None:
            continue
        visible_entry = visible_entry_by_task_id.get(task.id)
        if visible_entry is not None:
            decision = visible_entry.decision
            if visible_entry.attention_action is not None:
                attention_rows.append((task, visible_entry.attention_action))
                continue
        else:
            decision = decide_failed_task_recovery(store, task, max_recovery_attempts=max_recovery_attempts, read_context=report_read_context)
        if decision.action in {"resume", "retry", "reconcile"}:
            launch = decision.launch_mode
            print(
                f"{decision.action:<6} {_format_recovery_report_subject(row, task)} {task.task_type:<9} via {launch:<7} reason={decision.reason_code} "
                f"attempt={decision.attempt_index}/{decision.attempt_limit}"
            )
            actionable += 1
            if decision.action == "resume":
                resume += 1
            if decision.action == "retry":
                retry += 1
            if decision.action == "reconcile":
                reconcile += 1
            continue
        if task.id in visible_task_ids:
            continue
        skipped += 1
        if show_skipped:
            launch = decision.launch_mode
            print(
                f"{decision.action:<6} {_format_recovery_report_subject(row, task)} {task.task_type:<9} via {launch:<7} reason={decision.reason_code} "
                f"attempt={decision.attempt_index}/{decision.attempt_limit}"
            )
        else:
            hidden_skipped += 1
    if attention_rows:
        print()
        print(f"{NEEDS_ATTENTION_LABEL} ({len(attention_rows)} task{'s' if len(attention_rows) != 1 else ''}):")
        for task, action in attention_rows:
            print(f"  {_watch_needs_attention_message(task, action)}")
    print()
    skipped_summary = skipped if show_skipped else hidden_skipped
    if show_skipped:
        print(
            f"Summary: {actionable} actionable ({resume} resume, {retry} retry, {reconcile} reconcile), "
            f"{len(attention_rows)} needs attention, {skipped_summary} skipped"
        )
    else:
        print(
            f"Summary: {actionable} actionable ({resume} resume, {retry} retry), "
            f"{len(attention_rows)} needs attention, {skipped_summary} skipped hidden"
        )
    return _RecoveryReport(actionable_count=actionable, resume_count=resume, retry_count=retry)


def _compute_failure_backoff_seconds(config: Config, streak: int) -> int:
    if streak <= 0:
        return 0
    initial = config.watch.failure_backoff_initial
    maximum = config.watch.failure_backoff_max
    return min(initial * (2 ** (streak - 1)), maximum)


def _analyze_watch_cycle(
    *,
    config: Config,
    store: SqliteTaskStore,
    git: Git,
    slots: int,
    tags: tuple[str, ...] | None,
    any_tag: bool,
    recovery_slots: int,
    recovery_mode: str | None,
    max_recovery_attempts: int,
) -> _WatchCycleAnalysis:
    del slots, recovery_slots, recovery_mode
    cache_scope = git.cached() if hasattr(git, "cached") else contextlib.nullcontext(git)
    with cache_scope:
        target_branch = git.default_branch()
        scope_gaps = tuple(
            collect_scoped_tag_scope_gaps(
                store,
                tag_filters=tags,
                any_tag=any_tag,
                config=config,
                git=git,
                target_branch=target_branch,
            )
        )
        impl_based_on_ids = collect_non_dropped_implement_source_ids(store.get_all())
        owner_rows, watch_read_context = _query_owner_rows_with_context(
            store=store,
            config=config,
            git=git,
            target_branch=target_branch,
            tags=tags,
            any_tag=any_tag,
            max_recovery_attempts=max_recovery_attempts,
            include_skipped=True,
        )
        lifecycle_rows = tuple(
            row
            for row in owner_rows
            if row.lifecycle_action_task is not None and row.lifecycle_action_task.status != "failed"
        )
        recovery_rows = [
            row
            for row in owner_rows
            if row.recovery_leaf_task is not None
            and row.recovery_action_task is not None
            and row.recovery_leaf_task is not None
            and row.recovery_action_task.id == row.recovery_leaf_task.id
        ]
        recovery_rows.sort(
            key=lambda row: (
                row.recovery_leaf_task.created_at
                if row.recovery_leaf_task and row.recovery_leaf_task.created_at
                else datetime.min.replace(tzinfo=UTC),
                task_id_numeric_key(row.owner_task.id),
            )
        )
        recovery_lane_entry_by_failed_id: dict[str, RecoveryLaneEntry] = {
            entry.task.id: entry
            for entry in collect_recovery_lane_entries(
                store,
                tags=tags,
                any_tag=any_tag,
                max_recovery_attempts=max_recovery_attempts,
                git=git,
                target_branch=target_branch,
            )
            if entry.task.id is not None
        }

        action_plan: list[tuple[LineageOwnerRow, DbTask, dict[str, Any]]] = []
        for row in lifecycle_rows:
            task = row.lifecycle_action_task or row.owner_task
            parked_action = _watch_parked_lineage_action(row)
            precomputed_skip_action = (
                row.next_action
                if row.next_action is not None and classify_advance_action(row.next_action) == "skip"
                else None
            )
            action_plan.append(
                (
                    row,
                    task,
                    parked_action
                    if parked_action is not None
                    else precomputed_skip_action
                    if precomputed_skip_action is not None
                    else determine_next_action(
                        config,
                        store,
                        git,
                        task,
                        target_branch,
                        impl_based_on_ids=impl_based_on_ids,
                        read_context=watch_read_context,
                    ),
                )
            )
        action_plan.sort(key=lambda item: lifecycle_action_execution_sort_key(item[1], item[2]))

        pending_recovery_task_ids: set[str] = set()
        recovery_attention_rows: list[tuple[DbTask, FailedRecoveryDecision, dict[str, Any]]] = []
        recovery_visible_skips: list[tuple[DbTask, DbTask, FailedRecoveryDecision]] = []
        actionable_failed: list[
            tuple[LineageOwnerRow, DbTask, FailedRecoveryDecision, dict[str, Any], bool, DbTask]
        ] = []
        for row in recovery_rows:
            failed = row.recovery_leaf_task
            assert failed is not None
            if failed.id is None:
                continue
            decision = decide_failed_task_recovery(
                store,
                failed,
                max_recovery_attempts=max_recovery_attempts,
                read_context=watch_read_context,
            )
            if decision.action != "skip" and decision.recovery_task_id is not None:
                pending_recovery_task_ids.add(decision.recovery_task_id)
            if decision.action == "skip":
                if should_hide_failed_recovery_decision(decision):
                    continue
                recovery_entry = recovery_lane_entry_by_failed_id.get(str(failed.id))
                owner_task = recovery_entry.owner_task if recovery_entry is not None else row.owner_task
                attention_action = (
                    _reroot_failed_recovery_attention_action(
                        owner_task=owner_task,
                        failed_task=failed,
                        attention_action=recovery_entry.attention_action,
                    )
                    if recovery_entry is not None and recovery_entry.attention_action is not None
                    else _owner_failed_recovery_attention_action(
                        store=store,
                        owner_task=owner_task,
                        failed_task=failed,
                        decision=decision,
                        max_recovery_attempts=max_recovery_attempts,
                    )
                )
                if (
                    attention_action is not None
                    and classify_advance_action(attention_action) == "needs_attention"
                ):
                    recovery_attention_rows.append((owner_task, decision, attention_action))
                    continue
                if not _is_watch_observable_recovery_skip(decision):
                    recovery_visible_skips.append((owner_task, failed, decision))
                    continue
            recovery_action = failed_recovery_decision_to_action(failed, decision)
            worker_consuming_recovery = is_worker_consuming_advance_action(str(recovery_action.get("type", "")))
            action_task = _resolve_recovery_action_task(
                store,
                failed_task=failed,
                recovery_task_id=decision.recovery_task_id,
            )
            actionable_failed.append(
                (row, failed, decision, recovery_action, worker_consuming_recovery, action_task)
            )
        return _WatchCycleAnalysis(
            target_branch=target_branch,
            scope_gaps=scope_gaps,
            owner_rows=tuple(owner_rows),
            watch_read_context=watch_read_context,
            lifecycle_rows=lifecycle_rows,
            recovery_rows=tuple(recovery_rows),
            recovery_lane_entry_by_failed_id=recovery_lane_entry_by_failed_id,
            action_plan=tuple(action_plan),
            recovery_attention_rows=tuple(recovery_attention_rows),
            recovery_visible_skips=tuple(recovery_visible_skips),
            actionable_failed=tuple(actionable_failed),
            pending_recovery_task_ids=frozenset(pending_recovery_task_ids),
        )


def _run_cycle(
    *,
    config: Config,
    store: SqliteTaskStore,
    batch: int,
    max_iterations: int,
    dry_run: bool,
    log: _WatchLog,
    tags: tuple[str, ...] | None = None,
    any_tag: bool = False,
    quiet: bool = False,
    recovery_slots: int = 1,
    recovery_mode: str | None = None,
    restart_failed: bool = False,
    restart_failed_batch: int | None = None,
    max_recovery_attempts: int = 1,
    show_skipped: bool = False,
    auto_restart_on_drift: bool = True,
    installed_package_drift: _InstalledPackageDriftState | None = None,
) -> _CycleResult:
    from ._common import (
        prune_terminal_dead_workers,
        reconcile_dead_pending_recovery_tasks,
        reconcile_in_progress_tasks,
    )

    tags = normalize_tag_filters(tags)
    if restart_failed:
        recovery_slots = batch if restart_failed_batch is None else restart_failed_batch
        recovery_mode = "recovery-only"
    elif restart_failed_batch is not None:
        recovery_slots = restart_failed_batch

    log.begin_cycle()
    _warn_if_installed_gza_changed(
        log,
        installed_package_drift,
        auto_restart_on_drift=auto_restart_on_drift,
    )
    if not dry_run:
        reconcile_in_progress_tasks(config)
        prune_terminal_dead_workers(config)
        reconcile_dead_pending_recovery_tasks(config)
        reconcile_stale_watch_no_progress_parks(store)

    snapshot = get_concurrency_snapshot(config, store, cleanup_stale=False)
    running_task_ids = list(snapshot.running_task_ids)
    anonymous_worker_count = snapshot.anonymous_worker_count
    running_task_id_set = set(running_task_ids)
    pending_count = len(_pending_runnable_tasks(store, tags=tags, any_tag=any_tag))
    blocked_pending_count = sum(
        1
        for pending_task in store.get_pending(limit=None)
        if pending_task.task_type != "internal"
        and task_matches_tag_filters(task_tags=pending_task.tags, tag_filters=tags, any_tag=any_tag)
        and store.is_task_blocked(pending_task)[0]
    )
    running = snapshot.running
    effective_batch = min(batch, snapshot.limit)
    slots = max(0, effective_batch - running)
    work_done = False
    started_task_ids: set[str] = set()
    step1_handled_child_task_ids: set[str] = set()

    log.emit(
        "WAKE",
        _format_wake_message(
            running=running,
            runnable_pending=pending_count,
            blocked_pending=blocked_pending_count,
            slots=slots,
            running_task_ids=running_task_ids,
            anonymous_worker_count=anonymous_worker_count,
        ),
    )
    scope_message = _format_scope_message(tags, any_tag=any_tag)
    if scope_message is not None:
        log.emit("INFO", scope_message)

    def _reserve_watch_launch(worker_label: str, subject_task_id: str) -> LaunchPermit | None:
        try:
            return launch_permit(config, store)
        except MaxConcurrentTasksError as exc:
            log.emit(
                "SKIP",
                f"{subject_task_id}: {exc}",
                dedupe_key=f"watch-max-concurrent:{worker_label}:{subject_task_id}",
            )
            return None

    def _prepare_watch_reserved_task(
        task: DbTask,
        *,
        permit: LaunchPermit,
        rollback_on_failure: bool,
    ) -> DbTask | None:
        prepared_task = _prepare_task_for_immediate_execution(
            config,
            task,
            rollback_on_failure=rollback_on_failure,
        )
        if prepared_task is None:
            permit.release()
            return None
        if prepared_task.id is not None:
            reserve_task_launch_permit(str(prepared_task.id), permit)
        return prepared_task

    def _release_watch_reserved_task(task_id: str | None) -> None:
        release_task_launch_permit(task_id)

    # 1) Execute advance actions for completed tasks (includes completed plans
    # with no implement child, aligned with gza advance).
    # Merges run first; worker-spawning actions consume available slots.
    isolation_enabled = bool(getattr(config, "main_checkout_isolate", False))
    git = Git(config.project_dir)
    analysis = _analyze_watch_cycle(
        config=config,
        store=store,
        git=git,
        slots=slots,
        tags=tags,
        any_tag=any_tag,
        recovery_slots=recovery_slots,
        recovery_mode=recovery_mode,
        max_recovery_attempts=max_recovery_attempts,
    )
    target_branch = analysis.target_branch
    for gap in analysis.scope_gaps:
        log.emit_attention(
            attention_key=f"scope-gap:{gap.owner_id}:{gap.blocking_child_id}",
            message=_format_scope_gap_message(gap, tags=tags, any_tag=any_tag),
        )
    lifecycle_rows = list(analysis.lifecycle_rows)
    recovery_lane_entry_by_failed_id = analysis.recovery_lane_entry_by_failed_id
    worker_args = argparse.Namespace(no_docker=False, max_turns=None, resume=False)

    def _watch_spawn_worker(task_obj: DbTask, task_kind: str) -> int:
        assert task_obj.id is not None
        task_id = str(task_obj.id)
        return _spawn_worker_with_failure_log(
            quiet=quiet,
            log=log,
            failure_message=f"{task_id} {task_kind}: worker spawn failed",
            dedupe_key=f"spawn-worker-failed:{task_id}",
            spawn_fn=lambda: _spawn_background_worker(
                worker_args,
                config,
                task_id=task_id,
                quiet=quiet,
                prepared_task=task_obj,
                startup_quiet=True,
            ),
        )

    def _watch_spawn_resume_worker(task_obj: DbTask, task_kind: str) -> int:
        assert task_obj.id is not None
        task_id = str(task_obj.id)
        return _spawn_worker_with_failure_log(
            quiet=quiet,
            log=log,
            failure_message=f"{task_id} {task_kind}: resume worker spawn failed",
            dedupe_key=f"spawn-resume-failed:{task_id}",
            spawn_fn=lambda: _spawn_background_resume_worker(
                worker_args,
                config,
                new_task_id=task_id,
                quiet=quiet,
                prepared_task=task_obj,
                startup_quiet=True,
            ),
        )

    def _watch_spawn_iterate(task_obj: DbTask, task_kind: str) -> int:
        iterate_args = argparse.Namespace(
            max_iterations=max_iterations,
            no_docker=False,
            resume=False,
            retry=False,
            auto_iterate=True,
        )
        return _spawn_worker_with_failure_log(
            quiet=quiet,
            log=log,
            failure_message=f"{task_obj.id} {task_kind}: iterate worker spawn failed",
            dedupe_key=f"spawn-iterate-failed:{task_obj.id}",
            spawn_fn=lambda: _spawn_background_iterate(
                iterate_args,
                config,
                task_obj,
                startup_quiet=True,
            ),
        )

    def _create_rebase_from_task(parent_task: DbTask) -> DbTask:
        assert parent_task.id is not None
        assert parent_task.branch is not None
        return _create_rebase_task(
            store,
            parent_task.id,
            parent_task.branch,
            target_branch,
            trigger_source="watch",
        )

    def _create_targeted_rebase_from_task(parent_task: DbTask, rebase_target: str) -> DbTask:
        assert parent_task.id is not None
        assert parent_task.branch is not None
        return _create_rebase_task(
            store,
            parent_task.id,
            parent_task.branch,
            rebase_target,
            trigger_source="watch",
        )

    def _create_implement_from_task(parent_task: DbTask) -> DbTask:
        return _create_implementation_task_from_source(
            store,
            parent_task,
            prompt=_unimplemented_implement_prompt(parent_task),
            trigger_source="watch",
        )

    def _create_plan_review_from_task(parent_task: DbTask) -> DbTask:
        return _create_plan_review_task(store, parent_task, trigger_source="watch")

    def _create_plan_improve_from_task(parent_task: DbTask, review_task: DbTask) -> DbTask:
        return _create_plan_improve_task(store, parent_task, review_task, trigger_source="watch")

    def _create_review_adjudication_from_task(
        impl_task: DbTask,
        review_task: DbTask,
        finding: Any,
        dispute_metadata: dict[str, Any],
    ) -> DbTask:
        return _create_review_adjudication_task(
            store,
            impl_task,
            review_task,
            finding,
            dispute_metadata=dispute_metadata,
            trigger_source="watch",
        )

    executor_context = AdvanceActionExecutionContext(
        store=store,
        trigger_source="watch",
        dry_run=dry_run,
        max_resume_attempts=max_recovery_attempts,
        use_iterate_for_create_implement=True,
        use_iterate_for_needs_rebase=False,
        can_spawn_worker=lambda _kind: slots > 0,
        no_worker_capacity_message=lambda worker_label: (
            f"SKIP: no watch worker slots available for {worker_label}"
        ),
        prepare_task_for_background_start=lambda task, rollback_on_failure: _prepare_task_for_immediate_execution(
            config,
            task,
            rollback_on_failure=rollback_on_failure,
        ),
        prepare_create_review=lambda t: _prepare_create_review_action(store, t, trigger_source="watch"),
        create_resume_task=lambda t: _create_resume_task(store, t, trigger_source="watch"),
        create_retry_task=lambda t: _create_retry_task(store, t, trigger_source="watch"),
        create_rebase_task=_create_rebase_from_task,
        create_implement_task=_create_implement_from_task,
        create_plan_review_task=_create_plan_review_from_task,
        create_plan_improve_task=_create_plan_improve_from_task,
        create_review_adjudication_task=_create_review_adjudication_from_task,
        materialize_plan_slices=lambda plan_task, review_task, manifest: _materialize_plan_review_slices(
            config,
            store,
            plan_task,
            review_task,
            manifest,
            trigger_source="plan-review",
            require_review_before_merge=config.require_review_before_merge,
        ),
        create_targeted_rebase_task=_create_targeted_rebase_from_task,
        spawn_worker=_watch_spawn_worker,
        spawn_resume_worker=_watch_spawn_resume_worker,
        spawn_iterate_worker=_watch_spawn_iterate,
        is_rebase_target_already_merged=lambda t: _resolve_and_persist_post_merge_rebase_state(
            store,
            git,
            t,
            target_branch,
            merge_source=_resolve_current_merge_source(git, t.branch) if t.branch else None,
        ).already_merged,
        config=config,
        git=git,
        spawn_iterate_recovery=lambda task_obj, mode, prepared_task: _spawn_worker_with_failure_log(
            quiet=quiet,
            log=log,
            failure_message=f"{task_obj.id} {mode}: iterate worker spawn failed",
            dedupe_key=f"spawn-iterate-failed:{task_obj.id}:{mode}",
            spawn_fn=lambda: _spawn_background_iterate(
                argparse.Namespace(
                    max_iterations=max_iterations,
                    no_docker=False,
                    resume=False,
                    retry=False,
                    auto_iterate=True,
                ),
                config,
                prepared_task,
                prepared_task_id=str(prepared_task.id),
                prepared_resume=mode == "resume",
                prepared_phase="preloop",
                startup_quiet=True,
            ),
        ),
        prefer_iterate_for_action=lambda task, action: _watch_iterate_impl_target(
            store=store,
            git=git,
            task=task,
            action=action,
            running_task_ids=running_task_id_set,
            target_branch=target_branch,
            max_recovery_attempts=max_recovery_attempts,
        ),
        reconcile_diverged_branch=lambda t: _reconcile_diverged_branch_with_origin(config, git, t),
    )
    current_branch = git.current_branch()
    merge_git: Git | None = None
    merge_actions_available = True
    merge_skip_reason = "merge-not-default-branch"
    can_merge = True

    def _rebuild_isolated_checkout() -> bool:
        nonlocal merge_git, can_merge, merge_skip_reason, merge_actions_available
        try:
            merge_git = _run_with_optional_stdout_suppressed(
                quiet,
                lambda: ensure_watch_main_checkout(config, git, target_branch, rebuild=True),
            )
            merge_actions_available = True
            can_merge = True
            merge_skip_reason = "merge-not-default-branch"
            log.emit("INFO", "isolated merge checkout rebuilt")
            return True
        except GitError as exc:
            merge_git = None
            merge_actions_available = False
            can_merge = False
            merge_skip_reason = "merge-isolated-checkout-unavailable"
            log.emit("ERROR", f"isolated merge checkout rebuild failed: {exc}")
            return False

    if isolation_enabled and not dry_run:
        try:
            merge_git = _run_with_optional_stdout_suppressed(
                quiet,
                lambda: ensure_watch_main_checkout(config, git, target_branch),
            )
        except GitError as exc:
            log.emit(
                "WARN",
                f"isolated merge checkout refresh failed; rebuilding: {exc}",
            )
            _rebuild_isolated_checkout()

    merge_halted_for_cycle = False
    merge_verify_git: Git | None = None
    if isolation_enabled:
        merge_verify_git = merge_git
    elif current_branch == target_branch:
        merge_verify_git = git
    if merge_verify_git is not None:
        main_verify = _run_with_optional_stdout_suppressed(
            quiet,
            lambda: check_main_integration_verify(
                config,
                store,
                merge_verify_git,
                reason="watch-main-verify",
                red_reruns=2,
            ),
        )
        _maybe_file_main_verify_remediation(
            dry_run=dry_run,
            store=store,
            tags=tags,
            any_tag=any_tag,
            log=log,
            check=main_verify,
        )
        if main_verify.merges_halted:
            merge_halted_for_cycle = True
            if main_verify.state.task.id is not None:
                log.emit_attention(
                    attention_key=f"main-integration-verify:{main_verify.state.task.id}:{MAIN_INTEGRATION_VERIFY_REASON}",
                    message=main_verify.state.alert_message or "main verify is red; merges halted",
                )

    if lifecycle_rows:
        action_plan = list(analysis.action_plan)
        lifecycle_summary = format_cycle_lifecycle_action_summary(
            (row.owner_task, action) for row, _task, action in action_plan
        )
        if lifecycle_summary is not None:
            log.emit("INFO", lifecycle_summary)
        has_merge_action = any(action.get("type") in {"merge", "merge_with_followups"} for _, _, action in action_plan)
        can_merge = merge_actions_available
        if has_merge_action:
            if isolation_enabled:
                if dry_run:
                    can_merge = True
                else:
                    can_merge = merge_git is not None
            else:
                can_merge = _run_with_optional_stdout_suppressed(
                    quiet,
                    lambda: _require_default_branch(git, current_branch, "merge"),
                )
        execution_decisions = plan_lifecycle_execution(
            action_plan,
            free_worker_slots=slots,
            get_action=lambda item: item[2],
        )

        for execution_decision in execution_decisions:
            row, task, action = execution_decision.item
            display_task = row.owner_task
            action_type = action.get("type")
            if classify_advance_action(action) == "needs_attention":
                attention_key: str
                recovery_entry = (
                    recovery_lane_entry_by_failed_id.get(row.recovery_leaf_task.id)
                    if row.recovery_leaf_task is not None and row.recovery_leaf_task.id is not None
                    else None
                )
                if recovery_entry is None:
                    recovery_entry = recovery_lane_entry_by_failed_id.get(get_action_subject_task_id(action) or "")
                if recovery_entry is not None and recovery_entry.attention_action is not None:
                    display_task = recovery_entry.owner_task
                    action = _reroot_failed_recovery_attention_action(
                        owner_task=display_task,
                        failed_task=recovery_entry.task,
                        attention_action=action,
                    )
                    attention_key = (
                        f"failed-recovery-attention:{display_task.id}:{recovery_entry.decision.reason_code}"
                    )
                else:
                    display_task = _resolve_watch_attention_display_task(store, row)
                    attention_key = f"advance-attention:{display_task.id}:{action_type}"
                # Lineage-progress attention comes from the advance action plan only.
                log.emit_attention(
                    attention_key=attention_key,
                    message=_watch_needs_attention_message(display_task, action),
                )
                continue

            if classify_advance_action(action) == "skip":
                if _maybe_repair_target_already_merged_skip(
                    store=store,
                    git=git,
                    task=task,
                    display_task=display_task,
                    action=action,
                    target_branch=target_branch,
                    dry_run=dry_run,
                    log=log,
                ):
                    work_done = True
                    continue
                log.emit(
                    "SKIP",
                    _watch_skip_message(display_task, action),
                    dedupe_key=f"advance-skip:{action_type}:{display_task.id}",
                )
                continue

            if not execution_decision.selected:
                continue

            if action_type in {"merge", "merge_with_followups"}:
                if merge_halted_for_cycle:
                    log.emit(
                        "SKIP",
                        f"{display_task.id}: merges halted while local main verify is red",
                        dedupe_key=f"main-integration-verify-skip:{display_task.id}",
                    )
                    continue
                if not can_merge:
                    if isolation_enabled and merge_skip_reason == "merge-isolated-checkout-unavailable":
                        log.emit(
                            "SKIP",
                            "merge actions skipped: isolated checkout unavailable",
                            dedupe_key="merge-isolated-checkout-unavailable",
                        )
                        continue
                    log.emit(
                        "SKIP",
                        "merge actions skipped: not on default branch",
                        dedupe_key="merge-not-default-branch",
                    )
                    continue
                if dry_run:
                    merge_event = None
                    if display_task.id is not None:
                        merge_event = _resolve_watch_merge_log_event(
                            store,
                            task_id=display_task.id,
                            target_branch=target_branch,
                        )
                    if merge_event is not None:
                        log.emit(
                            "MERGE",
                            f"{merge_event.display_task_id} -> {merge_event.target_branch} [dry-run]",
                        )
                    work_done = True
                    continue
                merge_execution_git = merge_git if (isolation_enabled and merge_git is not None) else git
                merge_execution_branch = target_branch if isolation_enabled else current_branch
                merge_event = None
                merge_status_before: str | None = None
                if display_task.id is not None:
                    merge_event = _resolve_watch_merge_log_event(
                        store,
                        task_id=display_task.id,
                        target_branch=target_branch,
                    )
                    if merge_event is not None:
                        merge_status_before = (
                            (_task_snapshot(store).get(merge_event.display_task_id) or {}).get("merge_status")
                        )
                merge_result = _run_with_optional_stdout_suppressed(
                    quiet,
                    lambda: _execute_merge_action(
                        config,
                        store,
                        git,
                        task,
                        action,
                        target_branch=target_branch,
                        current_branch=current_branch,
                        merge_git=merge_execution_git,
                        merge_current_branch=merge_execution_branch,
                        already_merged_behavior="mark_merged",
                        merge_source=MERGE_SOURCE_WATCH,
                        quiet_mechanics=True,
                    ),
                )
                rc = merge_result.rc
                if rc == 0 and merge_event is not None:
                    merge_status_after = (
                        (_task_snapshot(store).get(merge_event.display_task_id) or {}).get("merge_status")
                    )
                    if (
                        merge_status_before != "merged"
                        and merge_status_after == "merged"
                        and not log.was_merge_logged(merge_event.merge_key)
                    ):
                        log.emit("MERGE", f"{merge_event.display_task_id} -> {merge_event.target_branch}")
                        log.note_merge_logged(merge_event.merge_key)
                if rc == 0 and merge_execution_git is not None:
                    main_verify = _run_with_optional_stdout_suppressed(
                        quiet,
                        lambda: check_main_integration_verify(
                            config,
                            store,
                            merge_execution_git,
                            reason="watch-post-merge",
                            red_reruns=2,
                        ),
                    )
                    _maybe_file_main_verify_remediation(
                        dry_run=dry_run,
                        store=store,
                        tags=tags,
                        any_tag=any_tag,
                        log=log,
                        check=main_verify,
                    )
                    if main_verify.merges_halted:
                        merge_halted_for_cycle = True
                        if main_verify.state.task.id is not None:
                            log.emit_attention(
                                attention_key=f"main-integration-verify:{main_verify.state.task.id}:{MAIN_INTEGRATION_VERIFY_REASON}",
                                message=main_verify.state.alert_message or "main verify is red; merges halted",
                            )
                for followup_task in merge_result.created_followups:
                    log.emit("FOLLOW", f"{followup_task.id} created from {display_task.id}")
                for followup_task in merge_result.reused_followups:
                    log.emit("FOLLOW", f"{followup_task.id} reused from {display_task.id}")
                if getattr(merge_result, "status", None) == "blocked_dirty_checkout":
                    log.emit_attention(
                        attention_key="merge-blocked-dirty-checkout",
                        message="merges blocked: main checkout has uncommitted changes - commit or stash them first",
                    )
                    break
                if rc == 0:
                    work_done = True
                else:
                    conflict_handled = False
                    conflict_assessment: _IsolatedMergeFailureAssessment | None = None
                    if isolation_enabled and task.branch is not None:
                        conflict_assessment = _assess_isolated_merge_failure(
                            merge_execution_git,
                            task.branch,
                            target_branch,
                        )
                        if conflict_assessment.is_conflict:
                            conflict_handled = True
                            try:
                                cleanup_failed_merge_checkout(merge_execution_git)
                            except GitError as cleanup_error:
                                log.emit(
                                    "WARN",
                                    (
                                        f"{display_task.id}: isolated checkout cleanup failed after conflict: "
                                        f"{cleanup_error}"
                                    ),
                                )
                                _rebuild_isolated_checkout()
                            try:
                                reserved_launch = _reserve_watch_launch("rebase", str(display_task.id))
                                if reserved_launch is None:
                                    continue
                                rebase_task = _create_rebase_from_task(task)
                            except Exception as rebase_error:
                                if reserved_launch is not None:
                                    reserved_launch.release()
                                log.emit("ERROR", f"{display_task.id}: failed to create rebase task ({rebase_error})")
                                continue
                            assert rebase_task.id is not None
                            prepared_rebase_task = _prepare_watch_reserved_task(
                                rebase_task,
                                permit=reserved_launch,
                                rollback_on_failure=True,
                            )
                            if prepared_rebase_task is None:
                                log.emit(
                                    "ERROR",
                                    f"{display_task.id}: failed to prepare merge-conflict rebase task {rebase_task.id}",
                                )
                                continue
                            step1_handled_child_task_ids.add(str(rebase_task.id))
                            work_done = True
                            if slots > 0:
                                rebase_rc = _watch_spawn_worker(prepared_rebase_task, "rebase")
                                _release_watch_reserved_task(str(prepared_rebase_task.id))
                                if rebase_rc == 0:
                                    log.emit("START", f"{prepared_rebase_task.id} rebase")
                                    started_task_ids.add(str(prepared_rebase_task.id))
                                    slots -= 1
                                else:
                                    log.emit(
                                        "SKIP",
                                        f"{display_task.id}: merge conflict rebase worker spawn failed",
                                        dedupe_key=f"merge-conflict-rebase-spawn-failed:{display_task.id}",
                                    )
                            else:
                                _release_watch_reserved_task(str(prepared_rebase_task.id))
                                log.emit(
                                    "SKIP",
                                    f"{display_task.id}: merge conflict queued rebase {rebase_task.id} (no free slots)",
                                    dedupe_key=f"merge-conflict-rebase-queued:{display_task.id}",
                                )
                            log.emit(
                                "SKIP",
                                f"{display_task.id}: merge conflict routed to rebase",
                                dedupe_key=f"merge-conflict:{display_task.id}",
                            )
                    if conflict_handled:
                        continue
                    if (
                        conflict_assessment is not None
                        and conflict_assessment.reason == "branch already merged"
                        and task.id is not None
                    ):
                        repaired = reconcile_task_branch_merge_truth(
                            store,
                            git,
                            str(task.id),
                            target_branch=target_branch,
                            include_diff_stats=True,
                            persist=True,
                        )
                        if repaired.ok and repaired.merge_status == "merged":
                            log.emit(
                                "REPAIR",
                                f"{display_task.id}: marked merged after shared reconciliation against {target_branch}",
                            )
                            work_done = True
                            continue
                    if conflict_assessment is not None and conflict_assessment.reason is not None:
                        log.emit(
                            "SKIP",
                            (
                                f"{display_task.id}: merge failed ({conflict_assessment.reason}); "
                                "not routing to rebase"
                            ),
                            dedupe_key=f"merge-failed-non-conflict:{display_task.id}",
                        )
                        continue
                    log.emit(
                        "SKIP",
                        f"{display_task.id}: merge failed",
                        dedupe_key=f"merge-failed:{display_task.id}",
                    )
                continue
            if not dry_run and display_task.id is not None:
                if _maybe_emit_active_watch_recovery_backoff(
                    store=store,
                    log=log,
                    subject_task=display_task,
                    action=action,
                ):
                    continue
                no_progress_attention = _maybe_park_watch_no_progress(
                    config=config,
                    store=store,
                    subject_task=display_task,
                    action=action,
                    action_task=task,
                    failed_task=None,
                    no_progress_cycles=config.watch.no_progress_cycles,
                )
                if _watch_no_progress_result_deferred_for_transient_backoff(no_progress_attention):
                    _maybe_emit_active_watch_recovery_backoff(
                        store=store,
                        log=log,
                        subject_task=display_task,
                        action=action,
                    )
                    continue
                if no_progress_attention is not None:
                    log.emit_attention(
                        attention_key=f"advance-attention:{display_task.id}:{action_type}:watch-no-progress",
                        message=_watch_needs_attention_message(display_task, no_progress_attention),
                    )
                    continue

            exec_result = execute_advance_action(task=task, action=action, context=executor_context)
            child_id = exec_result.handled_task_id
            guarded_pending_task_id = exec_result.guarded_pending_task_id

            if exec_result.status == "skip":
                if guarded_pending_task_id is not None:
                    step1_handled_child_task_ids.add(str(guarded_pending_task_id))
                message = exec_result.message
                if action_type == "improve" and display_task.id is not None:
                    message = f"{display_task.id}: {message}"
                _maybe_emit_recurring_guarded_pending_skip_attention(
                    store=store,
                    log=log,
                    guarded_pending_task_id=guarded_pending_task_id,
                    guard_message=exec_result.message,
                )
                attention = resolve_execution_needs_attention(task, exec_result)
                if attention is not None and display_task.id is not None:
                    attention_task = getattr(attention, "task", display_task)
                    attention_fallback = (
                        _resolve_incomplete_owner_task(store, cast(Any, row))
                        if "subject_task_id" not in attention.action
                        else attention_task
                    )
                    display_task = resolve_subject_task(
                        store,
                        attention.action,
                        row,
                        fallback_task=attention_fallback,
                    )
                    # Orthogonal to advance-plan classification: the action tried to run
                    # and the execution layer reported a worker/startup attention state.
                    log.emit_attention(
                        attention_key=f"advance-attention:{display_task.id}:{attention.action['type']}",
                        message=_watch_needs_attention_message(display_task, attention.action),
                    )
                    continue
                log.emit(
                    "SKIP",
                    message,
                    dedupe_key=f"advance-worker-skip:{action_type}:{display_task.id}:{message}",
                )
                continue

            if exec_result.status == "error":
                if guarded_pending_task_id is not None:
                    step1_handled_child_task_ids.add(str(guarded_pending_task_id))
                if not exec_result.attempted_spawn and display_task.id is not None:
                    event = "REPAIR" if action_type == "reconcile_branch_divergence" else "ERROR"
                    log.emit(
                        event,
                        f"{display_task.id}: {exec_result.message}",
                        dedupe_key=f"advance-worker-error:{action_type}:{display_task.id}:{exec_result.message}",
                    )
                if child_id is not None and action_type in {
                    "create_review",
                    "improve",
                    "create_implement",
                    "needs_rebase",
                    "run_review",
                    "run_improve",
                }:
                    step1_handled_child_task_ids.add(str(child_id))
                continue

            if exec_result.status == "dry_run":
                if guarded_pending_task_id is not None:
                    step1_handled_child_task_ids.add(str(guarded_pending_task_id))
                if exec_result.worker_label == "iterate" and child_id is not None:
                    log.emit("START", f"{child_id} iterate [dry-run]")
                    started_task_ids.add(str(child_id))
                elif action_type == "create_review" and display_task.id is not None:
                    log.emit("START", f"(new) review for {display_task.id} [dry-run]")
                elif action_type == "run_review" and child_id is not None:
                    log.emit("START", f"{child_id} review [dry-run]")
                    started_task_ids.add(str(child_id))
                elif action_type == "improve":
                    failed_id = exec_result.failed_improve.id if exec_result.failed_improve is not None else None
                    if exec_result.improve_mode == "resume" and failed_id is not None:
                        log.emit("START", f"(resume) improve for {failed_id} [dry-run]")
                    elif exec_result.improve_mode == "retry" and failed_id is not None:
                        log.emit("START", f"(retry) improve for {failed_id} [dry-run]")
                    elif display_task.id is not None:
                        log.emit("START", f"(new) improve for {display_task.id} [dry-run]")
                elif action_type == "run_improve" and child_id is not None:
                    log.emit("START", f"{child_id} improve [dry-run]")
                    started_task_ids.add(str(child_id))
                elif action_type == "create_implement" and display_task.id is not None:
                    log.emit("START", f"(new) implement for {display_task.id} [dry-run]")
                elif action_type == "needs_rebase" and display_task.id is not None:
                    log.emit("START", f"(new) rebase for {display_task.id} [dry-run]")
                elif action_type == "reconcile_branch_divergence" and display_task.id is not None:
                    if exec_result.worker_label == "rebase" and child_id is not None:
                        log.emit("START", f"{child_id} rebase [dry-run]")
                        started_task_ids.add(str(child_id))
                    else:
                        log.emit("START", f"{display_task.id} reconcile divergence [dry-run]")
                if exec_result.worker_consuming:
                    slots -= 1
                work_done = True
                continue

            if (
                child_id is not None
                and exec_result.worker_label != "iterate"
                and action_type in {"create_review", "improve", "create_implement", "needs_rebase"}
            ):
                step1_handled_child_task_ids.add(str(child_id))
            if guarded_pending_task_id is not None:
                step1_handled_child_task_ids.add(str(guarded_pending_task_id))

            if exec_result.status == "success" and child_id is not None:
                if exec_result.worker_label == "iterate":
                    log.emit("START", f"{child_id} iterate")
                elif action_type in {"create_review", "run_review"}:
                    log.emit("START", f"{child_id} review")
                elif action_type in {"improve", "run_improve"}:
                    log.emit("START", f"{child_id} improve")
                elif action_type == "create_implement":
                    log.emit("START", f"{child_id} implement")
                elif action_type == "needs_rebase" or exec_result.worker_label == "rebase":
                    log.emit("START", f"{child_id} rebase")
                refreshed_action_task = store.get(child_id) if child_id is not None else None
                no_progress_attention = _maybe_finalize_watch_no_progress_for_background_action(
                    config=config,
                    store=store,
                    subject_task=display_task,
                    action=action,
                    action_task_before=task,
                    action_task_after=refreshed_action_task,
                    failed_task=None,
                    no_progress_cycles=config.watch.no_progress_cycles,
                )
                started_task_ids.add(str(child_id))
                if exec_result.worker_consuming:
                    slots -= 1
                work_done = True
                if no_progress_attention is not None:
                    log.emit_attention(
                        attention_key=f"advance-attention:{display_task.id}:{action_type}:watch-no-progress",
                        message=_watch_needs_attention_message(display_task, no_progress_attention),
                    )
            elif exec_result.status == "success" and action_type == "reconcile_branch_divergence":
                refreshed_display_task = store.get(str(display_task.id)) if display_task.id is not None else None
                no_progress_attention = _finalize_watch_no_progress_after_execution(
                    config=config,
                    store=store,
                    subject_task=display_task,
                    action=action,
                    action_task_before=task,
                    action_task_after=refreshed_display_task,
                    failed_task=None,
                    no_progress_cycles=config.watch.no_progress_cycles,
                )
                if display_task.id is not None:
                    log.emit(
                        "REPAIR",
                        f"{display_task.id}: {exec_result.success_message or exec_result.message}",
                        dedupe_key=f"advance-reconcile:{display_task.id}",
                    )
                    if no_progress_attention is not None:
                        log.emit_attention(
                            attention_key=f"advance-attention:{display_task.id}:{action_type}:watch-no-progress",
                            message=_watch_needs_attention_message(display_task, no_progress_attention),
                        )
                work_done = True

    # 2) Recovery queue for failed tasks.
    pending_recovery_task_ids = set(analysis.pending_recovery_task_ids)
    actionable_failed = list(analysis.actionable_failed)
    parked_recovery_subject_ids: set[str] = set()
    for owner_task, decision, attention_action in analysis.recovery_attention_rows:
        owner_id = owner_task.id or "unknown"
        log.emit_attention(
            attention_key=f"failed-recovery-attention:{owner_id}:{decision.reason_code}",
            message=_watch_needs_attention_message(owner_task, attention_action),
        )
    for owner_task, failed, decision in analysis.recovery_visible_skips:
        if recovery_slots > 0 and show_skipped:
            log.emit(
                "SKIP",
                f"{owner_task.id} failed {failed.task_type}: {decision.reason_code}",
                dedupe_key=f"recovery-skip:{owner_task.id}:{decision.reason_code}",
            )
    if not dry_run:
        for _row, failed, decision, recovery_action, _worker_consuming, action_task in actionable_failed:
            if failed.id is None:
                continue
            candidate = build_watch_progress_candidate(
                store,
                subject_task=failed,
                action=recovery_action,
                action_task=action_task,
                failed_task=failed,
            )
            if get_active_watch_no_progress_attention(store, candidate=candidate) is not None:
                parked_recovery_subject_ids.add(str(failed.id))
                if decision.recovery_task_id is not None:
                    pending_recovery_task_ids.add(decision.recovery_task_id)
    pending_tasks = list(_pending_runnable_tasks(store, tags=tags, any_tag=any_tag))
    pending_candidates = [
        task
        for task in pending_tasks
        if task.id is not None
        and str(task.id) not in started_task_ids
        and str(task.id) not in pending_recovery_task_ids
        and str(task.id) not in step1_handled_child_task_ids
    ]
    active_backoff_recovery_subject_ids: set[str] = set()
    if not dry_run:
        for _row, failed, _decision, recovery_action, _worker_consuming, _action_task in actionable_failed:
            if failed.id is None:
                continue
            if str(failed.id) in parked_recovery_subject_ids:
                continue
            if (
                _resolve_active_watch_recovery_backoff(
                    store=store,
                    subject_task=failed,
                    action=recovery_action,
                )
                is not None
            ):
                active_backoff_recovery_subject_ids.add(str(failed.id))
    launchable_recovery_subject_ids = {
        str(failed.id)
        for _row, failed, _decision, _action, _worker_consuming, _action_task in actionable_failed
        if str(failed.id) not in parked_recovery_subject_ids
        and str(failed.id) not in active_backoff_recovery_subject_ids
    }
    worker_consuming_launchable_recovery_count = sum(
        1
        for _row, failed, _decision, _action, worker_consuming, _action_task in actionable_failed
        if worker_consuming and str(failed.id) in launchable_recovery_subject_ids
    )
    slot_allocation = allocate_watch_slots(
        slots=slots,
        recovery_slots_config=recovery_slots,
        actionable_recovery_count=len(launchable_recovery_subject_ids),
        worker_consuming_recovery_count=worker_consuming_launchable_recovery_count,
        pending_count=len(pending_candidates),
        gate_pending_on_actionable_recovery=(
            recovery_mode == "recovery-only"
            and bool(launchable_recovery_subject_ids)
        ),
    )
    reserved_recovery_slots = slot_allocation.recovery_slots
    pending_slots = slot_allocation.pending_slots
    nonparked_recovery_subject_ids = set(launchable_recovery_subject_ids)
    recovery_started_this_cycle = False
    for row, failed, decision, recovery_action, worker_consuming_recovery, action_task in actionable_failed:
        if recovery_slots <= 0:
            break
        if failed.id is None:
            continue
        if str(failed.id) in parked_recovery_subject_ids:
            if not dry_run:
                candidate = build_watch_progress_candidate(
                    store,
                    subject_task=failed,
                    action=recovery_action,
                    action_task=action_task,
                    failed_task=failed,
                )
                active_attention = get_active_watch_no_progress_attention(store, candidate=candidate)
                if active_attention is not None:
                    log.emit_attention(
                        attention_key=f"recovery-attention:{failed.id}:{decision.action}:watch-no-progress",
                        message=_watch_needs_attention_message(failed, active_attention),
                    )
            continue
        if not dry_run:
            if _maybe_emit_active_watch_recovery_backoff(
                store=store,
                log=log,
                subject_task=failed,
                action=recovery_action,
            ):
                continue
        if worker_consuming_recovery and reserved_recovery_slots <= 0:
            continue
        if not dry_run:
            no_progress_attention = _maybe_park_watch_no_progress(
                config=config,
                store=store,
                subject_task=failed,
                action=recovery_action,
                action_task=action_task,
                failed_task=failed,
                no_progress_cycles=config.watch.no_progress_cycles,
            )
            if _watch_no_progress_result_deferred_for_transient_backoff(no_progress_attention):
                _maybe_emit_active_watch_recovery_backoff(
                    store=store,
                    log=log,
                    subject_task=failed,
                    action=recovery_action,
                )
                continue
            if no_progress_attention is not None:
                parked_recovery_subject_ids.add(str(failed.id))
                nonparked_recovery_subject_ids.discard(str(failed.id))
                if decision.recovery_task_id is not None:
                    pending_recovery_task_ids.add(decision.recovery_task_id)
                log.emit_attention(
                    attention_key=f"recovery-attention:{failed.id}:{decision.action}:watch-no-progress",
                    message=_watch_needs_attention_message(failed, no_progress_attention),
                )
                continue
        if decision.action == "skip":
            if recovery_slots > 0 and show_skipped:
                log.emit(
                    "SKIP",
                    f"{row.owner_task.id} failed {failed.task_type}: {decision.reason_code}",
                    dedupe_key=f"recovery-skip:{row.owner_task.id}:{decision.reason_code}",
                )
            continue
        if decision.action == "reconcile":
            if dry_run:
                log.emit(
                    "RECOVR",
                    (
                        f"{failed.id} reconcile branch publication "
                        f"[owner={row.owner_task.id}] "
                        f"(reason={decision.reason_code}, attempt {decision.attempt_index}/{decision.attempt_limit}) [dry-run]"
                    ),
                )
                work_done = True
                continue
            exec_result = execute_advance_action(task=failed, action=recovery_action, context=executor_context)
            if exec_result.status == "skip":
                attention = resolve_execution_needs_attention(failed, exec_result)
                if attention is not None:
                    log.emit_attention(
                        attention_key=f"recovery-attention:{failed.id}:reconcile",
                        message=_watch_needs_attention_message(attention.task, attention.action),
                    )
                else:
                    log.emit(
                        "SKIP",
                        f"{failed.id}: {exec_result.message}",
                        dedupe_key=f"recovery-reconcile-skip:{failed.id}:{exec_result.message}",
                    )
                continue
            if exec_result.status == "error":
                log.emit(
                    "REPAIR",
                    f"{failed.id}: {exec_result.message}",
                    dedupe_key=f"recovery-reconcile-error:{failed.id}:{exec_result.message}",
                )
                continue
            if exec_result.status == "success":
                refreshed_failed = store.get(failed.id) if failed.id is not None else None
                no_progress_attention = _finalize_watch_no_progress_after_execution(
                    config=config,
                    store=store,
                    subject_task=failed,
                    action=recovery_action,
                    action_task_before=action_task,
                    action_task_after=refreshed_failed or action_task,
                    failed_task=failed,
                    no_progress_cycles=config.watch.no_progress_cycles,
                )
                recovery_started_this_cycle = True
                log.emit(
                    "RECOVR",
                    (
                        f"{failed.id} reconcile branch publication "
                        f"(reason={decision.reason_code}, attempt {decision.attempt_index}/{decision.attempt_limit})"
                    ),
                )
                log.emit(
                    "REPAIR",
                    f"{failed.id}: {exec_result.success_message or exec_result.message}",
                    dedupe_key=f"recovery-reconcile-success:{failed.id}",
                )
                if no_progress_attention is not None:
                    log.emit_attention(
                        attention_key=f"recovery-attention:{failed.id}:{decision.action}:watch-no-progress",
                        message=_watch_needs_attention_message(failed, no_progress_attention),
                    )
                work_done = True
                continue
            continue
        if decision.action == "resume":
            if dry_run:
                destination = decision.recovery_task_id or "(new task)"
                log.emit(
                    "RECOVR",
                    (
                        f"{failed.id} resume via {decision.launch_mode} -> {destination} "
                        f"[owner={row.owner_task.id}] "
                        f"(reason={decision.reason_code}, attempt {decision.attempt_index}/{decision.attempt_limit}) [dry-run]"
                    ),
                )
                slots -= 1
                reserved_recovery_slots -= 1
                work_done = True
                continue
            if decision.launch_mode == "worker":
                reserved_launch = _reserve_watch_launch("resume", str(failed.id))
                if reserved_launch is None:
                    continue
                if decision.reuse_existing:
                    assert decision.recovery_task_id is not None
                    recovered_task_id = decision.recovery_task_id
                    recovered_task = store.get(recovered_task_id)
                    assert recovered_task is not None
                else:
                    try:
                        recovered_task = _create_resume_task(store, failed, trigger_source="watch")
                    except Exception:
                        reserved_launch.release()
                        raise
                    assert recovered_task.id is not None
                    recovered_task_id = str(recovered_task.id)
                prepared_recovered_task = _prepare_watch_reserved_task(
                    recovered_task,
                    permit=reserved_launch,
                    rollback_on_failure=not decision.reuse_existing,
                )
                if prepared_recovered_task is None:
                    continue
                pending_recovery_task_ids.add(recovered_task_id)
                rc = _spawn_worker_with_failure_log(
                    quiet=quiet,
                    log=log,
                    failure_message=f"{failed.id} -> {recovered_task_id}: resume worker spawn failed",
                    dedupe_key=f"spawn-resume-failed:{failed.id}:{recovered_task_id}",
                    spawn_fn=lambda: _spawn_background_resume_worker(
                        argparse.Namespace(no_docker=False, max_turns=None),
                        config,
                        recovered_task_id,
                        quiet=quiet,
                        prepared_task=prepared_recovered_task,
                        startup_quiet=True,
                    ),
                )
                _release_watch_reserved_task(recovered_task_id)
            else:
                reserved_launch = _reserve_watch_launch("iterate", str(failed.id))
                if reserved_launch is None:
                    continue
                if decision.reuse_existing:
                    assert decision.recovery_task_id is not None
                    recovered_task = store.get(decision.recovery_task_id)
                    assert recovered_task is not None
                else:
                    try:
                        recovered_task = _create_resume_task(store, failed, trigger_source="watch")
                    except Exception:
                        reserved_launch.release()
                        raise
                prepared_recovered_task = _prepare_watch_reserved_task(
                    recovered_task,
                    permit=reserved_launch,
                    rollback_on_failure=not decision.reuse_existing,
                )
                if prepared_recovered_task is None:
                    continue
                resume_recovered_task = prepared_recovered_task
                recovered_task_id = str(prepared_recovered_task.id)
                pending_recovery_task_ids.add(recovered_task_id)
                rc = _spawn_worker_with_failure_log(
                    quiet=quiet,
                    log=log,
                    failure_message=f"{failed.id} -> {recovered_task_id}: iterate worker spawn failed",
                    dedupe_key=f"spawn-iterate-failed:{failed.id}:{recovered_task_id}",
                    spawn_fn=lambda: _spawn_background_iterate(
                        argparse.Namespace(
                            max_iterations=max_iterations,
                            no_docker=False,
                            resume=False,
                            retry=False,
                            auto_iterate=True,
                        ),
                        config,
                        resume_recovered_task,
                        prepared_task_id=recovered_task_id,
                        prepared_resume=True,
                        prepared_phase="preloop",
                        startup_quiet=True,
                    ),
                )
                _release_watch_reserved_task(recovered_task_id)
        else:
            if dry_run:
                destination = decision.recovery_task_id or "(new task)"
                log.emit(
                    "RECOVR",
                    (
                        f"{failed.id} retry via {decision.launch_mode} -> {destination} "
                        f"[owner={row.owner_task.id}] "
                        f"(reason={decision.reason_code}, attempt {decision.attempt_index}/{decision.attempt_limit}) [dry-run]"
                    ),
                )
                slots -= 1
                reserved_recovery_slots -= 1
                work_done = True
                continue
            reserved_launch = _reserve_watch_launch(
                "iterate" if decision.launch_mode != "worker" else "retry",
                str(failed.id),
            )
            if reserved_launch is None:
                continue
            if decision.reuse_existing:
                assert decision.recovery_task_id is not None
                recovered_task_id = decision.recovery_task_id
                existing_recovered_task = store.get(recovered_task_id)
                assert existing_recovered_task is not None
                recovered_task = existing_recovered_task
            else:
                try:
                    recovered_task = _create_retry_task(
                        store,
                        failed,
                        trigger_source="watch",
                        automatic_recovery=True,
                    )
                except Exception:
                    reserved_launch.release()
                    raise
                assert recovered_task.id is not None
                recovered_task_id = str(recovered_task.id)
            prepared_recovered_task = _prepare_watch_reserved_task(
                recovered_task,
                permit=reserved_launch,
                rollback_on_failure=not decision.reuse_existing,
            )
            if prepared_recovered_task is None:
                continue
            retry_recovered_task = prepared_recovered_task
            recovered_task_id = str(prepared_recovered_task.id)
            pending_recovery_task_ids.add(recovered_task_id)
            rc = (
                _spawn_worker_with_failure_log(
                    quiet=quiet,
                    log=log,
                    failure_message=f"{failed.id} -> {recovered_task_id}: worker spawn failed",
                    dedupe_key=f"spawn-worker-failed:{failed.id}:{recovered_task_id}",
                    spawn_fn=lambda: _spawn_background_worker(
                        argparse.Namespace(no_docker=False, max_turns=None, resume=False),
                        config,
                        task_id=recovered_task_id,
                        quiet=quiet,
                        prepared_task=prepared_recovered_task,
                        startup_quiet=True,
                    ),
                )
                if decision.launch_mode == "worker"
                else _spawn_worker_with_failure_log(
                    quiet=quiet,
                    log=log,
                    failure_message=f"{failed.id} -> {recovered_task_id}: iterate worker spawn failed",
                    dedupe_key=f"spawn-iterate-failed:{failed.id}:{recovered_task_id}",
                    spawn_fn=lambda: _spawn_background_iterate(
                        argparse.Namespace(
                            max_iterations=max_iterations,
                            no_docker=False,
                            resume=False,
                            retry=False,
                            auto_iterate=True,
                        ),
                        config,
                        retry_recovered_task,
                        prepared_task_id=recovered_task_id,
                        prepared_resume=False,
                        prepared_phase="preloop",
                        startup_quiet=True,
                    ),
                )
            )
            _release_watch_reserved_task(recovered_task_id)

        if rc != 0:
            continue
        refreshed_recovered_task = store.get(recovered_task_id)
        no_progress_attention = _maybe_finalize_watch_no_progress_for_background_action(
            config=config,
            store=store,
            subject_task=failed,
            action=recovery_action,
            action_task_before=action_task,
            action_task_after=refreshed_recovered_task,
            failed_task=failed,
            no_progress_cycles=config.watch.no_progress_cycles,
        )
        recovery_started_this_cycle = True
        started_task_ids.add(recovered_task_id)
        slots -= 1
        reserved_recovery_slots -= 1
        work_done = True
        log.emit(
            "RECOVR",
            (
                f"{failed.id} {decision.action} via {decision.launch_mode} -> {recovered_task_id} "
                f"(reason={decision.reason_code}, attempt {decision.attempt_index}/{decision.attempt_limit})"
            ),
        )
        if no_progress_attention is not None:
            log.emit_attention(
                attention_key=f"recovery-attention:{failed.id}:{decision.action}:watch-no-progress",
                message=_watch_needs_attention_message(failed, no_progress_attention),
            )

    if recovery_mode == "recovery-only" and pending_slots <= 0 and not recovery_started_this_cycle:
        if not nonparked_recovery_subject_ids:
            pending_slots = slots

    # 3) Start new queued tasks (consumes slots)
    def consume_pending_slot() -> None:
        nonlocal pending_slots, slots
        pending_slots -= 1
        slots = max(0, slots - 1)

    if pending_slots > 0:
        log.emit("QUEUE", "pending queue active")
    if pending_slots > 0:
        for task in pending_tasks:
            if pending_slots <= 0:
                break
            assert task.id is not None
            if str(task.id) in started_task_ids:
                continue
            if str(task.id) in pending_recovery_task_ids:
                continue
            if str(task.id) in step1_handled_child_task_ids:
                continue
            task_type = task.task_type or "implement"
            pending_action = _pending_queue_dispatch_action(task)
            if task_type == "implement":
                if dry_run:
                    dry_run_prompt = _format_prompt_for_width(
                        task.prompt,
                        prefix=16 + len(f"{task.id} {task_type} \""),
                        suffix=len('" [dry-run]'),
                    )
                    log.emit("START", f"{task.id} {task_type} \"{dry_run_prompt}\" [dry-run]")
                    started_task_ids.add(str(task.id))
                    consume_pending_slot()
                    work_done = True
                    continue
                if _maybe_emit_active_watch_recovery_backoff(
                    store=store,
                    log=log,
                    subject_task=task,
                    action=pending_action,
                ):
                    continue
                no_progress_attention = _maybe_park_watch_no_progress(
                    config=config,
                    store=store,
                    subject_task=task,
                    action=pending_action,
                    action_task=task,
                    failed_task=None,
                    no_progress_cycles=config.watch.no_progress_cycles,
                )
                if _watch_no_progress_result_deferred_for_transient_backoff(no_progress_attention):
                    _maybe_emit_active_watch_recovery_backoff(
                        store=store,
                        log=log,
                        subject_task=task,
                        action=pending_action,
                    )
                    continue
                if no_progress_attention is not None:
                    log.emit_attention(
                        attention_key=f"pending-attention:{task.id}:{pending_action['type']}:watch-no-progress",
                        message=_watch_needs_attention_message(task, no_progress_attention),
                    )
                    continue
                iterate_args = argparse.Namespace(
                    max_iterations=max_iterations,
                    no_docker=False,
                    resume=False,
                    retry=False,
                    auto_iterate=True,
                )
                pending_recovery_mode = resolve_pending_recovery_execution_mode(task)
                reserved_launch = _reserve_watch_launch("iterate", str(task.id))
                if reserved_launch is None:
                    continue
                prepared_pending_task = _prepare_watch_reserved_task(
                    task,
                    permit=reserved_launch,
                    rollback_on_failure=False,
                )
                if prepared_pending_task is None:
                    log.emit(
                        "START_FAILED",
                        f"{task.id} {task_type}: iterate startup preparation failed",
                        dedupe_key=f"prepare-iterate-failed:{task.id}",
                    )
                    continue
                rc = _spawn_worker_with_failure_log(
                    quiet=quiet,
                    log=log,
                    failure_message=f"{task.id} {task_type}: iterate worker spawn failed",
                    dedupe_key=f"spawn-iterate-failed:{task.id}",
                    spawn_fn=lambda: _spawn_background_iterate(
                        iterate_args,
                        config,
                        task,
                        prepared_task_id=str(prepared_pending_task.id),
                        prepared_resume=pending_recovery_mode == "resume",
                        prepared_phase="preloop",
                        startup_quiet=True,
                    ),
                )
                _release_watch_reserved_task(str(prepared_pending_task.id))
                if rc != 0:
                    continue
                refreshed_pending_task = store.get(str(task.id))
                no_progress_attention = _maybe_finalize_watch_no_progress_for_background_action(
                    config=config,
                    store=store,
                    subject_task=task,
                    action=pending_action,
                    action_task_before=task,
                    action_task_after=refreshed_pending_task,
                    failed_task=None,
                    no_progress_cycles=config.watch.no_progress_cycles,
                )
                consume_pending_slot()
                work_done = True
                started_task_ids.add(str(task.id))
                started_prompt = _format_prompt_for_width(
                    task.prompt,
                    prefix=16 + len(f"{task.id} {task_type} \""),
                    suffix=len('"'),
                )
                log.emit("START", f"{task.id} {task_type} \"{started_prompt}\"")
                if no_progress_attention is not None:
                    log.emit_attention(
                        attention_key=f"pending-attention:{task.id}:{pending_action['type']}:watch-no-progress",
                        message=_watch_needs_attention_message(task, no_progress_attention),
                    )
                continue

            if dry_run:
                dry_run_prompt = _format_prompt_for_width(
                    task.prompt,
                    prefix=16 + len(f"{task.id} {task_type} \""),
                    suffix=len('" [dry-run]'),
                )
                log.emit("START", f"{task.id} {task_type} \"{dry_run_prompt}\" [dry-run]")
                started_task_ids.add(str(task.id))
                consume_pending_slot()
                work_done = True
                continue
            if _maybe_emit_active_watch_recovery_backoff(
                store=store,
                log=log,
                subject_task=task,
                action=pending_action,
            ):
                continue
            no_progress_attention = _maybe_park_watch_no_progress(
                config=config,
                store=store,
                subject_task=task,
                action=pending_action,
                action_task=task,
                failed_task=None,
                no_progress_cycles=config.watch.no_progress_cycles,
            )
            if _watch_no_progress_result_deferred_for_transient_backoff(no_progress_attention):
                _maybe_emit_active_watch_recovery_backoff(
                    store=store,
                    log=log,
                    subject_task=task,
                    action=pending_action,
                )
                continue
            if no_progress_attention is not None:
                log.emit_attention(
                    attention_key=f"pending-attention:{task.id}:{pending_action['type']}:watch-no-progress",
                    message=_watch_needs_attention_message(task, no_progress_attention),
                )
                continue
            worker_args = argparse.Namespace(no_docker=False, max_turns=None, resume=False)
            rc = _spawn_worker_with_failure_log(
                quiet=quiet,
                log=log,
                failure_message=f"{task.id} {task_type}: worker spawn failed",
                dedupe_key=f"spawn-worker-failed:{task.id}",
                spawn_fn=lambda: _spawn_background_worker(
                    worker_args,
                    config,
                    task_id=task.id,
                    quiet=quiet,
                    startup_quiet=True,
                ),
            )
            if rc != 0:
                continue
            refreshed_pending_task = store.get(str(task.id))
            no_progress_attention = _maybe_finalize_watch_no_progress_for_background_action(
                config=config,
                store=store,
                subject_task=task,
                action=pending_action,
                action_task_before=task,
                action_task_after=refreshed_pending_task,
                failed_task=None,
                no_progress_cycles=config.watch.no_progress_cycles,
            )
            consume_pending_slot()
            work_done = True
            started_task_ids.add(str(task.id))
            started_prompt = _format_prompt_for_width(
                task.prompt,
                prefix=16 + len(f"{task.id} {task_type} \""),
                suffix=len('"'),
            )
            log.emit("START", f"{task.id} {task_type} \"{started_prompt}\"")
            if no_progress_attention is not None:
                log.emit_attention(
                    attention_key=f"pending-attention:{task.id}:{pending_action['type']}:watch-no-progress",
                    message=_watch_needs_attention_message(task, no_progress_attention),
                )

    pending_count = len(_pending_runnable_tasks(store, tags=tags, any_tag=any_tag))
    _emit_cycle_attention_summary(log)
    log.end_cycle()
    return _CycleResult(
        work_done=work_done,
        running=_count_live_workers(config, store),
        pending=pending_count,
    )


def _has_explicit_max_concurrent(config: Config) -> bool:
    return "max_concurrent" in config.source_map


def _system_can_run_tasks(config: Config) -> bool:
    if not config.use_docker:
        return True
    return wait_for_docker_ready(config.docker_startup_timeout)


def cmd_watch(args: argparse.Namespace) -> int:
    """Run continuous scheduler loop that maintains N concurrent workers."""
    config = Config.load(args.project_dir)

    batch = args.batch if args.batch is not None else config.watch.batch
    poll = args.poll if args.poll is not None else config.watch.poll
    max_idle = args.max_idle if args.max_idle is not None else config.watch.max_idle
    max_iterations = (
        args.max_iterations if args.max_iterations is not None else config.watch.max_iterations
    )
    recovery_mode = getattr(args, "recovery_mode", None)
    if recovery_mode is None and getattr(args, "restart_failed", False):
        recovery_mode = "recovery-only"
    auto_restart_on_drift = bool(getattr(args, "auto_restart_on_drift", True))
    recovery_slots_arg = getattr(args, "recovery_slots", None)
    if recovery_slots_arg is None:
        recovery_slots_arg = getattr(args, "restart_failed_batch", None)
    recovery_slots = recovery_slots_arg if recovery_slots_arg is not None else config.watch.recovery_slots
    max_recovery_attempts = (
        args.max_resume_attempts
        if getattr(args, "max_resume_attempts", None) is not None
        else config.max_resume_attempts
    )
    dry_run = bool(getattr(args, "dry_run", False))
    show_skipped = bool(getattr(args, "show_skipped", False))
    quiet = bool(getattr(args, "quiet", False))
    try:
        tag_filters, any_tag = parse_cli_tag_filters(args)
    except ValueError as exc:
        print(f"Error: {exc}")
        return 1

    if batch < 1:
        print("Error: --batch must be a positive integer")
        return 1
    if poll < 1:
        print("Error: --poll must be a positive integer")
        return 1
    if max_idle is not None and max_idle < 1:
        print("Error: --max-idle must be a positive integer")
        return 1
    if max_iterations < 1:
        print("Error: --max-iterations must be a positive integer")
        return 1
    if recovery_slots < 0:
        print("Error: --recovery-slots must be a non-negative integer")
        return 1
    if recovery_mode == "recovery-only":
        recovery_slots = batch
    elif recovery_mode == "pending-only":
        recovery_slots = 0
    if max_recovery_attempts < 0:
        print("Error: --max-resume-attempts must be non-negative")
        return 1
    if config.watch.failure_backoff_initial < 1:
        print("Error: watch.failure_backoff_initial must be a positive integer")
        return 1
    if config.watch.failure_backoff_max < config.watch.failure_backoff_initial:
        print("Error: watch.failure_backoff_max must be >= watch.failure_backoff_initial")
        return 1
    if config.watch.failure_halt_after is not None and config.watch.failure_halt_after < 1:
        print("Error: watch.failure_halt_after must be null or a positive integer")
        return 1

    startup_cap_warning: str | None = None
    if not _has_explicit_max_concurrent(config):
        config = replace(config, max_concurrent=batch)
    elif batch > config.max_concurrent:
        source = config.source_map.get("max_concurrent")
        startup_cap_warning = (
            f"requested batch={batch} capped to {config.max_concurrent} by max_concurrent"
            + (f" from {source}" if source else "")
        )

    store = get_store(config)
    log = _WatchLog(config.project_dir / ".gza" / "watch.log", quiet=quiet)
    if startup_cap_warning is not None:
        log.emit("WARN", startup_cap_warning)
    installed_package_drift = _InstalledPackageDriftState(
        startup_fingerprint=_installed_gza_package_fingerprint()
    )
    stop_requested = False
    stop_signal: int | None = None
    sigint_count = 0

    def _handle_shutdown(_signum: int, _frame: object) -> None:
        nonlocal stop_requested, stop_signal, sigint_count
        if _signum == signal.SIGINT:
            sigint_count += 1
            if sigint_count >= 2:
                raise KeyboardInterrupt
        stop_requested = True
        stop_signal = _signum
        log.emit("INFO", "shutting down (workers left running)")
        if quiet:
            print("shutting down (workers left running)", file=sys.stderr, flush=True)

    old_sigint = signal.signal(signal.SIGINT, _handle_shutdown)
    old_sigterm = signal.signal(signal.SIGTERM, _handle_shutdown)
    reexec_fingerprint: str | None = None

    try:
        idle_seconds = 0
        failure_streak = 0
        previous_snapshot = _task_snapshot(store)
        system_hold_active = False

        # Preview the first watch pass and ask for confirmation before executing
        if recovery_mode == "recovery-only" and dry_run:
            _dry_run_git = Git(config.project_dir)
            _dry_run_target_branch = _dry_run_git.default_branch()
            _emit_recovery_dry_run_report(
                store=store,
                tags=tag_filters,
                any_tag=any_tag,
                max_recovery_attempts=max_recovery_attempts,
                show_skipped=show_skipped,
                git=_dry_run_git,
                target_branch=_dry_run_target_branch,
            )
            return 0

        resumed_reexec = bool(getattr(args, "resumed_reexec", False))
        skip_confirm = dry_run or bool(getattr(args, "yes", False)) or resumed_reexec
        if resumed_reexec:
            log.emit(
                "INFO",
                "auto-resumed after code update (skipping first-pass confirmation)",
            )
        if not skip_confirm:
            preview_result = _run_cycle(
                config=config,
                store=store,
                batch=batch,
                max_iterations=max_iterations,
                dry_run=True,
                quiet=False,
                log=log,
                tags=tag_filters,
                any_tag=any_tag,
                recovery_slots=recovery_slots,
                recovery_mode=recovery_mode,
                max_recovery_attempts=max_recovery_attempts,
                show_skipped=show_skipped,
                auto_restart_on_drift=auto_restart_on_drift,
                installed_package_drift=installed_package_drift,
            )
            if preview_result.work_done:
                try:
                    answer = input("\nProceed? [y/N] ").strip().lower()
                except EOFError:
                    answer = ""
                except KeyboardInterrupt:
                    raise
                if answer not in ("y", "yes"):
                    print("Aborted.")
                    return 0

        while True:
            if stop_requested:
                break

            if not _system_can_run_tasks(config):
                pending_count = len(_pending_runnable_tasks(store, tags=tag_filters, any_tag=any_tag))
                log.emit(
                    "HOLD",
                    (
                        "System unavailable: Docker daemon unreachable - "
                        f"holding queue ({pending_count} pending), nothing started/failed"
                    ),
                )
                system_hold_active = True
                if stop_requested:
                    break
                _sleep_interruptibly(poll, lambda: stop_requested)
                continue

            if system_hold_active:
                log.emit("RESUME", "Docker available again - resuming")
                system_hold_active = False

            pre_cycle_snapshot = _task_snapshot(store)
            _emit_transition_events(
                previous_snapshot,
                pre_cycle_snapshot,
                store=store,
                config=config,
                log=log,
                restart_failed_mode=recovery_mode == "recovery-only",
                max_recovery_attempts=max_recovery_attempts,
            )
            previous_snapshot = pre_cycle_snapshot

            cycle_result = _run_cycle(
                config=config,
                store=store,
                batch=batch,
                max_iterations=max_iterations,
                dry_run=dry_run,
                quiet=quiet,
                log=log,
                tags=tag_filters,
                any_tag=any_tag,
                recovery_slots=recovery_slots,
                recovery_mode=recovery_mode,
                max_recovery_attempts=max_recovery_attempts,
                show_skipped=show_skipped,
                auto_restart_on_drift=auto_restart_on_drift,
                installed_package_drift=installed_package_drift,
            )

            current_snapshot = _task_snapshot(store)
            _emit_transition_events(
                previous_snapshot,
                current_snapshot,
                store=store,
                config=config,
                log=log,
                restart_failed_mode=recovery_mode == "recovery-only",
                max_recovery_attempts=max_recovery_attempts,
            )
            completed_ids = _collect_completed_transition_ids(
                previous_snapshot,
                current_snapshot,
                store=store,
                tags=tag_filters,
                any_tag=any_tag,
            )
            if completed_ids and failure_streak > 0:
                failure_streak = 0
                log.emit(
                    "INFO",
                    f"failure backoff reset after completion(s): {', '.join(completed_ids[:5])}",
                )
            unhandled_failures = _collect_unhandled_failures(
                previous_snapshot,
                current_snapshot,
                store=store,
                max_recovery_attempts=max_recovery_attempts,
                restart_failed_mode=recovery_mode == "recovery-only",
                tags=tag_filters,
                any_tag=any_tag,
            )
            previous_snapshot = current_snapshot

            if _should_reexec_watch(
                auto_restart_on_drift=auto_restart_on_drift,
                dry_run=dry_run,
                stop_requested=stop_requested,
                cycle_result=cycle_result,
                drift_state=installed_package_drift,
            ):
                reexec_fingerprint = installed_package_drift.pending_restart_fingerprint
                assert reexec_fingerprint is not None
                log.emit(
                    "INFO",
                    (
                        "re-execing watch to load updated gza "
                        f"{installed_package_drift.startup_fingerprint}"
                        f"->{reexec_fingerprint}"
                    ),
                )
                break

            if unhandled_failures:
                failure_streak += len(unhandled_failures)
                backoff_seconds = _compute_failure_backoff_seconds(config, failure_streak)
                summary = ", ".join(
                    f"{failure.task_id}={failure.reason}" for failure in unhandled_failures[:3]
                )
                if len(unhandled_failures) > 3:
                    summary += ", ..."
                log.emit(
                    "BACKOFF",
                    (
                        f"{len(unhandled_failures)} non-auto-resumable failure(s); "
                        f"sleeping {backoff_seconds}s before starting more work "
                        f"(streak {failure_streak}"
                        + (f"; latest: {summary}" if summary else "")
                        + ")"
                    ),
                )
                halt_after = config.watch.failure_halt_after
                if halt_after is not None and failure_streak >= halt_after:
                    log.emit(
                        "INFO",
                        (
                            "failure halt threshold reached "
                            f"({failure_streak} consecutive non-auto-resumable failures >= {halt_after}); "
                            "stopping watch for human intervention"
                        ),
                    )
                    break
                if stop_requested:
                    break
                _sleep_interruptibly(backoff_seconds, lambda: stop_requested)
                continue

            if cycle_result.work_done:
                idle_seconds = 0
            log.emit(
                "SLEEP",
                f"sleeping {poll}s ({cycle_result.pending} pending, {cycle_result.running} running)",
            )
            if not cycle_result.work_done:
                idle_seconds += poll
                if max_idle is not None and idle_seconds >= max_idle:
                    log.emit("INFO", f"max idle time reached ({max_idle}s), exiting")
                    break

            if stop_requested:
                break
            _sleep_interruptibly(poll, lambda: stop_requested)
    finally:
        signal.signal(signal.SIGINT, old_sigint)
        signal.signal(signal.SIGTERM, old_sigterm)

    if stop_signal is not None:
        return 128 + stop_signal

    if reexec_fingerprint is not None:
        exec_argv = _watch_reexec_argv(args)
        try:
            os.execv(sys.executable, exec_argv)
        except OSError as exc:
            log.emit("ERROR", f"watch re-exec failed: {exc}")
            if quiet:
                print(f"watch re-exec failed: {exc}", file=sys.stderr, flush=True)
            return 1

    return 0


def cmd_main_verify(args: argparse.Namespace) -> int:
    """Force or inspect the canonical local-target integration verify gate."""
    config = Config.load(args.project_dir)
    store = get_store(config)
    git = Git(config.project_dir)
    verify_git = git
    if config.main_checkout_isolate:
        target_branch = git.default_branch()
        verify_git = ensure_watch_main_checkout(config, git, target_branch)

    check = check_main_integration_verify(
        config,
        store,
        verify_git,
        reason="operator-main-verify",
        force=bool(getattr(args, "force", False)),
        red_reruns=2 if bool(getattr(args, "force", False)) else 0,
    )
    status = check.state.verify_status or "unknown"
    phase = f" phase={check.state.failing_phase}" if check.state.failing_phase else ""
    if check.merges_halted:
        print(check.state.alert_message or f"main verify {status}{phase}; merges halted")
        return 1
    print(
        f"main verify {status}{phase}; merges allowed"
        if check.performed_verify
        else f"main verify {status}{phase}; cached result still current and merges allowed"
    )
    return 0


def cmd_queue(args: argparse.Namespace) -> int:
    """Inspect and adjust pending queue urgency."""
    config = Config.load(args.project_dir)
    store = get_store(config)
    service = TaskQueryService(store)
    action = getattr(args, "queue_action", None)
    full = bool(getattr(args, "full", False))
    try:
        tag_filters, any_tag = parse_cli_tag_filters(args)
    except ValueError as exc:
        print(f"Error: {exc}")
        return 1

    normalized_tag_filters = normalize_tag_filters(tag_filters)

    if action in {"bump", "unbump", "move", "next", "clear"}:
        task_id = resolve_id(config, args.task_id)
        task = store.get(task_id)
        if task is None:
            print(f"Error: Task {task_id} not found")
            return 1
        if task.status != "pending":
            print(f"Error: Task {task_id} is not pending (status: {task.status})")
            return 1
        if task.task_type == "internal":
            print(f"Error: Task {task_id} is internal and not part of the runnable queue")
            return 1

        runnable_pending_ids = {
            str(row.task.id)
            for row in service.run(
                TaskQueryPresets.queue(limit=None, tags=normalized_tag_filters, any_tag=any_tag)
            ).rows
            if isinstance(row, TaskRow) and row.task.id is not None
        }
        is_currently_runnable = str(task_id) in runnable_pending_ids

        if action in {"move", "next", "clear"} and normalized_tag_filters is not None:
            if not task_matches_tag_filters(
                task_tags=task.tags or (),
                tag_filters=normalized_tag_filters,
                any_tag=any_tag,
            ):
                print(_format_queue_scope_error(task_id, normalized_tag_filters, any_tag=any_tag))
                return 1

        if action in {"bump", "unbump"}:
            new_urgent = action == "bump"
            set_task_urgency(store, task_id, urgent=new_urgent)
            if new_urgent:
                if is_currently_runnable:
                    print(f"✓ Bumped task {task_id} to urgent queue")
                else:
                    print(f"✓ Bumped task {task_id} (not currently runnable; urgency will apply once runnable)")
            else:
                if is_currently_runnable:
                    print(f"✓ Removed task {task_id} from urgent queue")
                else:
                    print(f"✓ Removed urgent flag from task {task_id} (task is not currently runnable)")
            return 0

        if action == "clear":
            clear_task_queue_position_scoped(
                store,
                task_id,
                tags=normalized_tag_filters,
                any_tag=any_tag,
            )
            if is_currently_runnable:
                print(f"✓ Cleared explicit queue order for task {task_id}")
            else:
                print(f"✓ Cleared explicit queue order for task {task_id} (task is not currently runnable)")
            return 0

        position = 1 if action == "next" else int(args.position)
        if position < 1:
            print("Error: queue position must be >= 1")
            return 1
        set_task_queue_position_scoped(
            store,
            task_id,
            position=position,
            tags=normalized_tag_filters,
            any_tag=any_tag,
        )
        if position == 1:
            message = f"✓ Moved task {task_id} to queue position 1"
        else:
            message = f"✓ Moved task {task_id} to queue position {position}"
        if is_currently_runnable:
            print(message)
        else:
            print(f"{message} (task is not currently runnable; ordering will apply once runnable)")
        return 0

    recovery_entries: list[RecoveryLaneEntry] = []
    lifecycle_entries: list[LifecycleActionEntry] = []
    scope_gaps: list[ScopedTagScopeGap] = []
    if full:
        queue_git = Git(config.project_dir)
        queue_target_branch = queue_git.default_branch()
        scope_gaps = collect_scoped_tag_scope_gaps(
            store,
            tag_filters=normalized_tag_filters,
            any_tag=any_tag,
            config=config,
            git=queue_git,
            target_branch=queue_target_branch,
        )
        recovery_entries = collect_recovery_lane_entries(
            store,
            tags=normalized_tag_filters,
            any_tag=any_tag,
            max_recovery_attempts=config.max_resume_attempts,
            git=queue_git,
            target_branch=queue_target_branch,
        )
        lifecycle_entries = collect_lifecycle_action_entries(
            store,
            config=config,
            git=queue_git,
            target_branch=queue_target_branch,
            tags=normalized_tag_filters,
            any_tag=any_tag,
            max_recovery_attempts=config.max_resume_attempts,
            persist_post_merge_rebase_state=False,
        )
    queue_rows = [
        row
        for row in service.run(
            TaskQueryPresets.queue_listing(limit=None, tags=normalized_tag_filters, any_tag=any_tag)
        ).rows
        if isinstance(row, TaskRow)
    ]
    runnable_pending = [row.task for row in queue_rows if not bool(row.values.get("blocked"))]
    blocked_pending = [row for row in queue_rows if bool(row.values.get("blocked"))]
    if not runnable_pending and not blocked_pending and not recovery_entries and not lifecycle_entries:
        if tag_filters:
            print(f"No pending tasks matching tags: {', '.join(tag_filters)}")
            for gap in scope_gaps:
                print(f"Scope gap: {_format_scope_gap_message(gap, tags=normalized_tag_filters, any_tag=any_tag)}")
        else:
            print("No pending tasks")
        return 0

    limit_arg = getattr(args, "limit", 10)
    show_all = bool(getattr(args, "all", False)) or limit_arg in {0, -1}
    display_limit = None if show_all else max(1, int(limit_arg))
    visible_runnable = runnable_pending if display_limit is None else runnable_pending[:display_limit]
    rendered_rows = [
        QueueRenderRow(task=task, position_text=str(index))
        for index, task in enumerate(visible_runnable, 1)
    ]

    def _blocked_by_text(row: TaskRow) -> str:
        task = row.task
        operator_label = blocked_dependency_label(store, task)
        if operator_label is not None:
            return operator_label
        blocking_id = row.values.get("blocking_id")
        merge_state = row.values.get("blocking_merge_state")
        merge_owner = row.values.get("blocking_merge_owner_id")
        source_branch = row.values.get("blocking_source_branch")
        target_branch = row.values.get("blocking_target_branch")
        if isinstance(merge_state, str) and merge_state:
            detail = f"blocked by dependency {blocking_id or task.depends_on or 'unknown'} merge unit {merge_state}"
            if isinstance(merge_owner, str) and merge_owner and merge_owner != blocking_id:
                detail += f" owned by {merge_owner}"
            if isinstance(source_branch, str) and source_branch:
                detail += f" on {source_branch}"
            if isinstance(target_branch, str) and target_branch:
                detail += f" -> {target_branch}"
            return detail
        blocking = str(blocking_id) if isinstance(blocking_id, str) and blocking_id else (
            _precondition_blocking_dependency_id(task, config) or task.depends_on
        )
        return f"blocked by {blocking}" if blocking else "blocked by dependency"

    rendered_rows.extend(
        QueueRenderRow(
            task=row.task,
            position_text="-",
            blocked=True,
            blocked_by_text=_blocked_by_text(row),
        )
        for row in blocked_pending
    )
    widths = queue_render_widths(rendered_rows)

    if full:
        console.print(
            build_queue_summary(
                "Recovery lane: `advance` / `watch` only. Evaluated ahead of pending pickup."
            )
        )
        if recovery_entries:
            for entry in recovery_entries:
                console.print(_format_queue_recovery_lane_detail(entry))
        else:
            console.print("No recovery candidates")

        console.print()
        console.print(
            build_queue_summary(
                "Lifecycle actions: `advance` / `watch` lifecycle work visible ahead of pending pickup."
            )
        )
        if lifecycle_entries:
            print_lifecycle_action_entries(console, lifecycle_entries)
        else:
            console.print("No lifecycle actions")

        console.print()
    console.print(
        build_queue_summary(
            "Pending lane: `gza queue` preview only. `gza work` / `watch` start from this lane."
        )
    )
    if not runnable_pending and not blocked_pending:
        console.print("No pending tasks")
        if full:
            _print_queue_scope_gaps(
                gaps=scope_gaps,
                tags=normalized_tag_filters,
                any_tag=any_tag,
            )
        return 0
    print_queue_rows(
        console,
        [row for row in rendered_rows if not row.blocked],
        widths=widths,
    )

    if display_limit is not None and len(runnable_pending) > display_limit:
        remaining = len(runnable_pending) - display_limit
        plural = "tasks" if remaining != 1 else "task"
        console.print(build_queue_summary(
            f"({remaining} more runnable {plural}; use -n 0, -n -1, or --all to show everything)"
        ))

    print_queue_rows(
        console,
        [row for row in rendered_rows if row.blocked],
        widths=widths,
    )
    if full:
        _print_queue_scope_gaps(
            gaps=scope_gaps,
            tags=normalized_tag_filters,
            any_tag=any_tag,
        )

    return 0


def _format_queue_recovery_lane_detail(entry: RecoveryLaneEntry) -> str:
    if entry.attention_action is not None:
        return format_needs_attention_entry_for_display(entry.task, action=entry.attention_action)
    decision = entry.decision
    return (
        f"{decision.action:<6} {entry.task.id} [{entry.task.task_type}] "
        f"{shorten_prompt(entry.task.prompt, prompt_available_width(prefix=32, suffix=0))} "
        f"via {decision.launch_mode} reason={decision.reason_code} "
        f"attempt={decision.attempt_index}/{decision.attempt_limit}"
    )
