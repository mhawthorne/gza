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
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import Any, Literal, TypeVar, cast

from rich.text import Text

from .. import colors as _colors, lineage
from ..advance_engine import (
    _resolve_and_persist_post_merge_rebase_state,
    _resolve_current_merge_source,
)
from ..canonical_checkout import (
    CANONICAL_CHECKOUT_ATTENTION_REASON,
    check_canonical_checkout_invariant,
)
from ..concurrency import (
    ConcurrencySnapshot,
    LaunchPermit,
    MaxConcurrentTasksError,
    _collect_live_running_state as _shared_collect_live_running_state,
    get_concurrency_snapshot as _shared_get_concurrency_snapshot,
    launch_permit,
    release_task_launch_permit,
    reserve_task_launch_permit,
)
from ..config import Config
from ..console import console, prompt_available_width, shorten_prompt
from ..db import (
    MERGE_SOURCE_WATCH,
    DuplicateActiveChildError,
    SqliteTaskStore,
    Task as DbTask,
    WatchProgressObservation,
    WatchRecoveryBackoff,
    active_merge_unit_where_sql,
    task_id_numeric_key,
)
from ..dispatch_preview import (
    DispatchPreviewEntry,
    DispatchSelectionMode,
    build_dispatch_preview,
    normalize_dispatch_selection_mode,
    plan_watch_dispatch_entries,
)
from ..git import Git, GitError, resolve_ref_if_possible
from ..git_health import GIT_HEALTH_PROMPT, GIT_HEALTH_REASON, check_git_health
from ..lifecycle_completion import (
    merge_state_is_terminal_for_lifecycle,
    task_is_complete_for_lifecycle,
)
from ..lineage_query import (
    LineageOwnerQuery,
    LineageOwnerRow,
    query_lineage_owner_rows_in_read_session,
    resolve_lineage_owner_task_id,
)
from ..main_integration_verify import (
    MAIN_INTEGRATION_VERIFY_REASON,
    MAIN_INTEGRATION_VERIFY_REMEDIATION_TRIGGER_SOURCE,
    MAIN_INTEGRATION_VERIFY_TAG,
    MainIntegrationVerifyCheck,
    MainIntegrationVerifyRemediation,
    MainIntegrationVerifyState,
    check_main_integration_verify,
    persist_main_integration_verify_alert_message,
)
from ..merge_state import (
    effective_no_work_merge_state,
    resolve_task_merge_state_for_target,
)
from ..operator_state import blocked_dependency_label
from ..pickup import (
    effective_edit_time,
    get_runnable_pending_tasks,
    is_in_quiet_period,
    is_worker_consuming_advance_action,
)
from ..providers.base import wait_for_docker_ready
from ..query import resolve_lineage_owner_task
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
from ..status_ops import apply_manual_task_status
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
from ..unstick import (
    ParkedTaskCandidate,
    clear_parked_candidate_state,
    discover_parked_tasks,
    skip_reason_for_landed_or_moot,
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
from . import _lifecycle_actions as _shared_lifecycle_actions
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
    _repair_plan_review_slice_materialization,
    _spawn_background_resume_worker,
    _spawn_background_worker,
    clear_task_queue_position_scoped,
    format_duplicate_active_child_message,
    format_duplicate_rebase_message,
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
    reproject_selected_merge_actions,
)
from ._queue_render import (
    QueueRenderRow,
    build_queue_summary,
    format_quiet_available_at,
    partition_queue_rows,
    print_queue_rows,
    queue_render_widths,
)
from ._recovery_lane import RecoveryLaneEntry, collect_recovery_lane_entries
from .advance_engine import (
    NEEDS_ATTENTION_LABEL,
    WATCH_SURFACE_ONCE_NEEDS_ATTENTION_REASONS,
    build_needs_attention_entry_for_display,
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
    {*WATCH_SURFACE_ONCE_NEEDS_ATTENTION_REASONS, WATCH_NO_PROGRESS_BACKSTOP_REASON}
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
    return tuple(dict.fromkeys(("system", MAIN_INTEGRATION_VERIFY_TAG, *scope_tags)))


def _merge_main_verify_remediation_tags(
    existing_tags: Sequence[tuple[str, ...] | None],
    scope_tags: tuple[str, ...] | None,
) -> tuple[str, ...]:
    merged = list(_main_verify_remediation_tags(scope_tags))
    for tag_set in existing_tags:
        for tag in tag_set or ():
            if tag not in merged:
                merged.append(tag)
    return tuple(merged)


@dataclass(frozen=True)
class _MainVerifyRemediationIdentity:
    signature: str
    tree_fingerprint: str | None


@dataclass(frozen=True)
class _MainVerifyRemediationEnsureResult:
    task: DbTask | None
    outcome: Literal["created", "reused", "exhausted"]


@dataclass(frozen=True)
class _MainVerifyRemediationSelection:
    canonical: DbTask | None
    duplicates: tuple[DbTask, ...] = ()


MAIN_VERIFY_REMEDIATION_DUPLICATE_DROP_REASON = "superseded_same_signature_remediation"


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
    failure = getattr(remediation, "failure", None)
    artifact_path = getattr(remediation, "artifact_path", None)
    failing_test_ids = tuple(getattr(remediation, "failing_test_ids", ()))
    verify_excerpt = getattr(remediation, "verify_excerpt", None)
    if failure:
        body.append(f"Verify failure: {failure}")
    if artifact_path:
        body.append(f"Verify artifact: {artifact_path}")
    if failing_test_ids:
        body.append(f"Failing test IDs: {', '.join(failing_test_ids)}")
    if verify_excerpt:
        body.extend(["Verify excerpt:", "", *_render_inert_prompt_excerpt(verify_excerpt)])
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


def _render_inert_prompt_excerpt(excerpt: str) -> list[str]:
    # Indent every line so excerpt content cannot terminate a fence and escape
    # into live prompt instructions.
    return [f"    {line}" if line else "    " for line in excerpt.splitlines()]


def _main_verify_remediation_identity(
    remediation: MainIntegrationVerifyRemediation,
) -> _MainVerifyRemediationIdentity:
    return _MainVerifyRemediationIdentity(
        signature=remediation.signature,
        tree_fingerprint=remediation.tree_fingerprint,
    )


def _main_verify_remediation_identity_from_prompt(
    prompt: str,
) -> _MainVerifyRemediationIdentity | None:
    signature = _main_verify_remediation_signature_from_prompt(prompt)
    if signature is None:
        return None
    return _MainVerifyRemediationIdentity(
        signature=signature,
        tree_fingerprint=_main_verify_remediation_tree_fingerprint_from_prompt(prompt),
    )


def _main_verify_remediation_identity_matches(
    existing: _MainVerifyRemediationIdentity,
    requested: _MainVerifyRemediationIdentity,
) -> bool:
    return existing.signature == requested.signature


def _main_verify_remediation_ledger_fingerprint(_tree_fingerprint: str | None) -> str | None:
    # Remediation ownership is signature-only. Tree fingerprint remains prompt
    # context and freshness evidence, but it no longer keys watch's durable reuse
    # ledger.
    return None


def _main_verify_remediation_is_still_unmerged(
    store: SqliteTaskStore,
    task: DbTask,
) -> bool:
    if task.id is None:
        return task.merge_status != "merged"
    merge_unit = store.resolve_merge_unit_for_task(task.id)
    if merge_unit is not None:
        return effective_no_work_merge_state(task, merge_unit.state) not in {"merged", "empty", "redundant"}
    return task.merge_status != "merged"


def _main_verify_remediation_task_is_reusable(
    store: SqliteTaskStore,
    task: DbTask,
) -> bool:
    if task.task_type != "implement":
        return False
    if task.trigger_source != MAIN_INTEGRATION_VERIFY_REMEDIATION_TRIGGER_SOURCE:
        return False
    if task.status in {"pending", "in_progress", "failed"}:
        return True
    if task.status in {"completed", "unmerged"}:
        return _main_verify_remediation_is_still_unmerged(store, task)
    return False


def _legacy_main_verify_remediation_rank(
    existing: _MainVerifyRemediationIdentity,
    requested: _MainVerifyRemediationIdentity,
) -> int:
    if requested.tree_fingerprint is not None and existing.tree_fingerprint == requested.tree_fingerprint:
        return 3
    if requested.tree_fingerprint is None and existing.tree_fingerprint is not None:
        return 2
    if existing.tree_fingerprint is None:
        return 1
    return 0


def _main_verify_remediation_selection_rank(
    task: DbTask,
    existing: _MainVerifyRemediationIdentity,
    requested: _MainVerifyRemediationIdentity,
) -> tuple[int, int]:
    return (1 if task.status == "in_progress" else 0, _legacy_main_verify_remediation_rank(existing, requested))


def _select_legacy_open_main_verify_remediation_tasks(
    store: SqliteTaskStore,
    *,
    identity: _MainVerifyRemediationIdentity,
) -> _MainVerifyRemediationSelection:
    preferred: DbTask | None = None
    preferred_rank = (-1, -1)
    matches: list[DbTask] = []
    for task in store.get_all():
        if not _main_verify_remediation_task_is_reusable(store, task):
            continue
        existing_identity = _main_verify_remediation_identity_from_prompt(task.prompt)
        if existing_identity is None:
            continue
        if not _main_verify_remediation_identity_matches(existing_identity, identity):
            continue
        matches.append(task)
        rank = _main_verify_remediation_selection_rank(task, existing_identity, identity)
        if rank > preferred_rank:
            preferred = task
            preferred_rank = rank
    if preferred is None:
        return _MainVerifyRemediationSelection(canonical=None)
    duplicates = tuple(task for task in matches if task.id != preferred.id)
    return _MainVerifyRemediationSelection(canonical=preferred, duplicates=duplicates)


def _retire_duplicate_main_verify_remediation_tasks(
    *,
    store: SqliteTaskStore,
    identity: _MainVerifyRemediationIdentity,
    canonical: DbTask,
    duplicates: Sequence[DbTask],
) -> tuple[str, ...]:
    merged_tags = _merge_main_verify_remediation_tags(
        [tuple(task.tags or ()) for task in (canonical, *duplicates)],
        scope_tags=None,
    )
    if canonical.id is None:
        return merged_tags
    for duplicate in duplicates:
        if duplicate.id is None or duplicate.id == canonical.id:
            continue
        fresh = store.get(duplicate.id)
        if fresh is None or not _main_verify_remediation_task_is_reusable(store, fresh):
            continue
        fresh_identity = _main_verify_remediation_identity_from_prompt(fresh.prompt)
        if fresh_identity is None or not _main_verify_remediation_identity_matches(fresh_identity, identity):
            continue
        if fresh.status == "in_progress":
            # Preserve live worker evidence until an existing worker-aware
            # reconciliation path proves the task can be stopped or retired.
            continue
        fresh.status = "dropped"
        fresh.started_at = None
        fresh.running_pid = None
        fresh.completed_at = datetime.now(UTC)
        fresh.failure_reason = None
        fresh.completion_reason = None
        fresh.drop_reason = (
            f"{MAIN_VERIFY_REMEDIATION_DUPLICATE_DROP_REASON}:{identity.signature}:{canonical.id}"
        )
        fresh.urgent = False
        fresh.queue_position = None
        store.update(fresh)
    return merged_tags


def _find_open_main_verify_remediation_tasks(
    store: SqliteTaskStore,
    *,
    signature: str,
    tree_fingerprint: str | None,
) -> _MainVerifyRemediationSelection:
    identity = _MainVerifyRemediationIdentity(
        signature=signature,
        tree_fingerprint=tree_fingerprint,
    )
    ledger_fingerprint = _main_verify_remediation_ledger_fingerprint(identity.tree_fingerprint)
    selection = _select_legacy_open_main_verify_remediation_tasks(store, identity=identity)
    if selection.canonical is not None and selection.canonical.id is not None:
        store.record_main_verify_remediation_active_task(
            signature=identity.signature,
            tree_fingerprint=ledger_fingerprint,
            task_id=selection.canonical.id,
        )
    return selection


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
    config: Config,
    store: SqliteTaskStore,
    remediation: MainIntegrationVerifyRemediation,
    state,
    tags: tuple[str, ...] | None,
    any_tag: bool,
) -> _MainVerifyRemediationEnsureResult:
    identity = _main_verify_remediation_identity(remediation)
    selection = _find_open_main_verify_remediation_tasks(
        store,
        signature=identity.signature,
        tree_fingerprint=identity.tree_fingerprint,
    )
    existing = selection.canonical
    ledger_fingerprint = _main_verify_remediation_ledger_fingerprint(identity.tree_fingerprint)
    if existing is not None:
        merged_legacy_tags = _retire_duplicate_main_verify_remediation_tasks(
            store=store,
            identity=identity,
            canonical=existing,
            duplicates=selection.duplicates,
        )
        desired_prompt = _main_verify_remediation_prompt(remediation, head_sha=state.head_sha)
        desired_tags = _merge_main_verify_remediation_tags([merged_legacy_tags], tags)
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
        if existing.id is not None:
            store.record_main_verify_remediation_active_task(
                signature=identity.signature,
                tree_fingerprint=ledger_fingerprint,
                task_id=existing.id,
                last_observed_head_sha=state.head_sha,
                last_observed_failure=remediation.failure,
            )
        return _MainVerifyRemediationEnsureResult(task=existing, outcome="reused")

    attempt_state = store.get_main_verify_remediation_attempt_state(
        signature=identity.signature,
        tree_fingerprint=ledger_fingerprint,
    )
    if (
        attempt_state is not None
        and attempt_state.consumed_attempt_count >= config.watch.main_verify_remediation_max_attempts
    ):
        store.mark_main_verify_remediation_exhausted(
            signature=identity.signature,
            tree_fingerprint=ledger_fingerprint,
            last_observed_head_sha=state.head_sha,
            last_observed_failure=remediation.failure,
        )
        return _MainVerifyRemediationEnsureResult(task=None, outcome="exhausted")

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
    if task.id is not None:
        store.record_main_verify_remediation_active_task(
            signature=identity.signature,
            tree_fingerprint=ledger_fingerprint,
            task_id=task.id,
            last_observed_head_sha=state.head_sha,
            last_observed_failure=remediation.failure,
        )
    return _MainVerifyRemediationEnsureResult(task=task, outcome="created")


def _maybe_file_main_verify_remediation(
    *,
    dry_run: bool,
    config: Config,
    store: SqliteTaskStore,
    tags: tuple[str, ...] | None,
    any_tag: bool,
    log: "_WatchLog",
    check: MainIntegrationVerifyCheck,
) -> MainIntegrationVerifyState | None:
    remediation = getattr(check, "remediation", None)
    if remediation is None or dry_run:
        return None
    result = _ensure_main_verify_remediation_task(
        config=config,
        store=store,
        remediation=remediation,
        state=check.state,
        tags=tags,
        any_tag=any_tag,
    )
    phase = remediation.failing_phase or remediation.signature
    fingerprint_label = remediation.tree_fingerprint or "unavailable"
    if result.outcome == "exhausted":
        attempts = config.watch.main_verify_remediation_max_attempts
        base_message = check.state.alert_message or "main verify is red; merges halted"
        message = (
            f"{base_message}; automatic remediation exhausted after {attempts}/{attempts} "
            f"attempts for {phase} on {fingerprint_label}; human intervention required"
        )
        persisted_state = persist_main_integration_verify_alert_message(
            store,
            state=check.state,
            alert_message=message,
        )
        _emit_main_verify_attention(log=log, state=persisted_state, now=datetime.now(UTC))
        log.emit(
            "REMEDY",
            f"automatic remediation exhausted after {attempts}/{attempts} attempts for "
            f"{phase} on {fingerprint_label}",
        )
        return persisted_state
    assert result.task is not None
    log.emit(
        "REMEDY",
        f"{result.task.id}: {result.outcome} {remediation.kind} remediation for {phase}; moved to queue position 1",
    )
    return None


def _resolve_merged_main_verify_remediation_task(
    *,
    store: SqliteTaskStore,
    task: DbTask,
    display_task: DbTask,
) -> DbTask | None:
    candidate_ids: list[str] = []

    def add_candidate(candidate: DbTask | None) -> None:
        if candidate is None or candidate.id is None or candidate.id in candidate_ids:
            return
        candidate_ids.append(candidate.id)

    add_candidate(task)
    add_candidate(display_task)
    for subject in (task, display_task):
        if subject.id is None:
            continue
        unit = store.resolve_merge_unit_for_task(subject.id)
        if unit is None:
            continue
        add_candidate(store.resolve_merge_unit_owner_task(unit))
        for attached in store.list_tasks_for_merge_unit(unit.id):
            add_candidate(attached)

    for task_id in candidate_ids:
        candidate = store.get(task_id)
        if candidate is None:
            continue
        if candidate.trigger_source == MAIN_INTEGRATION_VERIFY_REMEDIATION_TRIGGER_SOURCE:
            return candidate
    return None


def _handle_post_merge_main_verify_remediation_verdict(
    *,
    config: Config,
    store: SqliteTaskStore,
    log: "_WatchLog",
    task: DbTask,
    display_task: DbTask,
    check: MainIntegrationVerifyCheck,
) -> None:
    remediation_task = _resolve_merged_main_verify_remediation_task(
        store=store,
        task=task,
        display_task=display_task,
    )
    if remediation_task is None or remediation_task.id is None:
        return

    merged_identity = _main_verify_remediation_identity_from_prompt(remediation_task.prompt)
    if merged_identity is None:
        return

    current_remediation = getattr(check, "remediation", None)
    current_identity = (
        _main_verify_remediation_identity(current_remediation) if current_remediation is not None else None
    )
    same_identity = current_identity is not None and _main_verify_remediation_identity_matches(
        merged_identity,
        current_identity,
    )
    head_sha = getattr(check.state, "head_sha", None)

    if check.merges_halted and current_remediation is not None and same_identity:
        apply_manual_task_status(
            config=config,
            store=store,
            task=remediation_task,
            status="dropped",
            reason="main verify remained red after merged remediation; attempt consumed",
        )
        attempt_state = store.record_main_verify_remediation_consumed_attempt(
            signature=merged_identity.signature,
            tree_fingerprint=_main_verify_remediation_ledger_fingerprint(merged_identity.tree_fingerprint),
            task_id=remediation_task.id,
            last_observed_head_sha=head_sha,
            last_observed_failure=current_remediation.failure,
        )
        consumed_count = attempt_state.consumed_attempt_count if attempt_state is not None else 0
        attempt_budget = config.watch.main_verify_remediation_max_attempts
        phase = current_remediation.failing_phase or current_remediation.signature
        fingerprint_label = current_remediation.tree_fingerprint or "unavailable"
        log.emit(
            "REMEDY",
            f"{remediation_task.id}: post-merge verify still red for {phase} on {fingerprint_label}; "
            f"dropped remediation attempt {consumed_count}/{attempt_budget}",
        )
        return

    clear_detail = None
    if not check.merges_halted:
        clear_detail = "post-merge verify green; cleared active remediation state"
    elif current_remediation is not None:
        clear_detail = "post-merge verify red for a different remediation identity; cleared active state"
    else:
        clear_detail = "post-merge verify red without remediation identity; cleared active state"

    store.clear_main_verify_remediation_active_task(
        signature=merged_identity.signature,
        tree_fingerprint=_main_verify_remediation_ledger_fingerprint(merged_identity.tree_fingerprint),
        last_observed_head_sha=head_sha,
        last_observed_failure=current_remediation.failure if current_remediation is not None else None,
    )
    log.emit("REMEDY", f"{remediation_task.id}: {clear_detail}")


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
    fallback_task = (
        _resolve_watch_iterate_impl_for_task(store, row.lifecycle_action_task)
        if row.lifecycle_action_task is not None
        else None
    ) or row.owner_task or _resolve_incomplete_owner_task(store, cast(Any, row))
    return resolve_subject_task(
        store,
        action,
        row,
        fallback_task=fallback_task,
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
    owner_task_ids: tuple[str, ...] | None = None,
    task_ids: tuple[str, ...] | None = None,
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
            owner_task_ids=owner_task_ids,
            task_ids=task_ids,
        ),
        config=config,
        git=git,
        target_branch=target_branch,
    )
    return list(rows), read_context


def _query_owner_rows(
    *,
    store: SqliteTaskStore,
    config: Config | None = None,
    git: Git | None = None,
    target_branch: str | None = None,
    tags: tuple[str, ...] | None = None,
    any_tag: bool = False,
    owner_task_ids: tuple[str, ...] | None = None,
    task_ids: tuple[str, ...] | None = None,
    max_recovery_attempts: int,
    include_skipped: bool,
) -> list[LineageOwnerRow]:
    rows, _ = _query_owner_rows_with_context(
        store=store,
        config=config,
        git=git,
        target_branch=target_branch,
        tags=tags,
        any_tag=any_tag,
        owner_task_ids=owner_task_ids,
        task_ids=task_ids,
        max_recovery_attempts=max_recovery_attempts,
        include_skipped=include_skipped,
    )
    return rows


def _resolve_watch_scope_owner_ids(
    store: SqliteTaskStore,
    task_ids: tuple[str, ...],
    *,
    max_recovery_attempts: int,
    config: Config | None = None,
    git: Git | None = None,
    target_branch: str | None = None,
) -> tuple[str, ...]:
    """Resolve explicit watch task IDs to canonical owner IDs, preserving order."""
    owner_ids: list[str] = []
    seen: set[str] = set()
    for raw_task_id in task_ids:
        if not _TASK_ID_RE.match(raw_task_id):
            raise ValueError(f"invalid task ID: {raw_task_id}")
        task = store.get(raw_task_id)
        if task is None:
            raise ValueError(f"unknown task ID: {raw_task_id}")

        rows = _query_owner_rows(
            store=store,
            config=config,
            git=git,
            target_branch=target_branch,
            max_recovery_attempts=max_recovery_attempts,
            include_skipped=True,
            task_ids=(raw_task_id,),
        )
        if rows:
            owner_task = _resolve_incomplete_owner_task(store, cast(Any, rows[0]))
        else:
            owner_task = resolve_lineage_owner_task(store, task)
        if owner_task.id is None:
            raise ValueError(f"unable to resolve owner for task ID: {raw_task_id}")
        if owner_task.id in seen:
            continue
        seen.add(owner_task.id)
        owner_ids.append(owner_task.id)
    return tuple(owner_ids)
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
                message=(f"review task {review_task.id} points to non-implementation task {review_task.depends_on}"),
                guarded_pending_task_id=guarded_pending_task_id,
            )
        anchor_impl = impl_task if task.id == review_task.id else _resolve_watch_iterate_impl_for_task(store, task)
        if anchor_impl is None or anchor_impl.id != impl_task.id:
            return _watch_iterate_result(
                action_type=action_type,
                status="skip",
                message=(f"review task {review_task.id} resolves to {impl_task.id}, not completed task {task.id}"),
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
                message=(f"improve task {improve_task.id} resolves to {impl_task.id}, not completed task {task.id}"),
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
    if config is not None and _watch_task_has_live_registered_worker(config, observation.action_task_id):
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
    if (
        config is not None
        and action_task_after is not None
        and action_task_after.id is not None
        and _watch_task_has_live_registered_worker(config, str(action_task_after.id))
    ):
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
        and (failed_improve is None or failed_improve.id is None or backoff.last_failure_task_id == failed_improve.id)
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
    next_retry_key = backoff.next_retry_at.astimezone(UTC).isoformat() if backoff.next_retry_at is not None else "none"
    log.emit(
        "BACKOFF",
        (
            f"{log_subject_task.id} {action_label} delayed {remaining_seconds}s after transient failure "
            f"(last={last_failure_task_id} {last_failure_reason})"
        ),
        dedupe_key=(
            "transient-recovery-backoff:"
            f"{candidate.subject_kind}:{candidate.subject_id}:{candidate.action_type}:{candidate.action_reason}:"
            f"{last_failure_task_id}:{next_retry_key}"
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
            "at the next cycle boundary to load new code"
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
    drift_state: _InstalledPackageDriftState | None,
) -> bool:
    if not auto_restart_on_drift or dry_run or stop_requested or drift_state is None:
        return False
    if drift_state.pending_restart_fingerprint is None:
        return False
    return True


def _watch_reexec_argv(args: argparse.Namespace) -> list[str]:
    argv = [sys.executable, "-m", "gza", "watch", "--project", str(args.project_dir)]
    scoped_mode = bool(getattr(args, "task_ids", None))
    if getattr(args, "batch", None) is not None:
        argv.extend(["--batch", str(args.batch)])
    if getattr(args, "poll", None) is not None:
        argv.extend(["--poll", str(args.poll)])
    if getattr(args, "max_idle", None) is not None:
        argv.extend(["--max-idle", str(args.max_idle)])
    if getattr(args, "max_iterations", None) is not None:
        argv.extend(["--max-iterations", str(args.max_iterations)])
    requested_dispatch_mode = getattr(args, "dispatch_mode", None)
    if requested_dispatch_mode is None and hasattr(args, "recovery_mode"):
        requested_dispatch_mode = getattr(args, "recovery_mode", None)
    if requested_dispatch_mode is None and getattr(args, "restart_failed", False):
        requested_dispatch_mode = "recovery_only"
    dispatch_mode = _normalize_watch_dispatch_selection_mode(
        dispatch_mode=cast(str | None, requested_dispatch_mode),
        recovery_slots=getattr(args, "recovery_slots", None),
        scoped_mode=scoped_mode,
    )
    if dispatch_mode == "recovery_only":
        argv.append("--recovery-only")
    elif dispatch_mode == "recovery_first_explicit":
        argv.append("--recovery-first")
    elif dispatch_mode == "pending_only":
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
    for task_id in getattr(args, "task_ids", None) or ():
        argv.append(str(task_id))
    return argv


def _format_scope_message(
    tags: tuple[str, ...] | None,
    *,
    any_tag: bool,
    scoped_owner_ids: tuple[str, ...] | None = None,
) -> str | None:
    """Return a stable watch-scope message when an explicit scope is active."""
    if scoped_owner_ids:
        return f"scope: owners={','.join(scoped_owner_ids)} mode=explicit"
    if not tags:
        return None
    mode = "any" if any_tag else "all"
    return f"scope: tags={','.join(tags)} mode={mode}"


def _format_queue_scope_error(task_id: str, tags: tuple[str, ...], *, any_tag: bool) -> str:
    """Return consistent fail-closed messaging for queue ordering scope mismatch."""
    mode = "any" if any_tag else "all"
    return f"Error: Task {task_id} does not match tag scope ({mode}: {', '.join(tags)}); queue ordering was not changed"


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


def format_red_duration(red_since: datetime, now: datetime) -> str:
    elapsed = max(0, int((now - red_since).total_seconds()))
    total_minutes = elapsed // 60
    total_hours = elapsed // 3600
    total_days = elapsed // 86400
    if total_days > 0:
        return f"{total_days}d{(total_hours % 24)}h"
    if total_hours > 0:
        return f"{total_hours}h{(total_minutes % 60)}m"
    return f"{total_minutes}m"


def _format_main_verify_attention_message(state: Any, *, now: datetime) -> str:
    message = state.alert_message or "main verify is red; merges halted"
    red_since = getattr(state, "red_since", None)
    if red_since is None:
        return message
    return f"{message} (red for {format_red_duration(red_since, now)})"


def _emit_main_verify_attention(*, log: "_WatchLog", state: Any, now: datetime) -> None:
    task = getattr(state, "task", None)
    task_id = getattr(task, "id", None)
    if task_id is None:
        return
    log.emit_attention(
        attention_key=f"main-integration-verify:{task_id}:{MAIN_INTEGRATION_VERIFY_REASON}",
        message=_format_main_verify_attention_message(state, now=now),
    )


def _sleep_interruptibly(seconds: int, stop_requested: Callable[[], bool], *, quantum: float = 1.0) -> None:
    """Sleep for up to `seconds`, exiting early if stop was requested."""
    remaining = float(seconds)
    while remaining > 0:
        if stop_requested():
            return
        step = min(quantum, remaining)
        time.sleep(step)
        remaining -= step


def _task_snapshot(store: SqliteTaskStore) -> dict[str, dict[str, str | None]]:
    snap: dict[str, dict[str, str | None]] = {}
    with store._connect() as conn:  # noqa: SLF001 - CLI internal polling helper
        cur = conn.execute(
            """
            SELECT
                id,
                status,
                task_type,
                prompt,
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
                          AND """
            + active_merge_unit_where_sql("mu")
            + """
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
                      AND """
            + active_merge_unit_where_sql("mu")
            + """
                    ORDER BY mu.updated_at DESC, mu.id DESC
                    LIMIT 1
                ) AS merge_target_branch
            FROM tasks
            """
        )
        for row in cur.fetchall():
            if row["task_type"] == "internal" and row["prompt"] == GIT_HEALTH_PROMPT:
                continue
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
        if event in {"SKIP", "BACKOFF"} and dedupe_key is not None:
            self._skip_keys_this_cycle.add(dedupe_key)
            if dedupe_key in self._skip_keys_prev_cycle:
                return
        prefix = f"{_format_hms()} {event:<{_WATCH_EVENT_LABEL_WIDTH}} "
        continuation_prefix = " " * len(prefix)
        parts = message.splitlines() or [""]
        line = "\n".join(
            (prefix if idx == 0 else continuation_prefix) + part.rstrip() for idx, part in enumerate(parts)
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
    scoped_owner_ids: tuple[str, ...] | None = None,
) -> tuple[str, ...]:
    # Detector-owned transitions come from snapshot diffs, regardless of which
    # process caused the state change. START remains detector-owned for the
    # confirmed pending->in_progress seam, but the actual formatted START emit
    # is handled by the caller so recovery annotations can reuse the shared
    # formatter without forking transition detection.
    confirmed_start_ids: list[str] = []
    for task_id in sorted(new.keys()):
        old_row = old.get(task_id) or {}
        old_status = old_row.get("status")
        new_row = new[task_id]
        new_status = new_row.get("status")
        if not _transition_task_matches_scope(store, task_id, scoped_owner_ids):
            continue

        task_type = new_row.get("task_type") or "implement"
        elapsed = _format_elapsed(new_row.get("started_at"), new_row.get("completed_at"))
        elapsed_suffix = f" ({elapsed})" if elapsed else ""
        if old_status != new_status:
            if old_status == "pending" and new_status == "in_progress":
                confirmed_start_ids.append(task_id)
            if new_status == "completed":
                completion_reason = new_row.get("completion_reason")
                reason_suffix = f": {completion_reason}" if completion_reason else ""
                if task_type == "review":
                    task = store.get(task_id)
                    impl_id = new_row.get("depends_on") or "unknown"
                    verdict = format_review_outcome(config, task) if task is not None else "UNKNOWN"
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
    return tuple(confirmed_start_ids)


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
    _, running_task_ids, _, _ = _collect_live_running_state(config, store)
    return len(running_task_ids)


def _collect_live_running_state(config: Config, store: SqliteTaskStore) -> tuple[set[int], list[str], int, int]:
    live_pids, running_task_ids, anonymous_worker_count, starting_worker_count = _shared_collect_live_running_state(
        config, store
    )
    hidden_internal_pids: set[int] = set()
    visible_task_ids: list[str] = []
    for task_id in running_task_ids:
        task = store.get(task_id)
        if task is not None and task.task_type == "internal" and "behavior-monitor" in task.tags:
            if task.running_pid is not None:
                hidden_internal_pids.add(task.running_pid)
            continue
        visible_task_ids.append(task_id)
    filtered_live_pids = {pid for pid in live_pids if pid not in hidden_internal_pids}
    return filtered_live_pids, visible_task_ids, anonymous_worker_count, starting_worker_count


def get_concurrency_snapshot(
    config: Config,
    store: SqliteTaskStore,
    *,
    current_pid: int | None = None,
    cleanup_stale: bool = True,
) -> ConcurrencySnapshot:
    """Return watch-local occupancy while hiding behavior-monitor internal work."""
    snapshot = _shared_get_concurrency_snapshot(
        config,
        store,
        current_pid=current_pid,
        cleanup_stale=cleanup_stale,
    )
    hidden_internal_pids: set[int] = set()
    visible_task_ids: list[str] = []
    for task_id in snapshot.running_task_ids:
        task = store.get(task_id)
        if task is not None and task.task_type == "internal" and "behavior-monitor" in task.tags:
            if task.running_pid is not None:
                hidden_internal_pids.add(task.running_pid)
            continue
        visible_task_ids.append(task_id)
    filtered_live_pids = frozenset(pid for pid in snapshot.live_pids if pid not in hidden_internal_pids)
    running = len(visible_task_ids)
    return ConcurrencySnapshot(
        limit=snapshot.limit,
        running=running,
        available=max(0, snapshot.limit - running),
        live_pids=filtered_live_pids,
        running_task_ids=tuple(visible_task_ids),
        anonymous_worker_count=snapshot.anonymous_worker_count,
        current_pid_counted=bool(current_pid and current_pid in filtered_live_pids),
        starting_worker_count=snapshot.starting_worker_count,
    )


def _format_wake_message(
    *,
    running: int,
    runnable_pending: int,
    blocked_pending: int,
    slots: int,
    running_task_ids: list[str],
    anonymous_worker_count: int = 0,
    starting_worker_count: int = 0,
) -> str:
    message = (
        f"checking... ({running} running, pending={runnable_pending} runnable, "
        f"blocked={blocked_pending}, {slots} slots)"
    )
    if running_task_ids or anonymous_worker_count > 0 or starting_worker_count > 0:
        worker_lines = ["live workers:"]
        worker_lines.extend(f"- {task_id}" for task_id in running_task_ids)
        if anonymous_worker_count == 1:
            worker_lines.append("- 1 worker without an active task id")
        elif anonymous_worker_count > 1:
            worker_lines.append(f"- {anonymous_worker_count} workers without active task ids")
        if starting_worker_count == 1:
            worker_lines.append("- 1 worker starting before task activation")
        elif starting_worker_count > 1:
            worker_lines.append(f"- {starting_worker_count} workers starting before task activation")
        message += "\n" + "\n".join(worker_lines)
    return message


def _pending_runnable_tasks(
    store: SqliteTaskStore,
    *,
    config: Config | None = None,
    tags: tuple[str, ...] | None = None,
    any_tag: bool = False,
    excluded_owner_ids: frozenset[str] = frozenset(),
) -> list[DbTask]:
    quiet_seconds = int(getattr(config, "quiet_period_seconds", 0) or 0)
    pending_tasks = get_runnable_pending_tasks(
        store,
        tags=tags,
        any_tag=any_tag,
        quiet_seconds=quiet_seconds,
    )
    if not excluded_owner_ids:
        return pending_tasks
    return [
        task
        for task in pending_tasks
        if task.id is not None
        and (_resolve_failure_owner_task_id(store, str(task.id)) not in excluded_owner_ids)
    ]


def _top_quiet_pending_task(
    store: SqliteTaskStore,
    *,
    config: Config | None = None,
    excluded_task_ids: set[str] | None = None,
    excluded_owner_ids: frozenset[str] = frozenset(),
    tags: tuple[str, ...] | None = None,
    any_tag: bool = False,
) -> tuple[DbTask, datetime] | None:
    quiet_seconds = int(getattr(config, "quiet_period_seconds", 0) or 0)
    if quiet_seconds <= 0:
        return None
    excluded_ids = excluded_task_ids or set()
    now = datetime.now(UTC)
    for task in store.get_pending(limit=None, tags=tags, any_tag=any_tag):
        if task.id is None or task.task_type == "internal":
            continue
        if str(task.id) in excluded_ids:
            continue
        if _resolve_failure_owner_task_id(store, str(task.id)) in excluded_owner_ids:
            continue
        if store.is_task_blocked(task)[0]:
            continue
        if not is_in_quiet_period(task, now=now, quiet_seconds=quiet_seconds):
            return None
        effective_at = effective_edit_time(task)
        if effective_at is None:
            return None
        return task, effective_at + timedelta(seconds=quiet_seconds)
    return None


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


def _watch_task_has_live_registered_worker(config: Config, task_id: str) -> bool:
    registry = WorkerRegistry(config.workers_path)
    for worker in registry.list_all(include_completed=False):
        if str(worker.task_id or "") != task_id:
            continue
        if worker.status != "running" or not registry.is_running(worker.worker_id):
            continue
        return True
    return False


@dataclass(frozen=True)
class _WatchDispatchTaskSnapshot:
    task_id: str | None
    status: str | None
    started_at: datetime | None
    completed_at: datetime | None
    running_pid: int | None
    failure_reason: str | None
    completion_reason: str | None


class _DispatchSettleProbeStatus(Enum):
    LIVE = "live"
    TERMINAL_BEFORE_RUNNING = "terminal_before_running"
    WAITING = "waiting"


class _DispatchSettleStatus(Enum):
    LIVE = "live"
    TERMINAL_BEFORE_RUNNING = "terminal_before_running"
    NO_LIVE_PROOF = "no_live_proof"


@dataclass(frozen=True)
class _WatchDispatchSettleProbe:
    status: _DispatchSettleProbeStatus
    reason: str
    task: DbTask | None

    @property
    def slot_consuming(self) -> bool:
        return self.status is _DispatchSettleProbeStatus.LIVE


@dataclass(frozen=True)
class _PendingWatchDispatchSettle:
    task_id: str
    task_before: _WatchDispatchTaskSnapshot | None
    start_label: str
    dedupe_key: str


@dataclass(frozen=True)
class _WatchDispatchSettleResult:
    entry: _PendingWatchDispatchSettle
    status: _DispatchSettleStatus
    reason: str
    task: DbTask | None

    @property
    def slot_consuming(self) -> bool:
        return self.status is _DispatchSettleStatus.LIVE

    @property
    def compatibility_started(self) -> bool:
        """Preserve the legacy started/not-started wrapper contract for existing callers."""
        return self.status in {
            _DispatchSettleStatus.LIVE,
            _DispatchSettleStatus.TERMINAL_BEFORE_RUNNING,
        }


def _snapshot_watch_dispatch_task(task: object | None) -> _WatchDispatchTaskSnapshot | None:
    if task is None:
        return None
    return _WatchDispatchTaskSnapshot(
        task_id=getattr(task, "id", None),
        status=getattr(task, "status", None),
        started_at=getattr(task, "started_at", None),
        completed_at=getattr(task, "completed_at", None),
        running_pid=getattr(task, "running_pid", None),
        failure_reason=getattr(task, "failure_reason", None),
        completion_reason=getattr(task, "completion_reason", None),
    )


def _watch_dispatch_has_observed_terminal_outcome(
    *,
    task_before: _WatchDispatchTaskSnapshot | None,
    task_after: DbTask | None,
) -> bool:
    if task_after is None or task_after.status in {"pending", "in_progress"}:
        return False
    if task_before is None:
        return any(
            value is not None
            for value in (
                task_after.started_at,
                task_after.completed_at,
                task_after.running_pid,
                task_after.failure_reason,
                task_after.completion_reason,
            )
        )
    if task_before.task_id is not None and task_after.id != task_before.task_id:
        return False
    if task_before.status in {"pending", "in_progress"} and task_after.status not in {"pending", "in_progress"}:
        return True
    return (
        task_after.status != task_before.status
        or task_after.started_at != task_before.started_at
        or task_after.completed_at != task_before.completed_at
        or task_after.running_pid != task_before.running_pid
        or task_after.failure_reason != task_before.failure_reason
        or task_after.completion_reason != task_before.completion_reason
    )


def _watch_dispatch_start_state(
    *,
    config: Config,
    store: SqliteTaskStore,
    task_id: str,
    task_before: _WatchDispatchTaskSnapshot | None = None,
) -> _WatchDispatchSettleProbe:
    task = store.get(task_id)
    live_pids, running_task_ids, _, _ = _collect_live_running_state(config, store)
    live_registered_worker = _watch_task_has_live_registered_worker(config, task_id)
    if task_id in set(running_task_ids):
        if task is not None and task.status == "pending":
            return _WatchDispatchSettleProbe(
                status=_DispatchSettleProbeStatus.LIVE,
                reason=f"task {task_id} is pending with a live registered worker in preloop",
                task=task,
            )
        return _WatchDispatchSettleProbe(
            status=_DispatchSettleProbeStatus.LIVE,
            reason=f"task {task_id} reached running state",
            task=task,
        )
    if live_registered_worker:
        if task is not None and task.status == "pending":
            return _WatchDispatchSettleProbe(
                status=_DispatchSettleProbeStatus.LIVE,
                reason=f"task {task_id} is pending with a live registered worker in preloop",
                task=task,
            )
        return _WatchDispatchSettleProbe(
            status=_DispatchSettleProbeStatus.LIVE,
            reason=f"task {task_id} has a live registered worker",
            task=task,
        )
    if _watch_dispatch_has_observed_terminal_outcome(task_before=task_before, task_after=task):
        return _WatchDispatchSettleProbe(
            status=_DispatchSettleProbeStatus.TERMINAL_BEFORE_RUNNING,
            reason=f"task {task_id} reached an observable terminal outcome after dispatch",
            task=task,
        )
    if task is None:
        return _WatchDispatchSettleProbe(
            status=_DispatchSettleProbeStatus.WAITING,
            reason=f"task {task_id} no longer exists after dispatch",
            task=None,
        )
    if task.status == "in_progress":
        pid = task.running_pid
        if pid is None:
            return _WatchDispatchSettleProbe(
                status=_DispatchSettleProbeStatus.WAITING,
                reason=f"task {task_id} is in_progress but has no running_pid or live worker",
                task=task,
            )
        if pid not in live_pids:
            return _WatchDispatchSettleProbe(
                status=_DispatchSettleProbeStatus.WAITING,
                reason=f"task {task_id} is in_progress but running_pid {pid} is not live",
                task=task,
            )
    return _WatchDispatchSettleProbe(
        status=_DispatchSettleProbeStatus.WAITING,
        reason=f"task {task_id} remains {task.status} with no live worker",
        task=task,
    )


def _settle_watch_dispatch_starts(
    *,
    config: Config,
    store: SqliteTaskStore,
    pending_starts: Sequence[_PendingWatchDispatchSettle],
) -> list[_WatchDispatchSettleResult]:
    if not pending_starts:
        return []

    deadline = time.monotonic() + config.watch.slot_settle_seconds
    pending_by_task_id = {entry.task_id: entry for entry in pending_starts}
    unresolved_task_ids = set(pending_by_task_id)
    last_probe_by_task_id: dict[str, _WatchDispatchSettleProbe] = {}
    settled_by_task_id: dict[str, _WatchDispatchSettleResult] = {}

    while unresolved_task_ids:
        for task_id in tuple(unresolved_task_ids):
            entry = pending_by_task_id[task_id]
            probe = _watch_dispatch_start_state(
                config=config,
                store=store,
                task_id=task_id,
                task_before=entry.task_before,
            )
            last_probe_by_task_id[task_id] = probe
            if probe.status is _DispatchSettleProbeStatus.WAITING:
                continue
            settled_by_task_id[task_id] = _WatchDispatchSettleResult(
                entry=entry,
                status=(
                    _DispatchSettleStatus.LIVE
                    if probe.status is _DispatchSettleProbeStatus.LIVE
                    else _DispatchSettleStatus.TERMINAL_BEFORE_RUNNING
                ),
                reason=probe.reason,
                task=probe.task,
            )
            unresolved_task_ids.remove(task_id)
        if not unresolved_task_ids:
            break
        now = time.monotonic()
        if now >= deadline:
            break
        time.sleep(min(1.0, max(0.0, deadline - now)))

    for task_id in unresolved_task_ids:
        entry = pending_by_task_id[task_id]
        cached_probe = last_probe_by_task_id.get(task_id)
        final_probe: _WatchDispatchSettleProbe
        if cached_probe is None:
            final_probe = _watch_dispatch_start_state(
                config=config,
                store=store,
                task_id=task_id,
                task_before=entry.task_before,
            )
        else:
            final_probe = cached_probe
        settled_by_task_id[task_id] = _WatchDispatchSettleResult(
            entry=entry,
            status=_DispatchSettleStatus.NO_LIVE_PROOF,
            reason=final_probe.reason,
            task=final_probe.task,
        )

    return [settled_by_task_id[entry.task_id] for entry in pending_starts]


def _wait_for_watch_dispatch_start(
    *,
    config: Config,
    store: SqliteTaskStore,
    task_id: str,
    task_before: _WatchDispatchTaskSnapshot | None = None,
) -> tuple[bool, str, DbTask | None]:
    result = _settle_watch_dispatch_starts(
        config=config,
        store=store,
        pending_starts=[
            _PendingWatchDispatchSettle(
                task_id=task_id,
                task_before=task_before,
                start_label=task_id,
                dedupe_key=f"wait:{task_id}",
            )
        ],
    )[0]
    return result.compatibility_started, result.reason, result.task


def _watch_execution_requires_dispatch_confirmation(result: AdvanceActionExecutionResult) -> bool:
    """Return whether a shared advance result represents a real worker launch attempt."""
    return result.worker_consuming and result.attempted_spawn and result.worker_started and result.handled_task_id is not None


def _confirm_watch_dispatch_start(
    *,
    config: Config,
    store: SqliteTaskStore,
    log: "_WatchLog",
    task_id: str,
    task_before: _WatchDispatchTaskSnapshot | None,
    start_label: str,
    dedupe_key: str,
) -> tuple[bool, DbTask | None]:
    """Wait for bounded live or terminal execution proof and emit the shared undispatched log on failure."""
    started, dispatch_reason, refreshed_task = _wait_for_watch_dispatch_start(
        config=config,
        store=store,
        task_id=task_id,
        task_before=task_before,
    )
    if not started:
        log.emit(
            "START_UNDISPATCHED",
            (
                f"{start_label}: dispatch did not reach running within "
                f"{config.watch.slot_settle_seconds}s ({dispatch_reason})"
            ),
            dedupe_key=dedupe_key,
        )
        return False, refreshed_task
    return True, refreshed_task


def _format_expected_start_annotation(expected_start: "_ExpectedStart") -> str:
    return f"[{expected_start.recovery_action} of {expected_start.parent_failed_id}]"


def _format_start_line(
    task: DbTask,
    *,
    recovery_annotation: str | None = None,
    dry_run: bool = False,
) -> str:
    assert task.id is not None
    task_type = task.task_type or "implement"
    suffix = ""
    if recovery_annotation is not None:
        suffix += f" {recovery_annotation}"
    if dry_run:
        suffix += " [dry-run]"
    prompt = _format_prompt_for_width(
        task.prompt,
        prefix=16 + len(f"{task.id} {task_type} \""),
        suffix=len(f'"{suffix}'),
    )
    return f'{task.id} {task_type} "{prompt}"{suffix}'


def _format_expected_start_failed_line(task_id: str, expected_start: "_ExpectedStart") -> str:
    return (
        f"{task_id} {_format_expected_start_annotation(expected_start)}: "
        "spawned worker never reached in_progress"
    )


def _format_follow_line(
    task_id: str,
    source_task_id: str,
    *,
    reused: bool,
    investigation: bool = False,
) -> str:
    subject = f"{task_id} investigation" if investigation else task_id
    status = "reused" if reused else "created"
    return f"{subject} queued ({status}, from {source_task_id})"


def _format_sleep_message(
    *,
    poll: int,
    pending: int,
    running: int,
    confirmed_start_count: int,
    anonymous_worker_count: int = 0,
    starting_worker_count: int = 0,
) -> str:
    message = f"sleeping {poll}s ({pending} pending, {running} running"
    worker_suffixes: list[str] = []
    if anonymous_worker_count > 0:
        worker_suffixes.append(f"+{anonymous_worker_count} draining")
    if starting_worker_count > 0:
        worker_suffixes.append(f"+{starting_worker_count} starting")
    if worker_suffixes:
        message += " (" + ", ".join(worker_suffixes) + ")"
    if confirmed_start_count > 0:
        message += f"; +{confirmed_start_count} started this pass"
    return message + ")"


@dataclass
class _CycleResult:
    work_done: bool
    running: int
    pending: int
    scoped_done: bool | None = None
    scoped_active: int = 0
    anonymous_worker_count: int = 0
    starting_worker_count: int = 0
    expected_starts: dict[str, "_ExpectedStart"] = field(default_factory=dict)
    confirmed_start_count: int = 0


_DispatchObserver = Callable[[str, Literal["started", "direct", "capacity_blocked"], str], None]


@dataclass(frozen=True)
class _ExpectedStart:
    recovery_action: str
    parent_failed_id: str
    launch_mode: str
    confirmation_boundaries_seen: int = 0


@dataclass(frozen=True)
class _WatchCyclePlan:
    running_task_ids: tuple[str, ...]
    anonymous_worker_count: int
    pending_count: int
    blocked_pending_count: int
    running: int
    effective_batch: int
    slots: int
    analysis: "_WatchCycleAnalysis"
    starting_worker_count: int = 0


@dataclass(frozen=True)
class WatchSlotAllocation:
    recovery_slots: int
    pending_slots: int


@dataclass(frozen=True)
class _ObservedFailure:
    task_id: str
    owner_task_id: str
    task_type: str
    reason: str


@dataclass(frozen=True)
class _OwnerFailureBackoffState:
    owner_task_id: str
    streak: int
    backoff_until: datetime | None
    last_failure_task_id: str
    last_reason: str


@dataclass
class _RecoveryReport:
    actionable_count: int
    resume_count: int
    retry_count: int


@dataclass(frozen=True)
class _BlindParkedAutoRearmDecision:
    candidate: ParkedTaskCandidate
    status: Literal["rearmed", "skipped"]
    detail: str


@dataclass(frozen=True)
class _BlindParkedAutoRearmResult:
    decisions: tuple[_BlindParkedAutoRearmDecision, ...]

    @property
    def rearmed_owner_ids(self) -> tuple[str, ...]:
        owner_ids: list[str] = []
        for decision in self.decisions:
            owner_id = decision.candidate.owner_task.id
            if decision.status == "rearmed" and owner_id is not None:
                owner_ids.append(owner_id)
        return tuple(owner_ids)


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
    recovery_visible_skips: tuple[tuple[DbTask, DbTask, FailedRecoveryDecision, dict[str, Any]], ...]
    active_recovery_subject_ids: frozenset[str] = frozenset()
    actionable_failed: tuple[
        tuple[LineageOwnerRow, DbTask, FailedRecoveryDecision, dict[str, Any], bool, DbTask],
        ...,
    ] = ()
    pending_recovery_task_ids: frozenset[str] = frozenset()


SCOPED_WATCH_COMPLETE_MESSAGE = "scoped watch complete; all named owner units are terminal or parked"


def _normalize_watch_dispatch_selection_mode(
    *,
    dispatch_mode: str | None,
    recovery_slots: int | None,
    scoped_mode: bool = False,
) -> DispatchSelectionMode:
    if dispatch_mode == "recovery-only":
        dispatch_mode = "recovery_only"
    elif dispatch_mode == "pending-only":
        dispatch_mode = "pending_only"
    elif dispatch_mode == "recovery-first":
        dispatch_mode = "recovery_first_explicit"
    if scoped_mode and dispatch_mode is None:
        return "default"
    return normalize_dispatch_selection_mode(
        cast(DispatchSelectionMode | None, dispatch_mode),
        recovery_slots=recovery_slots,
    )


def _filter_watch_dispatch_preview_entries(
    entries: Sequence[DispatchPreviewEntry],
    *,
    store: SqliteTaskStore,
    started_task_ids: set[str],
    pending_recovery_task_ids: set[str],
    step1_handled_child_task_ids: set[str],
    excluded_owner_ids: frozenset[str] = frozenset(),
) -> tuple[DispatchPreviewEntry, ...]:
    filtered: list[DispatchPreviewEntry] = []
    for entry in entries:
        task_id = entry.task.id
        if task_id is None or str(task_id) in started_task_ids:
            continue
        owner_id = entry.owner_task.id if entry.owner_task is not None and entry.owner_task.id is not None else None
        if owner_id is None:
            owner_id = _resolve_failure_owner_task_id(store, str(task_id))
        if owner_id in excluded_owner_ids:
            continue
        if entry.lane == "pending" and (
            str(task_id) in pending_recovery_task_ids or str(task_id) in step1_handled_child_task_ids
        ):
            continue
        filtered.append(entry)
    return tuple(filtered)


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
    if gate_pending_on_actionable_recovery and recovery_slots_config > 0 and actionable_recovery_count > 0:
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
        console.print(f"Scope gap: {_format_scope_gap_message(gap, tags=tags, any_tag=any_tag)}")


def _collect_completed_transition_ids(
    old: dict[str, dict[str, str | None]],
    new: dict[str, dict[str, str | None]],
    *,
    store: SqliteTaskStore,
    tags: tuple[str, ...] | None = None,
    any_tag: bool = False,
    scoped_owner_ids: tuple[str, ...] | None = None,
) -> list[str]:
    completed_ids: list[str] = []
    for task_id, _old_status, new_row in _iter_status_transitions(old, new):
        if new_row.get("status") != "completed":
            continue
        if not _transition_task_matches_scope(store, task_id, scoped_owner_ids):
            continue
        if not _task_matches_tags(store, task_id, tags, any_tag):
            continue
        completed_ids.append(task_id)
    return completed_ids


def _resolve_failure_owner_task_id(store: SqliteTaskStore, task_id: str) -> str:
    owner_task_id = resolve_lineage_owner_task_id(store, task_id)
    return owner_task_id if owner_task_id is not None else task_id


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
    scoped_owner_ids: tuple[str, ...] | None = None,
) -> list[_ObservedFailure]:
    failures: list[_ObservedFailure] = []
    for task_id, _old_status, new_row in _iter_status_transitions(old, new):
        if new_row.get("status") != "failed":
            continue
        if not _transition_task_matches_scope(store, task_id, scoped_owner_ids):
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
                owner_task_id=_resolve_failure_owner_task_id(store, task_id),
                task_type=new_row.get("task_type") or "implement",
                reason=reason,
            )
        )
    return failures


def _emit_cycle_attention_summary(log: _WatchLog) -> None:
    messages = log.visible_attention_messages()
    if not messages:
        return
    if log._sticky_attention_this_cycle == log._sticky_attention_prev_cycle:  # noqa: SLF001 - watch-local sticky state
        plural = "s" if len(messages) != 1 else ""
        log.emit("INFO", f"{len(messages)} task{plural} still need attention (unchanged)")
        return
    plural = "s" if len(messages) != 1 else ""
    lines = [f"{NEEDS_ATTENTION_LABEL} ({len(messages)} task{plural}):"]
    lines.extend(f"  {message}" for message in messages)
    log.emit("INFO", "\n".join(lines))


def _process_expected_start_boundary(
    *,
    store: SqliteTaskStore,
    config: Config,
    log: _WatchLog,
    expected_starts: dict[str, _ExpectedStart],
    snapshot: Mapping[str, dict[str, str | None]],
    confirmed_start_ids: Sequence[str] = (),
) -> int:
    if not expected_starts:
        return 0
    _live_pids, running_task_ids, _anonymous_worker_count, _starting_worker_count = _collect_live_running_state(
        config, store
    )
    confirmed_transition_ids = set(confirmed_start_ids)
    confirmed_running_task_ids = set(running_task_ids)
    confirmed_count = 0
    for task_id in list(expected_starts):
        expected_start = expected_starts[task_id]
        row = snapshot.get(task_id) or {}
        status = row.get("status")
        live_registered_worker = _watch_task_has_live_registered_worker(config, task_id)
        confirmed = (
            task_id in confirmed_transition_ids
            or status == "in_progress"
            or task_id in confirmed_running_task_ids
            or live_registered_worker
        )
        if confirmed:
            task = store.get(task_id)
            if task is not None and task.id is not None:
                log.emit(
                    "START",
                    _format_start_line(
                        task,
                        recovery_annotation=_format_expected_start_annotation(expected_start),
                    ),
                )
                confirmed_count += 1
            expected_starts.pop(task_id, None)
            continue
        if status in {"completed", "failed", "dropped"}:
            expected_starts.pop(task_id, None)
            continue
        if status == "pending" and task_id not in confirmed_running_task_ids:
            if expected_start.confirmation_boundaries_seen == 0:
                expected_starts[task_id] = replace(
                    expected_start,
                    confirmation_boundaries_seen=1,
                )
                continue
            log.emit(
                "START_FAILED",
                _format_expected_start_failed_line(task_id, expected_start),
                dedupe_key=f"expected-start-no-show:{task_id}:{expected_start.parent_failed_id}",
            )
            expected_starts.pop(task_id, None)
    return confirmed_count


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
            row.recovery_leaf_task.created_at
            if row.recovery_leaf_task and row.recovery_leaf_task.created_at
            else datetime.min.replace(tzinfo=UTC),
            task_id_numeric_key(row.owner_task.id),
        )
    )
    visible_entry_by_task_id = {entry.task.id: entry for entry in entries if entry.task.id is not None}
    reconcile = 0
    rebase = 0
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
            action = visible_entry.action or {}
        else:
            decision = decide_failed_task_recovery(
                store, task, max_recovery_attempts=max_recovery_attempts, read_context=report_read_context
            )
            action = {}
        action_type = str(action.get("type", ""))
        if action_type == "needs_rebase":
            reason = action.get("reason")
            reason_text = f" reason={reason}" if isinstance(reason, str) and reason else ""
            deferred = action.get("deferred_action_type")
            deferred_text = f" deferred={deferred}" if isinstance(deferred, str) and deferred else ""
            print(
                f"{action_type:<12} {_format_recovery_report_subject(row, task)} {task.task_type:<9} "
                f"{str(action.get('description', '')).strip()}{reason_text}{deferred_text}"
            )
            actionable += 1
            rebase += 1
            continue
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
    actionable_breakdown = (
        f"{rebase} needs_rebase, {resume} resume, {retry} retry, {reconcile} reconcile"
        if rebase > 0
        else f"{resume} resume, {retry} retry, {reconcile} reconcile"
    )
    hidden_breakdown = (
        f"{rebase} needs_rebase, {resume} resume, {retry} retry"
        if rebase > 0
        else f"{resume} resume, {retry} retry"
    )
    if show_skipped:
        print(
            f"Summary: {actionable} actionable ({actionable_breakdown}), "
            f"{len(attention_rows)} needs attention, {skipped_summary} skipped"
        )
    else:
        print(
            f"Summary: {actionable} actionable ({hidden_breakdown}), "
            f"{len(attention_rows)} needs attention, {skipped_summary} skipped hidden"
        )
    return _RecoveryReport(actionable_count=actionable, resume_count=resume, retry_count=retry)


def _collect_scoped_recovery_lane_entries(
    *,
    config: Config,
    store: SqliteTaskStore,
    git: Git,
    target_branch: str,
    recovery_rows: list[LineageOwnerRow],
    read_context: RecoveryReadContext,
    max_recovery_attempts: int,
) -> dict[str, RecoveryLaneEntry]:
    entries: dict[str, RecoveryLaneEntry] = {}
    for row in recovery_rows:
        failed = row.recovery_leaf_task
        if (
            failed is None
            or failed.id is None
            or row.recovery_action_task is None
            or row.recovery_action_task.id != failed.id
        ):
            continue
        decision = decide_failed_task_recovery(
            store,
            failed,
            max_recovery_attempts=max_recovery_attempts,
            read_context=read_context,
        )
        action = _determine_recovery_lane_action(
            config=config,
            store=store,
            git=git,
            failed_task=failed,
            target_branch=target_branch,
            max_recovery_attempts=max_recovery_attempts,
            read_context=read_context,
            decision=decision,
        )
        if action is not None and classify_advance_action(action) == "actionable":
            entries[str(failed.id)] = RecoveryLaneEntry(
                owner_task=row.owner_task,
                task=failed,
                decision=decision,
                action=action,
            )
            continue
        if action is not None and classify_advance_action(action) == "needs_attention":
            entries[str(failed.id)] = RecoveryLaneEntry(
                owner_task=row.owner_task,
                task=failed,
                decision=decision,
                action=action,
                attention_action=action,
            )
            continue
        attention_action = _owner_failed_recovery_attention_action(
            store=store,
            owner_task=row.owner_task,
            failed_task=failed,
            decision=decision,
            max_recovery_attempts=max_recovery_attempts,
        )
        if attention_action is None:
            continue
        entries[str(failed.id)] = RecoveryLaneEntry(
            owner_task=row.owner_task,
            task=failed,
            decision=decision,
            action=action,
            attention_action=attention_action,
        )
    return entries


def _determine_recovery_lane_action(
    *,
    config: Config,
    store: SqliteTaskStore,
    git: Git,
    failed_task: DbTask,
    target_branch: str,
    max_recovery_attempts: int,
    read_context: RecoveryReadContext,
    decision: FailedRecoveryDecision,
    impl_based_on_ids: set[str] | None = None,
) -> dict[str, Any]:
    from .advance_engine import determine_next_action as _shared_determine_next_action

    active_recovery_task = (
        store.get(decision.recovery_task_id)
        if isinstance(decision.recovery_task_id, str) and decision.recovery_task_id
        else None
    )
    has_active_recovery_child = active_recovery_task is not None and active_recovery_task.status in {"pending", "in_progress"}
    active_recovery_mode = (
        resolve_pending_recovery_execution_mode(active_recovery_task)
        if active_recovery_task is not None
        else None
    )

    shared_action = _shared_determine_next_action(
        config,
        store,
        git,
        failed_task,
        target_branch,
        impl_based_on_ids=impl_based_on_ids,
        max_resume_attempts=max_recovery_attempts,
        read_context=read_context,
    )
    if str(shared_action.get("type", "")) == "needs_rebase":
        return shared_action
    if has_active_recovery_child:
        assert active_recovery_task is not None and active_recovery_task.id is not None
        if active_recovery_task.recovery_origin in {"resume", "retry"} or active_recovery_mode in {"resume", "retry"}:
            return shared_action
        return {
            "type": "skip",
            "description": f"SKIP: recovery task {active_recovery_task.id} already {active_recovery_task.status}",
            "reason": f"recovery_already_{active_recovery_task.status}",
        }
    if decision.reason_code in {"recovery_already_pending", "recovery_already_running"}:
        return failed_recovery_decision_to_action(failed_task, decision)

    return shared_action


def _compute_failure_backoff_seconds(config: Config, streak: int) -> int:
    if streak <= 0:
        return 0
    initial = config.watch.failure_backoff_initial
    maximum = config.watch.failure_backoff_max
    return min(initial * (2 ** (streak - 1)), maximum)


def _active_failure_backoff_owner_ids(
    failure_backoffs: Mapping[str, _OwnerFailureBackoffState],
    *,
    now: datetime,
) -> frozenset[str]:
    return frozenset(
        owner_id
        for owner_id, state in failure_backoffs.items()
        if state.backoff_until is not None and state.backoff_until > now
    )


def _reset_failure_backoff_for_completed_owners(
    *,
    store: SqliteTaskStore,
    completed_ids: Sequence[str],
    failure_backoffs: dict[str, _OwnerFailureBackoffState],
    log: _WatchLog,
) -> None:
    cleared: list[tuple[str, str]] = []
    for task_id in completed_ids:
        owner_id = _resolve_failure_owner_task_id(store, task_id)
        if owner_id not in failure_backoffs:
            continue
        failure_backoffs.pop(owner_id, None)
        cleared.append((owner_id, task_id))
    if not cleared:
        return
    summary = ", ".join(f"{owner_id} via {task_id}" for owner_id, task_id in cleared[:5])
    if len(cleared) > 5:
        summary += ", ..."
    log.emit("INFO", f"failure backoff reset after completion(s): {summary}")


def _record_failure_backoff_updates(
    *,
    config: Config,
    store: SqliteTaskStore,
    failures: Sequence[_ObservedFailure],
    failure_backoffs: dict[str, _OwnerFailureBackoffState],
    log: _WatchLog,
    now: datetime,
) -> bool:
    if not failures:
        return False
    for failure in failures:
        previous = failure_backoffs.get(failure.owner_task_id)
        streak = (previous.streak if previous is not None else 0) + 1
        backoff_seconds = _compute_failure_backoff_seconds(config, streak)
        backoff_until = now + timedelta(seconds=backoff_seconds)
        failure_backoffs[failure.owner_task_id] = _OwnerFailureBackoffState(
            owner_task_id=failure.owner_task_id,
            streak=streak,
            backoff_until=backoff_until,
            last_failure_task_id=failure.task_id,
            last_reason=failure.reason,
        )
        log.emit(
            "BACKOFF",
            (
                f"{failure.owner_task_id}: sleeping unit {backoff_seconds}s before retrying more work "
                f"(streak {streak}; latest: {failure.task_id}={failure.reason}; "
                "other units remain dispatchable)"
            ),
            dedupe_key=(
                f"owner-backoff:{failure.owner_task_id}:{streak}:"
                f"{backoff_until.astimezone(UTC).isoformat()}"
            ),
        )
    halt_after = config.watch.failure_halt_after
    if halt_after is None:
        return False
    failing_owner_count = len(failure_backoffs)
    if failing_owner_count < halt_after:
        return False
    owners = ", ".join(sorted(failure_backoffs)[:5])
    if failing_owner_count > 5:
        owners += ", ..."
    log.emit(
        "INFO",
        (
            "failure halt threshold reached "
            f"({failing_owner_count} failing unit(s) >= {halt_after}; owners: {owners}); "
            "stopping watch for human intervention"
        ),
    )
    return True


def _analyze_watch_cycle(
    *,
    config: Config,
    store: SqliteTaskStore,
    git: Git,
    slots: int,
    tags: tuple[str, ...] | None,
    any_tag: bool,
    recovery_slots: int,
    recovery_mode: DispatchSelectionMode | None,
    max_recovery_attempts: int,
    scoped_owner_ids: tuple[str, ...] | None = None,
    excluded_owner_ids: frozenset[str] = frozenset(),
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
            owner_task_ids=scoped_owner_ids,
            max_recovery_attempts=max_recovery_attempts,
            include_skipped=True,
        )
        if excluded_owner_ids:
            owner_rows = [row for row in owner_rows if row.owner_task.id not in excluded_owner_ids]
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
        if scoped_owner_ids is None:
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
        else:
            recovery_lane_entry_by_failed_id = _collect_scoped_recovery_lane_entries(
                config=config,
                store=store,
                git=git,
                target_branch=target_branch,
                recovery_rows=recovery_rows,
                read_context=watch_read_context,
                max_recovery_attempts=max_recovery_attempts,
            )

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
        recovery_visible_skips: list[tuple[DbTask, DbTask, FailedRecoveryDecision, dict[str, Any]]] = []
        active_recovery_subject_ids: set[str] = set()
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
            recovery_action = _determine_recovery_lane_action(
                config=config,
                store=store,
                git=git,
                failed_task=failed,
                target_branch=target_branch,
                max_recovery_attempts=max_recovery_attempts,
                read_context=watch_read_context,
                decision=decision,
                impl_based_on_ids=impl_based_on_ids,
            )
            recovery_action_type = str(recovery_action.get("type", ""))
            recovery_action_class = classify_advance_action(recovery_action)
            recovery_task_id = recovery_action.get("recovery_task_id")
            if recovery_action_class == "actionable" and isinstance(recovery_task_id, str) and recovery_task_id:
                pending_recovery_task_ids.add(recovery_task_id)
            if recovery_action_class == "needs_attention":
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
                if attention_action is not None and classify_advance_action(attention_action) == "needs_attention":
                    recovery_attention_rows.append((owner_task, decision, attention_action))
                    continue
                if recovery_action_class != "skip" and not _is_watch_observable_recovery_skip(decision):
                    recovery_visible_skips.append((owner_task, failed, decision, recovery_action))
                    continue
            if recovery_action_class == "skip":
                action_task = _resolve_recovery_action_task(
                    store,
                    failed_task=failed,
                    recovery_task_id=decision.recovery_task_id,
                )
                parked_candidate_action = (
                    failed_recovery_decision_to_action(failed, decision)
                    if decision.reason_code in {"recovery_already_pending", "recovery_already_running"}
                    else recovery_action
                )
                candidate = build_watch_progress_candidate(
                    store,
                    subject_task=failed,
                    action=parked_candidate_action,
                    action_task=action_task,
                    failed_task=failed,
                )
                parked_attention = get_active_watch_no_progress_attention(store, candidate=candidate)
                if parked_attention is not None:
                    recovery_attention_rows.append((row.owner_task, decision, parked_attention))
                    active_recovery_subject_ids.add(str(failed.id))
                    continue
                if (
                    str(recovery_action.get("reason", "")).startswith("recovery_already_")
                    or decision.reason_code in {"recovery_already_pending", "recovery_already_running"}
                ):
                    active_recovery_subject_ids.add(str(failed.id))
                recovery_visible_skips.append((row.owner_task, failed, decision, recovery_action))
                continue
            if recovery_action_class != "actionable":
                continue
            worker_consuming_recovery = is_worker_consuming_advance_action(recovery_action_type)
            action_task = (
                store.get(str(recovery_action.get("active_rebase_task_id")))
                if isinstance(recovery_action.get("active_rebase_task_id"), str)
                else None
            ) or _resolve_recovery_action_task(
                store,
                failed_task=failed,
                recovery_task_id=recovery_task_id if isinstance(recovery_task_id, str) else None,
            )
            actionable_failed.append((row, failed, decision, recovery_action, worker_consuming_recovery, action_task))
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
            active_recovery_subject_ids=frozenset(active_recovery_subject_ids),
            actionable_failed=tuple(actionable_failed),
            pending_recovery_task_ids=frozenset(pending_recovery_task_ids),
        )


def _evaluate_blind_parked_auto_rearm(
    *,
    config: Config,
    store: SqliteTaskStore,
    git: Git,
    target_branch: str,
    target_sha: str | None,
    tags: tuple[str, ...] | None,
    any_tag: bool,
    scoped_owner_ids: tuple[str, ...] | None,
) -> _BlindParkedAutoRearmResult:
    """Blindly auto-rearm eligible parked subjects using the shared parked clear service."""
    policy = config.watch.parked_auto_rearm
    if not policy.enabled:
        return _BlindParkedAutoRearmResult(decisions=())

    cooldown = timedelta(hours=policy.cooldown_hours)
    candidates, _stale_cleared = discover_parked_tasks(
        store,
        config=config,
        git=git,
        target_branch=target_branch,
    )
    scoped_owner_id_set = frozenset(scoped_owner_ids or ())
    now = datetime.now(UTC)
    decisions: list[_BlindParkedAutoRearmDecision] = []

    for candidate in candidates:
        owner_task = candidate.owner_task
        owner_id = owner_task.id
        subject_id = candidate.subject_task.id
        if owner_id is None or subject_id is None:
            continue
        if scoped_owner_ids is not None and owner_id not in scoped_owner_id_set:
            continue
        if tags is not None and not task_matches_tag_filters(
            task_tags=owner_task.tags,
            tag_filters=tags,
            any_tag=any_tag,
        ):
            continue

        guard_reason = skip_reason_for_landed_or_moot(
            store,
            git=git,
            target_branch=target_branch,
            task=owner_task,
        )
        if guard_reason is not None:
            decisions.append(_BlindParkedAutoRearmDecision(candidate, "skipped", guard_reason))
            continue

        rearm_state = store.get_parked_task_rearm(
            subject_kind="task",
            subject_id=subject_id,
            attention_reason=candidate.attention_reason,
        )
        auto_attempt_count = rearm_state.auto_attempt_count if rearm_state is not None else 0
        if auto_attempt_count >= policy.budget:
            decisions.append(_BlindParkedAutoRearmDecision(candidate, "skipped", "budget exhausted"))
            continue

        last_auto_attempt_at = rearm_state.last_auto_attempt_at if rearm_state is not None else None
        if last_auto_attempt_at is not None and now < last_auto_attempt_at + cooldown:
            decisions.append(_BlindParkedAutoRearmDecision(candidate, "skipped", "cooldown active"))
            continue

        if policy.require_target_advanced:
            if not target_sha:
                decisions.append(
                    _BlindParkedAutoRearmDecision(candidate, "skipped", "target SHA unavailable")
                )
                continue
            last_target_sha = rearm_state.last_auto_attempt_target_sha if rearm_state is not None else None
            if last_target_sha == target_sha:
                decisions.append(
                    _BlindParkedAutoRearmDecision(candidate, "skipped", "target SHA unchanged")
                )
                continue

        clear_parked_candidate_state(
            store,
            candidate,
            record_manual_retry_limit_rearm=False,
        )
        store.record_parked_task_auto_rearm_attempt(
            subject_kind="task",
            subject_id=subject_id,
            attention_reason=candidate.attention_reason,
            subject_task_id=subject_id,
            target_sha=target_sha,
        )
        decisions.append(_BlindParkedAutoRearmDecision(candidate, "rearmed", "blind auto-rearm"))

    return _BlindParkedAutoRearmResult(decisions=tuple(decisions))

def _scoped_member_task_ids(owner_rows: list[LineageOwnerRow], owner_task_ids: tuple[str, ...]) -> set[str]:
    member_ids = set(owner_task_ids)
    for row in owner_rows:
        member_ids.update(str(member.id) for member in row.members if member.id is not None)
        if row.lifecycle_action_task is not None and row.lifecycle_action_task.id is not None:
            member_ids.add(str(row.lifecycle_action_task.id))
        if row.recovery_action_task is not None and row.recovery_action_task.id is not None:
            member_ids.add(str(row.recovery_action_task.id))
        if row.recovery_leaf_task is not None and row.recovery_leaf_task.id is not None:
            member_ids.add(str(row.recovery_leaf_task.id))
    return member_ids


def _missing_scoped_owner_row_is_active(
    *,
    store: SqliteTaskStore,
    owner_id: str,
    running_task_ids: set[str],
) -> bool:
    owner_task = store.get(owner_id)
    if owner_task is None:
        return False

    member_tasks: list[DbTask] = [owner_task]
    member_ids = {owner_id}
    for task in store.get_all():
        task_id = task.id
        if task_id is None or task_id in member_ids:
            continue
        if resolve_lineage_owner_task_id(store, task_id) != owner_id:
            continue
        member_tasks.append(task)
        member_ids.add(task_id)

    if member_ids.intersection(running_task_ids):
        return True

    for task in member_tasks:
        if task.status == "dropped":
            continue
        if not task_is_complete_for_lifecycle(task, merge_state=task.merge_status):
            return True
    return False


def _scoped_owner_active_count(
    *,
    store: SqliteTaskStore,
    owner_rows: list[LineageOwnerRow],
    scoped_owner_ids: tuple[str, ...],
    running_task_ids: set[str],
    planned_active_owner_ids: frozenset[str] = frozenset(),
) -> int:
    rows_by_owner_id = {row.owner_task.id: row for row in owner_rows if row.owner_task.id is not None}
    active = 0
    for owner_id in scoped_owner_ids:
        row = rows_by_owner_id.get(owner_id)
        if row is None:
            if _missing_scoped_owner_row_is_active(
                store=store,
                owner_id=owner_id,
                running_task_ids=running_task_ids,
            ):
                active += 1
            continue
        member_ids = _scoped_member_task_ids([row], (owner_id,))
        if member_ids.intersection(running_task_ids):
            active += 1
            continue
        if any(member.status in {"pending", "in_progress"} for member in row.members):
            active += 1
            continue
        if owner_id in planned_active_owner_ids:
            active += 1
            continue
    return active


def _scoped_watch_active_count(
    *,
    config: Config,
    store: SqliteTaskStore,
    batch: int,
    tags: tuple[str, ...] | None,
    any_tag: bool,
    recovery_slots: int,
    recovery_mode: DispatchSelectionMode | None,
    max_recovery_attempts: int,
    scoped_owner_ids: tuple[str, ...],
) -> int:
    plan = _build_watch_cycle_plan(
        config=config,
        store=store,
        batch=batch,
        tags=tags,
        any_tag=any_tag,
        recovery_slots=recovery_slots,
        recovery_mode=recovery_mode,
        max_recovery_attempts=max_recovery_attempts,
        scoped_owner_ids=scoped_owner_ids,
    )
    planned_active_owner_ids = _collect_planned_active_owner_ids(plan.analysis)
    return _scoped_owner_active_count(
        store=store,
        owner_rows=list(plan.analysis.owner_rows),
        scoped_owner_ids=scoped_owner_ids,
        running_task_ids=set(plan.running_task_ids),
        planned_active_owner_ids=planned_active_owner_ids,
    )


def _collect_planned_active_owner_ids(analysis: _WatchCycleAnalysis) -> frozenset[str]:
    active_owner_ids: set[str] = set()
    for row, _task, action in analysis.action_plan:
        owner_id = row.owner_task.id
        if owner_id is None:
            continue
        if classify_advance_action(action) not in {"skip", "needs_attention"}:
            active_owner_ids.add(str(owner_id))
    for row, _failed, decision, _action, _worker_consuming, _action_task in analysis.actionable_failed:
        owner_id = row.owner_task.id
        if owner_id is None:
            continue
        if decision.action in {"resume", "retry", "reconcile"}:
            active_owner_ids.add(str(owner_id))
    return frozenset(active_owner_ids)


def _transition_task_matches_scope(
    store: SqliteTaskStore,
    task_id: str,
    scoped_owner_ids: tuple[str, ...] | None,
) -> bool:
    if scoped_owner_ids is None:
        return True
    if task_id in scoped_owner_ids:
        return True
    task = store.get(task_id)
    if task is None:
        return False
    owner = resolve_lineage_owner_task(store, task)
    return owner.id in set(scoped_owner_ids)


def _build_watch_cycle_plan(
    *,
    config: Config,
    store: SqliteTaskStore,
    batch: int,
    tags: tuple[str, ...] | None,
    any_tag: bool,
    recovery_slots: int,
    recovery_mode: DispatchSelectionMode | None,
    max_recovery_attempts: int,
    scoped_owner_ids: tuple[str, ...] | None = None,
    excluded_owner_ids: frozenset[str] = frozenset(),
) -> _WatchCyclePlan:
    snapshot = get_concurrency_snapshot(config, store, cleanup_stale=False)
    running_task_ids = snapshot.running_task_ids
    pending_count = (
        0
        if scoped_owner_ids is not None
        else len(
            _pending_runnable_tasks(
                store,
                tags=tags,
                any_tag=any_tag,
                excluded_owner_ids=excluded_owner_ids,
            )
        )
    )
    blocked_pending_count = (
        0
        if scoped_owner_ids is not None
        else sum(
            1
            for pending_task in store.get_pending(limit=None)
            if pending_task.task_type != "internal"
            and task_matches_tag_filters(task_tags=pending_task.tags, tag_filters=tags, any_tag=any_tag)
            and (
                pending_task.id is not None
                and _resolve_failure_owner_task_id(store, str(pending_task.id)) not in excluded_owner_ids
            )
            and store.is_task_blocked(pending_task)[0]
        )
    )
    running = snapshot.running
    effective_batch = min(batch, config.max_concurrent)
    slots = max(0, effective_batch - running)
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
        scoped_owner_ids=scoped_owner_ids,
        excluded_owner_ids=excluded_owner_ids,
    )
    return _WatchCyclePlan(
        running_task_ids=running_task_ids,
        anonymous_worker_count=snapshot.anonymous_worker_count,
        starting_worker_count=getattr(snapshot, "starting_worker_count", 0),
        pending_count=pending_count,
        blocked_pending_count=blocked_pending_count,
        running=running,
        effective_batch=effective_batch,
        slots=slots,
        analysis=analysis,
    )


def _dispatch_scoped_watch_once(
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
    recovery_mode: DispatchSelectionMode | None = None,
    max_recovery_attempts: int = 1,
    show_skipped: bool = False,
    auto_restart_on_drift: bool = True,
    installed_package_drift: _InstalledPackageDriftState | None = None,
    precomputed_plan: _WatchCyclePlan | None = None,
    begin_cycle: bool = True,
    end_cycle: bool = True,
    emit_cycle_header: bool = True,
    emit_lifecycle_summary: bool = True,
    scoped_owner_ids: tuple[str, ...],
    dispatch_observer: _DispatchObserver | None = None,
    new_worker_start_cap: int | None = None,
    excluded_owner_ids: frozenset[str] = frozenset(),
) -> _CycleResult:
    """Run one scoped watch dispatch pass through the shared watch execution path."""
    return _run_cycle(
        config=config,
        store=store,
        batch=batch,
        max_iterations=max_iterations,
        dry_run=dry_run,
        log=log,
        tags=tags,
        any_tag=any_tag,
        quiet=quiet,
        recovery_slots=recovery_slots,
        recovery_mode=recovery_mode,
        max_recovery_attempts=max_recovery_attempts,
        show_skipped=show_skipped,
        auto_restart_on_drift=auto_restart_on_drift,
        installed_package_drift=installed_package_drift,
        precomputed_plan=precomputed_plan,
        begin_cycle=begin_cycle,
        end_cycle=end_cycle,
        emit_cycle_header=emit_cycle_header,
        emit_lifecycle_summary=emit_lifecycle_summary,
        scoped_owner_ids=scoped_owner_ids,
        dispatch_observer=dispatch_observer,
        new_worker_start_cap=new_worker_start_cap,
        excluded_owner_ids=excluded_owner_ids,
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
    recovery_mode: DispatchSelectionMode | None = None,
    restart_failed: bool = False,
    restart_failed_batch: int | None = None,
    max_recovery_attempts: int = 1,
    show_skipped: bool = False,
    auto_restart_on_drift: bool = True,
    installed_package_drift: _InstalledPackageDriftState | None = None,
    precomputed_plan: _WatchCyclePlan | None = None,
    begin_cycle: bool = True,
    end_cycle: bool = True,
    emit_cycle_header: bool = True,
    emit_lifecycle_summary: bool = True,
    scoped_owner_ids: tuple[str, ...] | None = None,
    dispatch_observer: _DispatchObserver | None = None,
    new_worker_start_cap: int | None = None,
    excluded_owner_ids: frozenset[str] = frozenset(),
) -> _CycleResult:
    from ._common import (
        prune_terminal_dead_workers,
        reconcile_dead_pending_recovery_tasks,
        reconcile_in_progress_tasks,
    )

    tags = normalize_tag_filters(tags)
    scoped_mode = scoped_owner_ids is not None
    if restart_failed:
        recovery_slots = batch if restart_failed_batch is None else restart_failed_batch
        recovery_mode = "recovery_only"
    elif restart_failed_batch is not None:
        recovery_slots = restart_failed_batch
    recovery_mode = _normalize_watch_dispatch_selection_mode(
        dispatch_mode=recovery_mode,
        recovery_slots=recovery_slots,
        scoped_mode=scoped_mode,
    )

    if begin_cycle:
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

    plan = precomputed_plan or _build_watch_cycle_plan(
        config=config,
        store=store,
        batch=batch,
        tags=tags,
        any_tag=any_tag,
        recovery_slots=recovery_slots,
        recovery_mode=recovery_mode,
        max_recovery_attempts=max_recovery_attempts,
        scoped_owner_ids=scoped_owner_ids,
        excluded_owner_ids=excluded_owner_ids,
    )
    running_task_ids = list(plan.running_task_ids)
    anonymous_worker_count = plan.anonymous_worker_count
    starting_worker_count = getattr(plan, "starting_worker_count", 0)
    running_task_id_set = set(running_task_ids)
    pending_count = plan.pending_count
    blocked_pending_count = plan.blocked_pending_count
    running = plan.running
    slots = plan.slots
    work_done = False
    confirmed_start_count = 0
    started_task_ids: set[str] = set()
    expected_starts: dict[str, _ExpectedStart] = {}
    step1_handled_child_task_ids: set[str] = set()
    reserved_recovery_slots = 0
    remaining_new_worker_starts = (
        max(0, new_worker_start_cap)
        if new_worker_start_cap is not None
        else None
    )

    def _check_canonical_checkout_boundary(action: str) -> None:
        if dry_run:
            return
        watch_ops_log = config.project_dir / ".gza" / "watch.ops.jsonl"
        status = check_canonical_checkout_invariant(
            config,
            expected_branch=plan.analysis.target_branch,
            action=action,
            ops_log_file=watch_ops_log,
            canonical_git=git,
        )
        if not status.restored and not status.needs_attention:
            return
        dirty = ", ".join(status.dirty_tracked_paths[:5])
        if len(status.dirty_tracked_paths) > 5:
            dirty += ", ..."
        detail = f"; tracked changes: {dirty}" if dirty else ""
        restored = " restored" if status.restored else ""
        log.emit_attention(
            attention_key=f"{CANONICAL_CHECKOUT_ATTENTION_REASON}:{action}",
            message=(
                f"{CANONICAL_CHECKOUT_ATTENTION_REASON}: canonical checkout was on "
                f"{status.current_branch or 'unknown'}, expected {status.expected_branch};"
                f"{restored or ' operator attention required'}{detail}"
            ),
        )

    if emit_cycle_header:
        log.emit(
            "WAKE",
            _format_wake_message(
                running=running,
                runnable_pending=pending_count,
                blocked_pending=blocked_pending_count,
                slots=slots,
                running_task_ids=running_task_ids,
                anonymous_worker_count=anonymous_worker_count,
                starting_worker_count=starting_worker_count,
            ),
        )
        scope_message = _format_scope_message(tags, any_tag=any_tag, scoped_owner_ids=scoped_owner_ids)
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

    def _consume_worker_slot_if_needed(
        result: AdvanceActionExecutionResult,
        *,
        reserve_recovery_slot: bool = False,
    ) -> None:
        nonlocal slots, reserved_recovery_slots
        if not result.worker_consuming:
            return
        _consume_new_worker_start(reserve_recovery_slot=reserve_recovery_slot)

    def _consume_new_worker_start(*, reserve_recovery_slot: bool = False) -> None:
        nonlocal slots, reserved_recovery_slots, remaining_new_worker_starts
        slots = max(0, slots - 1)
        if reserve_recovery_slot:
            reserved_recovery_slots = max(0, reserved_recovery_slots - 1)
        if remaining_new_worker_starts is not None:
            remaining_new_worker_starts = max(0, remaining_new_worker_starts - 1)

    def _free_worker_start_slots() -> int:
        if remaining_new_worker_starts is None:
            return slots
        return min(slots, remaining_new_worker_starts)

    def _observe_dispatch(
        owner_task_id: str | None,
        outcome: Literal["started", "direct", "capacity_blocked"],
        action_type: str,
    ) -> None:
        if dispatch_observer is None or owner_task_id is None:
            return
        dispatch_observer(str(owner_task_id), outcome, action_type)

    # 1) Execute advance actions for completed tasks (includes completed plans
    # with no implement child, aligned with gza advance).
    # Merges run first; worker-spawning actions consume available slots.
    isolation_enabled = bool(getattr(config, "main_checkout_isolate", False))
    git = Git(config.project_dir)
    analysis = plan.analysis
    target_branch = analysis.target_branch
    _check_canonical_checkout_boundary("watch-pass-start")
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
        can_spawn_worker=lambda _kind: _free_worker_start_slots() > 0,
        no_worker_capacity_message=lambda worker_label: f"SKIP: no watch worker slots available for {worker_label}",
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
        repair_plan_slice_materialization=lambda plan_task, review_task, manifest, partial_task_ids, repair_trigger_source: (
            _repair_plan_review_slice_materialization(
                config,
                store,
                plan_task,
                review_task,
                manifest,
                partial_task_ids=partial_task_ids,
                trigger_source=repair_trigger_source,
                require_review_before_merge=config.require_review_before_merge,
            )
        ),
        create_targeted_rebase_task=_create_targeted_rebase_from_task,
        spawn_worker=_watch_spawn_worker,
        spawn_resume_worker=_watch_spawn_resume_worker,
        spawn_iterate_worker=_watch_spawn_iterate,
        is_rebase_target_already_merged=lambda t: (
            _resolve_and_persist_post_merge_rebase_state(
                store,
                git,
                t,
                target_branch,
                merge_source=_resolve_current_merge_source(git, t.branch) if t.branch else None,
            ).already_merged
        ),
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
        reconcile_diverged_branch=lambda t: _reconcile_diverged_branch_with_origin(
            config,
            git,
            t,
            target_branch=target_branch,
        ),
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
        refreshed_main_verify_state = _maybe_file_main_verify_remediation(
            dry_run=dry_run,
            config=config,
            store=store,
            tags=tags,
            any_tag=any_tag,
            log=log,
            check=main_verify,
        )
        main_verify_state = refreshed_main_verify_state or getattr(main_verify, "state", None)
        if main_verify.merges_halted and main_verify_state is not None:
            merge_halted_for_cycle = True
            _emit_main_verify_attention(log=log, state=main_verify_state, now=datetime.now(UTC))

    if lifecycle_rows:
        action_plan = list(analysis.action_plan)
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
            free_worker_slots=_free_worker_start_slots(),
            get_action=lambda item: item[2],
        )
        if can_merge and not merge_halted_for_cycle:
            execution_decisions = reproject_selected_merge_actions(
                execution_decisions,
                reproject_action=lambda item: determine_next_action(
                    config,
                    store,
                    git,
                    item[1],
                    target_branch,
                    impl_based_on_ids=collect_non_dropped_implement_source_ids(store.get_all()),
                    selected_for_merge=True,
                ),
            )
        lifecycle_summary = format_cycle_lifecycle_action_summary(
            (decision.item[0].owner_task, decision.action) for decision in execution_decisions
        )
        if emit_lifecycle_summary and lifecycle_summary is not None:
            log.emit("INFO", lifecycle_summary)

        for execution_decision in execution_decisions:
            row, task, action = execution_decision.item
            action = dict(execution_decision.action)
            display_task = row.owner_task
            action_type = action.get("type")
            if classify_advance_action(action) == "needs_attention":
                attention_key: str
                recovery_entry = (
                    recovery_lane_entry_by_failed_id.get(get_action_subject_task_id(action) or "")
                    if classify_advance_action(action) == "needs_attention"
                    else None
                )
                if recovery_entry is not None and recovery_entry.attention_action is not None:
                    display_task = recovery_entry.owner_task
                    action = _reroot_failed_recovery_attention_action(
                        owner_task=display_task,
                        failed_task=recovery_entry.task,
                        attention_action=action,
                    )
                    attention_key = f"failed-recovery-attention:{display_task.id}:{recovery_entry.decision.reason_code}"
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

            if not _shared_lifecycle_actions.should_execute_lifecycle_action(
                action,
                free_worker_slots=_free_worker_start_slots(),
            ):
                _observe_dispatch(display_task.id, "capacity_blocked", str(action_type))
                log.emit(
                    "SKIP",
                    f"{display_task.id}: no watch worker slots available for {action_type}",
                    dedupe_key=f"advance-capacity-skip:{action_type}:{display_task.id}",
                )
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
                        merge_status_before = (_task_snapshot(store).get(merge_event.display_task_id) or {}).get(
                            "merge_status"
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
                if rc == 0:
                    for warning in getattr(merge_result, "promotion_warnings", ()):
                        log.emit("WARN", warning)
                if rc == 0 and merge_event is not None:
                    merge_status_after = (_task_snapshot(store).get(merge_event.display_task_id) or {}).get(
                        "merge_status"
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
                    _handle_post_merge_main_verify_remediation_verdict(
                        config=config,
                        store=store,
                        log=log,
                        task=task,
                        display_task=display_task,
                        check=main_verify,
                    )
                    refreshed_main_verify_state = _maybe_file_main_verify_remediation(
                        dry_run=dry_run,
                        config=config,
                        store=store,
                        tags=tags,
                        any_tag=any_tag,
                        log=log,
                        check=main_verify,
                    )
                    main_verify_state = refreshed_main_verify_state or getattr(main_verify, "state", None)
                    if main_verify.merges_halted and main_verify_state is not None:
                        merge_halted_for_cycle = True
                        _emit_main_verify_attention(log=log, state=main_verify_state, now=datetime.now(UTC))
                for followup_task in merge_result.created_followups:
                    log.emit(
                        "FOLLOW",
                        _format_follow_line(
                            str(followup_task.id),
                            str(display_task.id),
                            reused=False,
                        ),
                    )
                for followup_task in merge_result.reused_followups:
                    log.emit(
                        "FOLLOW",
                        _format_follow_line(
                            str(followup_task.id),
                            str(display_task.id),
                            reused=True,
                        ),
                    )
                for investigation_task_id in getattr(merge_result, "created_investigation_task_ids", ()):
                    log.emit(
                        "FOLLOW",
                        _format_follow_line(
                            str(investigation_task_id),
                            str(display_task.id),
                            reused=False,
                            investigation=True,
                        ),
                    )
                for investigation_task_id in getattr(merge_result, "reused_investigation_task_ids", ()):
                    log.emit(
                        "FOLLOW",
                        _format_follow_line(
                            str(investigation_task_id),
                            str(display_task.id),
                            reused=True,
                            investigation=True,
                        ),
                    )
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
                            reserved_launch: LaunchPermit | None = None
                            try:
                                reserved_launch = _reserve_watch_launch("rebase", str(display_task.id))
                                if reserved_launch is None:
                                    continue
                                rebase_task = _create_rebase_from_task(task)
                            except DuplicateActiveChildError as rebase_error:
                                if reserved_launch is not None:
                                    reserved_launch.release()
                                log.emit(
                                    "SKIP",
                                    f"{display_task.id}: {format_duplicate_rebase_message(rebase_error, parent_task_id=str(display_task.id))}",
                                    dedupe_key=f"watch-duplicate-rebase:{display_task.id}",
                                )
                                continue
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
                            if _free_worker_start_slots() > 0:
                                rebase_task_before = _snapshot_watch_dispatch_task(prepared_rebase_task)
                                rebase_rc = _watch_spawn_worker(prepared_rebase_task, "rebase")
                                _release_watch_reserved_task(str(prepared_rebase_task.id))
                                if rebase_rc == 0:
                                    started, _refreshed_rebase_task = _confirm_watch_dispatch_start(
                                        config=config,
                                        store=store,
                                        log=log,
                                        task_id=str(prepared_rebase_task.id),
                                        task_before=rebase_task_before,
                                        start_label=f"{display_task.id} -> {prepared_rebase_task.id}",
                                        dedupe_key=(
                                            f"merge-conflict-rebase-undispatched:{display_task.id}:"
                                            f"{prepared_rebase_task.id}"
                                        ),
                                    )
                                    if not started:
                                        log.emit(
                                            "SKIP",
                                            f"{display_task.id}: merge conflict routed to rebase",
                                            dedupe_key=f"merge-conflict:{display_task.id}",
                                        )
                                        continue
                                    log.emit("START", f"{prepared_rebase_task.id} rebase")
                                    started_task_ids.add(str(prepared_rebase_task.id))
                                    _consume_new_worker_start()
                                    work_done = True
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
                                work_done = True
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
                            (f"{display_task.id}: merge failed ({conflict_assessment.reason}); not routing to rebase"),
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
                if (
                    display_task.id is not None
                    and exec_result.message.startswith("SKIP: no watch worker slots available for ")
                ):
                    _observe_dispatch(display_task.id, "capacity_blocked", str(action_type))
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
                _consume_worker_slot_if_needed(exec_result)
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

            if exec_result.status == "success" and action_type == "clear_off_topic_verify_blocker":
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
                        dedupe_key=f"advance-clear-off-topic:{display_task.id}",
                    )
                    if no_progress_attention is not None:
                        log.emit_attention(
                            attention_key=f"advance-attention:{display_task.id}:{action_type}:watch-no-progress",
                            message=_watch_needs_attention_message(display_task, no_progress_attention),
                        )
                work_done = True
            elif exec_result.status == "success" and _watch_execution_requires_dispatch_confirmation(exec_result):
                assert child_id is not None
                started, refreshed_action_task = _confirm_watch_dispatch_start(
                    config=config,
                    store=store,
                    log=log,
                    task_id=str(child_id),
                    task_before=_snapshot_watch_dispatch_task(exec_result.created_task),
                    start_label=f"{display_task.id} {action_type}",
                    dedupe_key=f"advance-undispatched:{action_type}:{display_task.id}:{child_id}",
                )
                if not started:
                    continue
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
                _observe_dispatch(display_task.id, "started", str(action_type))
                _consume_worker_slot_if_needed(exec_result)
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
                    _observe_dispatch(display_task.id, "direct", str(action_type))
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
            elif exec_result.status == "success":
                if display_task.id is not None and not exec_result.worker_consuming:
                    _observe_dispatch(display_task.id, "direct", str(action_type))
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
                work_done = exec_result.work_done
                if no_progress_attention is not None and display_task.id is not None:
                    log.emit_attention(
                        attention_key=f"advance-attention:{display_task.id}:{action_type}:watch-no-progress",
                        message=_watch_needs_attention_message(display_task, no_progress_attention),
                    )

    if not dry_run:
        target_sha = resolve_ref_if_possible(git, target_branch).sha
        auto_rearm_result = _evaluate_blind_parked_auto_rearm(
            config=config,
            store=store,
            git=git,
            target_branch=target_branch,
            target_sha=target_sha,
            tags=tags,
            any_tag=any_tag,
            scoped_owner_ids=scoped_owner_ids,
        )
        for auto_rearm_decision in auto_rearm_result.decisions:
            owner_id = auto_rearm_decision.candidate.owner_task.id
            if auto_rearm_decision.status == "rearmed" and owner_id is not None:
                log.emit(
                    "REARM",
                    (
                        f"{owner_id}: blind auto-rearm cleared "
                        f"{auto_rearm_decision.candidate.attention_reason}"
                    ),
                    dedupe_key=f"blind-auto-rearm:{owner_id}:{auto_rearm_decision.candidate.attention_reason}",
                )
                work_done = True
        if auto_rearm_result.rearmed_owner_ids:
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
                scoped_owner_ids=scoped_owner_ids,
                excluded_owner_ids=excluded_owner_ids,
            )


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
    for owner_task, failed, decision, recovery_action in analysis.recovery_visible_skips:
        if recovery_slots > 0 and show_skipped:
            description = str(recovery_action.get("description", "")).strip()
            log.emit(
                "SKIP",
                description or f"{owner_task.id} failed {failed.task_type}: {decision.reason_code}",
                dedupe_key=(
                    f"recovery-skip:{owner_task.id}:"
                    f"{str(recovery_action.get('reason', decision.reason_code or 'skip'))}"
                ),
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
    dispatch_selection_mode = recovery_mode
    dispatch_preview = build_dispatch_preview(
        store,
        config=config,
        git=git,
        target_branch=target_branch,
        owner_rows=analysis.owner_rows,
        read_context=analysis.watch_read_context,
        tags=tags,
        any_tag=any_tag,
        max_recovery_attempts=max_recovery_attempts,
        selection_mode=dispatch_selection_mode,
        include_pending=scoped_owner_ids is None,
    )
    dispatch_candidate_entries = _filter_watch_dispatch_preview_entries(
        dispatch_preview.runnable_entries,
        store=store,
        started_task_ids=started_task_ids,
        pending_recovery_task_ids=pending_recovery_task_ids,
        step1_handled_child_task_ids=step1_handled_child_task_ids,
        excluded_owner_ids=excluded_owner_ids,
    )
    dispatch_recovery_preview_entries = _filter_watch_dispatch_preview_entries(
        dispatch_preview.recovery_entries,
        store=store,
        started_task_ids=started_task_ids,
        pending_recovery_task_ids=pending_recovery_task_ids,
        step1_handled_child_task_ids=step1_handled_child_task_ids,
        excluded_owner_ids=excluded_owner_ids,
    )
    dispatch_recovery_task_ids = {
        str(entry.task.id) for entry in dispatch_recovery_preview_entries if entry.task.id is not None
    }
    active_backoff_recovery_subject_ids: set[str] = set()
    if not dry_run:
        for _row, failed, _decision, recovery_action, _worker_consuming, _action_task in actionable_failed:
            if failed.id is None:
                continue
            if str(failed.id) not in dispatch_recovery_task_ids:
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
        if str(failed.id) in dispatch_recovery_task_ids
        if str(failed.id) not in parked_recovery_subject_ids
        and str(failed.id) not in active_backoff_recovery_subject_ids
    }
    dispatch_launchable_entries = tuple(
        entry
        for entry in dispatch_candidate_entries
        if entry.lane != "recovery"
        or str(entry.task.id) not in parked_recovery_subject_ids
        and str(entry.task.id) not in active_backoff_recovery_subject_ids
    )
    pending_tasks = (
        []
        if scoped_owner_ids is not None
        else [entry.task for entry in dispatch_launchable_entries if entry.lane == "pending" and entry.task.id is not None]
    )
    dispatch_plan = plan_watch_dispatch_entries(
        dispatch_launchable_entries,
        slots=slots,
        recovery_slot_cap=recovery_slots,
        selection_mode=dispatch_selection_mode,
        include_pending=scoped_owner_ids is None,
    )
    reserved_recovery_slots = dispatch_plan.recovery_worker_slots
    if remaining_new_worker_starts is not None:
        reserved_recovery_slots = min(reserved_recovery_slots, remaining_new_worker_starts)
    pending_slots = 0 if scoped_owner_ids is not None else dispatch_plan.pending_slots
    nonparked_recovery_subject_ids = set(launchable_recovery_subject_ids) | set(analysis.active_recovery_subject_ids)
    recovery_started_this_cycle = False
    for row, failed, decision, recovery_action, worker_consuming_recovery, action_task in actionable_failed:
        if failed.id is None:
            continue
        recovery_action_type = str(recovery_action.get("type", ""))
        if str(failed.id) not in dispatch_recovery_task_ids:
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
                        attention_key=f"recovery-attention:{failed.id}:{recovery_action_type}:watch-no-progress",
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
        if worker_consuming_recovery and (_free_worker_start_slots() <= 0 or reserved_recovery_slots <= 0):
            _observe_dispatch(row.owner_task.id, "capacity_blocked", recovery_action_type)
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
                    attention_key=f"recovery-attention:{failed.id}:{recovery_action_type}:watch-no-progress",
                    message=_watch_needs_attention_message(failed, no_progress_attention),
                )
                continue
        if recovery_action_type == "reconcile_branch_divergence":
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
                if exec_result.message.startswith("SKIP: no watch worker slots available for "):
                    _observe_dispatch(row.owner_task.id, "capacity_blocked", recovery_action_type)
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
                _observe_dispatch(row.owner_task.id, "direct", recovery_action_type)
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
                        attention_key=f"recovery-attention:{failed.id}:{recovery_action_type}:watch-no-progress",
                        message=_watch_needs_attention_message(failed, no_progress_attention),
                    )
                work_done = True
                _consume_worker_slot_if_needed(exec_result, reserve_recovery_slot=True)
                continue
            continue
        if recovery_action_type == "needs_rebase":
            if dry_run:
                deferred = recovery_action.get("deferred_action_type")
                deferred_text = f" deferred={deferred}" if isinstance(deferred, str) and deferred else ""
                log.emit(
                    "RECOVR",
                    (
                        f"{failed.id} needs_rebase [owner={row.owner_task.id}] "
                        f"({recovery_action.get('reason', 'recovery-preflight-rebase')}{deferred_text}) [dry-run]"
                    ),
                )
                _consume_worker_slot_if_needed(
                    AdvanceActionExecutionResult(
                        action_type="needs_rebase",
                        status="dry_run",
                        message="Would create rebase task",
                        worker_consuming=True,
                    ),
                    reserve_recovery_slot=True,
                )
                work_done = True
                continue
            exec_result = execute_advance_action(task=failed, action=recovery_action, context=executor_context)
            if exec_result.status == "skip":
                if exec_result.message.startswith("SKIP: no watch worker slots available for "):
                    _observe_dispatch(row.owner_task.id, "capacity_blocked", recovery_action_type)
                attention = resolve_execution_needs_attention(failed, exec_result)
                if attention is not None:
                    log.emit_attention(
                        attention_key=f"recovery-attention:{failed.id}:{recovery_action_type}",
                        message=_watch_needs_attention_message(attention.task, attention.action),
                    )
                else:
                    log.emit(
                        "SKIP",
                        f"{failed.id}: {exec_result.message}",
                        dedupe_key=f"recovery-needs-rebase-skip:{failed.id}:{exec_result.message}",
                    )
                continue
            if exec_result.status == "error":
                log.emit(
                    "REPAIR",
                    f"{failed.id}: {exec_result.message}",
                    dedupe_key=f"recovery-needs-rebase-error:{failed.id}:{exec_result.message}",
                )
                continue
            if exec_result.status != "success" or exec_result.created_task is None or exec_result.created_task.id is None:
                continue
            recovered_task_id = str(exec_result.created_task.id)
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
            _observe_dispatch(row.owner_task.id, "started", recovery_action_type)
            expected_starts[recovered_task_id] = _ExpectedStart(
                recovery_action="needs_rebase",
                parent_failed_id=str(failed.id),
                launch_mode="worker",
            )
            _consume_worker_slot_if_needed(exec_result, reserve_recovery_slot=True)
            work_done = True
            if no_progress_attention is not None:
                log.emit_attention(
                    attention_key=f"recovery-attention:{failed.id}:{recovery_action_type}:watch-no-progress",
                    message=_watch_needs_attention_message(failed, no_progress_attention),
                )
            continue
        if recovery_action_type == "resume":
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
                _consume_new_worker_start(reserve_recovery_slot=True)
                work_done = True
                continue
            if decision.launch_mode == "worker":
                reserved_launch = _reserve_watch_launch("resume", str(failed.id))
                if reserved_launch is None:
                    _observe_dispatch(row.owner_task.id, "capacity_blocked", recovery_action_type)
                    continue
                if decision.reuse_existing:
                    assert decision.recovery_task_id is not None
                    recovered_task_id = decision.recovery_task_id
                    recovered_task = store.get(recovered_task_id)
                    assert recovered_task is not None
                else:
                    try:
                        recovered_task = _create_resume_task(store, failed, trigger_source="watch")
                    except DuplicateActiveChildError as exc:
                        reserved_launch.release()
                        log.emit(
                            "SKIP",
                            (
                                f"{failed.id}: "
                                f"{format_duplicate_active_child_message(exc, parent_task_id=str(failed.id), task=failed)}"
                            ),
                            dedupe_key=f"recovery-resume-duplicate:{failed.id}:{exc.active_child.id}",
                        )
                        continue
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
                    _observe_dispatch(row.owner_task.id, "capacity_blocked", recovery_action_type)
                    continue
                if decision.reuse_existing:
                    assert decision.recovery_task_id is not None
                    recovered_task = store.get(decision.recovery_task_id)
                    assert recovered_task is not None
                else:
                    try:
                        recovered_task = _create_resume_task(store, failed, trigger_source="watch")
                    except DuplicateActiveChildError as exc:
                        reserved_launch.release()
                        log.emit(
                            "SKIP",
                            (
                                f"{failed.id}: "
                                f"{format_duplicate_active_child_message(exc, parent_task_id=str(failed.id), task=failed)}"
                            ),
                            dedupe_key=f"recovery-resume-duplicate:{failed.id}:{exc.active_child.id}:iterate",
                        )
                        continue
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
        elif recovery_action_type == "retry":
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
                _consume_new_worker_start(reserve_recovery_slot=True)
                work_done = True
                continue
            reserved_launch = _reserve_watch_launch(
                "iterate" if decision.launch_mode != "worker" else "retry",
                str(failed.id),
            )
            if reserved_launch is None:
                _observe_dispatch(row.owner_task.id, "capacity_blocked", recovery_action_type)
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
                except DuplicateActiveChildError as exc:
                    reserved_launch.release()
                    log.emit(
                        "SKIP",
                        (
                            f"{failed.id}: "
                            f"{format_duplicate_active_child_message(exc, parent_task_id=str(failed.id), task=failed)}"
                        ),
                        dedupe_key=f"recovery-retry-duplicate:{failed.id}:{exc.active_child.id}",
                    )
                    continue
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
        else:
            continue

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
        _observe_dispatch(row.owner_task.id, "started", recovery_action_type)
        expected_starts[recovered_task_id] = _ExpectedStart(
            recovery_action=recovery_action_type,
            parent_failed_id=str(failed.id),
            launch_mode=decision.launch_mode,
        )
        _consume_new_worker_start(reserve_recovery_slot=True)
        work_done = True
        if no_progress_attention is not None:
            log.emit_attention(
                attention_key=f"recovery-attention:{failed.id}:{recovery_action_type}:watch-no-progress",
                message=_watch_needs_attention_message(failed, no_progress_attention),
            )

    if (
        scoped_owner_ids is None
        and recovery_mode == "recovery_only"
        and pending_slots <= 0
        and not recovery_started_this_cycle
    ):
        if not nonparked_recovery_subject_ids:
            pending_fallback_preview = build_dispatch_preview(
                store,
                config=config,
                git=git,
                target_branch=target_branch,
                tags=tags,
                any_tag=any_tag,
                max_recovery_attempts=max_recovery_attempts,
                selection_mode="pending_only",
                include_recovery=False,
            )
            pending_tasks = [
                entry.task
                for entry in _filter_watch_dispatch_preview_entries(
                    pending_fallback_preview.runnable_entries,
                    store=store,
                    started_task_ids=started_task_ids,
                    pending_recovery_task_ids=pending_recovery_task_ids,
                    step1_handled_child_task_ids=step1_handled_child_task_ids,
                    excluded_owner_ids=excluded_owner_ids,
                )
                if entry.lane == "pending" and entry.task.id is not None
            ]
            pending_slots = slots

    # 3) Start new queued tasks (consumes slots)
    def consume_pending_slot() -> None:
        nonlocal pending_slots, slots
        pending_slots -= 1
        slots = max(0, slots - 1)

    quiet_skip = None
    if not scoped_mode:
        quiet_skip = _top_quiet_pending_task(
            store,
            config=config,
            excluded_task_ids=started_task_ids | pending_recovery_task_ids | step1_handled_child_task_ids,
            excluded_owner_ids=excluded_owner_ids,
            tags=tags,
            any_tag=any_tag,
        )
    if quiet_skip is not None:
        quiet_task, quiet_available_at = quiet_skip
        quiet_available_text = format_quiet_available_at(quiet_available_at) or quiet_available_at.isoformat()
        log.emit(
            "SKIP",
            f"{quiet_task.id} held by quiet period until {quiet_available_text}",
            dedupe_key=f"quiet:{quiet_task.id}:{quiet_available_at.astimezone(UTC).isoformat()}",
        )

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
                        prefix=16 + len(f'{task.id} {task_type} "'),
                        suffix=len('" [dry-run]'),
                    )
                    log.emit("START", f'{task.id} {task_type} "{dry_run_prompt}" [dry-run]')
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
                pending_task_before = _snapshot_watch_dispatch_task(prepared_pending_task)
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
                started, refreshed_pending_task = _confirm_watch_dispatch_start(
                    config=config,
                    store=store,
                    log=log,
                    task_id=str(task.id),
                    task_before=pending_task_before,
                    start_label=f"{task.id} {task_type}",
                    dedupe_key=f"pending-undispatched:{task.id}:iterate",
                )
                if not started:
                    continue
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
                confirmed_start_count += 1
                started_prompt = _format_prompt_for_width(
                    task.prompt,
                    prefix=16 + len(f'{task.id} {task_type} "'),
                    suffix=len('"'),
                )
                log.emit("START", f'{task.id} {task_type} "{started_prompt}"')
                if no_progress_attention is not None:
                    log.emit_attention(
                        attention_key=f"pending-attention:{task.id}:{pending_action['type']}:watch-no-progress",
                        message=_watch_needs_attention_message(task, no_progress_attention),
                    )
                continue

            if dry_run:
                dry_run_prompt = _format_prompt_for_width(
                    task.prompt,
                    prefix=16 + len(f'{task.id} {task_type} "'),
                    suffix=len('" [dry-run]'),
                )
                log.emit("START", f'{task.id} {task_type} "{dry_run_prompt}" [dry-run]')
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
            pending_task_before = _snapshot_watch_dispatch_task(task)
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
            started, refreshed_pending_task = _confirm_watch_dispatch_start(
                config=config,
                store=store,
                log=log,
                task_id=str(task.id),
                task_before=pending_task_before,
                start_label=f"{task.id} {task_type}",
                dedupe_key=f"pending-undispatched:{task.id}:worker",
            )
            if not started:
                continue
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
            confirmed_start_count += 1
            started_prompt = _format_prompt_for_width(
                task.prompt,
                prefix=16 + len(f'{task.id} {task_type} "'),
                suffix=len('"'),
            )
            log.emit("START", f'{task.id} {task_type} "{started_prompt}"')
            if no_progress_attention is not None:
                log.emit_attention(
                    attention_key=f"pending-attention:{task.id}:{pending_action['type']}:watch-no-progress",
                    message=_watch_needs_attention_message(task, no_progress_attention),
                )

    pending_count = (
        0
        if scoped_mode
        else len(
            _pending_runnable_tasks(
                store,
                config=config,
                tags=tags,
                any_tag=any_tag,
                excluded_owner_ids=excluded_owner_ids,
            )
        )
    )
    scoped_active = (
        _scoped_watch_active_count(
            config=config,
            store=store,
            batch=batch,
            tags=tags,
            any_tag=any_tag,
            recovery_slots=recovery_slots,
            recovery_mode=recovery_mode,
            max_recovery_attempts=max_recovery_attempts,
            scoped_owner_ids=scoped_owner_ids,
        )
        if scoped_mode and scoped_owner_ids is not None
        else 0
    )
    _check_canonical_checkout_boundary("watch-pass-end")
    _emit_cycle_attention_summary(log)
    if end_cycle:
        log.end_cycle()
    _live_pids, end_running_task_ids, end_anonymous_worker_count, end_starting_worker_count = _collect_live_running_state(
        config, store
    )
    return _CycleResult(
        work_done=work_done,
        running=len(end_running_task_ids),
        pending=pending_count,
        scoped_done=(scoped_active == 0) if scoped_mode else None,
        scoped_active=scoped_active,
        anonymous_worker_count=end_anonymous_worker_count,
        starting_worker_count=end_starting_worker_count,
        expected_starts=expected_starts,
        confirmed_start_count=confirmed_start_count,
    )


def _preview_initial_watch_cycle(
    *,
    config: Config,
    store: SqliteTaskStore,
    batch: int,
    max_iterations: int,
    log: _WatchLog,
    tags: tuple[str, ...] | None,
    any_tag: bool,
    recovery_slots: int,
    recovery_mode: DispatchSelectionMode | None,
    max_recovery_attempts: int,
    show_skipped: bool,
    auto_restart_on_drift: bool,
    installed_package_drift: _InstalledPackageDriftState | None,
    scoped_owner_ids: tuple[str, ...] | None = None,
    excluded_owner_ids: frozenset[str] = frozenset(),
) -> tuple[_CycleResult, _WatchCyclePlan]:
    plan = _build_watch_cycle_plan(
        config=config,
        store=store,
        batch=batch,
        tags=tags,
        any_tag=any_tag,
        recovery_slots=recovery_slots,
        recovery_mode=recovery_mode,
        max_recovery_attempts=max_recovery_attempts,
        scoped_owner_ids=scoped_owner_ids,
        excluded_owner_ids=excluded_owner_ids,
    )
    log.begin_cycle()
    if scoped_owner_ids is not None:
        result = _dispatch_scoped_watch_once(
            config=config,
            store=store,
            batch=batch,
            max_iterations=max_iterations,
            dry_run=True,
            quiet=False,
            log=log,
            tags=tags,
            any_tag=any_tag,
            recovery_slots=recovery_slots,
            recovery_mode=recovery_mode,
            max_recovery_attempts=max_recovery_attempts,
            show_skipped=show_skipped,
            auto_restart_on_drift=auto_restart_on_drift,
            installed_package_drift=installed_package_drift,
            precomputed_plan=plan,
            begin_cycle=False,
            end_cycle=False,
            scoped_owner_ids=scoped_owner_ids,
            excluded_owner_ids=excluded_owner_ids,
        )
    else:
        result = _run_cycle(
            config=config,
            store=store,
            batch=batch,
            max_iterations=max_iterations,
            dry_run=True,
            quiet=False,
            log=log,
            tags=tags,
            any_tag=any_tag,
            recovery_slots=recovery_slots,
            recovery_mode=recovery_mode,
            max_recovery_attempts=max_recovery_attempts,
            show_skipped=show_skipped,
            auto_restart_on_drift=auto_restart_on_drift,
            installed_package_drift=installed_package_drift,
            precomputed_plan=plan,
            begin_cycle=False,
            end_cycle=False,
            scoped_owner_ids=scoped_owner_ids,
            excluded_owner_ids=excluded_owner_ids,
        )
    return result, plan


def _has_explicit_max_concurrent(config: Config) -> bool:
    return "max_concurrent" in config.source_map


def _system_can_run_tasks(config: Config) -> bool:
    if not config.use_docker:
        return True
    return wait_for_docker_ready(config.docker_startup_timeout)


def _emit_git_health_hold(
    *,
    store: SqliteTaskStore,
    config: Config,
    log: _WatchLog,
    persist: bool,
    hold_active: bool,
) -> bool:
    """Check shared git health and emit HOLD/ATTENTION when dispatch must pause."""
    git_health_check = check_git_health(
        store,
        Git(config.project_dir),
        persist=persist,
    )
    if not git_health_check.dispatch_halted:
        return False
    if not hold_active:
        log.emit(
            "HOLD",
            "Shared git worktree metadata unhealthy - holding dispatch, nothing started/failed",
        )
    log.emit_attention(
        attention_key=f"git-health:{GIT_HEALTH_REASON}",
        message=git_health_check.state.alert_message
        or "git worktree health RED - dispatch halted",
    )
    return True


def cmd_watch(args: argparse.Namespace) -> int:
    """Run continuous scheduler loop that maintains N concurrent workers."""
    config = Config.load(args.project_dir)

    batch = args.batch if args.batch is not None else config.watch.batch
    poll = args.poll if args.poll is not None else config.watch.poll
    max_idle = args.max_idle if args.max_idle is not None else config.watch.max_idle
    max_iterations = args.max_iterations if args.max_iterations is not None else config.watch.max_iterations
    requested_dispatch_mode = getattr(args, "dispatch_mode", None)
    if requested_dispatch_mode is None and hasattr(args, "recovery_mode"):
        requested_dispatch_mode = getattr(args, "recovery_mode", None)
    restart_failed = bool(getattr(args, "restart_failed", False))
    if requested_dispatch_mode is None and restart_failed:
        requested_dispatch_mode = "recovery_only"
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
    raw_task_ids = tuple(str(task_id) for task_id in (getattr(args, "task_ids", None) or ()))
    raw_tag_scope = bool(getattr(args, "tags", None))
    raw_any_tag = bool(getattr(args, "any_tag", False) or getattr(args, "all_tags", False))
    if raw_task_ids and (raw_tag_scope or raw_any_tag):
        print("Error: positional task IDs cannot be combined with --tag or --all-tags")
        return 1
    if raw_task_ids and restart_failed:
        print("Error: positional task IDs cannot be combined with --restart-failed")
        return 1
    scoped_mode = bool(raw_task_ids)
    dispatch_mode = _normalize_watch_dispatch_selection_mode(
        dispatch_mode=cast(str | None, requested_dispatch_mode),
        recovery_slots=recovery_slots,
        scoped_mode=scoped_mode,
    )

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
    if dispatch_mode == "recovery_only":
        recovery_slots = batch
    elif dispatch_mode == "pending_only":
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
        startup_cap_warning = f"requested batch={batch} capped to {config.max_concurrent} by max_concurrent" + (
            f" from {source}" if source else ""
        )

    store = get_store(config)
    log = _WatchLog(config.project_dir / ".gza" / "watch.log", quiet=quiet)
    if startup_cap_warning is not None:
        log.emit("WARN", startup_cap_warning)
    installed_package_drift = _InstalledPackageDriftState(startup_fingerprint=_installed_gza_package_fingerprint())
    if raw_task_ids:
        try:
            scoped_owner_ids = _resolve_watch_scope_owner_ids(
                store,
                raw_task_ids,
                max_recovery_attempts=max_recovery_attempts,
                config=config,
            )
        except ValueError as exc:
            print(f"Error: {exc}")
            return 1
    else:
        scoped_owner_ids = None
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
        failure_backoffs: dict[str, _OwnerFailureBackoffState] = {}
        previous_snapshot = _task_snapshot(store)
        expected_starts: dict[str, _ExpectedStart] = {}
        system_hold_active = False
        git_health_hold_active = False

        # Preview the first watch pass and ask for confirmation before executing
        if dispatch_mode == "recovery_only" and dry_run and scoped_owner_ids is None:
            if _emit_git_health_hold(
                store=store,
                config=config,
                log=log,
                persist=False,
                hold_active=git_health_hold_active,
            ):
                return 0
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
        needs_initial_preview = not skip_confirm
        pending_first_cycle_plan: _WatchCyclePlan | None = None
        preview_cycle_open = False

        def _active_failure_owner_ids() -> frozenset[str]:
            return _active_failure_backoff_owner_ids(
                failure_backoffs,
                now=datetime.now(UTC),
            )

        def _process_failure_boundary(
            old_snapshot: dict[str, dict[str, str | None]],
            new_snapshot: dict[str, dict[str, str | None]],
        ) -> bool:
            completed_ids = _collect_completed_transition_ids(
                old_snapshot,
                new_snapshot,
                store=store,
                tags=tag_filters,
                any_tag=any_tag,
                scoped_owner_ids=scoped_owner_ids,
            )
            _reset_failure_backoff_for_completed_owners(
                store=store,
                completed_ids=completed_ids,
                failure_backoffs=failure_backoffs,
                log=log,
            )
            unhandled_failures = _collect_unhandled_failures(
                old_snapshot,
                new_snapshot,
                store=store,
                max_recovery_attempts=max_recovery_attempts,
                restart_failed_mode=dispatch_mode == "recovery_only",
                tags=tag_filters,
                any_tag=any_tag,
                scoped_owner_ids=scoped_owner_ids,
            )
            return _record_failure_backoff_updates(
                config=config,
                store=store,
                failures=unhandled_failures,
                failure_backoffs=failure_backoffs,
                log=log,
                now=datetime.now(UTC),
            )

        def _preview_initial_cycle_and_confirm() -> int | None:
            nonlocal needs_initial_preview, pending_first_cycle_plan, preview_cycle_open
            preview_result, preview_plan = _preview_initial_watch_cycle(
                config=config,
                store=store,
                batch=batch,
                max_iterations=max_iterations,
                log=log,
                tags=tag_filters,
                any_tag=any_tag,
                recovery_slots=recovery_slots,
                recovery_mode=dispatch_mode,
                max_recovery_attempts=max_recovery_attempts,
                show_skipped=show_skipped,
                auto_restart_on_drift=auto_restart_on_drift,
                installed_package_drift=installed_package_drift,
                scoped_owner_ids=scoped_owner_ids,
                excluded_owner_ids=_active_failure_owner_ids(),
            )
            needs_initial_preview = False
            if preview_result.work_done:
                if not sys.stdout.isatty():
                    log.end_cycle()
                    print(
                        "watch: stdout is not a terminal, so the initial confirmation "
                        "prompt cannot be shown. Re-run with -y to auto-confirm.",
                        file=sys.stderr,
                    )
                    return 1
                try:
                    sys.stdout.flush()
                    answer = input("\nProceed? [y/N] ").strip().lower()
                except EOFError:
                    answer = ""
                except KeyboardInterrupt:
                    raise
                if answer not in ("y", "yes"):
                    log.end_cycle()
                    print("Aborted.")
                    return 0
                pending_first_cycle_plan = preview_plan
                preview_cycle_open = True
                return None

            log.end_cycle()
            return None

        if resumed_reexec:
            log.emit(
                "INFO",
                "auto-resumed after code update (skipping first-pass confirmation)",
            )

        while True:
            if stop_requested:
                if preview_cycle_open:
                    log.end_cycle()
                break

            if not _system_can_run_tasks(config):
                if preview_cycle_open:
                    log.end_cycle()
                    preview_cycle_open = False
                    pending_first_cycle_plan = None
                if scoped_owner_ids is not None:
                    scoped_active = _scoped_watch_active_count(
                        config=config,
                        store=store,
                        batch=batch,
                        tags=tag_filters,
                        any_tag=any_tag,
                        recovery_slots=recovery_slots,
                        recovery_mode=dispatch_mode,
                        max_recovery_attempts=max_recovery_attempts,
                        scoped_owner_ids=scoped_owner_ids,
                    )
                    if scoped_active == 0:
                        log.emit("INFO", SCOPED_WATCH_COMPLETE_MESSAGE)
                        break
                    log.emit(
                        "HOLD",
                        (
                            "System unavailable: Docker daemon unreachable - "
                            f"holding scoped watch ({scoped_active} active owner units), nothing started/failed"
                        ),
                    )
                else:
                    pending_count = len(
                        _pending_runnable_tasks(
                            store,
                            config=config,
                            tags=tag_filters,
                            any_tag=any_tag,
                            excluded_owner_ids=_active_failure_owner_ids(),
                        )
                    )
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

            if _emit_git_health_hold(
                store=store,
                config=config,
                log=log,
                persist=not dry_run,
                hold_active=git_health_hold_active,
            ):
                if preview_cycle_open:
                    log.end_cycle()
                    preview_cycle_open = False
                    pending_first_cycle_plan = None
                    needs_initial_preview = not skip_confirm
                if dry_run:
                    return 0
                git_health_hold_active = True
                if stop_requested:
                    break
                _sleep_interruptibly(poll, lambda: stop_requested)
                continue

            if git_health_hold_active:
                log.emit("RESUME", "git worktree health restored - resuming dispatch")
                git_health_hold_active = False

            if needs_initial_preview:
                preview_abort_code = _preview_initial_cycle_and_confirm()
                if preview_abort_code is not None:
                    return preview_abort_code
                continue

            pre_cycle_snapshot = _task_snapshot(store)
            pre_cycle_confirmed_start_ids = _emit_transition_events(
                previous_snapshot,
                pre_cycle_snapshot,
                store=store,
                config=config,
                log=log,
                restart_failed_mode=dispatch_mode == "recovery_only",
                max_recovery_attempts=max_recovery_attempts,
                scoped_owner_ids=scoped_owner_ids,
            )
            cycle_confirmed_start_count = _process_expected_start_boundary(
                store=store,
                config=config,
                log=log,
                expected_starts=expected_starts,
                snapshot=pre_cycle_snapshot,
                confirmed_start_ids=pre_cycle_confirmed_start_ids,
            )
            if _process_failure_boundary(previous_snapshot, pre_cycle_snapshot):
                previous_snapshot = pre_cycle_snapshot
                break
            previous_snapshot = pre_cycle_snapshot
            excluded_owner_ids = _active_failure_owner_ids()

            if scoped_owner_ids is not None:
                cycle_result = _dispatch_scoped_watch_once(
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
                    recovery_mode=dispatch_mode,
                    max_recovery_attempts=max_recovery_attempts,
                    show_skipped=show_skipped,
                    auto_restart_on_drift=auto_restart_on_drift,
                    installed_package_drift=installed_package_drift,
                    precomputed_plan=pending_first_cycle_plan,
                    begin_cycle=not preview_cycle_open,
                    end_cycle=True,
                    emit_cycle_header=not preview_cycle_open,
                    emit_lifecycle_summary=not preview_cycle_open,
                    scoped_owner_ids=scoped_owner_ids,
                    excluded_owner_ids=excluded_owner_ids,
                )
            else:
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
                    recovery_mode=dispatch_mode,
                    max_recovery_attempts=max_recovery_attempts,
                    show_skipped=show_skipped,
                    auto_restart_on_drift=auto_restart_on_drift,
                    installed_package_drift=installed_package_drift,
                    precomputed_plan=pending_first_cycle_plan,
                    begin_cycle=not preview_cycle_open,
                    end_cycle=True,
                    emit_cycle_header=not preview_cycle_open,
                    emit_lifecycle_summary=not preview_cycle_open,
                    scoped_owner_ids=scoped_owner_ids,
                    excluded_owner_ids=excluded_owner_ids,
                )
            pending_first_cycle_plan = None
            preview_cycle_open = False
            cycle_result.confirmed_start_count += cycle_confirmed_start_count
            if cycle_result.expected_starts:
                expected_starts.update(cycle_result.expected_starts)

            current_snapshot = _task_snapshot(store)
            post_cycle_confirmed_start_ids = _emit_transition_events(
                previous_snapshot,
                current_snapshot,
                store=store,
                config=config,
                log=log,
                restart_failed_mode=dispatch_mode == "recovery_only",
                max_recovery_attempts=max_recovery_attempts,
                scoped_owner_ids=scoped_owner_ids,
            )
            cycle_result.confirmed_start_count += _process_expected_start_boundary(
                store=store,
                config=config,
                log=log,
                expected_starts=expected_starts,
                snapshot=current_snapshot,
                confirmed_start_ids=post_cycle_confirmed_start_ids,
            )
            if _process_failure_boundary(previous_snapshot, current_snapshot):
                previous_snapshot = current_snapshot
                break
            previous_snapshot = current_snapshot

            if _should_reexec_watch(
                auto_restart_on_drift=auto_restart_on_drift,
                dry_run=dry_run,
                stop_requested=stop_requested,
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

            if cycle_result.work_done:
                idle_seconds = 0
            if scoped_owner_ids is not None and cycle_result.scoped_done:
                log.emit("INFO", SCOPED_WATCH_COMPLETE_MESSAGE)
                break
            if dry_run and scoped_owner_ids is not None:
                break
            log.emit(
                "SLEEP",
                _format_sleep_message(
                    poll=poll,
                    pending=cycle_result.pending,
                    running=cycle_result.running,
                    confirmed_start_count=cycle_result.confirmed_start_count,
                    anonymous_worker_count=cycle_result.anonymous_worker_count,
                    starting_worker_count=cycle_result.starting_worker_count,
                ),
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
    requested_dispatch_mode = getattr(args, "dispatch_mode", None)
    if requested_dispatch_mode is None and hasattr(args, "full"):
        requested_dispatch_mode = "default" if bool(getattr(args, "full", False)) else "pending_only"
    dispatch_mode = normalize_dispatch_selection_mode(cast(DispatchSelectionMode | None, requested_dispatch_mode))
    show_recovery = dispatch_mode != "pending_only"
    show_lifecycle = dispatch_mode in {"default", "recovery_first_explicit"}
    show_pending = dispatch_mode != "recovery_only"
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
                TaskQueryPresets.queue_listing(limit=None, tags=normalized_tag_filters, any_tag=any_tag),
                config=config,
            ).rows
            if (
                isinstance(row, TaskRow)
                and row.task.id is not None
                and not bool(row.values.get("blocked"))
                and not bool(row.values.get("quiet"))
            )
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
    runnable_pending: list[DbTask] = []
    blocked_pending: list[TaskRow] = []
    if show_recovery or show_lifecycle:
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
        if show_recovery:
            recovery_entries = collect_recovery_lane_entries(
                store,
                tags=normalized_tag_filters,
                any_tag=any_tag,
                max_recovery_attempts=config.max_resume_attempts,
                git=queue_git,
                target_branch=queue_target_branch,
            )
        if show_lifecycle:
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
            TaskQueryPresets.queue_listing(limit=None, tags=normalized_tag_filters, any_tag=any_tag),
            config=config,
        ).rows
        if isinstance(row, TaskRow)
    ]
    runnable_rows, quiet_rows, blocked_pending = partition_queue_rows(queue_rows)
    runnable_pending = [row.task for row in runnable_rows]
    if not runnable_pending and not quiet_rows and not blocked_pending and not recovery_entries and not lifecycle_entries:
        if tag_filters:
            print(f"No pending tasks matching tags: {', '.join(tag_filters)}")
        else:
            print("No pending tasks")
        if show_recovery or show_lifecycle:
            for gap in scope_gaps:
                print(f"Scope gap: {_format_scope_gap_message(gap, tags=normalized_tag_filters, any_tag=any_tag)}")
        return 0

    if not show_pending:
        if show_recovery:
            console.print(
                build_queue_summary("Recovery lane: `advance` / `watch` only. Evaluated ahead of pending pickup.")
            )
            if recovery_entries:
                for entry in recovery_entries:
                    console.print(_format_queue_recovery_lane_detail(entry))
            else:
                console.print("No recovery candidates")
        if show_lifecycle:
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
        if show_recovery or show_lifecycle:
            _print_queue_scope_gaps(
                gaps=scope_gaps,
                tags=normalized_tag_filters,
                any_tag=any_tag,
            )
        return 0

    limit_arg = getattr(args, "limit", 10)
    show_all = bool(getattr(args, "all", False)) or limit_arg in {0, -1}
    display_limit = None if show_all else max(1, int(limit_arg))
    visible_runnable = runnable_pending if display_limit is None else runnable_pending[:display_limit]
    rendered_rows = [
        QueueRenderRow(task=task, position_text=str(index)) for index, task in enumerate(visible_runnable, 1)
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
        blocking = (
            str(blocking_id)
            if isinstance(blocking_id, str) and blocking_id
            else (_precondition_blocking_dependency_id(task, config) or task.depends_on)
        )
        return f"blocked by {blocking}" if blocking else "blocked by dependency"

    quiet_rendered_rows = [
        QueueRenderRow(
            task=row.task,
            position_text="-",
            blocked_by_text=_blocked_by_text(row) if bool(row.values.get("blocked")) else None,
            quiet_available_text=format_quiet_available_at(row.values.get("quiet_available_at")),
        )
        for row in quiet_rows
    ]

    rendered_rows.extend(
        QueueRenderRow(
            task=row.task,
            position_text="-",
            blocked=True,
            blocked_by_text=_blocked_by_text(row),
        )
        for row in blocked_pending
    )
    widths = queue_render_widths(rendered_rows + quiet_rendered_rows)

    if show_recovery:
        console.print(
            build_queue_summary("Recovery lane: `advance` / `watch` only. Evaluated ahead of pending pickup.")
        )
        if recovery_entries:
            for entry in recovery_entries:
                console.print(_format_queue_recovery_lane_detail(entry))
        else:
            console.print("No recovery candidates")

        console.print()
    if show_lifecycle:
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
        build_queue_summary("Pending lane: `gza queue` preview only. `gza work` / `watch` start from this lane.")
    )
    if not runnable_pending and not quiet_rows and not blocked_pending:
        console.print("No pending tasks")
        if show_recovery or show_lifecycle:
            _print_queue_scope_gaps(
                gaps=scope_gaps,
                tags=normalized_tag_filters,
                any_tag=any_tag,
            )
        return 0
    runnable_rendered_rows = [row for row in rendered_rows if not row.blocked]
    if runnable_rendered_rows:
        print_queue_rows(console, runnable_rendered_rows, widths=widths)
    elif quiet_rendered_rows or blocked_pending:
        console.print("No runnable tasks")

    if display_limit is not None and len(runnable_pending) > display_limit:
        remaining = len(runnable_pending) - display_limit
        plural = "tasks" if remaining != 1 else "task"
        console.print(
            build_queue_summary(f"({remaining} more runnable {plural}; use -n 0, -n -1, or --all to show everything)")
        )

    if quiet_rendered_rows:
        console.print()
        console.print(
            build_queue_summary(
                "Quiet lane: `gza queue` shows held tasks without giving them runnable positions."
            )
        )
        print_queue_rows(console, quiet_rendered_rows, widths=widths)

    print_queue_rows(
        console,
        [row for row in rendered_rows if row.blocked],
        widths=widths,
    )
    if show_recovery or show_lifecycle:
        _print_queue_scope_gaps(
            gaps=scope_gaps,
            tags=normalized_tag_filters,
            any_tag=any_tag,
        )

    return 0


def _format_queue_recovery_lane_detail(entry: RecoveryLaneEntry) -> Text:
    colors = _colors.QUEUE_COLORS
    task = entry.task
    task_id = str(task.id or "unknown")
    task_type = task.task_type or "task"

    if entry.attention_action is not None:
        display = build_needs_attention_entry_for_display(task, action=entry.attention_action)
        rendered = Text(display.text)
        task_type_start = len(task_id) + 1
        rendered.stylize(colors.task_id, 0, len(task_id))
        rendered.stylize(colors.task_type, task_type_start, task_type_start + len(task_type))
        if display.prompt_end > display.prompt_start:
            rendered.stylize(colors.prompt, display.prompt_start, display.prompt_end)
        return rendered

    action = entry.action or {}
    action_type = str(action.get("type", "")).strip()
    if action_type and action_type not in {"resume", "retry", "reconcile_branch_divergence"}:
        prefix = f"{action_type:<12} {task_id} [{task_type}] "
        prompt = shorten_prompt(task.prompt, prompt_available_width(prefix=len(prefix), suffix=0))
        rendered = Text()
        rendered.append(f"{action_type:<12} ")
        rendered.append(task_id, style=colors.task_id)
        rendered.append(" ")
        rendered.append(f"[{task_type}]", style=colors.task_type)
        rendered.append(" ")
        rendered.append(prompt, style=colors.prompt)
        description = str(action.get("description", "")).strip()
        if description:
            rendered.append(f" {description}")
        reason = action.get("reason")
        if isinstance(reason, str) and reason:
            rendered.append(f" reason={reason}")
        deferred = action.get("deferred_action_type")
        if isinstance(deferred, str) and deferred:
            rendered.append(f" deferred={deferred}")
        return rendered

    decision = entry.decision
    type_chip = f"[{task_type}]"
    prefix = f"{decision.action:<6} {task_id} {type_chip} "
    prompt = shorten_prompt(task.prompt, prompt_available_width(prefix=len(prefix), suffix=0))
    rendered = Text()
    rendered.append(f"{decision.action:<6} ")
    rendered.append(task_id, style=colors.task_id)
    rendered.append(" ")
    rendered.append(type_chip, style=colors.task_type)
    rendered.append(" ")
    rendered.append(prompt, style=colors.prompt)
    rendered.append(
        f" via {decision.launch_mode} reason={decision.reason_code} "
        f"attempt={decision.attempt_index}/{decision.attempt_limit}"
    )
    return rendered
