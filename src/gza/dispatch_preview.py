"""Shared dispatch-preview substrate for recovery and pending selection."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Literal

from .config import Config
from .db import SqliteTaskStore, Task as DbTask, task_id_numeric_key
from .git import Git
from .lineage_query import (
    LineageOwnerQuery,
    LineageOwnerRow,
    query_lineage_owner_rows_in_read_session,
)
from .pickup import get_runnable_pending_tasks, is_worker_consuming_advance_action
from .recovery_engine import (
    FailedRecoveryDecision,
    classify_failure_reason,
    decide_failed_task_recovery,
    resolve_pending_recovery_execution_mode,
    should_hide_failed_recovery_decision,
)
from .recovery_read_context import RecoveryReadContext
from .task_query import normalize_tag_filters

DispatchPreviewLane = Literal["recovery", "pending"]
DispatchSelectionMode = Literal["default", "recovery_only", "recovery_first_explicit", "pending_only"]
DispatchOrderPolicy = Literal["recovery_preferred_with_pending_floor"]

DEFAULT_DISPATCH_ORDER_POLICY: DispatchOrderPolicy = "recovery_preferred_with_pending_floor"
_RUNNABLE_RECOVERY_ACTIONS = frozenset({"resume", "retry", "reconcile"})


def normalize_dispatch_selection_mode(
    requested_mode: DispatchSelectionMode | None,
    *,
    recovery_slots: int | None = None,
) -> DispatchSelectionMode:
    """Normalize CLI-selected dispatch mode into the shared internal model."""
    if requested_mode is not None:
        return requested_mode
    if recovery_slots is not None and recovery_slots <= 0:
        return "pending_only"
    return "default"


@dataclass(frozen=True)
class DispatchPreviewEntry:
    """One ordered recovery or pending candidate in the shared dispatch preview."""

    lane: DispatchPreviewLane
    task: DbTask
    runnable: bool
    worker_consuming: bool
    owner_task: DbTask | None = None
    decision: FailedRecoveryDecision | None = None
    advance_action: dict[str, Any] | None = None
    lineage_row: LineageOwnerRow | None = None
    queue_position: int | None = None
    manual_only: bool = False

    @property
    def action(self) -> str | None:
        return None if self.decision is None else self.decision.action

    @property
    def reason_code(self) -> str | None:
        return None if self.decision is None else self.decision.reason_code


@dataclass(frozen=True)
class DispatchPreview:
    """Shared preview result for recovery and pending selection."""

    entries: tuple[DispatchPreviewEntry, ...]
    owner_rows: tuple[LineageOwnerRow, ...] = ()
    read_context: RecoveryReadContext | None = None

    @property
    def runnable_entries(self) -> tuple[DispatchPreviewEntry, ...]:
        return tuple(entry for entry in self.entries if entry.runnable)

    @property
    def needs_human_entries(self) -> tuple[DispatchPreviewEntry, ...]:
        return tuple(entry for entry in self.entries if entry.lane == "recovery" and not entry.runnable)

    @property
    def recovery_entries(self) -> tuple[DispatchPreviewEntry, ...]:
        return tuple(entry for entry in self.entries if entry.lane == "recovery")

    @property
    def pending_entries(self) -> tuple[DispatchPreviewEntry, ...]:
        return tuple(entry for entry in self.entries if entry.lane == "pending")


@dataclass(frozen=True)
class WatchDispatchPlan:
    """Shared watch dispatch slice derived from the runnable preview order."""

    entries: tuple[DispatchPreviewEntry, ...]
    recovery_worker_slots: int
    pending_slots: int


def build_dispatch_preview(
    store: SqliteTaskStore,
    *,
    config: Config | None = None,
    git: Git | None = None,
    target_branch: str | None = None,
    owner_rows: tuple[LineageOwnerRow, ...] | None = None,
    read_context: RecoveryReadContext | None = None,
    tags: tuple[str, ...] | None,
    any_tag: bool,
    max_recovery_attempts: int,
    selection_mode: DispatchSelectionMode = "default",
    order_policy: DispatchOrderPolicy = DEFAULT_DISPATCH_ORDER_POLICY,
    pending_limit: int | None = None,
    include_pending: bool = True,
    include_recovery: bool = True,
) -> DispatchPreview:
    """Build one ordered preview from the canonical recovery and pending sources."""
    normalized_tags = normalize_tag_filters(tags)
    _validate_dispatch_preview_policy(selection_mode=selection_mode, order_policy=order_policy)

    preview_owner_rows: tuple[LineageOwnerRow, ...] = ()
    preview_read_context: RecoveryReadContext | None = None
    recovery_entries: tuple[DispatchPreviewEntry, ...] = ()
    pending_entries: tuple[DispatchPreviewEntry, ...] = ()

    if include_recovery and selection_mode != "pending_only":
        preview_owner_rows, preview_read_context, recovery_entries = _build_recovery_preview_entries(
            store,
            config=config,
            git=git,
            target_branch=target_branch,
            owner_rows=owner_rows,
            read_context=read_context,
            tags=normalized_tags,
            any_tag=any_tag,
            max_recovery_attempts=max_recovery_attempts,
        )

    if include_pending and selection_mode != "recovery_only":
        pending_entries = _build_pending_preview_entries(
            store,
            tags=normalized_tags,
            any_tag=any_tag,
            selection_mode=selection_mode,
            pending_limit=pending_limit,
            quiet_seconds=int(getattr(config, "quiet_period_seconds", 0) or 0) if config is not None else 0,
        )

    return DispatchPreview(
        entries=tuple((*recovery_entries, *pending_entries)),
        owner_rows=preview_owner_rows,
        read_context=preview_read_context,
    )


def plan_watch_dispatch_entries(
    entries: tuple[DispatchPreviewEntry, ...],
    *,
    slots: int,
    recovery_slot_cap: int,
    selection_mode: DispatchSelectionMode,
    include_pending: bool = True,
) -> WatchDispatchPlan:
    """Return the watch execution slice for one ordered preview candidate set."""
    _validate_dispatch_preview_policy(
        selection_mode=selection_mode,
        order_policy=DEFAULT_DISPATCH_ORDER_POLICY,
    )
    if slots <= 0:
        return WatchDispatchPlan(entries=(), recovery_worker_slots=0, pending_slots=0)

    runnable_entries = tuple(entry for entry in entries if entry.runnable)
    if selection_mode == "recovery_only":
        recovery_worker_slots = min(
            slots,
            sum(1 for entry in runnable_entries if entry.lane == "recovery" and entry.worker_consuming),
        )
        pending_slots = 0
    elif selection_mode == "pending_only":
        recovery_worker_slots = 0
        pending_slots = min(
            slots,
            sum(1 for entry in runnable_entries if entry.lane == "pending"),
        )
    elif not include_pending:
        recovery_worker_slots = min(
            slots,
            sum(1 for entry in runnable_entries if entry.lane == "recovery" and entry.worker_consuming),
        )
        pending_slots = 0
    else:
        recovery_worker_slots = min(
            slots,
            max(0, recovery_slot_cap),
            sum(1 for entry in runnable_entries if entry.lane == "recovery" and entry.worker_consuming),
        )
        pending_slots = min(
            max(0, slots - recovery_worker_slots),
            sum(1 for entry in runnable_entries if entry.lane == "pending"),
        )

    remaining_recovery_worker_slots = recovery_worker_slots
    remaining_pending_slots = pending_slots
    planned_entries: list[DispatchPreviewEntry] = []
    for entry in runnable_entries:
        if entry.lane == "recovery":
            if not entry.worker_consuming:
                planned_entries.append(entry)
                continue
            if remaining_recovery_worker_slots <= 0:
                continue
            planned_entries.append(entry)
            remaining_recovery_worker_slots -= 1
            continue
        if remaining_pending_slots <= 0:
            continue
        planned_entries.append(entry)
        remaining_pending_slots -= 1

    return WatchDispatchPlan(
        entries=tuple(planned_entries),
        recovery_worker_slots=recovery_worker_slots,
        pending_slots=pending_slots,
    )


def _validate_dispatch_preview_policy(
    *,
    selection_mode: DispatchSelectionMode,
    order_policy: DispatchOrderPolicy,
) -> None:
    if selection_mode not in {"default", "recovery_only", "recovery_first_explicit", "pending_only"}:
        raise ValueError(f"Unsupported dispatch preview selection mode: {selection_mode}")
    if order_policy != DEFAULT_DISPATCH_ORDER_POLICY:
        raise ValueError(f"Unsupported dispatch preview order policy: {order_policy}")


def _build_recovery_preview_entries(
    store: SqliteTaskStore,
    *,
    config: Config | None,
    git: Git | None,
    target_branch: str | None,
    owner_rows: tuple[LineageOwnerRow, ...] | None,
    read_context: RecoveryReadContext | None,
    tags: tuple[str, ...] | None,
    any_tag: bool,
    max_recovery_attempts: int,
) -> tuple[tuple[LineageOwnerRow, ...], RecoveryReadContext, tuple[DispatchPreviewEntry, ...]]:
    from .cli.advance_engine import classify_advance_action, determine_next_action

    if owner_rows is None:
        owner_rows, read_context = query_lineage_owner_rows_in_read_session(
            store,
            LineageOwnerQuery(
                limit=None,
                tags=tags,
                any_tag=any_tag,
                include_skipped=True,
                exclude_dropped_from_planning=True,
                max_recovery_attempts=max_recovery_attempts,
            ),
            config=config,
            git=git,
            target_branch=target_branch,
        )
    else:
        owner_rows = tuple(owner_rows)
        read_context = read_context or RecoveryReadContext()
    failed_rows = [
        row
        for row in owner_rows
        if (
            row.recovery_leaf_task is not None
            and row.recovery_action_task is not None
            and row.recovery_action_task.id == row.recovery_leaf_task.id
        )
    ]
    failed_rows.sort(key=_recovery_owner_row_sort_key)

    entries: list[DispatchPreviewEntry] = []
    for row in failed_rows:
        task = row.recovery_leaf_task
        if task is None or task.id is None:
            continue
        decision = decide_failed_task_recovery(
            store,
            task,
            max_recovery_attempts=max_recovery_attempts,
            read_context=read_context,
        )
        if should_hide_failed_recovery_decision(decision):
            continue
        manual_only = classify_failure_reason(task.failure_reason or "UNKNOWN") == "manual"
        action = decision.action
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
        advance_action = (
            determine_next_action(
                config,
                store,
                git,
                task,
                target_branch,
                max_resume_attempts=max_recovery_attempts,
                read_context=read_context,
            )
            if config is not None and git is not None and target_branch
            else None
        )
        if (
            has_active_recovery_child
            and advance_action is not None
            and str(advance_action.get("type", "")) != "needs_rebase"
            and active_recovery_task is not None
            and active_recovery_task.recovery_origin not in {"resume", "retry"}
            and active_recovery_mode not in {"resume", "retry"}
        ):
            assert active_recovery_task is not None and active_recovery_task.id is not None
            advance_action = {
                "type": "skip",
                "description": f"SKIP: recovery task {active_recovery_task.id} already {active_recovery_task.status}",
                "reason": f"recovery_already_{active_recovery_task.status}",
            }
        if advance_action is None:
            runnable = action in _RUNNABLE_RECOVERY_ACTIONS
            worker_consuming = action in {"resume", "retry"}
        else:
            runnable = classify_advance_action(advance_action) == "actionable"
            worker_consuming = is_worker_consuming_advance_action(str(advance_action.get("type", "")))
        entries.append(
            DispatchPreviewEntry(
                lane="recovery",
                task=task,
                owner_task=row.owner_task,
                decision=decision,
                advance_action=advance_action,
                lineage_row=row,
                queue_position=task.queue_position,
                runnable=runnable,
                worker_consuming=worker_consuming,
                manual_only=manual_only,
            )
        )

    return tuple(owner_rows), read_context, tuple(entries)


def _build_pending_preview_entries(
    store: SqliteTaskStore,
    *,
    tags: tuple[str, ...] | None,
    any_tag: bool,
    selection_mode: DispatchSelectionMode,
    pending_limit: int | None,
    quiet_seconds: int = 0,
) -> tuple[DispatchPreviewEntry, ...]:
    pending_tasks = list(
        get_runnable_pending_tasks(store, tags=tags, any_tag=any_tag, quiet_seconds=quiet_seconds)
    )
    if selection_mode == "recovery_first_explicit":
        pending_tasks = [task for task in pending_tasks if task.queue_position is not None]
    if pending_limit is not None:
        pending_tasks = pending_tasks[:pending_limit]
    return tuple(
        DispatchPreviewEntry(
            lane="pending",
            task=task,
            runnable=True,
            worker_consuming=True,
            queue_position=task.queue_position,
        )
        for task in pending_tasks
    )


def _recovery_owner_row_sort_key(row: LineageOwnerRow) -> tuple[datetime, int]:
    created_at = (
        row.recovery_leaf_task.created_at
        if row.recovery_leaf_task is not None and row.recovery_leaf_task.created_at is not None
        else datetime.min.replace(tzinfo=UTC)
    )
    return (created_at, task_id_numeric_key(row.owner_task.id))
