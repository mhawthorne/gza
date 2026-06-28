"""Shared parked-task discovery and clear helpers for ``gza unstick``."""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Literal, cast

from .config import Config
from .db import SqliteTaskStore, Task as DbTask
from .git import Git
from .lineage_query import LineageOwnerQuery, LineageOwnerRow, query_lineage_owner_rows_in_read_session
from .sync_ops import build_branch_cohorts_for_tasks, reconcile_branch_merge_truth
from .task_query import normalize_tag_filters, task_matches_tag_filters
from .watch_progress import (
    WATCH_NO_PROGRESS_BACKSTOP_REASON,
    clear_watch_progress_subject,
    reconcile_stale_watch_no_progress_parks,
)

ParkReasonClass = Literal["backstop", "reconcile"]

RECONCILE_NEEDS_MANUAL_RESOLUTION_REASON = "reconcile-needs-manual-resolution"
SUPPORTED_PARK_REASON_CLASSES: tuple[ParkReasonClass, ...] = ("backstop", "reconcile")
_REASON_CLASS_BY_ATTENTION_REASON = cast(
    "Mapping[str, ParkReasonClass]",
    {
        WATCH_NO_PROGRESS_BACKSTOP_REASON: "backstop",
        RECONCILE_NEEDS_MANUAL_RESOLUTION_REASON: "reconcile",
    },
)


@dataclass(frozen=True)
class ParkedTaskCandidate:
    """One currently parked owner/reason pair that can be manually rearmed."""

    owner_task: DbTask
    subject_task: DbTask
    reason_class: ParkReasonClass
    attention_reason: str
    source: str


@dataclass(frozen=True)
class SelectedParkedTask:
    """One operator-selected owner, with an optional current parked candidate."""

    owner_task: DbTask
    current_candidate: ParkedTaskCandidate | None


@dataclass(frozen=True)
class UnstickOutcome:
    """Operator-visible result for one selected owner."""

    owner_task: DbTask
    reason_class: ParkReasonClass | None
    status: Literal["rearmed", "skipped"]
    detail: str


@dataclass(frozen=True)
class UnstickSelectionResult:
    """Structured selection result for the CLI surface."""

    candidates: tuple[ParkedTaskCandidate, ...]
    selected: tuple[SelectedParkedTask, ...]
    outcomes: tuple[UnstickOutcome, ...]
    stale_backstop_cleared: int = 0


def discover_parked_tasks(
    store: SqliteTaskStore,
    *,
    config: Config,
    git: Git,
    target_branch: str,
) -> tuple[tuple[ParkedTaskCandidate, ...], int]:
    """Return the currently parked backstop/reconcile owner candidates."""
    stale_backstop_cleared = reconcile_stale_watch_no_progress_parks(store)
    owner_rows, _read_context = query_lineage_owner_rows_in_read_session(
        store,
        LineageOwnerQuery(
            limit=None,
            statuses=("completed", "unmerged", "dropped"),
            include_skipped=True,
            exclude_dropped_from_planning=True,
            max_recovery_attempts=config.max_resume_attempts,
        ),
        config=config,
        git=git,
        target_branch=target_branch,
        persist_post_merge_rebase_state=False,
        persist_review_clearance=False,
    )

    member_owner_rows: dict[str, LineageOwnerRow] = {}
    for row in owner_rows:
        for member in row.members:
            if member.id is not None:
                member_owner_rows[member.id] = row

    candidates_by_key: dict[tuple[str, ParkReasonClass], ParkedTaskCandidate] = {}
    for row in owner_rows:
        owner_id = row.owner_task.id
        if owner_id is None:
            continue
        parked = _row_to_parked_candidate(store, row=row)
        if parked is None:
            continue
        candidates_by_key[(owner_id, parked.reason_class)] = parked

    if store.supports_watch_progress_observations():
        for observation in store.list_all_watch_progress_observations(parked_reason=WATCH_NO_PROGRESS_BACKSTOP_REASON):
            subject_task_id = observation.subject_task_id
            if not subject_task_id:
                continue
            subject_task = store.get(subject_task_id)
            if subject_task is None or subject_task.id is None:
                continue
            owner_row = member_owner_rows.get(subject_task.id)
            owner_task = owner_row.owner_task if owner_row is not None else subject_task
            owner_id = owner_task.id
            if owner_id is None:
                continue
            candidates_by_key.setdefault(
                (owner_id, "backstop"),
                ParkedTaskCandidate(
                    owner_task=owner_task,
                    subject_task=subject_task,
                    reason_class="backstop",
                    attention_reason=WATCH_NO_PROGRESS_BACKSTOP_REASON,
                    source="watch_progress",
                ),
            )

    ordered = tuple(
        sorted(
            candidates_by_key.values(),
            key=lambda candidate: candidate.owner_task.id or "",
        )
    )
    return ordered, stale_backstop_cleared


def select_and_clear_parked_tasks(
    store: SqliteTaskStore,
    *,
    config: Config,
    git: Git,
    target_branch: str,
    task_ids: Sequence[str] = (),
    tags: tuple[str, ...] | None = None,
    any_tag: bool = True,
    reason_classes: Sequence[ParkReasonClass] = (),
    select_all: bool = False,
) -> UnstickSelectionResult:
    """Select parked owners from the shared park service and clear eligible ones."""
    candidates, stale_backstop_cleared = discover_parked_tasks(
        store,
        config=config,
        git=git,
        target_branch=target_branch,
    )
    normalized_tags = normalize_tag_filters(tags)
    reason_filter = frozenset(reason_classes)
    selected = _select_targets(
        store,
        candidates=candidates,
        task_ids=task_ids,
        tags=normalized_tags,
        any_tag=any_tag,
        reason_filter=reason_filter,
        select_all=select_all,
    )
    outcomes = tuple(
        _apply_selected_target(
            store,
            git=git,
            target_branch=target_branch,
            selected=target,
        )
        for target in selected
    )
    return UnstickSelectionResult(
        candidates=candidates,
        selected=selected,
        outcomes=outcomes,
        stale_backstop_cleared=stale_backstop_cleared,
    )


def reason_class_for_attention_reason(attention_reason: str | None) -> ParkReasonClass | None:
    """Map a shared parked attention reason to the unstick reason class."""
    if not attention_reason:
        return None
    return _REASON_CLASS_BY_ATTENTION_REASON.get(attention_reason)


def _row_to_parked_candidate(store: SqliteTaskStore, *, row: LineageOwnerRow) -> ParkedTaskCandidate | None:
    action = row.next_action or {}
    attention_reason = action.get("needs_attention_reason")
    if not isinstance(attention_reason, str) or not attention_reason:
        return None
    reason_class = reason_class_for_attention_reason(attention_reason)
    if reason_class is None:
        return None
    subject_task = row.owner_task
    subject_task_id = action.get("subject_task_id")
    if isinstance(subject_task_id, str) and subject_task_id:
        action_subject_task = store.get(subject_task_id)
        if action_subject_task is not None:
            subject_task = action_subject_task
    return ParkedTaskCandidate(
        owner_task=row.owner_task,
        subject_task=subject_task,
        reason_class=reason_class,
        attention_reason=attention_reason,
        source="owner_row",
    )


def _select_targets(
    store: SqliteTaskStore,
    *,
    candidates: Sequence[ParkedTaskCandidate],
    task_ids: Sequence[str],
    tags: tuple[str, ...] | None,
    any_tag: bool,
    reason_filter: frozenset[ParkReasonClass],
    select_all: bool,
) -> tuple[SelectedParkedTask, ...]:
    by_owner_id: dict[str, list[ParkedTaskCandidate]] = defaultdict(list)
    for candidate in candidates:
        owner_id = candidate.owner_task.id
        if owner_id is not None:
            by_owner_id[owner_id].append(candidate)

    selected: list[SelectedParkedTask] = []
    seen_owner_reason: set[tuple[str, ParkReasonClass | None]] = set()

    def _matches_filters(owner_task: DbTask, candidate: ParkedTaskCandidate | None) -> bool:
        if tags is not None and not task_matches_tag_filters(task_tags=owner_task.tags, tag_filters=tags, any_tag=any_tag):
            return False
        if not reason_filter:
            return True
        return candidate is not None and candidate.reason_class in reason_filter

    def _append(owner_task: DbTask, candidate: ParkedTaskCandidate | None) -> None:
        owner_id = owner_task.id
        if owner_id is None:
            return
        reason_key = candidate.reason_class if candidate is not None else None
        dedupe_key = (owner_id, reason_key)
        if dedupe_key in seen_owner_reason:
            return
        seen_owner_reason.add(dedupe_key)
        selected.append(SelectedParkedTask(owner_task=owner_task, current_candidate=candidate))

    if task_ids:
        for task_id in task_ids:
            owner_task = _resolve_owner_task_for_selection(store, candidates=candidates, task_id=task_id)
            if owner_task is None or owner_task.id is None:
                continue
            matching_candidates = [candidate for candidate in by_owner_id.get(owner_task.id, ()) if _matches_filters(owner_task, candidate)]
            if matching_candidates:
                for candidate in matching_candidates:
                    _append(owner_task, candidate)
                continue
            if _matches_filters(owner_task, None):
                _append(owner_task, None)
        return tuple(selected)

    for candidate in candidates:
        if not select_all and tags is None and not reason_filter:
            continue
        if not _matches_filters(candidate.owner_task, candidate):
            continue
        _append(candidate.owner_task, candidate)
    return tuple(selected)


def _resolve_owner_task_for_selection(
    store: SqliteTaskStore,
    *,
    candidates: Sequence[ParkedTaskCandidate],
    task_id: str,
) -> DbTask | None:
    for candidate in candidates:
        if candidate.owner_task.id == task_id or candidate.subject_task.id == task_id:
            return candidate.owner_task
    task = store.get(task_id)
    if task is None:
        return None
    for candidate in candidates:
        if any(member_id == task_id for member_id in _candidate_member_ids(candidate, store=store)):
            return candidate.owner_task
    return task


def _candidate_member_ids(candidate: ParkedTaskCandidate, *, store: SqliteTaskStore) -> tuple[str, ...]:
    owner_id = candidate.owner_task.id
    if owner_id is None or not candidate.owner_task.branch:
        return ()
    return tuple(task.id for task in store.get_tasks_for_branch(candidate.owner_task.branch) if task.id is not None)


def _apply_selected_target(
    store: SqliteTaskStore,
    *,
    git: Git,
    target_branch: str,
    selected: SelectedParkedTask,
) -> UnstickOutcome:
    owner_task = selected.owner_task
    parked = selected.current_candidate
    guard_reason = _skip_reason_for_landed_or_moot(store, git=git, target_branch=target_branch, task=owner_task)
    if guard_reason is not None:
        return UnstickOutcome(
            owner_task=owner_task,
            reason_class=parked.reason_class if parked is not None else None,
            status="skipped",
            detail=guard_reason,
        )
    if parked is None:
        return UnstickOutcome(
            owner_task=owner_task,
            reason_class=None,
            status="skipped",
            detail="not currently parked",
        )
    clear_watch_progress_subject(store, subject_task=parked.subject_task)
    return UnstickOutcome(
        owner_task=owner_task,
        reason_class=parked.reason_class,
        status="rearmed",
        detail=f"cleared {parked.attention_reason}",
    )


def _skip_reason_for_landed_or_moot(
    store: SqliteTaskStore,
    *,
    git: Git,
    target_branch: str,
    task: DbTask,
) -> str | None:
    merge_state = (task.merge_status or "").strip()
    if merge_state == "merged":
        return "already merged"
    if merge_state == "empty":
        return "terminal empty"
    if merge_state == "redundant":
        return "terminal redundant"
    if not task.branch:
        return None
    local_branch_exists = git.branch_exists(task.branch)
    remote_branch_ref = f"origin/{task.branch}"
    remote_branch_exists = git.ref_exists(remote_branch_ref)
    remote_target_ref: str | None = None
    if not local_branch_exists and not remote_branch_exists:
        return "missing branch cannot prove unresolved"
    if not local_branch_exists:
        remote_target_candidate = f"origin/{target_branch}"
        if not git.ref_exists(remote_target_candidate):
            return "missing branch cannot prove unresolved"
        remote_target_ref = remote_target_candidate
    cohorts = build_branch_cohorts_for_tasks(store, [task])
    if not cohorts:
        return None
    result = reconcile_branch_merge_truth(
        git,
        cohorts,
        target_branch=target_branch,
        include_diff_stats=False,
        remote_target_ref=remote_target_ref,
    )[0]
    if result.merge_status == "merged":
        return "already merged"
    if result.merge_status == "empty":
        return "terminal empty"
    if result.merge_status == "redundant":
        return "terminal redundant"
    return None
