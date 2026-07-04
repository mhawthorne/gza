"""Shared restart-safe watch no-progress backstop helpers."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from .db import SqliteTaskStore, Task as DbTask, WatchProgressObservation, task_id_numeric_key
from .lineage import resolve_lineage_root
from .runner import REVIEW_BLOCKER_RESOLUTION_ARTIFACT_KIND

WATCH_NO_PROGRESS_BACKSTOP_REASON = "watch-no-progress-backstop"


@dataclass(frozen=True)
class WatchProgressCandidate:
    """Normalized watch observation input for one subject/action/evidence set."""

    subject_kind: str
    subject_id: str
    subject_task_id: str
    action_type: str
    action_reason: str
    action_task_id: str | None
    action_task_status: str | None
    action_task_started_at: datetime | None
    action_task_running_pid: int | None
    failed_task_id: str | None
    recovery_task_id: str | None
    merge_unit_id: str | None
    merge_unit_state: str | None
    merge_unit_head_sha: str | None
    evidence_fingerprint: str


def _normalize_action_reason(action: dict[str, Any]) -> str:
    reason = action.get("needs_attention_reason")
    if isinstance(reason, str) and reason:
        return reason
    reason = action.get("reason")
    if isinstance(reason, str) and reason:
        return reason
    description = action.get("description")
    if isinstance(description, str):
        return description.strip()
    return ""


def _build_evidence_fingerprint(payload: Mapping[str, object | None]) -> str:
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _normalize_time(value: datetime | None) -> datetime:
    if value is None:
        return datetime.min.replace(tzinfo=UTC)
    if value.tzinfo is not None:
        return value.astimezone(UTC)
    return value.replace(tzinfo=UTC)


def _resolve_subject(
    store: SqliteTaskStore,
    *,
    subject_task: DbTask,
) -> tuple[str, str, str | None, str | None, str | None]:
    if subject_task.id is None:
        raise AssertionError("watch progress subject task must be persisted")
    merge_unit = store.resolve_merge_unit_for_task(subject_task.id)
    if merge_unit is not None:
        return (
            "merge_unit",
            merge_unit.id,
            merge_unit.id,
            merge_unit.state,
            merge_unit.head_sha,
        )
    root = resolve_lineage_root(store, subject_task)
    if root.id is None:
        raise AssertionError("watch progress lineage root must be persisted")
    return ("lineage", root.id, None, None, None)


def _resolve_impl_for_review_progress(store: SqliteTaskStore, *, subject_task: DbTask, action_task: DbTask | None) -> DbTask | None:
    if subject_task.task_type == "implement" and subject_task.id is not None:
        return subject_task
    if action_task is not None:
        if action_task.task_type == "review" and action_task.depends_on:
            candidate = store.get(action_task.depends_on)
            if candidate is not None and candidate.task_type == "implement":
                return candidate
        if action_task.task_type == "internal" and action_task.depends_on:
            candidate = store.get(action_task.depends_on)
            if candidate is not None and candidate.task_type == "implement":
                return candidate
    current = subject_task
    visited: set[str] = set()
    while current.based_on:
        if current.id is not None:
            if current.id in visited:
                break
            visited.add(current.id)
        parent = store.get(current.based_on)
        if parent is None:
            break
        if parent.task_type == "implement":
            return parent
        current = parent
    return None


def _latest_review_resolution_progress_marker(
    store: SqliteTaskStore,
    *,
    subject_task: DbTask,
    action_task: DbTask | None,
) -> dict[str, object | None] | None:
    impl_task = _resolve_impl_for_review_progress(store, subject_task=subject_task, action_task=action_task)
    if impl_task is None or impl_task.id is None:
        return None
    reviews = [
        review
        for review in store.get_reviews_for_task(impl_task.id)
        if review.status == "completed" and review.id is not None
    ]
    if not reviews:
        return None
    latest_review = max(
        reviews,
        key=lambda review: (_normalize_time(review.completed_at or review.created_at), task_id_numeric_key(review.id)),
    )
    review_task_id = latest_review.id
    if review_task_id is None:
        return None
    artifacts = store.list_artifacts(review_task_id, kind=REVIEW_BLOCKER_RESOLUTION_ARTIFACT_KIND)
    if not artifacts:
        return {"review_task_id": review_task_id, "resolution_state": None, "resolution_task_id": None}
    latest_artifact = max(
        artifacts,
        key=lambda artifact: (
            _normalize_time(artifact.created_at),
            artifact.label or "",
            artifact.path,
        ),
    )
    metadata = latest_artifact.metadata or {}
    return {
        "review_task_id": review_task_id,
        "resolution_state": metadata.get("state"),
        "resolution_task_id": metadata.get("source_task_id"),
        "resolution_finding_id": metadata.get("finding_id"),
        "resolution_created_at": _normalize_time(latest_artifact.created_at).isoformat(),
    }


def build_watch_progress_candidate(
    store: SqliteTaskStore,
    *,
    subject_task: DbTask,
    action: dict[str, Any],
    action_task: DbTask | None = None,
    failed_task: DbTask | None = None,
) -> WatchProgressCandidate:
    """Build the durable watch observation for one selected action."""
    if subject_task.id is None:
        raise AssertionError("watch progress subject task must be persisted")
    subject_kind, subject_id, merge_unit_id, merge_unit_state, merge_unit_head_sha = _resolve_subject(
        store,
        subject_task=subject_task,
    )
    action_type = str(action.get("type", ""))
    action_reason = _normalize_action_reason(action)
    recovery_task_id = action.get("recovery_task_id")
    recovery_task_id_str = recovery_task_id if isinstance(recovery_task_id, str) and recovery_task_id else None
    action_task_id = action_task.id if action_task is not None else None
    action_task_status = action_task.status if action_task is not None else None
    action_task_started_at = action_task.started_at if action_task is not None else None
    action_task_running_pid = action_task.running_pid if action_task is not None else None
    failed_task_id = failed_task.id if failed_task is not None else None
    evidence: dict[str, Any] = {
        "subject_task_id": subject_task.id,
        "action_task_id": action_task_id,
        "action_task_status": action_task_status,
        "action_task_started_at": (
            _normalize_time(action_task_started_at).isoformat() if action_task_started_at is not None else None
        ),
        "action_task_running_pid": action_task_running_pid,
        "failed_task_id": failed_task_id,
        "recovery_task_id": recovery_task_id_str,
        "merge_unit_id": merge_unit_id,
        "merge_unit_state": merge_unit_state,
        "merge_unit_head_sha": merge_unit_head_sha,
        "action_type": action_type,
        "action_reason": action_reason,
    }
    review_resolution_marker = _latest_review_resolution_progress_marker(
        store,
        subject_task=subject_task,
        action_task=action_task,
    )
    if review_resolution_marker is not None:
        evidence["review_resolution_marker"] = review_resolution_marker
    return WatchProgressCandidate(
        subject_kind=subject_kind,
        subject_id=subject_id,
        subject_task_id=subject_task.id,
        action_type=action_type,
        action_reason=action_reason,
        action_task_id=action_task_id,
        action_task_status=action_task_status,
        action_task_started_at=action_task_started_at,
        action_task_running_pid=action_task_running_pid,
        failed_task_id=failed_task_id,
        recovery_task_id=recovery_task_id_str,
        merge_unit_id=merge_unit_id,
        merge_unit_state=merge_unit_state,
        merge_unit_head_sha=merge_unit_head_sha,
        evidence_fingerprint=_build_evidence_fingerprint(evidence),
    )


def build_watch_no_progress_attention_action(*, subject_task_id: str, action_type: str, streak: int) -> dict[str, Any]:
    """Build the shared parked attention action for repeated watch no-progress loops."""
    label = action_type.replace("_", " ") if action_type else "action"
    return {
        "type": "skip",
        "description": (
            "SKIP: watch selected the same "
            f"{label} action without durable progress for {streak} cycles; manual intervention required"
        ),
        "needs_attention_reason": WATCH_NO_PROGRESS_BACKSTOP_REASON,
        "subject_task_id": subject_task_id,
    }


def get_active_watch_no_progress_attention(
    store: SqliteTaskStore,
    *,
    candidate: WatchProgressCandidate,
) -> dict[str, Any] | None:
    """Return the persisted parked attention action when the same evidence is still active."""
    observation = store.get_watch_progress_observation(
        subject_kind=candidate.subject_kind,
        subject_id=candidate.subject_id,
        action_type=candidate.action_type,
        action_reason=candidate.action_reason,
    )
    if observation is None:
        return None
    if observation.parked_reason != WATCH_NO_PROGRESS_BACKSTOP_REASON:
        return None
    if _watch_no_progress_park_is_stale(store, observation=observation, candidate=candidate):
        _clear_watch_subject_state(
            store,
            subject_kind=observation.subject_kind,
            subject_id=observation.subject_id,
        )
        return None
    if observation.evidence_fingerprint != candidate.evidence_fingerprint:
        return None
    return build_watch_no_progress_attention_action(
        subject_task_id=candidate.subject_task_id,
        action_type=candidate.action_type,
        streak=observation.streak,
    )


def _watch_no_progress_park_is_stale(
    store: SqliteTaskStore,
    *,
    observation: WatchProgressObservation,
    candidate: WatchProgressCandidate | None = None,
) -> bool:
    subject_task_id = candidate.subject_task_id if candidate is not None else observation.subject_task_id
    if subject_task_id is None:
        return True
    subject_task = store.get(subject_task_id)
    if subject_task is None:
        return True

    action_task_id = candidate.action_task_id if candidate is not None else observation.action_task_id
    if action_task_id is not None:
        action_task = store.get(action_task_id)
        if action_task is None:
            return True
        if action_task.status == "pending" and action_task.started_at is None and action_task.running_pid is None:
            return True

    subject_kind = candidate.subject_kind if candidate is not None else observation.subject_kind
    if subject_kind != "merge_unit":
        return False

    merge_unit_id = candidate.merge_unit_id if candidate is not None else observation.merge_unit_id
    if merge_unit_id is None:
        return True
    merge_unit = store.get_merge_unit(merge_unit_id)
    if merge_unit is None:
        return True
    if merge_unit.state not in {"unmerged", "blocked", "stale"}:
        return True
    return subject_task.status in {"failed", "dropped"}


def reconcile_stale_watch_no_progress_parks(store: SqliteTaskStore) -> int:
    """Delete persisted parked watch observations whose basis no longer holds."""
    cleared: set[tuple[str, str]] = set()
    for observation in store.list_all_watch_progress_observations(parked_reason=WATCH_NO_PROGRESS_BACKSTOP_REASON):
        subject_key = (observation.subject_kind, observation.subject_id)
        if subject_key in cleared:
            continue
        if not _watch_no_progress_park_is_stale(store, observation=observation):
            continue
        _clear_watch_subject_state(
            store,
            subject_kind=observation.subject_kind,
            subject_id=observation.subject_id,
        )
        cleared.add(subject_key)
    return len(cleared)


def _clear_other_subject_actions(
    store: SqliteTaskStore,
    *,
    candidate: WatchProgressCandidate,
) -> WatchProgressObservation | None:
    existing = store.get_watch_progress_observation(
        subject_kind=candidate.subject_kind,
        subject_id=candidate.subject_id,
        action_type=candidate.action_type,
        action_reason=candidate.action_reason,
    )
    same_subject_rows = store.list_watch_progress_observations(
        subject_kind=candidate.subject_kind,
        subject_id=candidate.subject_id,
    )
    if any(
        row.action_type != candidate.action_type or row.action_reason != candidate.action_reason
        for row in same_subject_rows
    ):
        _clear_watch_subject_state(
            store,
            subject_kind=candidate.subject_kind,
            subject_id=candidate.subject_id,
        )
        return None
    return existing


def _clear_watch_subject_state(
    store: SqliteTaskStore,
    *,
    subject_kind: str,
    subject_id: str,
) -> None:
    """Clear all watch-owned persisted state for one subject after durable progress."""
    store.delete_watch_progress_subject(
        subject_kind=subject_kind,
        subject_id=subject_id,
    )
    store.delete_watch_recovery_backoff_subject(
        subject_kind=subject_kind,
        subject_id=subject_id,
    )


def clear_watch_progress_subject(
    store: SqliteTaskStore,
    *,
    subject_task: DbTask,
) -> None:
    """Clear persisted watch observations after durable progress for the subject."""
    if subject_task.id is None:
        return
    subject_kind, subject_id, _merge_unit_id, _merge_unit_state, _merge_unit_head_sha = _resolve_subject(
        store,
        subject_task=subject_task,
    )
    _clear_watch_subject_state(
        store,
        subject_kind=subject_kind,
        subject_id=subject_id,
    )


def observe_watch_progress_and_maybe_park(
    store: SqliteTaskStore,
    *,
    candidate: WatchProgressCandidate,
    no_progress_cycles: int,
) -> dict[str, Any] | None:
    """Persist the observation and return a parked attention action at the threshold."""
    if no_progress_cycles < 1:
        raise AssertionError("watch no-progress threshold must be positive")
    existing = _clear_other_subject_actions(store, candidate=candidate)

    same_evidence = existing is not None and existing.evidence_fingerprint == candidate.evidence_fingerprint
    streak = (existing.streak + 1) if (same_evidence and existing is not None) else 1
    parked_reason = (
        WATCH_NO_PROGRESS_BACKSTOP_REASON
        if streak >= no_progress_cycles
        else None
    )
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
            failed_task_id=candidate.failed_task_id,
            recovery_task_id=candidate.recovery_task_id,
            merge_unit_id=candidate.merge_unit_id,
            merge_unit_state=candidate.merge_unit_state,
            merge_unit_head_sha=candidate.merge_unit_head_sha,
            evidence_fingerprint=candidate.evidence_fingerprint,
            launch_evidence_fingerprint=None,
            streak=streak,
            parked_reason=parked_reason,
            observed_at=datetime.now(UTC),
        )
    )
    if parked_reason is None:
        return None
    return build_watch_no_progress_attention_action(
        subject_task_id=candidate.subject_task_id,
        action_type=candidate.action_type,
        streak=streak,
    )


def observe_selected_watch_action_without_dispatch(
    store: SqliteTaskStore,
    *,
    candidate: WatchProgressCandidate,
    no_progress_cycles: int,
) -> dict[str, Any] | None:
    """Count a re-selected watch action that stayed undispatched and left evidence unchanged."""
    return observe_watch_progress_and_maybe_park(
        store,
        candidate=candidate,
        no_progress_cycles=no_progress_cycles,
    )


def finalize_watch_progress_after_execution(
    store: SqliteTaskStore,
    *,
    before: WatchProgressCandidate,
    after: WatchProgressCandidate,
    no_progress_cycles: int,
) -> dict[str, Any] | None:
    """Record one executed watch action, clearing on progress and counting only true no-op repeats."""
    if before.subject_kind != after.subject_kind or before.subject_id != after.subject_id:
        raise AssertionError("watch progress execution finalizer requires a stable subject")
    if before.evidence_fingerprint != after.evidence_fingerprint:
        _clear_watch_subject_state(
            store,
            subject_kind=after.subject_kind,
            subject_id=after.subject_id,
        )
        return None
    return observe_watch_progress_and_maybe_park(
        store,
        candidate=after,
        no_progress_cycles=no_progress_cycles,
    )


def record_background_watch_execution_start(
    store: SqliteTaskStore,
    *,
    before: WatchProgressCandidate,
    after: WatchProgressCandidate,
) -> None:
    """Persist that watch actually launched detached work without counting it yet."""
    existing = _clear_other_subject_actions(store, candidate=after)
    completed_fingerprint = ""
    streak = 0
    if existing is not None:
        if existing.launch_evidence_fingerprint is None and existing.evidence_fingerprint:
            completed_fingerprint = existing.evidence_fingerprint
            streak = existing.streak
        else:
            completed_fingerprint = existing.evidence_fingerprint
            streak = existing.streak
    store.upsert_watch_progress_observation(
        WatchProgressObservation(
            subject_kind=after.subject_kind,
            subject_id=after.subject_id,
            action_type=after.action_type,
            action_reason=after.action_reason,
            subject_task_id=after.subject_task_id,
            action_task_id=after.action_task_id,
            action_task_status=after.action_task_status,
            action_task_started_at=after.action_task_started_at,
            action_task_running_pid=after.action_task_running_pid,
            failed_task_id=after.failed_task_id,
            recovery_task_id=after.recovery_task_id,
            merge_unit_id=after.merge_unit_id,
            merge_unit_state=after.merge_unit_state,
            merge_unit_head_sha=after.merge_unit_head_sha,
            evidence_fingerprint=completed_fingerprint,
            launch_evidence_fingerprint=before.evidence_fingerprint,
            streak=streak,
            parked_reason=None,
            observed_at=datetime.now(UTC),
        )
    )


def finalize_background_watch_execution(
    store: SqliteTaskStore,
    *,
    candidate: WatchProgressCandidate,
    no_progress_cycles: int,
) -> dict[str, Any] | None:
    """Finalize one previously launched detached action after its outcome becomes observable."""
    observation = store.get_watch_progress_observation(
        subject_kind=candidate.subject_kind,
        subject_id=candidate.subject_id,
        action_type=candidate.action_type,
        action_reason=candidate.action_reason,
    )
    if observation is None or observation.launch_evidence_fingerprint is None:
        return None
    if candidate.evidence_fingerprint != observation.launch_evidence_fingerprint:
        _clear_watch_subject_state(
            store,
            subject_kind=candidate.subject_kind,
            subject_id=candidate.subject_id,
        )
        return None
    prior_completed_fingerprint = observation.evidence_fingerprint or None
    if prior_completed_fingerprint == candidate.evidence_fingerprint:
        streak = observation.streak + 1
    else:
        streak = 1
    parked_reason = WATCH_NO_PROGRESS_BACKSTOP_REASON if streak >= no_progress_cycles else None
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
            failed_task_id=candidate.failed_task_id,
            recovery_task_id=candidate.recovery_task_id,
            merge_unit_id=candidate.merge_unit_id,
            merge_unit_state=candidate.merge_unit_state,
            merge_unit_head_sha=candidate.merge_unit_head_sha,
            evidence_fingerprint=candidate.evidence_fingerprint,
            launch_evidence_fingerprint=None,
            streak=streak,
            parked_reason=parked_reason,
            observed_at=datetime.now(UTC),
        )
    )
    if parked_reason is None:
        return None
    return build_watch_no_progress_attention_action(
        subject_task_id=candidate.subject_task_id,
        action_type=candidate.action_type,
        streak=streak,
    )


def refresh_watch_progress_after_state_change(
    store: SqliteTaskStore,
    *,
    candidate: WatchProgressCandidate,
) -> None:
    """Refresh persisted evidence after durable progress without counting another watch pass."""
    existing = store.get_watch_progress_observation(
        subject_kind=candidate.subject_kind,
        subject_id=candidate.subject_id,
        action_type=candidate.action_type,
        action_reason=candidate.action_reason,
    )
    if existing is None:
        return
    if existing.evidence_fingerprint == candidate.evidence_fingerprint:
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
            failed_task_id=candidate.failed_task_id,
            recovery_task_id=candidate.recovery_task_id,
            merge_unit_id=candidate.merge_unit_id,
            merge_unit_state=candidate.merge_unit_state,
            merge_unit_head_sha=candidate.merge_unit_head_sha,
            evidence_fingerprint=candidate.evidence_fingerprint,
            launch_evidence_fingerprint=None,
            streak=1,
            parked_reason=None,
            observed_at=datetime.now(UTC),
        )
    )
