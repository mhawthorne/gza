"""Shared restart-safe watch no-progress backstop helpers."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from .db import SqliteTaskStore, Task as DbTask, WatchProgressObservation, task_id_numeric_key
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


def _resolve_lineage_root(store: SqliteTaskStore, task: DbTask) -> DbTask:
    if task.id is None:
        return task
    graph_nodes: dict[str, DbTask] = {task.id: task}
    visited: set[str] = set()
    stack: list[str] = [parent_id for parent_id in (task.based_on, task.depends_on) if parent_id]
    while stack:
        ancestor_id = stack.pop()
        if ancestor_id in visited:
            continue
        visited.add(ancestor_id)
        ancestor = store.get(ancestor_id)
        if ancestor is None or ancestor.id is None:
            continue
        graph_nodes[ancestor.id] = ancestor
        for parent_id in (ancestor.based_on, ancestor.depends_on):
            if parent_id:
                stack.append(parent_id)
    node_ids = set(graph_nodes)
    roots = [
        candidate
        for candidate in graph_nodes.values()
        if not any(parent_id in node_ids for parent_id in (candidate.based_on, candidate.depends_on) if parent_id)
    ]
    candidates = roots or list(graph_nodes.values())
    return min(
        candidates,
        key=lambda candidate: (
            _normalize_time(candidate.created_at),
            task_id_numeric_key(candidate.id),
        ),
    )


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
    root = _resolve_lineage_root(store, subject_task)
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
    if observation.evidence_fingerprint != candidate.evidence_fingerprint:
        return None
    return build_watch_no_progress_attention_action(
        subject_task_id=candidate.subject_task_id,
        action_type=candidate.action_type,
        streak=observation.streak,
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
    store.delete_watch_progress_subject(
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
        store.delete_watch_progress_subject(
            subject_kind=candidate.subject_kind,
            subject_id=candidate.subject_id,
        )
        existing = None

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
            streak=1,
            parked_reason=None,
            observed_at=datetime.now(UTC),
        )
    )
