"""Shared helpers for exposing failed-task recovery alongside queue views."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from ..db import SqliteTaskStore, Task as DbTask, task_id_numeric_key
from ..lineage_query import LineageOwnerQuery, LineageOwnerRow, query_lineage_owner_rows
from ..recovery_engine import FailedRecoveryDecision, decide_failed_task_recovery
from ..task_query import normalize_tag_filters
from .advance_engine import failed_recovery_decision_to_attention_action


@dataclass(frozen=True)
class RecoveryLaneEntry:
    """One visible recovery-lane row for operator-facing queue surfaces."""

    owner_task: DbTask
    task: DbTask
    decision: FailedRecoveryDecision
    attention_action: dict[str, Any] | None = None


def collect_recovery_lane_entries(
    store: SqliteTaskStore,
    *,
    tags: tuple[str, ...] | None,
    any_tag: bool,
    max_recovery_attempts: int,
) -> list[RecoveryLaneEntry]:
    """Return visible recovery-lane entries in deterministic watch order."""
    owner_rows = list(
        query_lineage_owner_rows(
            store,
            LineageOwnerQuery(
                limit=None,
                tags=normalize_tag_filters(tags),
                any_tag=any_tag,
                include_skipped=True,
                exclude_dropped_from_planning=True,
                max_recovery_attempts=max_recovery_attempts,
            ),
        )
    )
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

    entries: list[RecoveryLaneEntry] = []
    for row in failed_rows:
        task = row.recovery_leaf_task
        if task is None or task.id is None:
            continue
        decision = decide_failed_task_recovery(store, task, max_recovery_attempts=max_recovery_attempts)
        if decision.action in {"resume", "retry"}:
            entries.append(RecoveryLaneEntry(owner_task=row.owner_task, task=task, decision=decision))
            continue
        attention_action = failed_recovery_decision_to_attention_action(
            store,
            task,
            decision,
            max_recovery_attempts=max_recovery_attempts,
        )
        if attention_action is None:
            continue
        entries.append(
            RecoveryLaneEntry(
                owner_task=row.owner_task,
                task=task,
                decision=decision,
                attention_action=attention_action,
            )
        )
    return entries


def _recovery_owner_row_sort_key(row: LineageOwnerRow) -> tuple[datetime, int]:
    created_at = (
        row.recovery_leaf_task.created_at
        if row.recovery_leaf_task is not None and row.recovery_leaf_task.created_at is not None
        else datetime.min.replace(tzinfo=UTC)
    )
    return (created_at, task_id_numeric_key(row.owner_task.id))
