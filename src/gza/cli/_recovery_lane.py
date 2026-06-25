"""Shared helpers for exposing failed-task recovery alongside queue views."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from ..db import SqliteTaskStore, Task as DbTask
from ..dispatch_preview import build_dispatch_preview
from ..git import Git
from ..recovery_engine import FailedRecoveryDecision
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
    git: Git | None = None,
    target_branch: str | None = None,
) -> list[RecoveryLaneEntry]:
    """Return visible recovery-lane entries in deterministic watch order."""
    preview = build_dispatch_preview(
        store,
        tags=tags,
        any_tag=any_tag,
        max_recovery_attempts=max_recovery_attempts,
        selection_mode="recovery_only",
        include_pending=False,
        git=git,
        target_branch=target_branch,
    )
    entries: list[RecoveryLaneEntry] = []
    for preview_entry in preview.recovery_entries:
        task = preview_entry.task
        owner_task = preview_entry.owner_task
        decision = preview_entry.decision
        if task.id is None or owner_task is None or decision is None:
            continue
        if decision.action in {"resume", "retry"}:
            entries.append(RecoveryLaneEntry(owner_task=owner_task, task=task, decision=decision))
            continue
        attention_action = failed_recovery_decision_to_attention_action(
            store,
            task,
            decision,
            max_recovery_attempts=max_recovery_attempts,
            read_context=preview.read_context,
        )
        if attention_action is None:
            continue
        entries.append(
            RecoveryLaneEntry(
                owner_task=owner_task,
                task=task,
                decision=decision,
                attention_action=attention_action,
            )
        )
    return entries
