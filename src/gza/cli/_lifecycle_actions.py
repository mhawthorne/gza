"""Shared lifecycle-action collection and rendering for operator surfaces."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Any

import gza.colors as _colors

from ..colors import pink
from ..console import prompt_available_width, shorten_prompt
from ..db import SqliteTaskStore, Task as DbTask
from ..git import Git
from ..lineage_query import LineageOwnerQuery, _query_lineage_owner_rows_with_context
from ..task_query import normalize_tag_filters
from .advance_engine import classify_advance_action, determine_next_action

ADVANCE_ACTION_ORDER: dict[str, int] = {"merge": 0, "merge_with_followups": 0}


@dataclass(frozen=True)
class LifecycleActionEntry:
    """One visible lifecycle action row for queue/next/watch operator surfaces."""

    owner_task: DbTask
    action_task: DbTask
    action: dict[str, Any]
    description: str


def collect_lifecycle_action_entries(
    store: SqliteTaskStore,
    *,
    config: Any,
    git: Git,
    target_branch: str,
    tags: tuple[str, ...] | None,
    any_tag: bool,
    max_recovery_attempts: int,
    persist_post_merge_rebase_state: bool = True,
) -> list[LifecycleActionEntry]:
    """Return actionable lifecycle rows in deterministic advance order."""
    with store.read_session():
        owner_rows, read_context = _query_lineage_owner_rows_with_context(
            store,
            LineageOwnerQuery(
                limit=None,
                statuses=("completed", "unmerged", "dropped"),
                tags=normalize_tag_filters(tags),
                any_tag=any_tag,
                include_skipped=True,
                exclude_dropped_from_planning=True,
                max_recovery_attempts=max_recovery_attempts,
            ),
            config=config,
            git=git,
            target_branch=target_branch,
            persist_post_merge_rebase_state=persist_post_merge_rebase_state,
        )

    entries: list[LifecycleActionEntry] = []
    for row in owner_rows:
        action_task = row.lifecycle_action_task
        if action_task is None or action_task.status == "failed":
            continue
        action = determine_next_action(
            config,
            store,
            git,
            action_task,
            target_branch,
            max_resume_attempts=max_recovery_attempts,
            persist_post_merge_rebase_state=persist_post_merge_rebase_state,
            read_context=read_context,
        )
        if classify_advance_action(action) != "actionable":
            continue
        entries.append(
            LifecycleActionEntry(
                owner_task=row.owner_task,
                action_task=action_task,
                action=action,
                description=str(action.get("description", "")).strip(),
            )
        )

    entries.sort(
        key=lambda entry: (
            ADVANCE_ACTION_ORDER.get(str(entry.action.get("type", "")), 1),
            1 if entry.action_task.task_type in {"plan", "explore"} else 0,
        )
    )
    return entries


def print_lifecycle_action_entries(console: Any, entries: Iterable[LifecycleActionEntry]) -> None:
    """Render lifecycle action rows using the shared advance preview format."""
    task_id_color = _colors.TASK_COLORS.task_id
    for entry in entries:
        display_task = entry.owner_task
        action_color = _advance_action_color(str(entry.action.get("type", "skip")))
        prompt_display = shorten_prompt(
            display_task.prompt,
            prompt_available_width(prefix=len(display_task.id or "") + 4, suffix=0),
        )
        console.print(f"  [{task_id_color}]{display_task.id}[/{task_id_color}] [{pink}]{prompt_display}[/{pink}]")
        console.print(f"      [{action_color}]→ {entry.description}[/{action_color}]")
        console.print()


def format_cycle_lifecycle_action_summary(
    items: Iterable[tuple[DbTask, Mapping[str, Any]]],
) -> str | None:
    """Build the one-line per-pass watch lifecycle summary."""
    parts = [
        f"{task.id}→{str(action.get('type', 'unknown'))}"
        for task, action in items
        if classify_advance_action(action) == "actionable" and task.id is not None
    ]
    if not parts:
        return None
    return f"Lifecycle actions ({len(parts)}): {', '.join(parts)}"


def _advance_action_color(action_type: str) -> str:
    if action_type in {"merge", "merge_with_followups"}:
        return _colors.STATUS_COLORS.completed
    if action_type in {"create_review", "create_plan_review", "needs_rebase", "create_improve"}:
        return _colors.STATUS_COLORS.in_progress
    if action_type in {"materialize_plan_slices", "create_implement", "resume", "retry", "reconcile"}:
        return _colors.STATUS_COLORS.pending
    return _colors.default_color
