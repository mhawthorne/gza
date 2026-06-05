"""Shared source-task follow-up semantics for plan/explore lineages."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from .db import Task as DbTask, task_id_numeric_key


@dataclass(frozen=True)
class SourceFollowupState:
    """Resolved descendant follow-up state for a completed source task."""

    active_plan_descendant: DbTask | None
    active_implement_descendant: DbTask | None
    has_non_dropped_plan_or_implement_descendant: bool
    has_non_dropped_implement_descendant: bool


def _task_event_time(task: DbTask) -> datetime:
    value = task.completed_at or task.created_at
    if value is None:
        return datetime.min
    if value.tzinfo is not None:
        return value.astimezone(UTC).replace(tzinfo=None)
    return value


def _pick_latest_task(tasks: Sequence[DbTask]) -> DbTask | None:
    if not tasks:
        return None
    return max(tasks, key=lambda item: (_task_event_time(item), task_id_numeric_key(item.id)))


def resolve_source_followup_state(
    task: DbTask,
    *,
    get_children: Callable[[str], Sequence[DbTask]],
) -> SourceFollowupState:
    """Summarize descendant follow-up state for plan/explore tasks.

    Dropped descendants are treated as abandoned follow-up and do not, by
    themselves, suppress the source task. Non-dropped plan/implement
    descendants count as real follow-up, and pending/in-progress descendants
    are surfaced separately as active handoff.
    """

    if task.id is None:
        return SourceFollowupState(
            active_plan_descendant=None,
            active_implement_descendant=None,
            has_non_dropped_plan_or_implement_descendant=False,
            has_non_dropped_implement_descendant=False,
        )

    queue = list(get_children(task.id))
    seen: set[str] = set()
    active_plan_descendants: list[DbTask] = []
    active_implement_descendants: list[DbTask] = []
    has_non_dropped_plan_or_implement_descendant = False
    has_non_dropped_implement_descendant = False

    while queue:
        child = queue.pop(0)
        if child.id is None or child.id in seen:
            continue
        seen.add(child.id)
        queue.extend(get_children(child.id))

        if child.task_type not in {"plan", "implement"}:
            continue
        if child.status == "dropped":
            continue

        has_non_dropped_plan_or_implement_descendant = True
        if child.task_type == "implement":
            has_non_dropped_implement_descendant = True
            if child.status in {"pending", "in_progress"}:
                active_implement_descendants.append(child)
            continue
        if child.status in {"pending", "in_progress"}:
            active_plan_descendants.append(child)

    return SourceFollowupState(
        active_plan_descendant=_pick_latest_task(active_plan_descendants),
        active_implement_descendant=_pick_latest_task(active_implement_descendants),
        has_non_dropped_plan_or_implement_descendant=has_non_dropped_plan_or_implement_descendant,
        has_non_dropped_implement_descendant=has_non_dropped_implement_descendant,
    )


def collect_non_dropped_implement_source_ids(tasks: Sequence[DbTask]) -> set[str]:
    """Return source task IDs referenced by non-dropped implement tasks."""

    source_ids: set[str] = set()
    for task in tasks:
        if task.task_type != "implement" or task.status == "dropped":
            continue
        if task.based_on is not None:
            source_ids.add(task.based_on)
        if task.depends_on is not None:
            source_ids.add(task.depends_on)
    return source_ids


def source_task_needs_implementation_followup(
    task: DbTask,
    followup_state: SourceFollowupState,
    *,
    non_dropped_implement_source_ids: set[str] | None = None,
) -> bool:
    """Return whether a completed plan/explore task still lacks real follow-up."""

    if task.status != "completed" or task.task_type not in {"plan", "explore"}:
        return False
    has_direct_implement_followup = (
        task.id is not None
        and non_dropped_implement_source_ids is not None
        and task.id in non_dropped_implement_source_ids
    )
    if task.task_type == "plan":
        return not (has_direct_implement_followup or followup_state.has_non_dropped_implement_descendant)
    return not (
        has_direct_implement_followup
        or followup_state.has_non_dropped_plan_or_implement_descendant
    )


def source_task_has_implementation_followup(
    task: DbTask,
    followup_state: SourceFollowupState,
    *,
    non_dropped_implement_source_ids: set[str] | None = None,
) -> bool:
    """Return whether a plan task already has implement follow-up.

    This is intentionally independent of task status so callers can preserve the
    actual descendant fact for branchless fallback classification.
    """

    has_direct_implement_followup = (
        task.id is not None
        and non_dropped_implement_source_ids is not None
        and task.id in non_dropped_implement_source_ids
    )
    return has_direct_implement_followup or followup_state.has_non_dropped_implement_descendant


def held_plan_has_blocked_awaiting_review_dependents(
    task: DbTask,
    *,
    get_dependents: Callable[[str], Sequence[DbTask]],
    get_dependency_readiness: Callable[[DbTask], Any],
) -> bool:
    """Return whether a completed held plan still blocks pending dependents awaiting review."""

    if task.id is None or task.task_type != "plan" or task.status != "completed" or task.auto_implement is not False:
        return False

    for dependent in get_dependents(task.id):
        if dependent.status != "pending":
            continue
        readiness = get_dependency_readiness(dependent)
        if (
            getattr(readiness, "ready", False) is False
            and getattr(readiness, "reason", None) == "plan_awaiting_review"
            and (getattr(readiness, "blocking_task_id", None) == task.id or dependent.depends_on == task.id)
        ):
            return True
    return False
