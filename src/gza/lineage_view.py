"""Centralized lineage and merge-unit navigation helpers.

New callers that need "owner/original/latest/all" lineage navigation should
prefer :class:`LineageView` instead of open-coding task walks. This v1 seam
intentionally wraps existing primitives; the broader consolidation of scattered
resolvers is deferred to v0.6.0/v0.7.0.
"""

from __future__ import annotations

import logging
from collections.abc import Iterable
from datetime import UTC, datetime

from .db import MergeUnit, SqliteTaskStore, Task as DbTask, task_id_numeric_key

_LOG = logging.getLogger(__name__)


def _normalize_event_time(value: datetime | None) -> datetime:
    if value is None:
        return datetime.min.replace(tzinfo=UTC)
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _task_event_time(task: DbTask) -> datetime:
    return _normalize_event_time(task.completed_at or task.created_at)


class LineageView:
    """Small seam for merge-unit lineage navigation."""

    def __init__(self, store: SqliteTaskStore, task_or_merge_unit: DbTask | MergeUnit) -> None:
        self._store = store
        self._task = task_or_merge_unit if isinstance(task_or_merge_unit, DbTask) else None
        self._merge_unit = task_or_merge_unit if isinstance(task_or_merge_unit, MergeUnit) else None

    def owner(self) -> DbTask | None:
        """Return the latest successful implementation attempt for this lineage."""
        successful = [task for task in self.all("implement") if self._is_successful_implement(task)]
        if len(successful) > 1:
            _LOG.warning(
                "LineageView.owner found multiple successful implement tasks for merge unit %s: %s",
                self._merge_unit_id(),
                ", ".join(task.id or "<unknown>" for task in successful),
            )
        if successful:
            return successful[-1]
        representative = self._representative_implement()
        if representative is not None and self._is_successful_implement(representative):
            return representative
        return None

    def original(self) -> DbTask | None:
        """Return the earliest implementation attempt for this lineage."""
        implementations = self.all("implement")
        return implementations[0] if implementations else None

    def latest(self, task_type: str | None = None) -> DbTask | None:
        """Return the latest task overall, or the latest task of a given type."""
        tasks = self._sorted_tasks(self._all_tasks() if task_type is None else self.all(task_type))
        return tasks[-1] if tasks else None

    def all(self, task_type: str) -> list[DbTask]:
        """Return all tasks of ``task_type`` ordered by lineage event time."""
        if task_type == "review":
            return self._all_reviews()
        return self._sorted_tasks(task for task in self._all_tasks() if task.task_type == task_type)

    def _all_reviews(self) -> list[DbTask]:
        review_by_id: dict[str, DbTask] = {}
        for task in self._all_tasks():
            if task.task_type == "review" and task.id is not None:
                review_by_id.setdefault(task.id, task)
        for impl in self.all("implement"):
            if impl.id is None:
                continue
            for review in self._store.get_reviews_for_task(impl.id):
                if review.id is not None:
                    review_by_id.setdefault(review.id, review)
        return self._sorted_tasks(review_by_id.values())

    def _all_tasks(self) -> list[DbTask]:
        if self._merge_unit is not None:
            return self._store.list_tasks_for_merge_unit(self._merge_unit.id)
        if self._task is None:
            return []
        if self._task.id is None:
            return [self._task]
        unit = self._store.resolve_merge_unit_for_task(self._task.id)
        if unit is None:
            unit = self._store.get_or_create_merge_unit_for_task(self._task)
        if unit is None:
            return [self._task]
        self._merge_unit = unit
        return self._store.list_tasks_for_merge_unit(unit.id)

    def _representative_implement(self) -> DbTask | None:
        unit = self._merge_unit
        if unit is None and self._task is not None and self._task.id is not None:
            unit = self._store.resolve_merge_unit_for_task(self._task.id)
            if unit is None:
                unit = self._store.get_or_create_merge_unit_for_task(self._task)
            self._merge_unit = unit
        if unit is None:
            return self._task if self._task is not None and self._task.task_type == "implement" else None
        representative = self._store.resolve_merge_unit_representative_task(unit, require_actionable=True)
        if representative is not None and representative.task_type == "implement":
            return representative
        return None

    def _merge_unit_id(self) -> str:
        if self._merge_unit is not None:
            return self._merge_unit.id
        if self._task is not None:
            return self._task.id or "<unattached>"
        return "<unknown>"

    @staticmethod
    def _sorted_tasks(tasks: Iterable[DbTask]) -> list[DbTask]:
        return sorted(
            list(tasks),
            key=lambda task: (_task_event_time(task), task_id_numeric_key(task.id)),
        )

    @staticmethod
    def _is_successful_implement(task: DbTask) -> bool:
        return task.task_type == "implement" and (task.status in {"completed", "merged"} or task.merge_status == "merged")
