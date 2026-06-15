"""Internal indexed read context shared by lineage and recovery helpers."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Literal

from .db import DependencyMergeUnitResolution, MergeUnit, Task as DbTask


@dataclass
class RecoveryReadContext:
    """Per-command read-only indexes and caches for recovery-heavy helpers."""

    tasks: tuple[DbTask, ...] | None = None
    task_by_id: Mapping[str, DbTask] = field(default_factory=dict)
    based_on_children: Mapping[str, Sequence[DbTask]] = field(default_factory=dict)
    depends_on_children: Mapping[str, Sequence[DbTask]] = field(default_factory=dict)
    root_by_task_id: Mapping[str, DbTask] = field(default_factory=dict)
    merge_units_by_task_id: Mapping[str, MergeUnit] = field(default_factory=dict)
    merge_context: object | None = None
    recovery_snapshots: dict[str, object] = field(default_factory=dict)
    lineage_by_root_task_id: dict[str, tuple[DbTask, ...]] = field(default_factory=dict)
    dependency_completion_by_task_id: dict[str, DbTask | None] = field(default_factory=dict)
    pending_prerequisite_no_work_reconciliations: dict[
        str,
        tuple[DbTask, Literal["empty", "redundant"]],
    ] = field(default_factory=dict)
    allow_reconcile_mutation: bool = True

    def get_task(self, task_id: str | None) -> DbTask | None:
        if task_id is None:
            return None
        return self.task_by_id.get(task_id)

    def failed_tasks(self) -> tuple[DbTask, ...]:
        if self.tasks is None:
            return ()
        return tuple(task for task in self.tasks if task.status == "failed")

    def resolve_merge_unit_for_task(self, task_id: str | None) -> MergeUnit | None:
        if task_id is None:
            return None
        return self.merge_units_by_task_id.get(task_id)

    def resolve_merge_unit_owner_task(self, merge_unit: MergeUnit | None) -> DbTask | None:
        if merge_unit is None:
            return None
        owner = self.get_task(merge_unit.owner_task_id)
        if owner is not None:
            return owner
        return None

    def get_based_on_children(self, task_id: str) -> tuple[DbTask, ...]:
        return tuple(self.based_on_children.get(task_id, ()))

    def get_based_on_children_by_type(self, task_id: str, task_type: str) -> tuple[DbTask, ...]:
        return tuple(child for child in self.based_on_children.get(task_id, ()) if child.task_type == task_type)

    def get_lineage_children(self, task_id: str, *, parent: DbTask | None = None) -> tuple[DbTask, ...]:
        children_by_id: dict[str, DbTask] = {}
        ordered_children: list[DbTask] = []
        for child in self.based_on_children.get(task_id, ()):
            child_id = child.id
            if child_id is not None and child_id in children_by_id:
                continue
            if child_id is not None:
                children_by_id[child_id] = child
            ordered_children.append(child)
        for child in self.depends_on_children.get(task_id, ()):
            child_id = child.id
            if child_id is not None and child_id in children_by_id:
                continue
            if child_id is not None:
                children_by_id[child_id] = child
            ordered_children.append(child)
        if parent is not None:
            from .query import _lineage_child_sort_key

            ordered_children.sort(key=lambda child: _lineage_child_sort_key(parent, child))
        return tuple(ordered_children)

    def resolve_lineage_root(self, task: DbTask) -> DbTask:
        if task.id is None:
            return task
        return self.root_by_task_id.get(task.id, task)

    def build_lineage(self, root_task: DbTask) -> tuple[DbTask, ...]:
        if root_task.id is None:
            return ()
        cached = self.lineage_by_root_task_id.get(root_task.id)
        if cached is not None:
            return cached

        attached_ids: set[str] = {root_task.id}
        lineage: list[DbTask] = [root_task]

        def _populate(parent: DbTask) -> None:
            parent_id = parent.id
            if parent_id is None:
                return
            for child in self.get_lineage_children(parent_id, parent=parent):
                child_id = child.id
                if child_id is None:
                    continue
                if child.depends_on is not None and child.depends_on != parent_id and child.depends_on in attached_ids:
                    continue
                if child_id in attached_ids:
                    continue
                attached_ids.add(child_id)
                lineage.append(child)
                _populate(child)

        _populate(root_task)
        flattened = tuple(lineage)
        self.lineage_by_root_task_id[root_task.id] = flattened
        return flattened

    def resolve_dependency_completion(self, task: DbTask) -> DbTask | None:
        if task.id is None:
            return None
        if task.id in self.dependency_completion_by_task_id:
            return self.dependency_completion_by_task_id[task.id]
        resolved = self._resolve_dependency_completion_uncached(task)
        self.dependency_completion_by_task_id[task.id] = resolved
        return resolved

    def _resolve_dependency_completion_uncached(self, task: DbTask) -> DbTask | None:
        if task.depends_on is None:
            return None

        dep = self.get_task(task.depends_on)
        if dep is None:
            return None

        if dep.status == "completed":
            return dep

        if dep.status not in {"failed", "dropped"} or dep.id is None:
            return None

        visited: set[str] = set()
        queue: list[str] = [dep.id]
        while queue:
            current_id = queue.pop(0)
            if current_id in visited:
                continue
            visited.add(current_id)
            for child in self.get_based_on_children(current_id):
                if child.id is None:
                    continue
                if child.status == "completed":
                    return child
                queue.append(child.id)
        return None

    def resolve_dependency_merge_unit(self, task: DbTask) -> DependencyMergeUnitResolution:
        if task.depends_on is None:
            return DependencyMergeUnitResolution(attached_task=None, merge_unit=None)

        direct_dep = self.get_task(task.depends_on)
        if direct_dep is None or direct_dep.id is None:
            return DependencyMergeUnitResolution(attached_task=None, merge_unit=None)

        resolved_dep = self.resolve_dependency_completion(task)
        candidate_ids: list[str] = [direct_dep.id]
        if resolved_dep is not None and resolved_dep.id is not None and resolved_dep.id != direct_dep.id:
            candidate_ids.append(resolved_dep.id)

        visited: set[str] = set()
        queue: list[str] = list(candidate_ids)
        while queue:
            current_id = queue.pop(0)
            if current_id in visited:
                continue
            visited.add(current_id)
            current_task = direct_dep if current_id == direct_dep.id else self.get_task(current_id)
            if current_task is None or current_task.id is None:
                continue
            merge_unit = self.resolve_merge_unit_for_task(current_task.id)
            if merge_unit is not None:
                return DependencyMergeUnitResolution(attached_task=current_task, merge_unit=merge_unit)
            for child in self.get_based_on_children(current_task.id):
                if child.id is not None and child.id not in visited:
                    queue.append(child.id)

        return DependencyMergeUnitResolution(attached_task=None, merge_unit=None)

    def record_prerequisite_no_work_reconciliation(
        self,
        task: DbTask,
        merge_state: Literal["empty", "redundant"],
    ) -> None:
        if task.id is None:
            return
        self.pending_prerequisite_no_work_reconciliations[task.id] = (task, merge_state)
