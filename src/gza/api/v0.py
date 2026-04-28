"""gza.api.v0 — Experimental public Python API for task querying.

This module is EXPERIMENTAL. Signatures may change without notice.
Pin to a specific gza version if you depend on this API.

Stability: v0 (experimental)
See migration guide at bottom of this module for v0 → v1 path.

Migration path: v0 → v1
========================

What v0 guarantees (experimental):
- Module path ``gza.api.v0`` exists and is importable
- ``GzaClient(project_dir)`` constructor exists
- Method names listed below exist
- ``Task`` dataclass from ``gza.db`` is re-exported

What v0 does NOT guarantee:
- Parameter names or order
- Exact fields on ``Task``
- Exception types beyond ``KeyError`` for not-found lookups
- Thread safety of ``GzaClient``

Planned v1 changes (future, not implemented here):
- Returns ``gza.api.v1.TaskView`` instead of ``db.Task`` directly
- ``GzaClient`` also accepts ``db_path`` directly
- ``get_history`` gains ``since`` and ``until`` datetime parameters
- ``get_lineage_tree()`` returning a structured tree
- ``get_task(id) -> TaskView`` for single-task lookup
- ``get_run_steps(task_id)`` for iteration/step data
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from gza import query as _query
from gza.config import Config
from gza.db import SqliteTaskStore, Task
from gza.task_query import SortSpec, TaskQuery, TaskQueryPresets, TaskQueryService, TaskRow

__all__ = ["GzaClient", "Task", "IncompleteSnapshot"]


@dataclass
class IncompleteSnapshot:
    """Point-in-time view of incomplete tasks."""

    pending: list[Task]
    in_progress: list[Task]

    @property
    def total(self) -> int:
        return len(self.pending) + len(self.in_progress)


class GzaClient:
    """Experimental query client for a gza project.

    Parameters
    ----------
    project_dir:
        Path to the project root (directory containing gza.yaml).
        Defaults to the current working directory if not specified.

    Notes
    -----
    ``GzaClient`` is not thread-safe for concurrent writes and is not
    async-compatible. It is intended for CLI/script use.
    """

    def __init__(self, project_dir: str | Path | None = None) -> None:
        resolved = Path(project_dir).resolve() if project_dir is not None else Path.cwd()
        self._config = Config.load(resolved, discover=True)
        self._store = SqliteTaskStore.from_config(self._config)

    # ------------------------------------------------------------------ #
    # Lineage queries                                                       #
    # ------------------------------------------------------------------ #

    def get_lineage(self, task_id: str) -> list[Task]:
        """Return all tasks in the lineage tree containing task_id as a flat list.

        The result is deduplicated and returned in deterministic pre-order
        traversal from the canonical lineage tree. It starts from the
        resolved lineage root and includes linked descendants.

        Parameters
        ----------
        task_id:
            The numeric database ID of any task in the lineage tree
            (implement, review, or improve). The root is resolved
            automatically.

        Returns
        -------
        list[Task]
            Pre-order flattened list of tasks from the canonical lineage tree.
            Returns a single-element list if the task has no related tasks.

        Raises
        ------
        KeyError
            If task_id does not exist.
        """
        task = self._store.get(task_id)
        if task is None:
            raise KeyError(f"Task {task_id} not found")
        root = _query.resolve_lineage_root(self._store, task)
        tree = _query.build_lineage_tree(self._store, root)
        return _query.flatten_lineage_tree(tree)

    def get_lineage_root(self, task_id: str) -> Task:
        """Resolve the root implementation task for any task in a chain.

        Parameters
        ----------
        task_id:
            Numeric ID of any task.

        Returns
        -------
        Task
            The root task (typically task_type='implement'). Returns the
            task itself if no root can be resolved.

        Raises
        ------
        KeyError
            If task_id does not exist.
        """
        task = self._store.get(task_id)
        if task is None:
            raise KeyError(f"Task {task_id} not found")
        return _query.resolve_lineage_root(self._store, task)

    # ------------------------------------------------------------------ #
    # Incomplete queries                                                    #
    # ------------------------------------------------------------------ #

    def get_incomplete(self) -> IncompleteSnapshot:
        """Return a snapshot of all incomplete (pending + in-progress) tasks.

        Returns
        -------
        IncompleteSnapshot
            Dataclass with ``.pending``, ``.in_progress``, and ``.total``.

        Notes
        -----
        ``pending`` uses raw pending rows from storage (``store.get_pending()``),
        including non-runnable pending tasks (for example ``internal`` or
        dependency-blocked tasks). Use :meth:`get_pending` for runnable pickup
        order semantics.
        """
        service = TaskQueryService(self._store)
        pending = [
            row.task
            for row in service.run(
                TaskQuery(
                    scope="tasks",
                    limit=None,
                    statuses=("pending",),
                    sort=SortSpec(field="pickup_order", descending=False),
                )
            ).rows
            if isinstance(row, TaskRow)
        ]
        in_progress = [
            row.task
            for row in service.run(
                TaskQuery(
                    scope="tasks",
                    limit=None,
                    statuses=("in_progress",),
                    sort=SortSpec(field="created_at", descending=False),
                )
            ).rows
            if isinstance(row, TaskRow)
        ]
        return IncompleteSnapshot(pending=pending, in_progress=in_progress)

    def get_pending(self, limit: int | None = None) -> list[Task]:
        """Return pending tasks in pickup order.

        Parameters
        ----------
        limit:
            Maximum number of tasks to return. None means all.

        Notes
        -----
        This uses default worker pickup semantics: internal tasks and
        dependency-blocked tasks are excluded. Ordering is urgent-first, with
        recently bumped urgent tasks first, then FIFO by creation time.
        """
        service = TaskQueryService(self._store)
        return [
            row.task
            for row in service.run(TaskQueryPresets.queue(limit=limit)).rows
            if isinstance(row, TaskRow)
        ]

    def get_in_progress(self) -> list[Task]:
        """Return in-progress tasks, oldest-started first."""
        service = TaskQueryService(self._store)
        return [
            row.task
            for row in service.run(
                TaskQuery(
                    scope="tasks",
                    limit=None,
                    statuses=("in_progress",),
                    sort=SortSpec(field="created_at", descending=False),
                )
            ).rows
            if isinstance(row, TaskRow)
        ]

    # ------------------------------------------------------------------ #
    # Lookback / history queries                                           #
    # ------------------------------------------------------------------ #

    def get_history(
        self,
        limit: int | None = 10,
        *,
        status: str | None = None,
        task_type: str | None = None,
    ) -> list[Task]:
        """Return completed/failed/unmerged tasks, most recent first.

        Parameters
        ----------
        limit:
            Maximum results. None returns all.
        status:
            Filter by status: 'completed', 'failed', or 'unmerged'.
            None returns all three.
        task_type:
            Filter by type: 'explore', 'plan', 'implement',
            'review', 'improve', 'fix', or 'internal'. None returns all
            non-internal types. Use ``task_type="internal"`` to include
            internal tasks.

        Returns
        -------
        list[Task]
            Tasks ordered by completed_at DESC.
        """
        return _query.query_history(
            self._store,
            _query.HistoryFilter(limit=limit, status=status, task_type=task_type),
        )

    def get_recent_completed(self, limit: int = 15) -> list[Task]:
        """Return the N most recently completed tasks.

        Parameters
        ----------
        limit:
            Number of tasks to return (default 15).
        """
        return _query.query_history(
            self._store,
            _query.HistoryFilter(limit=limit, status="completed"),
        )
