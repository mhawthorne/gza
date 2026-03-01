"""Tests for the gza.query module (history filtering and lineage)."""

from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from gza.db import SqliteTaskStore, Task
from gza.query import (
    HistoryFilter,
    TaskLineageNode,
    get_task_lineage,
    is_lineage_complete,
    query_history,
    query_history_with_lineage,
)


def _make_task(**kwargs) -> Task:
    """Helper: build a minimal Task with sensible defaults."""
    defaults: dict = {
        "id": None,
        "prompt": "test task",
        "status": "completed",
        "task_type": "task",
        "has_commits": False,
        "merge_status": None,
    }
    defaults.update(kwargs)
    return Task(**defaults)


class TestIsLineageComplete:
    """Unit tests for the is_lineage_complete pure function."""

    def test_failed_task_is_incomplete(self):
        task = _make_task(status="failed")
        assert is_lineage_complete(task) is False

    def test_completed_merged_is_complete(self):
        task = _make_task(status="completed", merge_status="merged")
        assert is_lineage_complete(task) is True

    def test_completed_unmerged_is_incomplete(self):
        task = _make_task(status="completed", merge_status="unmerged", has_commits=True)
        assert is_lineage_complete(task) is False

    def test_completed_no_commits_is_complete(self):
        """Non-code tasks (explore/plan/review) have no commits → complete."""
        task = _make_task(status="completed", has_commits=False, merge_status=None)
        assert is_lineage_complete(task) is True

    def test_completed_has_commits_no_merge_status_is_incomplete(self):
        """Code-producing task with no merge tracking → incomplete."""
        task = _make_task(status="completed", has_commits=True, merge_status=None)
        assert is_lineage_complete(task) is False

    def test_unmerged_status_is_incomplete(self):
        """Legacy 'unmerged' status → incomplete."""
        task = _make_task(status="unmerged", has_commits=True, merge_status=None)
        assert is_lineage_complete(task) is False

    def test_pending_status_is_incomplete(self):
        task = _make_task(status="pending")
        assert is_lineage_complete(task) is False


class TestQueryHistory:
    """Integration tests for query_history using an in-memory SqliteTaskStore."""

    def _make_store(self, tmp_path: Path) -> SqliteTaskStore:
        return SqliteTaskStore(tmp_path / "test.db")

    def _add_completed(
        self,
        store: SqliteTaskStore,
        prompt: str,
        task_type: str = "task",
        merge_status: str | None = None,
        has_commits: bool = False,
        days_ago: int = 0,
    ) -> Task:
        task = store.add(prompt, task_type=task_type)
        task.status = "completed"
        task.merge_status = merge_status
        task.has_commits = has_commits
        now = datetime.now(timezone.utc)
        task.completed_at = now - timedelta(days=days_ago)
        task.created_at = now - timedelta(days=days_ago)
        store.update(task)
        return task

    def _add_failed(
        self,
        store: SqliteTaskStore,
        prompt: str,
        days_ago: int = 0,
    ) -> Task:
        task = store.add(prompt, task_type="task")
        task.status = "failed"
        now = datetime.now(timezone.utc)
        task.completed_at = now - timedelta(days=days_ago)
        task.created_at = now - timedelta(days=days_ago)
        store.update(task)
        return task

    def test_incomplete_filters_failed(self, tmp_path: Path):
        store = self._make_store(tmp_path)
        failed = self._add_failed(store, "failed task")
        self._add_completed(store, "merged task", merge_status="merged")

        f = HistoryFilter(incomplete=True, limit=None)
        results = query_history(store, f)
        prompts = [t.prompt for t in results]
        assert "failed task" in prompts
        assert "merged task" not in prompts

    def test_incomplete_filters_unmerged(self, tmp_path: Path):
        store = self._make_store(tmp_path)
        self._add_completed(store, "unmerged task", merge_status="unmerged", has_commits=True)
        self._add_completed(store, "merged task", merge_status="merged")

        f = HistoryFilter(incomplete=True, limit=None)
        results = query_history(store, f)
        prompts = [t.prompt for t in results]
        assert "unmerged task" in prompts
        assert "merged task" not in prompts

    def test_incomplete_excludes_no_commit_completed(self, tmp_path: Path):
        """Non-code tasks (has_commits=False) are considered complete."""
        store = self._make_store(tmp_path)
        self._add_completed(store, "explore task", task_type="explore", has_commits=False)

        f = HistoryFilter(incomplete=True, limit=None)
        results = query_history(store, f)
        assert len(results) == 0

    def test_lookback_days_excludes_old_tasks(self, tmp_path: Path):
        store = self._make_store(tmp_path)
        self._add_completed(store, "old task", days_ago=10)
        self._add_completed(store, "recent task", days_ago=1)

        f = HistoryFilter(lookback_days=5, limit=None)
        results = query_history(store, f)
        prompts = [t.prompt for t in results]
        assert "recent task" in prompts
        assert "old task" not in prompts

    def test_lookback_days_includes_recent_tasks(self, tmp_path: Path):
        store = self._make_store(tmp_path)
        self._add_completed(store, "today task", days_ago=0)
        self._add_completed(store, "week ago task", days_ago=7)

        f = HistoryFilter(lookback_days=3, limit=None)
        results = query_history(store, f)
        prompts = [t.prompt for t in results]
        assert "today task" in prompts
        assert "week ago task" not in prompts

    def test_incomplete_and_lookback_combined(self, tmp_path: Path):
        store = self._make_store(tmp_path)
        # recent + incomplete
        self._add_failed(store, "recent failed", days_ago=1)
        # old + incomplete (excluded by lookback)
        self._add_failed(store, "old failed", days_ago=30)
        # recent + complete (excluded by incomplete filter)
        self._add_completed(store, "recent merged", merge_status="merged")

        f = HistoryFilter(incomplete=True, lookback_days=7, limit=None)
        results = query_history(store, f)
        prompts = [t.prompt for t in results]
        assert "recent failed" in prompts
        assert "old failed" not in prompts
        assert "recent merged" not in prompts

    def test_limit_applied_after_incomplete_filter(self, tmp_path: Path):
        store = self._make_store(tmp_path)
        for i in range(5):
            self._add_failed(store, f"failed task {i}")
        self._add_completed(store, "merged task", merge_status="merged")

        f = HistoryFilter(incomplete=True, limit=3)
        results = query_history(store, f)
        assert len(results) == 3

    def test_flat_query_default_limit(self, tmp_path: Path):
        store = self._make_store(tmp_path)
        for i in range(15):
            self._add_completed(store, f"task {i}")

        f = HistoryFilter(limit=10)
        results = query_history(store, f)
        assert len(results) == 10


class TestGetTaskLineage:
    """Tests for get_task_lineage traversal."""

    def _make_store(self, tmp_path: Path) -> SqliteTaskStore:
        return SqliteTaskStore(tmp_path / "test.db")

    def _add_task(
        self, store: SqliteTaskStore, prompt: str, based_on: int | None = None
    ) -> Task:
        task = store.add(prompt, task_type="task", based_on=based_on)
        task.status = "completed"
        task.completed_at = datetime.now(timezone.utc)
        store.update(task)
        return task

    def test_depth_zero_returns_node_with_no_lineage(self, tmp_path: Path):
        store = self._make_store(tmp_path)
        parent = self._add_task(store, "parent")
        child = self._add_task(store, "child", based_on=parent.id)

        node = get_task_lineage(store, child.id, depth=0)
        assert node.task.id == child.id
        assert node.ancestors == []
        assert node.descendants == []

    def test_depth_one_returns_direct_parent(self, tmp_path: Path):
        store = self._make_store(tmp_path)
        parent = self._add_task(store, "parent")
        child = self._add_task(store, "child", based_on=parent.id)

        node = get_task_lineage(store, child.id, depth=1)
        assert node.task.id == child.id
        assert len(node.ancestors) == 1
        assert node.ancestors[0].task.id == parent.id

    def test_depth_one_returns_direct_children(self, tmp_path: Path):
        store = self._make_store(tmp_path)
        parent = self._add_task(store, "parent")
        child1 = self._add_task(store, "child 1", based_on=parent.id)
        child2 = self._add_task(store, "child 2", based_on=parent.id)

        node = get_task_lineage(store, parent.id, depth=1)
        assert node.task.id == parent.id
        desc_ids = {n.task.id for n in node.descendants}
        assert child1.id in desc_ids
        assert child2.id in desc_ids

    def test_depth_two_traverses_two_levels(self, tmp_path: Path):
        store = self._make_store(tmp_path)
        grandparent = self._add_task(store, "grandparent")
        parent = self._add_task(store, "parent", based_on=grandparent.id)
        child = self._add_task(store, "child", based_on=parent.id)

        node = get_task_lineage(store, child.id, depth=2)
        assert node.task.id == child.id
        assert len(node.ancestors) == 1
        parent_node = node.ancestors[0]
        assert parent_node.task.id == parent.id
        assert len(parent_node.ancestors) == 1
        assert parent_node.ancestors[0].task.id == grandparent.id

    def test_missing_based_on_handled_gracefully(self, tmp_path: Path):
        store = self._make_store(tmp_path)
        task = self._add_task(store, "standalone")
        # No based_on, no children

        node = get_task_lineage(store, task.id, depth=2)
        assert node.task.id == task.id
        assert node.ancestors == []
        assert node.descendants == []

    def test_raises_for_unknown_task_id(self, tmp_path: Path):
        store = self._make_store(tmp_path)
        with pytest.raises(ValueError, match="not found"):
            get_task_lineage(store, 9999, depth=1)


class TestQueryHistoryWithLineage:
    """Tests for query_history_with_lineage."""

    def _make_store(self, tmp_path: Path) -> SqliteTaskStore:
        return SqliteTaskStore(tmp_path / "test.db")

    def _add_completed(
        self,
        store: SqliteTaskStore,
        prompt: str,
        based_on: int | None = None,
    ) -> Task:
        task = store.add(prompt, task_type="task", based_on=based_on)
        task.status = "completed"
        task.completed_at = datetime.now(timezone.utc)
        store.update(task)
        return task

    def test_returns_lineage_nodes(self, tmp_path: Path):
        store = self._make_store(tmp_path)
        parent = self._add_completed(store, "parent")
        child = self._add_completed(store, "child", based_on=parent.id)

        f = HistoryFilter(lineage_depth=1, limit=None)
        nodes = query_history_with_lineage(store, f)
        # Both parent and child appear in history; child has ancestor populated
        child_node = next(n for n in nodes if n.task.id == child.id)
        assert len(child_node.ancestors) == 1
        assert child_node.ancestors[0].task.id == parent.id

    def test_empty_history_returns_empty_list(self, tmp_path: Path):
        store = self._make_store(tmp_path)
        f = HistoryFilter(lineage_depth=1, limit=None)
        nodes = query_history_with_lineage(store, f)
        assert nodes == []
