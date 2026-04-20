"""Tests for the gza.query module (history filtering and lineage)."""

from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from gza.db import SqliteTaskStore, Task
from gza.query import (
    HistoryFilter,
    get_task_lineage,
    is_lineage_complete,
    query_incomplete,
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
        now = datetime.now(UTC)
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
        now = datetime.now(UTC)
        task.completed_at = now - timedelta(days=days_ago)
        task.created_at = now - timedelta(days=days_ago)
        store.update(task)
        return task

    def test_incomplete_filters_failed(self, tmp_path: Path):
        store = self._make_store(tmp_path)
        self._add_failed(store, "failed task")
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

    def test_days_excludes_old_tasks(self, tmp_path: Path):
        store = self._make_store(tmp_path)
        self._add_completed(store, "old task", days_ago=10)
        self._add_completed(store, "recent task", days_ago=1)

        f = HistoryFilter(days=5, limit=None)
        results = query_history(store, f)
        prompts = [t.prompt for t in results]
        assert "recent task" in prompts
        assert "old task" not in prompts

    def test_days_includes_recent_tasks(self, tmp_path: Path):
        store = self._make_store(tmp_path)
        self._add_completed(store, "today task", days_ago=0)
        self._add_completed(store, "week ago task", days_ago=7)

        f = HistoryFilter(days=3, limit=None)
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

        f = HistoryFilter(incomplete=True, days=7, limit=None)
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
        task.completed_at = datetime.now(UTC)
        store.update(task)
        return task

    def test_depth_zero_returns_only_root(self, tmp_path: Path):
        store = self._make_store(tmp_path)
        parent = self._add_task(store, "parent")
        child = self._add_task(store, "child", based_on=parent.id)

        node = get_task_lineage(store, child.id, depth=0)
        assert node.task.id == parent.id
        assert node.children == []

    def test_depth_one_returns_direct_children(self, tmp_path: Path):
        store = self._make_store(tmp_path)
        parent = self._add_task(store, "parent")
        child = self._add_task(store, "child", based_on=parent.id)

        node = get_task_lineage(store, child.id, depth=1)
        assert node.task.id == parent.id
        assert len(node.children) == 1
        assert node.children[0].task.id == child.id

    def test_depth_two_traverses_two_levels(self, tmp_path: Path):
        store = self._make_store(tmp_path)
        grandparent = self._add_task(store, "grandparent")
        parent = self._add_task(store, "parent", based_on=grandparent.id)
        child = self._add_task(store, "child", based_on=parent.id)

        node = get_task_lineage(store, child.id, depth=2)
        assert node.task.id == grandparent.id
        assert len(node.children) == 1
        parent_node = node.children[0]
        assert parent_node.task.id == parent.id
        assert len(parent_node.children) == 1
        assert parent_node.children[0].task.id == child.id

    def test_raises_for_unknown_task_id(self, tmp_path: Path):
        store = self._make_store(tmp_path)
        with pytest.raises(KeyError, match="not found"):
            get_task_lineage(store, "test-project-zzz", depth=1)


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
        task.completed_at = datetime.now(UTC)
        store.update(task)
        return task

    def test_returns_lineage_nodes(self, tmp_path: Path):
        store = self._make_store(tmp_path)
        parent = self._add_completed(store, "parent")
        child = self._add_completed(store, "child", based_on=parent.id)

        f = HistoryFilter(lineage_depth=1, limit=None)
        nodes = query_history_with_lineage(store, f)
        assert len(nodes) == 1
        assert nodes[0].task.id == parent.id
        assert len(nodes[0].children) == 1
        assert nodes[0].children[0].task.id == child.id

    def test_empty_history_returns_empty_list(self, tmp_path: Path):
        store = self._make_store(tmp_path)
        f = HistoryFilter(lineage_depth=1, limit=None)
        nodes = query_history_with_lineage(store, f)
        assert nodes == []


class TestQueryIncomplete:
    """Tests for unresolved-lineage query behavior."""

    def _store(self, tmp_path: Path) -> SqliteTaskStore:
        return SqliteTaskStore(tmp_path / "test.db")

    def _complete(self, task: Task, *, merge_status: str | None = None) -> None:
        task.status = "completed"
        task.completed_at = datetime.now(UTC)
        task.merge_status = merge_status
        if task.task_type in {"task", "implement", "improve", "rebase"}:
            task.has_commits = True

    def _fail(self, task: Task) -> None:
        task.status = "failed"
        task.completed_at = datetime.now(UTC)

    def test_merged_root_hides_completed_review_and_improve(self, tmp_path: Path):
        store = self._store(tmp_path)

        root = store.add("implement root", task_type="implement")
        self._complete(root, merge_status="merged")
        store.update(root)
        assert root.id is not None

        review = store.add("review", task_type="review", based_on=root.id, depends_on=root.id)
        self._complete(review, merge_status="unmerged")
        store.update(review)
        assert review.id is not None

        improve = store.add("improve", task_type="improve", based_on=root.id, depends_on=review.id, same_branch=True)
        self._complete(improve, merge_status="unmerged")
        store.update(improve)

        lineages = query_incomplete(store, HistoryFilter(limit=None))
        assert lineages == []

    def test_failed_improve_under_merged_root_remains_unresolved(self, tmp_path: Path):
        store = self._store(tmp_path)

        root = store.add("implement root", task_type="implement")
        self._complete(root, merge_status="merged")
        store.update(root)
        assert root.id is not None

        improve = store.add("improve failed", task_type="improve", based_on=root.id, same_branch=True)
        self._fail(improve)
        store.update(improve)
        assert improve.id is not None

        lineages = query_incomplete(store, HistoryFilter(limit=None))
        assert len(lineages) == 1
        unresolved_ids = {task.id for task in lineages[0].unresolved_tasks}
        assert improve.id in unresolved_ids
        assert root.id not in unresolved_ids

    def test_unmerged_root_keeps_unresolved_completed_improve_descendant_visible(self, tmp_path: Path):
        store = self._store(tmp_path)

        root = store.add("implement root", task_type="implement")
        self._complete(root, merge_status="unmerged")
        store.update(root)
        assert root.id is not None

        improve = store.add("improve done", task_type="improve", based_on=root.id, same_branch=True)
        self._complete(improve, merge_status="unmerged")
        store.update(improve)

        lineages = query_incomplete(store, HistoryFilter(limit=None))
        assert len(lineages) == 1
        unresolved_ids = {task.id for task in lineages[0].unresolved_tasks}
        assert unresolved_ids == {root.id, improve.id}

        child_ids = {child.task.id for child in lineages[0].tree.children}
        assert improve.id in child_ids

    def test_retry_chain_failed_failed_completed_keeps_only_latest_unresolved(self, tmp_path: Path):
        store = self._store(tmp_path)

        first = store.add("first attempt", task_type="implement")
        self._fail(first)
        store.update(first)
        assert first.id is not None

        second = store.add("second attempt", task_type="implement", based_on=first.id)
        self._fail(second)
        store.update(second)
        assert second.id is not None

        third = store.add("third attempt", task_type="implement", based_on=second.id)
        self._complete(third, merge_status="unmerged")
        store.update(third)
        assert third.id is not None

        lineages = query_incomplete(store, HistoryFilter(limit=None))
        assert len(lineages) == 1
        unresolved_ids = {task.id for task in lineages[0].unresolved_tasks}
        assert unresolved_ids == {third.id}

    def test_legacy_unmerged_status_task_remains_visible(self, tmp_path: Path):
        store = self._store(tmp_path)

        root = store.add("legacy unmerged root", task_type="implement")
        root.status = "unmerged"
        root.completed_at = datetime.now(UTC)
        root.has_commits = True
        store.update(root)
        assert root.id is not None

        lineages = query_incomplete(store, HistoryFilter(limit=None))
        assert len(lineages) == 1
        assert lineages[0].root.id == root.id
        unresolved_ids = {task.id for task in lineages[0].unresolved_tasks}
        assert unresolved_ids == {root.id}

    def test_dropped_root_task_remains_visible(self, tmp_path: Path):
        store = self._store(tmp_path)

        root = store.add("dropped root", task_type="implement")
        root.status = "dropped"
        root.completed_at = datetime.now(UTC)
        root.has_commits = True
        store.update(root)
        assert root.id is not None

        lineages = query_incomplete(store, HistoryFilter(limit=None))
        assert len(lineages) == 1
        assert lineages[0].root.id == root.id
        unresolved_ids = {task.id for task in lineages[0].unresolved_tasks}
        assert unresolved_ids == {root.id}

    def test_branching_retry_lineage_keeps_all_unresolved_siblings_under_root(self, tmp_path: Path):
        store = self._store(tmp_path)

        root = store.add("root failed", task_type="implement")
        self._fail(root)
        store.update(root)
        assert root.id is not None

        completed_retry = store.add("retry completed", task_type="implement", based_on=root.id)
        self._complete(completed_retry, merge_status="unmerged")
        store.update(completed_retry)
        assert completed_retry.id is not None

        failed_sibling = store.add("retry failed sibling", task_type="implement", based_on=root.id)
        self._fail(failed_sibling)
        store.update(failed_sibling)
        assert failed_sibling.id is not None

        lineages = query_incomplete(store, HistoryFilter(limit=None))
        assert len(lineages) == 1
        lineage = lineages[0]
        assert lineage.root.id == root.id
        unresolved_ids = {task.id for task in lineage.unresolved_tasks}
        assert unresolved_ids == {completed_retry.id, failed_sibling.id}
        child_ids = {child.task.id for child in lineage.tree.children}
        assert {completed_retry.id, failed_sibling.id}.issubset(child_ids)

    def test_completed_rebase_under_merged_root_is_hidden(self, tmp_path: Path):
        store = self._store(tmp_path)

        root = store.add("implement root", task_type="implement")
        self._complete(root, merge_status="merged")
        store.update(root)
        assert root.id is not None

        rebase = store.add("rebase done", task_type="rebase", based_on=root.id, same_branch=True)
        self._complete(rebase, merge_status="unmerged")
        store.update(rebase)

        lineages = query_incomplete(store, HistoryFilter(limit=None))
        assert lineages == []

    def test_completed_no_commit_plan_root_is_excluded(self, tmp_path: Path):
        store = self._store(tmp_path)

        plan = store.add("plan complete", task_type="plan")
        plan.status = "completed"
        plan.completed_at = datetime.now(UTC)
        plan.has_commits = False
        plan.merge_status = None
        store.update(plan)

        lineages = query_incomplete(store, HistoryFilter(limit=None))
        assert lineages == []

    def test_failed_rebase_under_merged_root_remains_visible(self, tmp_path: Path):
        store = self._store(tmp_path)

        root = store.add("implement root", task_type="implement")
        self._complete(root, merge_status="merged")
        store.update(root)
        assert root.id is not None

        rebase = store.add("rebase failed", task_type="rebase", based_on=root.id, same_branch=True)
        self._fail(rebase)
        store.update(rebase)
        assert rebase.id is not None

        lineages = query_incomplete(store, HistoryFilter(limit=None))
        assert len(lineages) == 1
        unresolved_ids = {task.id for task in lineages[0].unresolved_tasks}
        assert unresolved_ids == {rebase.id}

    def test_completed_unmerged_root_superseded_by_merged_retry_is_hidden(self, tmp_path: Path):
        """Regression: completed-but-unmerged root should be suppressed once a
        later same-lineage retry has merged."""
        store = self._store(tmp_path)

        first = store.add("first attempt", task_type="implement")
        self._complete(first, merge_status="unmerged")
        store.update(first)
        assert first.id is not None

        second = store.add("second attempt", task_type="implement", based_on=first.id)
        self._complete(second, merge_status="merged")
        store.update(second)
        assert second.id is not None

        lineages = query_incomplete(store, HistoryFilter(limit=None))
        assert lineages == []

    def test_completed_unmerged_chain_resolved_by_descendant_merge(self, tmp_path: Path):
        """Regression: completed-unmerged ancestors should be suppressed when a
        deeper descendant has merged (multi-hop rollup)."""
        store = self._store(tmp_path)

        first = store.add("first", task_type="implement")
        self._complete(first, merge_status="unmerged")
        store.update(first)
        assert first.id is not None

        second = store.add("second", task_type="implement", based_on=first.id)
        self._complete(second, merge_status="unmerged")
        store.update(second)
        assert second.id is not None

        third = store.add("third", task_type="implement", based_on=second.id)
        self._complete(third, merge_status="merged")
        store.update(third)

        lineages = query_incomplete(store, HistoryFilter(limit=None))
        assert lineages == []

    def test_completed_unmerged_root_with_only_completed_unmerged_retry_remains(self, tmp_path: Path):
        """Negative: an unmerged retry does not supersede an unmerged ancestor."""
        store = self._store(tmp_path)

        first = store.add("first", task_type="implement")
        self._complete(first, merge_status="unmerged")
        store.update(first)
        assert first.id is not None

        second = store.add("second", task_type="implement", based_on=first.id)
        self._complete(second, merge_status="unmerged")
        store.update(second)
        assert second.id is not None

        lineages = query_incomplete(store, HistoryFilter(limit=None))
        assert len(lineages) == 1
        assert lineages[0].root.id == first.id
        unresolved_ids = {task.id for task in lineages[0].unresolved_tasks}
        # Both tasks remain unresolved, so both should stay visible in the
        # displayed attention rows under the shared root lineage.
        assert unresolved_ids == {first.id, second.id}

    def test_failed_root_is_suppressed_by_completed_descendant_regardless_of_type(self, tmp_path: Path):
        store = self._store(tmp_path)

        root = store.add("failed implement", task_type="implement")
        self._fail(root)
        store.update(root)
        assert root.id is not None

        improve = store.add("completed improve", task_type="improve", based_on=root.id, same_branch=True)
        self._complete(improve, merge_status="merged")
        store.update(improve)

        lineages = query_incomplete(store, HistoryFilter(limit=None))
        assert lineages == []

    def test_unmerged_root_is_not_suppressed_by_merged_improve_descendant(self, tmp_path: Path):
        store = self._store(tmp_path)

        root = store.add("unmerged implement", task_type="implement")
        self._complete(root, merge_status="unmerged")
        store.update(root)
        assert root.id is not None

        improve = store.add("merged improve", task_type="improve", based_on=root.id, same_branch=True)
        self._complete(improve, merge_status="merged")
        store.update(improve)

        lineages = query_incomplete(store, HistoryFilter(limit=None))
        assert len(lineages) == 1
        assert lineages[0].root.id == root.id
        unresolved_ids = {task.id for task in lineages[0].unresolved_tasks}
        assert unresolved_ids == {root.id}
