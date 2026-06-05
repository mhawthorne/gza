"""Tests for the gza.query module (history filtering and lineage)."""

from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from gza.db import MergeUnit, SqliteTaskStore, Task
from gza.lineage_query import LineageOwnerSnapshot, is_lineage_resolved
from gza.query import (
    _classify_child_relationship,
    _resolve_effective_shared_branch_retry_head,
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


class TestClassifyChildRelationship:
    """Unit tests for lineage child relationship classification."""

    def test_rebase_child_of_rebase_parent_is_labeled_rebase(self):
        parent = _make_task(id="gza-1", task_type="rebase")
        child = _make_task(id="gza-2", task_type="rebase", based_on="gza-1")

        assert _classify_child_relationship(parent, child) == "rebase"

    def test_explicit_resume_recovery_origin_wins_over_session_heuristic(self):
        parent = _make_task(id="gza-1", task_type="implement", session_id=None)
        child = _make_task(
            id="gza-2",
            task_type="implement",
            based_on="gza-1",
            recovery_origin="resume",
            session_id="sess-child",
        )

        assert _classify_child_relationship(parent, child) == "resume"

    def test_explicit_retry_recovery_origin_wins_over_session_heuristic(self):
        parent = _make_task(id="gza-1", task_type="implement", session_id="sess-parent")
        child = _make_task(
            id="gza-2",
            task_type="implement",
            based_on="gza-1",
            recovery_origin="retry",
            session_id="sess-parent",
        )

        assert _classify_child_relationship(parent, child) == "retry"

    def test_manual_same_type_follow_up_is_not_mislabeled_as_recovery(self):
        parent = _make_task(id="gza-1", task_type="implement", session_id="sess-parent")
        child = _make_task(
            id="gza-2",
            task_type="implement",
            based_on="gza-1",
            recovery_origin="manual",
            session_id="sess-parent",
        )

        assert _classify_child_relationship(parent, child) == "implement"


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

    def test_store_merged_unit_overrides_legacy_unmerged_status(self, tmp_path: Path):
        store = SqliteTaskStore(tmp_path / "test.db")
        task = store.add("legacy unmerged status but merged unit", task_type="implement")
        store.mark_completed(task, has_commits=True, branch="feature/legacy-unmerged-status")
        assert task.id is not None

        unit = store.resolve_merge_unit_for_task(task.id)
        assert unit is not None
        store.set_merge_unit_state(unit.id, "merged")

        task = store.get(task.id)
        assert task is not None
        task.status = "unmerged"
        task.merge_status = None
        store.update(task)

        refreshed = store.get(task.id)
        assert refreshed is not None
        assert is_lineage_complete(refreshed, store=store) is True

    def test_completed_uses_merge_unit_state_when_store_and_target_are_provided(self, tmp_path: Path):
        store = SqliteTaskStore(tmp_path / "test.db")

        task = store.add("Implementation task", task_type="implement")
        task.status = "completed"
        task.completed_at = datetime.now(UTC)
        task.has_commits = True
        task.branch = "feature/unit-state"
        task.merge_status = "unmerged"
        store.update(task)
        assert task.id is not None

        unit = store.create_merge_unit(
            source_branch=task.branch,
            target_branch="release",
            owner_task_id=task.id,
            state="unmerged",
        )
        store.attach_task_to_merge_unit(task.id, unit.id, "owner")
        unit = store.resolve_merge_unit_for_task(task.id)
        assert unit is not None
        assert unit.target_branch == "release"
        store.set_merge_unit_state(unit.id, "merged")

        stale_row = store.get(task.id)
        assert stale_row is not None
        stale_row.merge_status = "unmerged"
        store.update(stale_row)

        refreshed = store.get(task.id)
        assert refreshed is not None
        assert is_lineage_complete(refreshed) is False
        assert is_lineage_complete(refreshed, store=store, target_branch="release") is True

    def test_store_merged_unit_does_not_override_failed_status(self, tmp_path: Path):
        store = SqliteTaskStore(tmp_path / "test.db")
        task = store.add("failed task with merged unit", task_type="implement")
        store.mark_completed(task, has_commits=True, branch="feature/failed-task-merged-unit")
        task = store.get(task.id)
        assert task is not None
        task.status = "failed"
        task.completed_at = datetime.now(UTC)
        store.update(task)
        assert task.id is not None

        unit = store.resolve_merge_unit_for_task(task.id)
        assert unit is not None
        store.set_merge_unit_state(unit.id, "merged")

        refreshed = store.get(task.id)
        assert refreshed is not None
        assert is_lineage_complete(refreshed, store=store) is False

    def test_pending_status_is_incomplete(self):
        task = _make_task(status="pending")
        assert is_lineage_complete(task) is False

    def test_store_merge_unit_state_overrides_stale_task_row(self, tmp_path: Path):
        store = SqliteTaskStore(tmp_path / "test.db")
        task = store.add("merged by unit", task_type="implement")
        store.mark_completed(task, has_commits=True, branch="feature/merged-by-unit")
        assert task.id is not None

        unit = store.resolve_merge_unit_for_task(task.id)
        assert unit is not None
        store.set_merge_unit_state(unit.id, "merged")

        task = store.get(task.id)
        assert task is not None
        task.merge_status = "unmerged"
        store.update(task)

        refreshed = store.get(task.id)
        assert refreshed is not None
        assert is_lineage_complete(refreshed, store=store) is True


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

    def test_failed_status_and_lookback_combined(self, tmp_path: Path):
        store = self._make_store(tmp_path)
        # recent + failed
        self._add_failed(store, "recent failed", days_ago=1)
        # old + failed (excluded by lookback)
        self._add_failed(store, "old failed", days_ago=30)
        # recent + completed (excluded by failed status filter)
        self._add_completed(store, "recent merged", merge_status="merged")

        f = HistoryFilter(status="failed", days=7, limit=None)
        results = query_history(store, f)
        prompts = [t.prompt for t in results]
        assert "recent failed" in prompts
        assert "old failed" not in prompts
        assert "recent merged" not in prompts

    def test_failed_status_respects_limit(self, tmp_path: Path):
        store = self._make_store(tmp_path)
        for i in range(5):
            self._add_failed(store, f"failed task {i}")
        self._add_completed(store, "merged task", merge_status="merged")

        f = HistoryFilter(status="failed", limit=3)
        results = query_history(store, f)
        assert len(results) == 3

    def test_flat_query_default_limit(self, tmp_path: Path):
        store = self._make_store(tmp_path)
        for i in range(15):
            self._add_completed(store, f"task {i}")

        f = HistoryFilter(limit=10)
        results = query_history(store, f)
        assert len(results) == 10

    def test_negative_filters_apply_to_history_query(self, tmp_path: Path):
        store = self._make_store(tmp_path)

        keep = store.add("keep history task", task_type="implement", tags=("release",))
        keep.status = "completed"
        keep.completed_at = datetime.now(UTC)
        store.update(keep)

        excluded_status = store.add("exclude status history task", task_type="implement", tags=("release",))
        excluded_status.status = "failed"
        excluded_status.completed_at = datetime.now(UTC)
        store.update(excluded_status)

        excluded_type = store.add("exclude type history task", task_type="plan", tags=("release",))
        excluded_type.status = "completed"
        excluded_type.completed_at = datetime.now(UTC)
        store.update(excluded_type)

        excluded_tag = store.add("exclude tag history task", task_type="implement", tags=("release", "blocked"))
        excluded_tag.status = "completed"
        excluded_tag.completed_at = datetime.now(UTC)
        store.update(excluded_tag)

        f = HistoryFilter(
            limit=None,
            status="completed",
            status_not="failed",
            task_type_not="plan",
            tags=("release",),
            tags_not=("blocked",),
        )
        results = query_history(store, f)

        assert [task.prompt for task in results] == ["keep history task"]

    def test_unmerged_status_not_uses_merge_chain_semantics(self, tmp_path: Path):
        store = self._make_store(tmp_path)

        semantic_unmerged = self._add_completed(
            store,
            "semantic unmerged task",
            task_type="implement",
            merge_status="unmerged",
            has_commits=True,
        )
        merged = self._add_completed(
            store,
            "merged task",
            task_type="implement",
            merge_status="merged",
            has_commits=True,
        )

        legacy_unmerged = store.add("legacy unmerged task", task_type="implement")
        legacy_unmerged.status = "unmerged"
        legacy_unmerged.completed_at = datetime.now(UTC)
        legacy_unmerged.has_commits = True
        store.update(legacy_unmerged)

        positive = query_history(store, HistoryFilter(limit=None, status="unmerged"))
        positive_ids = {task.id for task in positive}
        assert semantic_unmerged.id in positive_ids
        assert legacy_unmerged.id in positive_ids
        assert merged.id not in positive_ids

        negative = query_history(store, HistoryFilter(limit=None, status_not="unmerged"))
        negative_ids = [task.id for task in negative]
        assert semantic_unmerged.id not in negative_ids
        assert legacy_unmerged.id not in negative_ids
        assert merged.id in negative_ids

        both = query_history(
            store,
            HistoryFilter(limit=None, status="unmerged", status_not="unmerged"),
        )
        assert both == []


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

    def test_failed_improve_under_merged_root_is_suppressed(self, tmp_path: Path):
        store = self._store(tmp_path)

        root = store.add("implement root", task_type="implement")
        self._complete(root, merge_status="merged")
        store.update(root)
        assert root.id is not None

        improve = store.add("improve failed", task_type="improve", based_on=root.id, same_branch=True)
        self._fail(improve)
        store.update(improve)
        assert improve.id is not None

        assert query_incomplete(store, HistoryFilter(limit=None)) == []

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

    def test_unmerged_root_keeps_completed_improve_without_merge_status_visible(self, tmp_path: Path):
        store = self._store(tmp_path)

        root = store.add("implement root", task_type="implement")
        self._complete(root, merge_status="unmerged")
        store.update(root)
        assert root.id is not None

        improve = store.add("improve done", task_type="improve", based_on=root.id, same_branch=True)
        self._complete(improve, merge_status=None)
        store.update(improve)

        lineages = query_incomplete(store, HistoryFilter(limit=None))
        assert len(lineages) == 1
        unresolved_ids = {task.id for task in lineages[0].unresolved_tasks}
        assert unresolved_ids == {root.id, improve.id}

    def test_stale_unmerged_task_row_hidden_when_merge_unit_is_merged(self, tmp_path: Path):
        store = self._store(tmp_path)

        root = store.add("implement root", task_type="implement")
        store.mark_completed(root, has_commits=True, branch="feature/stale-merged-root")
        assert root.id is not None

        unit = store.resolve_merge_unit_for_task(root.id)
        assert unit is not None
        store.set_merge_unit_state(unit.id, "merged")

        root = store.get(root.id)
        assert root is not None
        root.merge_status = "unmerged"
        store.update(root)

        assert query_incomplete(store, HistoryFilter(limit=None)) == []

    def test_stale_merged_task_row_remains_visible_when_merge_unit_is_unmerged(self, tmp_path: Path):
        store = self._store(tmp_path)

        root = store.add("stale merged task row with unmerged unit", task_type="implement")
        store.mark_completed(root, has_commits=True, branch="feature/stale-merged-task-row")
        assert root.id is not None

        unit = store.resolve_merge_unit_for_task(root.id)
        assert unit is not None

        refreshed = store.get(root.id)
        assert refreshed is not None
        refreshed.merge_status = "merged"
        store.update(refreshed)

        lineages = query_incomplete(store, HistoryFilter(limit=None))
        assert len(lineages) == 1
        unresolved_ids = {task.id for task in lineages[0].unresolved_tasks}
        assert unresolved_ids == {root.id}

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

    def test_legacy_unmerged_status_hidden_when_merge_unit_is_merged(self, tmp_path: Path):
        store = self._store(tmp_path)

        root = store.add("legacy unmerged but merged unit", task_type="implement")
        store.mark_completed(root, has_commits=True, branch="feature/legacy-status-merged-unit")
        assert root.id is not None

        unit = store.resolve_merge_unit_for_task(root.id)
        assert unit is not None
        store.set_merge_unit_state(unit.id, "merged")

        root = store.get(root.id)
        assert root is not None
        root.status = "unmerged"
        root.merge_status = None
        store.update(root)

        assert query_incomplete(store, HistoryFilter(limit=None)) == []

    def test_empty_merge_unit_is_hidden_from_incomplete(self, tmp_path: Path):
        store = self._store(tmp_path)

        root = store.add("empty root", task_type="implement")
        store.mark_completed(root, has_commits=True, branch="feature/empty-root")
        assert root.id is not None

        unit = store.resolve_merge_unit_for_task(root.id)
        assert unit is not None
        store.set_merge_unit_state(unit.id, "empty")

        assert query_incomplete(store, HistoryFilter(limit=None)) == []

    def test_failed_review_attached_to_merged_unit_is_suppressed(self, tmp_path: Path):
        store = self._store(tmp_path)

        root = store.add("implement root", task_type="implement")
        store.mark_completed(root, has_commits=True, branch="feature/merged-root-failed-review")
        assert root.id is not None

        review = store.add("review failed", task_type="review", based_on=root.id, depends_on=root.id)
        review.status = "failed"
        review.completed_at = datetime.now(UTC)
        review.has_commits = False
        store.update(review)
        assert review.id is not None

        unit = store.resolve_merge_unit_for_task(root.id)
        assert unit is not None
        attached_unit = store.get_or_create_merge_unit_for_task(review)
        assert attached_unit is not None
        assert attached_unit.id == unit.id
        store.set_merge_unit_state(unit.id, "merged")

        assert query_incomplete(store, HistoryFilter(limit=None)) == []

    def test_merged_merge_unit_owner_stays_hidden_with_orphan_same_branch_descendant(self, tmp_path: Path):
        store = self._store(tmp_path)

        root = store.add("merged implement owner", task_type="implement")
        store.mark_completed(root, has_commits=True, branch="feature/merged-owner")
        assert root.id is not None

        unit = store.resolve_merge_unit_for_task(root.id)
        assert unit is not None
        store.set_merge_unit_state(unit.id, "merged")

        orphan = store.add(
            "orphan same-branch descendant on forked branch",
            task_type="improve",
            based_on=root.id,
            same_branch=True,
        )
        orphan.status = "completed"
        orphan.completed_at = datetime.now(UTC)
        orphan.has_commits = True
        orphan.branch = "feature/merged-owner-as-28"
        orphan.merge_status = "unmerged"
        store.update(orphan)

        assert query_incomplete(store, HistoryFilter(limit=None)) == []

    def test_dropped_root_task_is_hidden_from_incomplete(self, tmp_path: Path):
        store = self._store(tmp_path)

        root = store.add("dropped root", task_type="implement")
        root.status = "dropped"
        root.completed_at = datetime.now(UTC)
        root.has_commits = True
        store.update(root)
        assert root.id is not None

        assert query_incomplete(store, HistoryFilter(limit=None)) == []

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

    def test_completed_no_commit_plan_root_is_visible_as_unimplemented(self, tmp_path: Path):
        store = self._store(tmp_path)

        plan = store.add("plan complete", task_type="plan")
        plan.status = "completed"
        plan.completed_at = datetime.now(UTC)
        plan.has_commits = False
        plan.merge_status = None
        store.update(plan)

        lineages = query_incomplete(store, HistoryFilter(limit=None))
        assert len(lineages) == 1
        assert lineages[0].root.id == plan.id
        unresolved_ids = {task.id for task in lineages[0].unresolved_tasks}
        assert unresolved_ids == {plan.id}

    def test_failed_rebase_under_merged_root_is_suppressed(self, tmp_path: Path):
        store = self._store(tmp_path)

        root = store.add("implement root", task_type="implement")
        self._complete(root, merge_status="merged")
        store.update(root)
        assert root.id is not None

        rebase = store.add("rebase failed", task_type="rebase", based_on=root.id, same_branch=True)
        self._fail(rebase)
        store.update(rebase)
        assert rebase.id is not None

        assert query_incomplete(store, HistoryFilter(limit=None)) == []

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

    def test_unmerged_root_is_suppressed_by_merged_improve_descendant(self, tmp_path: Path):
        store = self._store(tmp_path)

        root = store.add("unmerged implement", task_type="implement")
        self._complete(root, merge_status="unmerged")
        store.update(root)
        assert root.id is not None

        improve = store.add("merged improve", task_type="improve", based_on=root.id, same_branch=True)
        self._complete(improve, merge_status="merged")
        store.update(improve)

        assert query_incomplete(store, HistoryFilter(limit=None)) == []

    def test_failed_root_with_merged_resume_hides_shared_branch_completed_descendants(self, tmp_path: Path):
        """Regression: failed root should use merged resume head merge truth."""
        store = self._store(tmp_path)

        root = store.add("failed implement root", task_type="implement")
        root.branch = "feat/shared"
        self._fail(root)
        store.update(root)
        assert root.id is not None

        resumed = store.add("resumed implement", task_type="implement", based_on=root.id)
        resumed.branch = "feat/shared"
        self._complete(resumed, merge_status="merged")
        store.update(resumed)
        assert resumed.id is not None

        review = store.add("completed review", task_type="review", based_on=resumed.id, depends_on=resumed.id)
        self._complete(review, merge_status="unmerged")
        review.branch = "feat/shared"
        store.update(review)

        improve = store.add("completed improve", task_type="improve", based_on=resumed.id, same_branch=True)
        improve.branch = "feat/shared"
        self._complete(improve, merge_status="unmerged")
        store.update(improve)

        lineages = query_incomplete(store, HistoryFilter(limit=None))
        assert lineages == []

    def test_failed_root_with_merged_retry_hides_lineage(self, tmp_path: Path):
        store = self._store(tmp_path)

        root = store.add("failed implement root", task_type="implement")
        root.branch = "feat/retry"
        self._fail(root)
        store.update(root)
        assert root.id is not None

        retry = store.add("retry implement", task_type="implement", based_on=root.id)
        retry.branch = "feat/retry"
        self._complete(retry, merge_status="merged")
        store.update(retry)

        lineages = query_incomplete(store, HistoryFilter(limit=None))
        assert lineages == []

    def test_failed_root_with_unmerged_resume_keeps_lineage_visible(self, tmp_path: Path):
        store = self._store(tmp_path)

        root = store.add("failed implement root", task_type="implement")
        root.branch = "feat/unmerged-resume"
        self._fail(root)
        store.update(root)
        assert root.id is not None

        resumed = store.add("resumed implement", task_type="implement", based_on=root.id)
        resumed.branch = "feat/unmerged-resume"
        self._complete(resumed, merge_status="unmerged")
        store.update(resumed)
        assert resumed.id is not None

        improve = store.add("completed improve", task_type="improve", based_on=resumed.id, same_branch=True)
        improve.branch = "feat/unmerged-resume"
        self._complete(improve, merge_status="unmerged")
        store.update(improve)

        lineages = query_incomplete(store, HistoryFilter(limit=None))
        assert len(lineages) == 1
        assert lineages[0].root.id == root.id
        unresolved_ids = {task.id for task in lineages[0].unresolved_tasks}
        assert unresolved_ids == {resumed.id, improve.id}

    def test_status_failed_excludes_completed_unmerged_rows(self, tmp_path: Path):
        store = self._store(tmp_path)

        failed = store.add("failed implement", task_type="implement", tags=("release",))
        self._fail(failed)
        store.update(failed)
        assert failed.id is not None

        completed = store.add("completed unmerged implement", task_type="implement", tags=("release",))
        self._complete(completed, merge_status="unmerged")
        store.update(completed)

        lineages = query_incomplete(store, HistoryFilter(limit=None, status="failed"))
        assert len(lineages) == 1
        unresolved_ids = {task.id for task in lineages[0].unresolved_tasks}
        assert unresolved_ids == {failed.id}

    def test_status_not_failed_excludes_failed_rows(self, tmp_path: Path):
        store = self._store(tmp_path)

        failed = store.add("failed implement", task_type="implement", tags=("release",))
        self._fail(failed)
        store.update(failed)
        assert failed.id is not None

        completed = store.add("completed unmerged implement", task_type="implement", tags=("release",))
        self._complete(completed, merge_status="unmerged")
        store.update(completed)
        assert completed.id is not None

        lineages = query_incomplete(store, HistoryFilter(limit=None, status_not="failed"))
        assert len(lineages) == 1
        unresolved_ids = {task.id for task in lineages[0].unresolved_tasks}
        assert unresolved_ids == {completed.id}

    def test_status_unmerged_uses_attached_merge_unit_state(self, tmp_path: Path):
        store = self._store(tmp_path)

        task = store.add("completed implement backed by merge unit", task_type="implement", tags=("release",))
        store.mark_completed(task, has_commits=True, branch="feature/status-unmerged-merge-unit")
        assert task.id is not None

        refreshed = store.get(task.id)
        assert refreshed is not None
        refreshed.merge_status = None
        store.update(refreshed)

        lineages = query_incomplete(store, HistoryFilter(limit=None, status="unmerged"))
        assert len(lineages) == 1
        unresolved_ids = {member.id for member in lineages[0].unresolved_tasks}
        assert unresolved_ids == {task.id}

    def test_status_not_unmerged_excludes_attached_merge_unit_state(self, tmp_path: Path):
        store = self._store(tmp_path)

        task = store.add("completed implement excluded by merge unit", task_type="implement", tags=("release",))
        store.mark_completed(task, has_commits=True, branch="feature/status-not-unmerged-merge-unit")
        assert task.id is not None

        refreshed = store.get(task.id)
        assert refreshed is not None
        refreshed.merge_status = None
        store.update(refreshed)

        assert query_incomplete(store, HistoryFilter(limit=None, status_not="unmerged")) == []

    def test_tags_not_excludes_blocked_rows_while_preserving_positive_tags(self, tmp_path: Path):
        store = self._store(tmp_path)

        allowed = store.add("allowed implement", task_type="implement", tags=("release",))
        self._complete(allowed, merge_status="unmerged")
        store.update(allowed)
        assert allowed.id is not None

        blocked = store.add("blocked implement", task_type="implement", tags=("release", "blocked"))
        self._complete(blocked, merge_status="unmerged")
        store.update(blocked)

        lineages = query_incomplete(
            store,
            HistoryFilter(limit=None, tags=("release",), tags_not=("blocked",)),
        )
        assert len(lineages) == 1
        unresolved_ids = {task.id for task in lineages[0].unresolved_tasks}
        assert unresolved_ids == {allowed.id}

    def test_effective_shared_branch_head_distinguishes_canonical_root_from_merged_resume(self, tmp_path: Path):
        store = self._store(tmp_path)

        root = store.add("failed implement root", task_type="implement")
        root.branch = "feat/effective-head"
        self._fail(root)
        store.update(root)
        assert root.id is not None

        resumed = store.add("resumed implement", task_type="implement", based_on=root.id)
        resumed.branch = "feat/effective-head"
        self._complete(resumed, merge_status="merged")
        store.update(resumed)
        assert resumed.id is not None

        effective_head = _resolve_effective_shared_branch_retry_head(store, root)
        assert effective_head.id == resumed.id
        resolved_root_id = get_task_lineage(store, resumed.id, 0).task.id
        assert resolved_root_id == root.id

    def test_query_incomplete_uses_unit_state_for_non_shared_branch_tasks(self, tmp_path: Path):
        store = self._store(tmp_path)

        root = store.add("unit-backed root", task_type="implement")
        root.status = "completed"
        root.completed_at = datetime.now(UTC)
        root.has_commits = True
        root.branch = "feature/query-incomplete"
        root.merge_status = "unmerged"
        store.update(root)
        assert root.id is not None

        unit = store.create_merge_unit(
            source_branch=root.branch,
            target_branch="release",
            owner_task_id=root.id,
            state="unmerged",
        )
        store.attach_task_to_merge_unit(root.id, unit.id, "owner")
        unit = store.resolve_merge_unit_for_task(root.id)
        assert unit is not None
        assert unit.target_branch == "release"
        store.set_merge_unit_state(unit.id, "unmerged")

        stale_row = store.get(root.id)
        assert stale_row is not None
        stale_row.merge_status = "merged"
        store.update(stale_row)

        lineages = query_incomplete(store, HistoryFilter(limit=None), target_branch="release")

        assert len(lineages) == 1
        assert lineages[0].root.id == root.id
        unresolved_ids = {task.id for task in lineages[0].unresolved_tasks}
        assert unresolved_ids == {root.id}


class TestIsLineageResolved:
    def _merge_unit(self, *, unit_id: str, owner_task_id: str, state: str) -> MergeUnit:
        now = datetime.now(UTC)
        return MergeUnit(
            id=unit_id,
            source_branch="feature/test",
            target_branch="main",
            state=state,
            owner_task_id=owner_task_id,
            head_sha=None,
            base_sha=None,
            created_at=now,
            updated_at=now,
            merged_at=now if state == "merged" else None,
            merged_by_task_id=None,
            pr_number=None,
            pr_state=None,
            pr_last_synced_at=None,
            sync_last_synced_at=None,
            diff_files_changed=None,
            diff_lines_added=None,
            diff_lines_removed=None,
            superseded_by_unit_id=None,
        )

    def test_completed_unmerged_task_row_keeps_snapshot_unresolved(self):
        task = _make_task(
            id="gza-1",
            status="completed",
            task_type="implement",
            has_commits=True,
            merge_status="unmerged",
        )
        snapshot = LineageOwnerSnapshot(
            owner_task=task,
            root_task=task,
            members=(task,),
            merge_units_by_task_id={},
            failed_leaves=(),
            recovery_completed_by_failed_id={},
        )

        resolution = is_lineage_resolved(snapshot)

        assert resolution.resolved is False
        assert resolution.reasons == ()

    def test_completed_task_with_unmerged_merge_unit_keeps_snapshot_unresolved(self):
        task = _make_task(
            id="gza-1",
            status="completed",
            task_type="implement",
            has_commits=True,
            merge_status=None,
        )
        merge_unit = self._merge_unit(unit_id="gza-mu-1", owner_task_id="gza-1", state="unmerged")
        snapshot = LineageOwnerSnapshot(
            owner_task=task,
            root_task=task,
            members=(task,),
            merge_units_by_task_id={task.id: merge_unit},
            failed_leaves=(),
            recovery_completed_by_failed_id={},
        )

        resolution = is_lineage_resolved(snapshot)

        assert resolution.resolved is False
        assert resolution.reasons == ()

    def test_completed_task_with_merged_merge_unit_resolves_snapshot(self):
        task = _make_task(
            id="gza-1",
            status="completed",
            task_type="implement",
            has_commits=True,
            merge_status="unmerged",
        )
        merge_unit = self._merge_unit(unit_id="gza-mu-1", owner_task_id="gza-1", state="merged")
        snapshot = LineageOwnerSnapshot(
            owner_task=task,
            root_task=task,
            members=(task,),
            merge_units_by_task_id={task.id: merge_unit},
            failed_leaves=(),
            recovery_completed_by_failed_id={},
        )

        resolution = is_lineage_resolved(snapshot)

        assert resolution.resolved is True
        assert resolution.reasons == ("branch_merged",)
        assert resolution.resolved_by_task_ids == ("gza-1",)

    def test_stale_merged_task_row_does_not_resolve_unmerged_merge_unit_snapshot(self):
        task = _make_task(
            id="gza-1",
            status="completed",
            task_type="implement",
            has_commits=True,
            merge_status="merged",
        )
        merge_unit = self._merge_unit(unit_id="gza-mu-1", owner_task_id="gza-1", state="unmerged")
        snapshot = LineageOwnerSnapshot(
            owner_task=task,
            root_task=task,
            members=(task,),
            merge_units_by_task_id={task.id: merge_unit},
            failed_leaves=(),
            recovery_completed_by_failed_id={},
        )

        resolution = is_lineage_resolved(snapshot)

        assert resolution.resolved is False
        assert resolution.reasons == ()
