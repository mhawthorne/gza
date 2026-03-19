"""Tests for gza.query lineage helpers."""

from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest

from gza.query import (
    build_lineage,
    build_lineage_tree,
    filter_lineage_tree,
    get_base_task_slug,
    get_improves_for_root,
    get_reviews_for_root,
    get_task_slug,
    resolve_lineage_root,
    task_time_for_lineage,
)
from gza.db import Task


def _make_task(**kwargs) -> Task:
    """Helper: build a minimal Task with sensible defaults."""
    defaults: dict = {
        "id": None,
        "prompt": "test task",
        "status": "completed",
        "task_type": "implement",
        "has_commits": False,
        "merge_status": None,
    }
    defaults.update(kwargs)
    return Task(**defaults)


# ---------------------------------------------------------------------------
# task_time_for_lineage
# ---------------------------------------------------------------------------


class TestTaskTimeForLineage:
    def test_prefers_completed_at(self):
        completed = datetime(2026, 3, 15, 12, 0, 0)
        created = datetime(2026, 3, 15, 10, 0, 0)
        task = _make_task(completed_at=completed, created_at=created)
        assert task_time_for_lineage(task) == completed

    def test_falls_back_to_created_at(self):
        created = datetime(2026, 3, 15, 10, 0, 0)
        task = _make_task(completed_at=None, created_at=created)
        assert task_time_for_lineage(task) == created

    def test_falls_back_to_datetime_min(self):
        task = _make_task(completed_at=None, created_at=None)
        assert task_time_for_lineage(task) == datetime.min

    def test_completed_at_preferred_over_none_created(self):
        completed = datetime(2026, 1, 1)
        task = _make_task(completed_at=completed, created_at=None)
        assert task_time_for_lineage(task) == completed


# ---------------------------------------------------------------------------
# get_task_slug / get_base_task_slug
# ---------------------------------------------------------------------------


class TestGetTaskSlug:
    @patch("gza.query._get_task_slug_from_task_id")
    def test_delegates_to_task_slug_module(self, mock_fn):
        mock_fn.return_value = "my-feature"
        task = _make_task(task_id="20260305-my-feature")
        result = get_task_slug(task)
        mock_fn.assert_called_once_with("20260305-my-feature")
        assert result == "my-feature"

    @patch("gza.query._get_task_slug_from_task_id")
    def test_none_task_id(self, mock_fn):
        mock_fn.return_value = None
        task = _make_task(task_id=None)
        result = get_task_slug(task)
        mock_fn.assert_called_once_with(None)
        assert result is None


class TestGetBaseTaskSlug:
    @patch("gza.query._get_base_task_slug")
    def test_delegates_to_task_slug_module(self, mock_fn):
        mock_fn.return_value = "my-feature"
        task = _make_task(task_id="20260305-my-feature-2")
        result = get_base_task_slug(task)
        mock_fn.assert_called_once_with("20260305-my-feature-2")
        assert result == "my-feature"

    @patch("gza.query._get_base_task_slug")
    def test_none_task_id(self, mock_fn):
        mock_fn.return_value = None
        task = _make_task(task_id=None)
        result = get_base_task_slug(task)
        mock_fn.assert_called_once_with(None)
        assert result is None


# ---------------------------------------------------------------------------
# get_reviews_for_root
# ---------------------------------------------------------------------------


class TestGetReviewsForRoot:
    def test_returns_empty_when_id_is_none(self):
        store = MagicMock()
        task = _make_task(id=None)
        assert get_reviews_for_root(store, task) == []
        store.get_reviews_for_task.assert_not_called()

    def test_returns_linked_reviews_when_found(self):
        store = MagicMock()
        review = _make_task(id=10, task_type="review")
        store.get_reviews_for_task.return_value = [review]
        root = _make_task(id=1)
        result = get_reviews_for_root(store, root)
        store.get_reviews_for_task.assert_called_once_with(1)
        assert result == [review]
        store.get_unlinked_reviews_for_slug.assert_not_called()

    def test_falls_back_to_unlinked_reviews(self):
        store = MagicMock()
        store.get_reviews_for_task.return_value = []
        unlinked_review = _make_task(id=20, task_type="review")
        store.get_unlinked_reviews_for_slug.return_value = [unlinked_review]
        root = _make_task(id=1, task_id="20260305-my-feature")
        result = get_reviews_for_root(store, root)
        assert result == [unlinked_review]

    def test_returns_empty_when_no_slug_for_fallback(self):
        store = MagicMock()
        store.get_reviews_for_task.return_value = []
        root = _make_task(id=1, task_id=None)
        result = get_reviews_for_root(store, root)
        assert result == []
        store.get_unlinked_reviews_for_slug.assert_not_called()

    def test_returns_empty_when_no_linked_and_no_unlinked(self):
        store = MagicMock()
        store.get_reviews_for_task.return_value = []
        store.get_unlinked_reviews_for_slug.return_value = []
        root = _make_task(id=1, task_id="20260305-my-feature")
        result = get_reviews_for_root(store, root)
        assert result == []


# ---------------------------------------------------------------------------
# get_improves_for_root
# ---------------------------------------------------------------------------


class TestGetImprovesForRoot:
    def test_returns_empty_when_id_is_none(self):
        store = MagicMock()
        task = _make_task(id=None)
        assert get_improves_for_root(store, task) == []
        store.get_improve_tasks_by_root.assert_not_called()

    def test_returns_improve_tasks(self):
        store = MagicMock()
        improve = _make_task(id=20, task_type="improve")
        store.get_improve_tasks_by_root.return_value = [improve]
        root = _make_task(id=1)
        result = get_improves_for_root(store, root)
        store.get_improve_tasks_by_root.assert_called_once_with(1)
        assert result == [improve]


# ---------------------------------------------------------------------------
# build_lineage
# ---------------------------------------------------------------------------


class TestBuildLineage:
    def test_single_root_task(self):
        store = MagicMock()
        root = _make_task(id=1, created_at=datetime(2026, 1, 1))
        store.get_lineage_children.return_value = []
        result = build_lineage(store, root)
        assert result == [root]

    def test_root_with_none_id_returns_empty(self):
        store = MagicMock()
        root = _make_task(id=None)
        result = build_lineage(store, root)
        assert result == []

    def test_includes_reviews_and_improves(self):
        store = MagicMock()
        root = _make_task(id=1, created_at=datetime(2026, 1, 1))
        review = _make_task(id=2, task_type="review", created_at=datetime(2026, 1, 2))
        improve = _make_task(id=3, task_type="improve", created_at=datetime(2026, 1, 3))

        def lineage_children(task_id):
            if task_id == 1:
                return [review, improve]
            return []

        store.get_lineage_children.side_effect = lineage_children
        result = build_lineage(store, root)
        assert len(result) == 3
        assert result[0] == root
        assert result[1] == review
        assert result[2] == improve

    def test_deduplication(self):
        store = MagicMock()
        root = _make_task(id=1, created_at=datetime(2026, 1, 1))
        shared = _make_task(id=2, task_type="review", created_at=datetime(2026, 1, 2))

        store.get_lineage_children.side_effect = (
            lambda task_id: [shared, shared] if task_id == 1 else []
        )

        result = build_lineage(store, root)
        ids = [t.id for t in result]
        assert ids.count(2) == 1

    def test_recursive_downstream(self):
        store = MagicMock()
        root = _make_task(id=1, created_at=datetime(2026, 1, 1))
        child = _make_task(id=2, created_at=datetime(2026, 1, 2))
        grandchild = _make_task(id=3, created_at=datetime(2026, 1, 3))

        def lineage_children(task_id):
            if task_id == 1:
                return [child]
            if task_id == 2:
                return [grandchild]
            return []

        store.get_lineage_children.side_effect = lineage_children

        result = build_lineage(store, root)
        assert len(result) == 3
        assert [t.id for t in result] == [1, 2, 3]

    def test_prefers_review_branch_when_child_has_two_parents(self):
        store = MagicMock()
        root = _make_task(id=1, task_type="implement", created_at=datetime(2026, 1, 1))
        review = _make_task(
            id=2,
            task_type="review",
            depends_on=1,
            created_at=datetime(2026, 1, 2),
        )
        improve = _make_task(
            id=3,
            task_type="improve",
            based_on=1,
            depends_on=2,
            created_at=datetime(2026, 1, 3),
        )
        sibling_impl = _make_task(
            id=4,
            task_type="implement",
            depends_on=1,
            created_at=datetime(2026, 1, 4),
        )

        def lineage_children(task_id):
            if task_id == 1:
                return [sibling_impl, improve, review]
            if task_id == 2:
                return [improve]
            return []

        store.get_lineage_children.side_effect = lineage_children
        result = build_lineage(store, root)
        assert [t.id for t in result] == [1, 2, 3, 4]

    def test_orders_by_dependency_depth_not_completion_time(self):
        store = MagicMock()
        root = _make_task(
            id=1,
            created_at=datetime(2026, 1, 1),
            completed_at=datetime(2026, 1, 10),
            status="completed",
        )
        child = _make_task(
            id=2,
            created_at=datetime(2026, 1, 2),
            completed_at=None,
            status="pending",
        )

        store.get_lineage_children.side_effect = (
            lambda task_id: [child] if task_id == 1 else []
        )

        result = build_lineage(store, root)
        assert [t.id for t in result] == [1, 2]

    def test_tree_node_relationship_labels(self):
        store = MagicMock()
        root = _make_task(id=1, task_type="implement", created_at=datetime(2026, 1, 1))
        review = _make_task(id=2, task_type="review", depends_on=1, created_at=datetime(2026, 1, 2))
        impl = _make_task(id=3, task_type="implement", based_on=1, created_at=datetime(2026, 1, 3))

        def lineage_children(task_id):
            if task_id == 1:
                return [impl, review]
            return []

        store.get_lineage_children.side_effect = lineage_children
        tree = build_lineage_tree(store, root)
        assert [child.relationship for child in tree.children] == ["review", "implement-based"]


# ---------------------------------------------------------------------------
# filter_lineage_tree
# ---------------------------------------------------------------------------


class TestFilterLineageTree:
    def test_prunes_disallowed_children_and_reparents_allowed_descendants(self):
        store = MagicMock()
        root = _make_task(id=1, task_type="implement", created_at=datetime(2026, 1, 1))
        downstream_impl = _make_task(id=2, task_type="implement", based_on=1, created_at=datetime(2026, 1, 2))
        review = _make_task(id=3, task_type="review", depends_on=2, created_at=datetime(2026, 1, 3))
        improve = _make_task(
            id=4,
            task_type="improve",
            based_on=1,
            depends_on=3,
            created_at=datetime(2026, 1, 4),
        )

        def lineage_children(task_id):
            if task_id == 1:
                return [downstream_impl]
            if task_id == 2:
                return [review]
            if task_id == 3:
                return [improve]
            return []

        store.get_lineage_children.side_effect = lineage_children

        tree = build_lineage_tree(store, root)
        filtered = filter_lineage_tree(tree, {"review", "improve"})

        assert filtered.task.id == 1
        assert [child.task.id for child in filtered.children] == [3]
        assert [child.task.id for child in filtered.children[0].children] == [4]
        assert filtered.children[0].depth == 1
        assert filtered.children[0].children[0].depth == 2


# ---------------------------------------------------------------------------
# resolve_lineage_root
# ---------------------------------------------------------------------------


class TestResolveLineageRoot:
    def test_returns_task_when_no_dependencies(self):
        store = MagicMock()
        task = _make_task(id=1, task_type="implement", based_on=None, depends_on=None)
        result = resolve_lineage_root(store, task)
        assert result == task

    def test_review_resolves_to_depends_on(self):
        store = MagicMock()
        parent = _make_task(id=1, task_type="implement", based_on=None)
        review = _make_task(id=2, task_type="review", depends_on=1)
        store.get.return_value = parent
        result = resolve_lineage_root(store, review)
        assert result == parent

    def test_improve_resolves_to_based_on(self):
        store = MagicMock()
        parent = _make_task(id=1, task_type="implement", based_on=None)
        improve = _make_task(id=2, task_type="improve", based_on=1)
        store.get.return_value = parent
        result = resolve_lineage_root(store, improve)
        assert result == parent

    def test_walks_up_based_on_chain(self):
        store = MagicMock()
        grandparent = _make_task(id=1, task_type="implement", based_on=None)
        parent = _make_task(id=2, task_type="implement", based_on=1)
        child = _make_task(id=3, task_type="implement", based_on=2)

        def mock_get(task_id):
            return {1: grandparent, 2: parent, 3: child}.get(task_id)

        store.get.side_effect = mock_get
        result = resolve_lineage_root(store, child)
        assert result == grandparent

    def test_handles_cycle_in_based_on_chain(self):
        store = MagicMock()
        task_a = _make_task(id=1, task_type="implement", based_on=2)
        task_b = _make_task(id=2, task_type="implement", based_on=1)

        def mock_get(task_id):
            return {1: task_a, 2: task_b}.get(task_id)

        store.get.side_effect = mock_get
        result = resolve_lineage_root(store, task_a)
        assert result.id in (1, 2)

    def test_review_depends_on_not_found(self):
        store = MagicMock()
        store.get.return_value = None
        review = _make_task(id=2, task_type="review", depends_on=999)
        result = resolve_lineage_root(store, review)
        assert result == review

    def test_based_on_chain_stops_at_none(self):
        store = MagicMock()
        parent = _make_task(id=1, task_type="implement", based_on=999)
        child = _make_task(id=2, task_type="implement", based_on=1)

        def mock_get(task_id):
            if task_id == 1:
                return parent
            return None

        store.get.side_effect = mock_get
        result = resolve_lineage_root(store, child)
        assert result == parent

    def test_implement_depends_on_resolves_upstream_root(self):
        store = MagicMock()
        root = _make_task(id=1, task_type="implement")
        dependent = _make_task(id=2, task_type="implement", depends_on=1)

        def mock_get(task_id):
            return {1: root, 2: dependent}.get(task_id)

        store.get.side_effect = mock_get
        result = resolve_lineage_root(store, dependent)
        assert result == root
