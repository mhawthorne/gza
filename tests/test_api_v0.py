"""Tests for gza.api.v0 — GzaClient experimental public API."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest

from gza.db import SqliteTaskStore

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def setup_config(tmp_path: Path, project_name: str = "test-project") -> None:
    """Write a minimal gza.yaml so Config.load() succeeds."""
    (tmp_path / "gza.yaml").write_text(
        f"project_name: {project_name}\n"
        "db_path: .gza/gza.db\n"
    )


def make_store(tmp_path: Path) -> SqliteTaskStore:
    """Create a SqliteTaskStore under tmp_path/.gza/gza.db."""
    db_path = tmp_path / ".gza" / "gza.db"
    db_path.parent.mkdir(parents=True, exist_ok=True)
    return SqliteTaskStore(db_path)


def make_client(tmp_path: Path):
    """Return a GzaClient pointed at tmp_path."""
    from gza.api.v0 import GzaClient
    return GzaClient(tmp_path)


# ---------------------------------------------------------------------------
# GzaClient — construction
# ---------------------------------------------------------------------------


class TestGzaClientConstruction:
    def test_import_from_module(self):
        from gza.api.v0 import GzaClient, IncompleteSnapshot, Task
        assert GzaClient is not None
        assert Task is not None
        assert IncompleteSnapshot is not None

    def test_client_with_explicit_path(self, tmp_path: Path):
        setup_config(tmp_path)
        client = make_client(tmp_path)
        assert client is not None

    def test_client_with_string_path(self, tmp_path: Path):
        setup_config(tmp_path)
        from gza.api.v0 import GzaClient
        client = GzaClient(str(tmp_path))
        assert client is not None


# ---------------------------------------------------------------------------
# GzaClient — get_lineage
# ---------------------------------------------------------------------------


class TestGetLineage:
    def test_get_lineage_docstring_describes_tree_preorder_not_chronological(self):
        from gza.api.v0 import GzaClient

        doc = GzaClient.get_lineage.__doc__ or ""

        assert "pre-order" in doc
        assert "chronological" not in doc

    def test_unknown_task_raises_key_error(self, tmp_path: Path):
        setup_config(tmp_path)
        make_store(tmp_path)  # ensure db is created
        client = make_client(tmp_path)
        with pytest.raises(KeyError, match="not found"):
            client.get_lineage("test-project-zzz")

    def test_single_task_returns_list_of_one(self, tmp_path: Path):
        setup_config(tmp_path)
        store = make_store(tmp_path)
        impl = store.add("Implement feature", task_type="implement")
        client = make_client(tmp_path)
        lineage = client.get_lineage(impl.id)
        assert len(lineage) == 1
        assert lineage[0].id == impl.id

    def test_lineage_includes_review(self, tmp_path: Path):
        setup_config(tmp_path)
        store = make_store(tmp_path)
        impl = store.add("Implement feature", task_type="implement")
        review = store.add("Review feature", task_type="review", depends_on=impl.id)
        client = make_client(tmp_path)
        lineage = client.get_lineage(impl.id)
        ids = {t.id for t in lineage}
        assert impl.id in ids
        assert review.id in ids

    def test_lineage_includes_improve(self, tmp_path: Path):
        setup_config(tmp_path)
        store = make_store(tmp_path)
        impl = store.add("Implement feature", task_type="implement")
        improve = store.add("Improve feature", task_type="improve", based_on=impl.id)
        client = make_client(tmp_path)
        lineage = client.get_lineage(impl.id)
        ids = {t.id for t in lineage}
        assert impl.id in ids
        assert improve.id in ids

    def test_lineage_deduplicated(self, tmp_path: Path):
        setup_config(tmp_path)
        store = make_store(tmp_path)
        impl = store.add("Implement feature", task_type="implement")
        store.add("Review feature", task_type="review", depends_on=impl.id)
        store.add("Improve feature", task_type="improve", based_on=impl.id)
        client = make_client(tmp_path)
        lineage = client.get_lineage(impl.id)
        ids = [t.id for t in lineage]
        assert len(ids) == len(set(ids)), "lineage should have no duplicate IDs"

    def test_get_lineage_from_review_resolves_root(self, tmp_path: Path):
        """Passing a review task ID should return the full lineage from the root."""
        setup_config(tmp_path)
        store = make_store(tmp_path)
        impl = store.add("Implement feature", task_type="implement")
        review = store.add("Review feature", task_type="review", depends_on=impl.id)
        client = make_client(tmp_path)
        lineage = client.get_lineage(review.id)
        ids = {t.id for t in lineage}
        assert impl.id in ids
        assert review.id in ids

    def test_get_lineage_from_improve_resolves_root(self, tmp_path: Path):
        setup_config(tmp_path)
        store = make_store(tmp_path)
        impl = store.add("Implement feature", task_type="implement")
        improve = store.add("Improve feature", task_type="improve", based_on=impl.id)
        client = make_client(tmp_path)
        lineage = client.get_lineage(improve.id)
        ids = {t.id for t in lineage}
        assert impl.id in ids
        assert improve.id in ids

    def test_lineage_orders_root_before_pending_descendants(self, tmp_path: Path):
        setup_config(tmp_path)
        store = make_store(tmp_path)

        root = store.add("Root feature", task_type="implement")
        root.status = "completed"
        root.completed_at = datetime(2026, 3, 1, tzinfo=UTC)
        store.update(root)

        child = store.add("Child feature", task_type="implement", based_on=root.id)
        grandchild = store.add("Grandchild feature", task_type="implement", based_on=child.id)

        client = make_client(tmp_path)
        lineage = client.get_lineage(root.id)
        lineage_ids = [task.id for task in lineage]

        assert lineage_ids.index(root.id) < lineage_ids.index(child.id) < lineage_ids.index(grandchild.id)


# ---------------------------------------------------------------------------
# GzaClient — get_lineage_root
# ---------------------------------------------------------------------------


class TestGetLineageRoot:
    def test_unknown_task_raises_key_error(self, tmp_path: Path):
        setup_config(tmp_path)
        make_store(tmp_path)
        client = make_client(tmp_path)
        with pytest.raises(KeyError, match="42"):
            client.get_lineage_root(42)

    def test_plain_task_returns_itself(self, tmp_path: Path):
        setup_config(tmp_path)
        store = make_store(tmp_path)
        task = store.add("Some task", task_type="task")
        client = make_client(tmp_path)
        root = client.get_lineage_root(task.id)
        assert root.id == task.id

    def test_review_resolves_to_implement(self, tmp_path: Path):
        setup_config(tmp_path)
        store = make_store(tmp_path)
        impl = store.add("Implement feature", task_type="implement")
        review = store.add("Review feature", task_type="review", depends_on=impl.id)
        client = make_client(tmp_path)
        root = client.get_lineage_root(review.id)
        assert root.id == impl.id

    def test_improve_resolves_to_implement(self, tmp_path: Path):
        setup_config(tmp_path)
        store = make_store(tmp_path)
        impl = store.add("Implement feature", task_type="implement")
        improve = store.add("Improve feature", task_type="improve", based_on=impl.id)
        client = make_client(tmp_path)
        root = client.get_lineage_root(improve.id)
        assert root.id == impl.id

    def test_implement_returns_itself(self, tmp_path: Path):
        setup_config(tmp_path)
        store = make_store(tmp_path)
        impl = store.add("Implement feature", task_type="implement")
        client = make_client(tmp_path)
        root = client.get_lineage_root(impl.id)
        assert root.id == impl.id


# ---------------------------------------------------------------------------
# GzaClient — get_incomplete
# ---------------------------------------------------------------------------


class TestGetIncomplete:
    def test_empty_db_returns_empty_snapshot(self, tmp_path: Path):
        setup_config(tmp_path)
        make_store(tmp_path)
        client = make_client(tmp_path)
        snap = client.get_incomplete()
        assert snap.pending == []
        assert snap.in_progress == []
        assert snap.total == 0

    def test_pending_task_appears_in_snapshot(self, tmp_path: Path):
        setup_config(tmp_path)
        store = make_store(tmp_path)
        store.add("Pending task")
        client = make_client(tmp_path)
        snap = client.get_incomplete()
        assert len(snap.pending) == 1
        assert snap.total == 1

    def test_in_progress_task_appears_in_snapshot(self, tmp_path: Path):
        setup_config(tmp_path)
        store = make_store(tmp_path)
        task = store.add("In-progress task")
        task.status = "in_progress"
        store.update(task)
        client = make_client(tmp_path)
        snap = client.get_incomplete()
        assert len(snap.in_progress) == 1
        assert snap.total == 1

    def test_completed_task_excluded_from_snapshot(self, tmp_path: Path):
        setup_config(tmp_path)
        store = make_store(tmp_path)
        task = store.add("Completed task")
        task.status = "completed"
        task.completed_at = datetime.now(UTC)
        store.update(task)
        client = make_client(tmp_path)
        snap = client.get_incomplete()
        assert snap.total == 0

    def test_total_counts_both_pending_and_in_progress(self, tmp_path: Path):
        setup_config(tmp_path)
        store = make_store(tmp_path)
        store.add("Pending 1")
        store.add("Pending 2")
        in_prog = store.add("In progress")
        in_prog.status = "in_progress"
        store.update(in_prog)
        client = make_client(tmp_path)
        snap = client.get_incomplete()
        assert len(snap.pending) == 2
        assert len(snap.in_progress) == 1
        assert snap.total == 3

    def test_pending_snapshot_includes_non_runnable_pending_rows(self, tmp_path: Path):
        setup_config(tmp_path)
        store = make_store(tmp_path)
        store.add("Runnable pending")
        store.add("Internal pending", task_type="internal")
        blocker = store.add("Blocking pending")
        store.add("Blocked pending", depends_on=blocker.id)

        client = make_client(tmp_path)
        snap = client.get_incomplete()
        pending_prompts = {task.prompt for task in snap.pending}

        assert "Runnable pending" in pending_prompts
        assert "Internal pending" in pending_prompts
        assert "Blocked pending" in pending_prompts


# ---------------------------------------------------------------------------
# GzaClient — get_pending
# ---------------------------------------------------------------------------


class TestGetPending:
    def test_returns_pending_tasks(self, tmp_path: Path):
        setup_config(tmp_path)
        store = make_store(tmp_path)
        store.add("Task A")
        store.add("Task B")
        client = make_client(tmp_path)
        pending = client.get_pending()
        assert len(pending) == 2

    def test_respects_limit(self, tmp_path: Path):
        setup_config(tmp_path)
        store = make_store(tmp_path)
        for i in range(5):
            store.add(f"Task {i}")
        client = make_client(tmp_path)
        pending = client.get_pending(limit=2)
        assert len(pending) == 2

    def test_limit_none_returns_all(self, tmp_path: Path):
        setup_config(tmp_path)
        store = make_store(tmp_path)
        for i in range(5):
            store.add(f"Task {i}")
        client = make_client(tmp_path)
        pending = client.get_pending(limit=None)
        assert len(pending) == 5

    def test_limit_zero_returns_empty(self, tmp_path: Path):
        setup_config(tmp_path)
        store = make_store(tmp_path)
        for i in range(3):
            store.add(f"Task {i}")
        client = make_client(tmp_path)
        result = client.get_pending(limit=0)
        assert result == []

    def test_excludes_non_pending_tasks(self, tmp_path: Path):
        setup_config(tmp_path)
        store = make_store(tmp_path)
        pending = store.add("Pending task")
        done = store.add("Completed task")
        done.status = "completed"
        done.completed_at = datetime.now(UTC)
        store.update(done)
        client = make_client(tmp_path)
        result = client.get_pending()
        ids = {t.id for t in result}
        assert pending.id in ids
        assert done.id not in ids

    def test_excludes_non_pickable_internal_and_blocked_tasks(self, tmp_path: Path):
        setup_config(tmp_path)
        store = make_store(tmp_path)
        runnable = store.add("Runnable pending")
        assert runnable.id is not None

        store.add("Internal pending", task_type="internal")
        blocker = store.add("Dependency blocker")
        store.add("Blocked pending", depends_on=blocker.id)

        client = make_client(tmp_path)
        result = client.get_pending()
        prompts = {t.prompt for t in result}

        assert "Runnable pending" in prompts
        assert "Internal pending" not in prompts
        assert "Blocked pending" not in prompts

    def test_bumped_urgent_task_is_returned_before_older_urgent_tasks(self, tmp_path: Path):
        setup_config(tmp_path)
        store = make_store(tmp_path)
        older_urgent = store.add("Older urgent", urgent=True)
        newer_urgent = store.add("Newer urgent", urgent=True)
        bumped = store.add("Bumped now")
        assert older_urgent.id is not None
        assert newer_urgent.id is not None
        assert bumped.id is not None
        store.set_urgent(bumped.id, True)

        client = make_client(tmp_path)
        result = client.get_pending()
        ids = [task.id for task in result]

        assert ids.index(bumped.id) < ids.index(older_urgent.id) < ids.index(newer_urgent.id)


# ---------------------------------------------------------------------------
# GzaClient — get_in_progress
# ---------------------------------------------------------------------------


class TestGetInProgress:
    def test_returns_in_progress_tasks(self, tmp_path: Path):
        setup_config(tmp_path)
        store = make_store(tmp_path)
        task = store.add("Working task")
        task.status = "in_progress"
        store.update(task)
        client = make_client(tmp_path)
        result = client.get_in_progress()
        assert len(result) == 1
        assert result[0].id == task.id

    def test_excludes_pending_tasks(self, tmp_path: Path):
        setup_config(tmp_path)
        store = make_store(tmp_path)
        store.add("Pending task")
        client = make_client(tmp_path)
        result = client.get_in_progress()
        assert result == []


# ---------------------------------------------------------------------------
# GzaClient — get_history
# ---------------------------------------------------------------------------


class TestGetHistory:
    def test_default_limit_ten(self, tmp_path: Path):
        setup_config(tmp_path)
        store = make_store(tmp_path)
        for i in range(15):
            task = store.add(f"Task {i}")
            task.status = "completed"
            task.completed_at = datetime.now(UTC)
            store.update(task)
        client = make_client(tmp_path)
        history = client.get_history()
        assert len(history) == 10

    def test_filter_by_status_completed(self, tmp_path: Path):
        setup_config(tmp_path)
        store = make_store(tmp_path)
        completed = store.add("Completed task")
        completed.status = "completed"
        completed.completed_at = datetime.now(UTC)
        store.update(completed)
        failed = store.add("Failed task")
        failed.status = "failed"
        failed.completed_at = datetime.now(UTC)
        store.update(failed)
        client = make_client(tmp_path)
        result = client.get_history(status="completed")
        assert all(t.status == "completed" for t in result)
        ids = {t.id for t in result}
        assert completed.id in ids
        assert failed.id not in ids

    def test_filter_by_status_failed(self, tmp_path: Path):
        setup_config(tmp_path)
        store = make_store(tmp_path)
        failed = store.add("Failed task")
        failed.status = "failed"
        failed.completed_at = datetime.now(UTC)
        store.update(failed)
        client = make_client(tmp_path)
        result = client.get_history(status="failed")
        assert len(result) == 1
        assert result[0].id == failed.id

    def test_filter_by_task_type(self, tmp_path: Path):
        setup_config(tmp_path)
        store = make_store(tmp_path)
        impl = store.add("Implement task", task_type="implement")
        impl.status = "completed"
        impl.completed_at = datetime.now(UTC)
        store.update(impl)
        review = store.add("Review task", task_type="review")
        review.status = "completed"
        review.completed_at = datetime.now(UTC)
        store.update(review)
        client = make_client(tmp_path)
        result = client.get_history(task_type="implement")
        assert all(t.task_type == "implement" for t in result)
        ids = {t.id for t in result}
        assert impl.id in ids
        assert review.id not in ids

    def test_limit_none_returns_all(self, tmp_path: Path):
        setup_config(tmp_path)
        store = make_store(tmp_path)
        for i in range(15):
            task = store.add(f"Task {i}")
            task.status = "completed"
            task.completed_at = datetime.now(UTC)
            store.update(task)
        client = make_client(tmp_path)
        result = client.get_history(limit=None)
        assert len(result) == 15

    def test_pending_tasks_excluded(self, tmp_path: Path):
        setup_config(tmp_path)
        store = make_store(tmp_path)
        pending = store.add("Pending task")
        completed = store.add("Completed task")
        completed.status = "completed"
        completed.completed_at = datetime.now(UTC)
        store.update(completed)
        client = make_client(tmp_path)
        result = client.get_history()
        ids = {t.id for t in result}
        assert pending.id not in ids
        assert completed.id in ids

    def test_default_excludes_internal_tasks(self, tmp_path: Path):
        setup_config(tmp_path)
        store = make_store(tmp_path)

        internal = store.add("Internal task", task_type="internal")
        internal.status = "completed"
        internal.completed_at = datetime.now(UTC)
        store.update(internal)

        completed = store.add("Completed task", task_type="implement")
        completed.status = "completed"
        completed.completed_at = datetime.now(UTC)
        store.update(completed)

        client = make_client(tmp_path)
        result = client.get_history(limit=None)
        prompts = {task.prompt for task in result}

        assert "Completed task" in prompts
        assert "Internal task" not in prompts

    def test_explicit_internal_type_includes_internal_tasks(self, tmp_path: Path):
        setup_config(tmp_path)
        store = make_store(tmp_path)

        internal = store.add("Internal task", task_type="internal")
        internal.status = "completed"
        internal.completed_at = datetime.now(UTC)
        store.update(internal)

        completed = store.add("Completed task", task_type="implement")
        completed.status = "completed"
        completed.completed_at = datetime.now(UTC)
        store.update(completed)

        client = make_client(tmp_path)
        result = client.get_history(limit=None, task_type="internal")
        prompts = {task.prompt for task in result}

        assert "Internal task" in prompts
        assert "Completed task" not in prompts


# ---------------------------------------------------------------------------
# GzaClient — get_recent_completed
# ---------------------------------------------------------------------------


class TestGetRecentCompleted:
    def test_returns_only_completed_tasks(self, tmp_path: Path):
        setup_config(tmp_path)
        store = make_store(tmp_path)
        completed = store.add("Completed task")
        completed.status = "completed"
        completed.completed_at = datetime.now(UTC)
        store.update(completed)
        failed = store.add("Failed task")
        failed.status = "failed"
        failed.completed_at = datetime.now(UTC)
        store.update(failed)
        client = make_client(tmp_path)
        result = client.get_recent_completed()
        ids = {t.id for t in result}
        assert completed.id in ids
        assert failed.id not in ids

    def test_default_limit_fifteen(self, tmp_path: Path):
        setup_config(tmp_path)
        store = make_store(tmp_path)
        for i in range(20):
            task = store.add(f"Task {i}")
            task.status = "completed"
            task.completed_at = datetime.now(UTC)
            store.update(task)
        client = make_client(tmp_path)
        result = client.get_recent_completed()
        assert len(result) == 15

    def test_custom_limit(self, tmp_path: Path):
        setup_config(tmp_path)
        store = make_store(tmp_path)
        for i in range(10):
            task = store.add(f"Task {i}")
            task.status = "completed"
            task.completed_at = datetime.now(UTC)
            store.update(task)
        client = make_client(tmp_path)
        result = client.get_recent_completed(limit=3)
        assert len(result) == 3

    def test_empty_db_returns_empty_list(self, tmp_path: Path):
        setup_config(tmp_path)
        make_store(tmp_path)
        client = make_client(tmp_path)
        result = client.get_recent_completed()
        assert result == []


# ---------------------------------------------------------------------------
# IncompleteSnapshot
# ---------------------------------------------------------------------------


class TestIncompleteSnapshot:
    def test_total_property(self):
        from gza.api.v0 import IncompleteSnapshot
        from gza.db import Task
        p1 = Task(id=1, prompt="p1")
        p2 = Task(id=2, prompt="p2")
        ip1 = Task(id=3, prompt="ip1")
        snap = IncompleteSnapshot(pending=[p1, p2], in_progress=[ip1])
        assert snap.total == 3

    def test_total_empty(self):
        from gza.api.v0 import IncompleteSnapshot
        snap = IncompleteSnapshot(pending=[], in_progress=[])
        assert snap.total == 0
