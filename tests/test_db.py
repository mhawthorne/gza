"""Tests for database operations and task chaining."""

import hashlib
import json
import os
import sqlite3
import stat
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import Mock, patch

import pytest

from gza import dependency_preconditions as dependency_preconditions_module
from gza.db import (
    DB_UNSET,
    SCHEMA_VERSION,
    ManualMigrationRequired,
    MergeTargetResolutionError,
    NewTaskParams,
    SchemaIntegrityError,
    SqliteTaskStore,
    StepRef,
    Task,
    _ClosingSqliteConnection,
    _is_readonly_operational_error,
    _is_readonly_snapshot_operational_error,
    check_migration_status,
    import_legacy_local_db,
    preview_v25_migration,
    preview_v26_migration,
    resolve_task_id,
    run_v25_migration,
    run_v26_migration,
    run_v27_migration,
)
from gza.review_tasks import build_auto_review_prompt
from gza.runner import _compute_slug_override
from gza.sync_ops import BranchCohort, sync_branch_cohorts


def _legacy_project_id(project_dir: Path, project_name: str) -> str:
    canonical_root = str(project_dir.resolve())
    seed = f"{canonical_root}\n{project_name.strip().lower()}"
    digest = hashlib.sha256(seed.encode("utf-8")).hexdigest()
    return f"p{digest[:31]}"


def test_generate_project_id_normalizes_readable_names(tmp_path: Path) -> None:
    from gza.config import _generate_project_id

    assert _generate_project_id(tmp_path, "tarantino-ui") == "tarantinoui"
    assert _generate_project_id(tmp_path, "My App.2") == "myapp2"
    assert _generate_project_id(tmp_path, "Release 2026.05") == "release202605"


def test_generate_project_id_rejects_unsluggable_names(tmp_path: Path) -> None:
    from gza.config import ConfigError, _generate_project_id

    with pytest.raises(ConfigError, match="cannot be converted into a valid 'project_id'"):
        _generate_project_id(tmp_path, "!!!")


def test_is_readonly_operational_error_accepts_explicit_readonly_variants() -> None:
    """Generic sqlite read-only classification should stay limited to explicit variants."""
    assert _is_readonly_operational_error(sqlite3.OperationalError("attempt to write a readonly database"))
    assert _is_readonly_operational_error(sqlite3.OperationalError("attempt to write a read-only database"))
    assert not _is_readonly_operational_error(sqlite3.OperationalError("disk I/O error"))
    assert not _is_readonly_operational_error(sqlite3.OperationalError("database is locked"))


def test_is_readonly_snapshot_operational_error_accepts_snapshot_variants() -> None:
    """Shared snapshot classifier should stay limited to explicit read-only variants."""
    assert _is_readonly_snapshot_operational_error(
        sqlite3.OperationalError("attempt to write a readonly database")
    )
    assert _is_readonly_snapshot_operational_error(
        sqlite3.OperationalError("attempt to write a read-only database")
    )
    assert not _is_readonly_snapshot_operational_error(sqlite3.OperationalError("disk I/O error"))
    assert not _is_readonly_snapshot_operational_error(sqlite3.OperationalError("database is locked"))


def test_set_task_changed_diff_persists_and_rebase_wrapper_still_works(tmp_path: Path) -> None:
    db_path = tmp_path / "test.db"
    store = SqliteTaskStore(db_path)

    improve = store.add("Improve feature", task_type="improve")
    assert improve.id is not None
    store.set_task_changed_diff(improve.id, False)
    refreshed_improve = store.get(improve.id)
    assert refreshed_improve is not None
    assert refreshed_improve.changed_diff is False

    rebase = store.add("Rebase feature", task_type="rebase")
    assert rebase.id is not None
    store.set_rebase_changed_diff(rebase.id, True)
    refreshed_rebase = store.get(rebase.id)
    assert refreshed_rebase is not None
    assert refreshed_rebase.changed_diff is True


def test_add_tasks_with_artifact_atomic_rollback_does_not_delete_reused_ids(tmp_path: Path) -> None:
    db_path = tmp_path / "test.db"
    store = SqliteTaskStore(db_path)

    seed = store.add("Seed task")
    assert seed.id == "gza-1"

    with patch.object(store, "_add_artifact_conn", side_effect=RuntimeError("artifact write failed")):
        with pytest.raises(RuntimeError, match="artifact write failed"):
            store.add_tasks_with_artifact_atomic(
                tasks=[NewTaskParams(prompt="Batch task", task_type="implement")],
                artifact_task_id=seed.id,
                artifact_kind="plan_review_materialization",
                artifact_label="plan_review_materialization",
                artifact_path=".gza/artifacts/gza-1/plan-review-materialization.txt",
                artifact_byte_size=0,
                artifact_sha256=hashlib.sha256(b"").hexdigest(),
            )

    replacement = store.add("Replacement task")
    assert replacement.id == "gza-2"

    refreshed = store.get(replacement.id)
    assert refreshed is not None
    assert refreshed.prompt == "Replacement task"


class TestTaskChaining:
    """Tests for task chaining functionality."""

    def test_schema_fields_persist(self, tmp_path: Path):
        """Test that new task chaining fields persist correctly."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        # Add a task with all new fields
        task = store.add(
            prompt="Test task",
            task_type="implement",
            based_on=None,
            group="test-group",
            depends_on=None,
            spec="specs/test.md",
            review_scope="slice F-A1 + F-A2: only the classifier slice",
            create_review=True,
            create_pr=True,
            same_branch=True,
        )

        assert task.id is not None
        assert task.group == "test-group"
        assert task.spec == "specs/test.md"
        assert task.create_review is True
        assert task.same_branch is True

        # Retrieve and verify
        retrieved = store.get(task.id)
        assert retrieved is not None
        assert retrieved.group == "test-group"
        assert retrieved.spec == "specs/test.md"
        assert retrieved.review_scope == "slice F-A1 + F-A2: only the classifier slice"
        assert retrieved.create_review is True
        assert retrieved.create_pr is True
        assert retrieved.same_branch is True

    def test_review_verify_metadata_round_trips(self, tmp_path: Path) -> None:
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        review = store.add("Review feature", task_type="review")
        assert review.id is not None
        review.review_verify_command = "uv run pytest tests/ -q"
        review.review_verify_status = "failed"
        review.review_verify_exit_status = "7"
        review.review_verify_failure = "mypy failed"
        review.review_verify_captured_at = datetime(2026, 6, 1, 19, 0, tzinfo=UTC)
        review.review_verify_head_sha = "deadbeef"
        review.review_verify_base_sha = "cafebabe"
        review.review_verify_branch = "feature/review-verify"
        store.update(review)

        refreshed = store.get(review.id)
        assert refreshed is not None
        assert refreshed.review_verify_command == "uv run pytest tests/ -q"
        assert refreshed.review_verify_status == "failed"
        assert refreshed.review_verify_exit_status == "7"
        assert refreshed.review_verify_failure == "mypy failed"
        assert refreshed.review_verify_captured_at == datetime(2026, 6, 1, 19, 0, tzinfo=UTC)
        assert refreshed.review_verify_head_sha == "deadbeef"
        assert refreshed.review_verify_base_sha == "cafebabe"
        assert refreshed.review_verify_branch == "feature/review-verify"

    def test_model_explicit_metadata_round_trips_and_serializes(self, tmp_path: Path) -> None:
        from gza.db import _task_to_dict

        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        explicit = store.add(
            "Explicit model task",
            model="claude-sonnet-4-6",
            model_is_explicit=True,
        )
        implicit = store.add("Implicit model task")
        assert explicit.id is not None
        assert implicit.id is not None

        reloaded_explicit = store.get(explicit.id)
        reloaded_implicit = store.get(implicit.id)

        assert reloaded_explicit is not None
        assert reloaded_explicit.model == "claude-sonnet-4-6"
        assert reloaded_explicit.model_is_explicit is True
        assert _task_to_dict(reloaded_explicit)["model_is_explicit"] is True

        assert reloaded_implicit is not None
        assert reloaded_implicit.model is None
        assert reloaded_implicit.model_is_explicit is False

        reloaded_explicit.model_is_explicit = False
        store.update(reloaded_explicit)

        updated = store.get(explicit.id)
        assert updated is not None
        assert updated.model == "claude-sonnet-4-6"
        assert updated.model_is_explicit is False

    def test_depends_on_relationship(self, tmp_path: Path):
        """Test that depends_on creates correct relationships."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        # Create task chain
        task1 = store.add("First task")
        task2 = store.add("Second task", depends_on=task1.id)
        task3 = store.add("Third task", depends_on=task2.id)

        assert task2.depends_on == task1.id
        assert task3.depends_on == task2.id

    def test_get_next_pending_respects_dependencies(self, tmp_path: Path):
        """Test that get_next_pending skips blocked tasks."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        # Create task chain where task2 depends on task1
        task1 = store.add("First task")
        task2 = store.add("Second task", depends_on=task1.id)
        task3 = store.add("Independent task")

        # task2 is blocked, so next should be task1 or task3
        next_task = store.get_next_pending()
        assert next_task is not None
        assert next_task.id in (task1.id, task3.id)

        # Complete task1
        task1.status = "completed"
        task1.completed_at = datetime.now(UTC)
        store.update(task1)

        # Now task2 should be available
        next_task = store.get_next_pending()
        assert next_task is not None
        # Could be task2 or task3 depending on order
        assert next_task.id in (task2.id, task3.id)

    def test_get_next_pending_skips_internal_tasks(self, tmp_path: Path):
        """Internal tasks should not be selected by default pending-task pickup."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        internal = store.add("Internal task", task_type="internal", skip_learnings=True)
        assert internal.status == "pending"

        assert store.get_next_pending() is None

    def test_pending_queue_orders_urgent_before_fifo(self, tmp_path: Path):
        """Pending queue ordering is urgent-first, FIFO within each lane."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        normal_1 = store.add("Normal 1")
        normal_2 = store.add("Normal 2")
        urgent_1 = store.add("Urgent 1", urgent=True)
        urgent_2 = store.add("Urgent 2", urgent=True)

        pending = store.get_pending()
        assert [task.id for task in pending] == [
            urgent_1.id,
            urgent_2.id,
            normal_1.id,
            normal_2.id,
        ]

    def test_bump_moves_task_to_front_of_urgent_pickup_lane(self, tmp_path: Path):
        """Bumping a task should make it the first pickup item, ahead of older urgent tasks."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        urgent_1 = store.add("Urgent 1", urgent=True)
        urgent_2 = store.add("Urgent 2", urgent=True)
        bumped = store.add("Will be bumped")
        assert bumped.id is not None

        store.set_urgent(bumped.id, True)

        pickup = store.get_pending_pickup()
        assert [task.id for task in pickup[:3]] == [bumped.id, urgent_1.id, urgent_2.id]

    def test_get_next_pending_prefers_urgent(self, tmp_path: Path):
        """get_next_pending picks urgent runnable tasks first."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        normal = store.add("Normal")
        urgent = store.add("Urgent")
        assert urgent.id is not None
        store.set_urgent(urgent.id, True)

        next_task = store.get_next_pending()
        assert next_task is not None
        assert next_task.id == urgent.id
        assert next_task.id != normal.id

    def test_explicit_queue_positions_sort_before_lane_order(self, tmp_path: Path):
        """Explicit queue positions should override urgent/FIFO fallback ordering."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        urgent = store.add("Urgent fallback", urgent=True)
        ordered_two = store.add("Ordered two")
        ordered_one = store.add("Ordered one")

        assert urgent.id is not None
        assert ordered_two.id is not None
        assert ordered_one.id is not None

        store.set_queue_position(ordered_two.id, 2)
        store.set_queue_position(ordered_one.id, 1)

        pending = store.get_pending_pickup()
        assert [task.id for task in pending[:3]] == [
            ordered_one.id,
            ordered_two.id,
            urgent.id,
        ]

    def test_clear_queue_position_closes_gap(self, tmp_path: Path):
        """Clearing explicit order should compact remaining positions."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        first = store.add("First ordered", group="release")
        second = store.add("Second ordered", group="release")
        third = store.add("Third ordered", group="release")
        assert first.id is not None
        assert second.id is not None
        assert third.id is not None

        store.set_queue_position(first.id, 1)
        store.set_queue_position(second.id, 2)
        store.set_queue_position(third.id, 3)
        store.clear_queue_position(second.id)

        refreshed_first = store.get(first.id)
        refreshed_second = store.get(second.id)
        refreshed_third = store.get(third.id)
        assert refreshed_first is not None
        assert refreshed_second is not None
        assert refreshed_third is not None
        assert refreshed_first.queue_position == 1
        assert refreshed_second.queue_position is None
        assert refreshed_third.queue_position == 2

    def test_queue_position_mutation_is_scoped_to_group_bucket(self, tmp_path: Path):
        """Setting/clearing explicit order only mutates positions within the task's group bucket."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        release_first = store.add("Release first", group="release")
        release_second = store.add("Release second", group="release")
        backlog_first = store.add("Backlog first", group="backlog")
        backlog_second = store.add("Backlog second", group="backlog")
        assert release_first.id is not None
        assert release_second.id is not None
        assert backlog_first.id is not None
        assert backlog_second.id is not None

        store.set_queue_position(release_first.id, 1)
        store.set_queue_position(release_second.id, 2)
        store.set_queue_position(backlog_first.id, 1)
        store.set_queue_position(backlog_second.id, 2)

        store.clear_queue_position(release_first.id)

        refreshed_release_first = store.get(release_first.id)
        refreshed_release_second = store.get(release_second.id)
        refreshed_backlog_first = store.get(backlog_first.id)
        refreshed_backlog_second = store.get(backlog_second.id)
        assert refreshed_release_first is not None
        assert refreshed_release_second is not None
        assert refreshed_backlog_first is not None
        assert refreshed_backlog_second is not None

        assert refreshed_release_first.queue_position is None
        assert refreshed_release_second.queue_position == 1
        assert refreshed_backlog_first.queue_position == 1
        assert refreshed_backlog_second.queue_position == 2

    def test_queue_position_mutation_does_not_cross_disjoint_multi_tag_buckets(self, tmp_path: Path):
        """Multi-tag queue mutations should stay isolated to the task's exact tag-set bucket."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        release_first = store.add("Release first", tags=("release", "backend"))
        release_second = store.add("Release second", tags=("release", "backend"))
        ops_first = store.add("Ops first", tags=("ops", "infra"))
        ops_second = store.add("Ops second", tags=("ops", "infra"))
        assert release_first.id is not None
        assert release_second.id is not None
        assert ops_first.id is not None
        assert ops_second.id is not None

        store.set_queue_position(release_first.id, 1)
        store.set_queue_position(release_second.id, 2)
        store.set_queue_position(ops_first.id, 1)
        store.set_queue_position(ops_second.id, 2)

        store.set_queue_position(release_second.id, 1)

        refreshed_release_first = store.get(release_first.id)
        refreshed_release_second = store.get(release_second.id)
        refreshed_ops_first = store.get(ops_first.id)
        refreshed_ops_second = store.get(ops_second.id)
        assert refreshed_release_first is not None
        assert refreshed_release_second is not None
        assert refreshed_ops_first is not None
        assert refreshed_ops_second is not None

        assert refreshed_release_second.queue_position == 1
        assert refreshed_release_first.queue_position == 2
        assert refreshed_ops_first.queue_position == 1
        assert refreshed_ops_second.queue_position == 2

        store.clear_queue_position(release_second.id)

        refreshed_release_first = store.get(release_first.id)
        refreshed_release_second = store.get(release_second.id)
        refreshed_ops_first = store.get(ops_first.id)
        refreshed_ops_second = store.get(ops_second.id)
        assert refreshed_release_first is not None
        assert refreshed_release_second is not None
        assert refreshed_ops_first is not None
        assert refreshed_ops_second is not None

        assert refreshed_release_second.queue_position is None
        assert refreshed_release_first.queue_position == 1
        assert refreshed_ops_first.queue_position == 1
        assert refreshed_ops_second.queue_position == 2

    def test_queue_position_mutation_with_tag_scope_ignores_unrelated_extra_tags(self, tmp_path: Path):
        """Tag-scoped queue mutations should share one ordering across tasks with extra tags."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        release_plain = store.add("Release plain", tags=("release",))
        release_backend = store.add("Release backend", tags=("release", "backend"))
        release_docs = store.add("Release docs", tags=("release", "docs"))
        ops_first = store.add("Ops first", tags=("ops", "infra"))
        ops_second = store.add("Ops second", tags=("ops", "infra"))
        assert release_plain.id is not None
        assert release_backend.id is not None
        assert release_docs.id is not None
        assert ops_first.id is not None
        assert ops_second.id is not None

        store.set_queue_position(release_plain.id, 1, tags=("release",))
        store.set_queue_position(release_backend.id, 2, tags=("release",))
        store.set_queue_position(release_docs.id, 3, tags=("release",))
        store.set_queue_position(ops_first.id, 1)
        store.set_queue_position(ops_second.id, 2)

        store.set_queue_position(release_backend.id, 1, tags=("release",))

        refreshed_release_plain = store.get(release_plain.id)
        refreshed_release_backend = store.get(release_backend.id)
        refreshed_release_docs = store.get(release_docs.id)
        refreshed_ops_first = store.get(ops_first.id)
        refreshed_ops_second = store.get(ops_second.id)
        assert refreshed_release_plain is not None
        assert refreshed_release_backend is not None
        assert refreshed_release_docs is not None
        assert refreshed_ops_first is not None
        assert refreshed_ops_second is not None

        assert refreshed_release_backend.queue_position == 1
        assert refreshed_release_plain.queue_position == 2
        assert refreshed_release_docs.queue_position == 3
        assert refreshed_ops_first.queue_position == 1
        assert refreshed_ops_second.queue_position == 2

        store.clear_queue_position(release_backend.id, tags=("release",))

        refreshed_release_plain = store.get(release_plain.id)
        refreshed_release_backend = store.get(release_backend.id)
        refreshed_release_docs = store.get(release_docs.id)
        refreshed_ops_first = store.get(ops_first.id)
        refreshed_ops_second = store.get(ops_second.id)
        assert refreshed_release_plain is not None
        assert refreshed_release_backend is not None
        assert refreshed_release_docs is not None
        assert refreshed_ops_first is not None
        assert refreshed_ops_second is not None

        assert refreshed_release_plain.queue_position == 1
        assert refreshed_release_backend.queue_position is None
        assert refreshed_release_docs.queue_position == 2
        assert refreshed_ops_first.queue_position == 1
        assert refreshed_ops_second.queue_position == 2

    def test_get_pending_pickup_excludes_non_pickable_pending_tasks(self, tmp_path: Path):
        """Pickup listing excludes internal and dependency-blocked pending tasks."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        runnable = store.add("Runnable pending")
        assert runnable.id is not None
        store.add("Internal pending", task_type="internal")
        blocker = store.add("Dependency blocker")
        blocked = store.add("Blocked pending", depends_on=blocker.id)

        pickup = store.get_pending_pickup()
        pickup_ids = {task.id for task in pickup}

        assert runnable.id in pickup_ids
        assert blocked.id not in pickup_ids
        assert all(task.task_type != "internal" for task in pickup)

    def test_get_pending_pickup_includes_pending_retry_child_of_failed_parent(self, tmp_path: Path):
        """Queued retry children of failed parents must remain visible to normal pickup."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        failed_parent = store.add("Failed parent", task_type="implement")
        assert failed_parent.id is not None
        failed_parent.status = "failed"
        failed_parent.failure_reason = "INFRASTRUCTURE_ERROR"
        failed_parent.completed_at = datetime.now(UTC)
        store.update(failed_parent)

        queued_retry = store.add(
            "Failed parent",
            task_type="implement",
            based_on=failed_parent.id,
        )
        assert queued_retry.id is not None

        pickup_ids = {task.id for task in store.get_pending_pickup()}
        assert queued_retry.id in pickup_ids

    def test_get_pending_pickup_includes_pending_resume_child_of_failed_parent(self, tmp_path: Path):
        """Queued resume children of failed parents must remain visible to normal pickup."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        failed_parent = store.add("Failed parent", task_type="implement")
        assert failed_parent.id is not None
        failed_parent.status = "failed"
        failed_parent.failure_reason = "TIMEOUT"
        failed_parent.session_id = "sess-123"
        failed_parent.completed_at = datetime.now(UTC)
        store.update(failed_parent)

        queued_resume = store.add(
            "Failed parent",
            task_type="implement",
            based_on=failed_parent.id,
        )
        queued_resume.session_id = failed_parent.session_id
        store.update(queued_resume)
        assert queued_resume.id is not None

        pickup_ids = {task.id for task in store.get_pending_pickup()}
        assert queued_resume.id in pickup_ids

    def test_get_in_progress_returns_only_in_progress_tasks(self, tmp_path: Path):
        """Test get_in_progress returns only in-progress tasks."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        pending = store.add("Pending task")
        in_progress = store.add("In-progress task")
        completed = store.add("Completed task")

        store.mark_in_progress(in_progress)
        completed.status = "completed"
        completed.completed_at = datetime.now(UTC)
        store.update(completed)
        store.update(pending)

        rows = store.get_in_progress()
        assert len(rows) == 1
        assert rows[0].id == in_progress.id

    def test_is_task_blocked(self, tmp_path: Path):
        """Test is_task_blocked correctly identifies blocked tasks."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        # Create dependent tasks
        task1 = store.add("First task", task_type="plan")
        task2 = store.add("Second task", depends_on=task1.id)
        task3 = store.add("Independent task")

        # task2 should be blocked
        is_blocked, blocking_id, blocking_status = store.is_task_blocked(task2)
        assert is_blocked is True
        assert blocking_id == task1.id
        assert blocking_status == "pending"

        # task3 should not be blocked
        is_blocked, blocking_id, blocking_status = store.is_task_blocked(task3)
        assert is_blocked is False
        assert blocking_id is None
        assert blocking_status is None

        # Complete task1
        task1.status = "completed"
        task1.completed_at = datetime.now(UTC)
        store.update(task1)

        # task2 should no longer be blocked
        is_blocked, blocking_id, blocking_status = store.is_task_blocked(task2)
        assert is_blocked is False

    def test_count_blocked_tasks(self, tmp_path: Path):
        """Test count_blocked_tasks returns correct count."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        # Create some blocked and unblocked tasks
        task1 = store.add("First task", task_type="plan")
        store.add("Second task", depends_on=task1.id)
        store.add("Third task", depends_on=task1.id)
        store.add("Independent task")

        # Should have 2 blocked tasks
        count = store.count_blocked_tasks()
        assert count == 2

        # Complete task1
        task1.status = "completed"
        task1.completed_at = datetime.now(UTC)
        store.update(task1)

        # Should have 0 blocked tasks
        count = store.count_blocked_tasks()
        assert count == 0

    def test_get_tag_status_counts(self, tmp_path: Path):
        """Tag status counts should come from the canonical tag API."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        # Create tasks in different groups
        task1 = store.add("Task 1", group="group-a")
        store.add("Task 2", group="group-a")
        store.add("Task 3", group="group-b")
        store.add("Task 4")  # No group

        # Mark one as completed
        task1.status = "completed"
        task1.completed_at = datetime.now(UTC)
        store.update(task1)

        tag_counts = store.get_tag_status_counts()

        assert "group-a" in tag_counts
        assert "group-b" in tag_counts
        assert tag_counts["group-a"]["completed"] == 1
        assert tag_counts["group-a"]["pending"] == 1
        assert tag_counts["group-b"]["pending"] == 1


class TestConnectionLifecycle:
    """Targeted regressions for connection cleanup in context-managed store paths."""

    def test_connect_context_closes_connection(self, tmp_path: Path):
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        conn = store._connect()
        with conn as active:
            active.execute("SELECT 1")

        with pytest.raises(sqlite3.ProgrammingError):
            conn.execute("SELECT 1")

    def test_repeated_store_operations_close_each_connection(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)
        seen_connections: list[sqlite3.Connection] = []
        original_connect = store._connect

        def _tracking_connect() -> sqlite3.Connection:
            conn = original_connect()
            seen_connections.append(conn)
            return conn

        monkeypatch.setattr(store, "_connect", _tracking_connect)

        for i in range(25):
            store.add(f"Task {i}")

        for conn in seen_connections:
            with pytest.raises(sqlite3.ProgrammingError):
                conn.execute("SELECT 1")

    def test_write_pragmas_are_only_applied_once_per_store(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
        db_path = tmp_path / "test.db"
        seen_pragmas: list[str] = []

        class TrackingConnection(_ClosingSqliteConnection):
            def execute(self, sql: str, parameters=(), /):
                if sql in {"PRAGMA journal_mode=WAL", "PRAGMA synchronous=NORMAL"}:
                    seen_pragmas.append(sql)
                return super().execute(sql, parameters)

        monkeypatch.setattr("gza.db._ClosingSqliteConnection", TrackingConnection)

        store = SqliteTaskStore(db_path)
        store.add("Task 1")
        store.add("Task 2")
        store.get_all()

        assert seen_pragmas == ["PRAGMA journal_mode=WAL", "PRAGMA synchronous=NORMAL"]

    def test_read_session_reuses_one_underlying_connection_for_many_reads(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)
        task = store.add("Task 1", group="release")
        task.status = "completed"
        task.completed_at = datetime.now(UTC)
        store.update(task)

        opened_connections: list[sqlite3.Connection] = []
        original_open_connection = store._open_connection

        def _tracking_open_connection(*, close_on_exit: bool) -> sqlite3.Connection:
            conn = original_open_connection(close_on_exit=close_on_exit)
            opened_connections.append(conn)
            return conn

        monkeypatch.setattr(store, "_open_connection", _tracking_open_connection)

        with store.read_session():
            assert [row.id for row in store.get_all()] == [task.id]
            assert store.get(task.id) is not None
            assert store.get_by_tag("release")[0].id == task.id
            assert store.get_tag_status_counts()["release"]["completed"] == 1

        assert len(opened_connections) == 1
        with pytest.raises(sqlite3.ProgrammingError):
            opened_connections[0].execute("SELECT 1")

    def test_nested_read_session_inner_exit_keeps_connection_open(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)
        store.add("Task 1")

        opened_connections: list[sqlite3.Connection] = []
        original_open_connection = store._open_connection

        def _tracking_open_connection(*, close_on_exit: bool) -> sqlite3.Connection:
            conn = original_open_connection(close_on_exit=close_on_exit)
            opened_connections.append(conn)
            return conn

        monkeypatch.setattr(store, "_open_connection", _tracking_open_connection)

        with store.read_session():
            outer_conn = store._read_session_conn
            assert outer_conn is not None
            with store.read_session():
                assert store._read_session_conn is outer_conn
                assert store.get_all()
            outer_conn.execute("SELECT 1")

        assert len(opened_connections) == 1
        with pytest.raises(sqlite3.ProgrammingError):
            opened_connections[0].execute("SELECT 1")

    def test_get_by_tag(self, tmp_path: Path):
        """Tag lookups should return tasks in creation order."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        # Create tasks in a group
        task1 = store.add("First", group="test-group")
        task2 = store.add("Second", group="test-group")
        store.add("Third", group="other-group")

        tasks = store.get_by_tag("test-group")
        assert len(tasks) == 2
        assert tasks[0].id == task1.id
        assert tasks[1].id == task2.id

    def test_rename_tag_updates_all_attached_tasks(self, tmp_path: Path):
        """Renaming a tag should update every task carrying that tag."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        first = store.add("First", group="release")
        second = store.add("Second", group="release")
        other = store.add("Other", group="backlog")

        updated = store.rename_tag("release", "launch")

        assert updated == 2
        refreshed_first = store.get(first.id)
        refreshed_second = store.get(second.id)
        refreshed_other = store.get(other.id)
        assert refreshed_first is not None
        assert refreshed_second is not None
        assert refreshed_other is not None
        assert refreshed_first.group == "launch"
        assert refreshed_second.group == "launch"
        assert refreshed_other.group == "backlog"

    def test_rename_tag_rejects_existing_destination_tag(self, tmp_path: Path):
        """Renaming into an existing tag should fail instead of merging."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        store.add("Release task", group="release")
        store.add("Backlog task", group="backlog")

        with pytest.raises(ValueError, match="already exists"):
            store.rename_tag("release", "backlog")

    def test_group_named_tag_apis_are_removed(self, tmp_path: Path):
        """Retired group-named tag helpers should not remain callable."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        assert not hasattr(store, "get_groups")
        assert not hasattr(store, "get_by_group")
        assert not hasattr(store, "rename_group")

    def test_next_task_after_follows_sequence_and_skips_gaps(self, tmp_path: Path):
        """next_task_after finds the next existing sequence ID, not +1 arithmetic."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path, prefix="gza")

        first = store.add("First task")
        second = store.add("Second task")
        third = store.add("Third task")
        assert first.id is not None
        assert second.id is not None
        assert third.id is not None

        # Create a sequence gap; ordinal navigation should skip to the next existing row.
        assert store.delete(second.id) is True

        next_after_first = store.next_task_after(first.id)
        assert next_after_first is not None
        assert next_after_first.id == third.id
        assert store.next_task_after(third.id) is None

    def test_next_task_after_invalid_or_missing_task_id_returns_none(self, tmp_path: Path):
        """next_task_after returns None for malformed IDs and no-successor IDs."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path, prefix="gza")

        only = store.add("Only task")
        assert only.id is not None

        assert store.next_task_after("not-an-id") is None
        assert store.next_task_after("GZA-1") is None
        assert store.next_task_after("bad-prefix-1") is None
        assert store.next_task_after(only.id) is None

    def test_add_repeatedly_does_not_leave_project_sequence_write_locked(self, tmp_path: Path):
        """Repeated task creation should not leave the sequence row write-locked."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path, prefix="gza")

        created_ids = [store.add(f"Task {i}").id for i in range(10)]

        assert created_ids == [f"gza-{i}" for i in range(1, 11)]

    def test_get_by_seq_looks_up_task_by_prefix_and_sequence(self, tmp_path: Path):
        """get_by_seq supports explicit ordinal lookups with optional prefix override."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path, prefix="gza")

        task = store.add("Task")
        assert task.id is not None

        by_seq = store.get_by_seq(1)
        assert by_seq is not None
        assert by_seq.id == task.id
        by_seq_with_prefix = store.get_by_seq(1, prefix="gza")
        assert by_seq_with_prefix is not None
        assert by_seq_with_prefix.id == task.id
        assert store.get_by_seq(999) is None
        assert store.get_by_seq(0) is None

    def test_update_task_with_new_fields(self, tmp_path: Path):
        """Test updating a task with new fields."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        # Create a task
        task = store.add("Test task")
        assert task.group is None
        assert task.create_review is False
        assert task.create_pr is False

        # Update with new fields
        task.group = "new-group"
        task.create_review = True
        task.create_pr = True
        task.same_branch = True
        store.update(task)

        # Retrieve and verify
        retrieved = store.get(task.id)
        assert retrieved.group == "new-group"
        assert retrieved.create_review is True
        assert retrieved.create_pr is True
        assert retrieved.same_branch is True

    def test_task_with_branch_field(self, tmp_path: Path):
        """Test that branch field is persisted correctly."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        task = store.add("Test task")
        assert task.branch is None

        # Mark completed with branch
        task.status = "completed"
        task.branch = "test-project/test-branch"
        task.completed_at = datetime.now(UTC)
        store.update(task)

        # Retrieve and verify
        retrieved = store.get(task.id)
        assert retrieved.branch == "test-project/test-branch"

    def test_migration_from_old_schema(self, tmp_path: Path):
        """Test that old database is migrated correctly."""
        import sqlite3

        db_path = tmp_path / "test.db"

        # Create old schema (v1) manually
        conn = sqlite3.connect(db_path)
        conn.execute("""
            CREATE TABLE schema_version (version INTEGER PRIMARY KEY)
        """)
        conn.execute("INSERT INTO schema_version (version) VALUES (1)")
        conn.execute("""
            CREATE TABLE tasks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                prompt TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending',
                task_type TEXT NOT NULL DEFAULT 'task',
                task_id TEXT,
                branch TEXT,
                log_file TEXT,
                report_file TEXT,
                based_on INTEGER REFERENCES tasks(id),
                has_commits INTEGER,
                duration_seconds REAL,
                num_turns INTEGER,
                cost_usd REAL,
                created_at TEXT NOT NULL,
                started_at TEXT,
                completed_at TEXT
            )
        """)
        conn.execute("""
            INSERT INTO tasks (prompt, status, created_at)
            VALUES ('Old task', 'pending', '2024-01-01T00:00:00')
        """)
        conn.commit()
        conn.close()

        # Open with SqliteTaskStore to trigger auto-migrations, then run manual v25
        with pytest.raises(ManualMigrationRequired):
            SqliteTaskStore(db_path)
        _run_v25_v26_v27_migrations(db_path, "gza")
        store = SqliteTaskStore(db_path)

        # Verify migration worked
        task = store.get("gza-1")
        assert task is not None
        assert task.prompt == "Old task"
        assert task.group is None  # New field should be NULL
        assert task.depends_on is None
        assert task.same_branch is False

        # Verify we can add new tasks with new fields
        new_task = store.add("New task", group="test-group", create_review=True)
        assert new_task.group == "test-group"
        assert new_task.create_review is True


class TestTaskMethods:
    """Tests for Task dataclass methods."""

    def test_is_explore(self):
        """Test is_explore method."""
        task = Task(id="gza-1", prompt="Test", task_type="explore")
        assert task.is_explore() is True

        task = Task(id="gza-1", prompt="Test", task_type="implement")
        assert task.is_explore() is False

    def test_is_blocked(self):
        """Test is_blocked method."""
        task = Task(id="gza-1", prompt="Test", depends_on="gza-5")
        assert task.is_blocked() is True

        task = Task(id="gza-1", prompt="Test", depends_on=None)
        assert task.is_blocked() is False


class TestOutputContentPersistence:
    """Tests for output_content field persistence."""

    def test_output_content_stored_and_retrieved(self, tmp_path: Path):
        """Test that output_content is stored and retrieved correctly."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        # Add a plan task
        task = store.add(prompt="Design authentication system", task_type="plan")

        # Simulate completing the task with output content
        plan_content = """# Authentication System Design

## Overview
This plan outlines the implementation of a JWT-based authentication system.

## Key Components
1. User registration endpoint
2. Login endpoint with JWT generation
"""
        store.mark_completed(
            task,
            report_file=".gza/plans/test-plan.md",
            output_content=plan_content,
            has_commits=False,
        )

        # Retrieve and verify
        retrieved = store.get(task.id)
        assert retrieved is not None
        assert retrieved.status == "completed"
        assert retrieved.output_content == plan_content

    def test_output_content_null_by_default(self, tmp_path: Path):
        """Test that output_content is None for tasks without it."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        task = store.add(prompt="Simple task")
        retrieved = store.get(task.id)
        assert retrieved.output_content is None

    def test_migration_from_v3_to_v4(self, tmp_path: Path):
        """Test that migration from v3 to v4 adds output_content column."""
        import sqlite3

        db_path = tmp_path / "test.db"

        # Create a v3 database manually (without output_content)
        conn = sqlite3.connect(db_path)
        conn.execute("CREATE TABLE schema_version (version INTEGER PRIMARY KEY)")
        conn.execute("INSERT INTO schema_version (version) VALUES (3)")
        conn.execute("""
            CREATE TABLE tasks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                prompt TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending',
                task_type TEXT NOT NULL DEFAULT 'task',
                task_id TEXT,
                branch TEXT,
                log_file TEXT,
                report_file TEXT,
                based_on INTEGER REFERENCES tasks(id),
                has_commits INTEGER,
                duration_seconds REAL,
                num_turns INTEGER,
                cost_usd REAL,
                created_at TEXT NOT NULL,
                started_at TEXT,
                completed_at TEXT,
                "group" TEXT,
                depends_on INTEGER REFERENCES tasks(id),
                spec TEXT,
                create_review INTEGER DEFAULT 0,
                same_branch INTEGER DEFAULT 0
            )
        """)

        # Insert a test task
        now = datetime.now(UTC).isoformat()
        conn.execute(
            "INSERT INTO tasks (prompt, task_type, created_at) VALUES (?, ?, ?)",
            ("Old task", "plan", now),
        )
        conn.commit()
        conn.close()

        # Open with SqliteTaskStore - auto-migrates up to v24, then manual v25
        with pytest.raises(ManualMigrationRequired):
            SqliteTaskStore(db_path)
        _run_v25_v26_v27_migrations(db_path, "gza")
        store = SqliteTaskStore(db_path)

        # Check schema version
        conn = sqlite3.connect(db_path)
        cur = conn.execute("SELECT version FROM schema_version")
        version = cur.fetchone()[0]
        conn.close()
        assert version == SCHEMA_VERSION

        # Verify old task can be retrieved (with NULL output_content)
        task = store.get("gza-1")
        assert task is not None
        assert task.output_content is None

        # Create new task with output_content
        new_task = store.add(prompt="New task", task_type="plan")
        store.mark_completed(
            new_task,
            output_content="This is the plan content",
            has_commits=False,
        )

        retrieved = store.get(new_task.id)
        assert retrieved.output_content == "This is the plan content"


class TestTaskResume:
    """Tests for task resume functionality."""

    def test_session_id_stored_and_retrieved(self, tmp_path: Path):
        """Test that session_id is stored and retrieved correctly."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        # Add a task
        task = store.add(prompt="Test task")
        assert task.session_id is None

        # Update with session_id
        task.session_id = "e9de1481-112a-4937-a06d-087a88a32999"
        store.update(task)

        # Retrieve and verify
        retrieved = store.get(task.id)
        assert retrieved is not None
        assert retrieved.session_id == "e9de1481-112a-4937-a06d-087a88a32999"

    def test_session_id_persists_on_failure(self, tmp_path: Path):
        """Test that session_id is persisted when a task fails."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        # Add a task and mark it as failed with session_id
        task = store.add(prompt="Test task")
        task.session_id = "test-session-123"
        store.mark_failed(task, log_file="logs/test.log")

        # Retrieve and verify
        retrieved = store.get(task.id)
        assert retrieved.status == "failed"
        assert retrieved.session_id == "test-session-123"

    def test_migration_from_v4_to_v5(self, tmp_path: Path):
        """Test that migration from v4 to v5 adds session_id column."""
        import sqlite3

        db_path = tmp_path / "test.db"

        # Create a v4 database manually (without session_id)
        conn = sqlite3.connect(db_path)
        conn.execute("CREATE TABLE schema_version (version INTEGER PRIMARY KEY)")
        conn.execute("INSERT INTO schema_version (version) VALUES (4)")
        conn.execute("""
            CREATE TABLE tasks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                prompt TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending',
                task_type TEXT NOT NULL DEFAULT 'task',
                task_id TEXT,
                branch TEXT,
                log_file TEXT,
                report_file TEXT,
                based_on INTEGER REFERENCES tasks(id),
                has_commits INTEGER,
                duration_seconds REAL,
                num_turns INTEGER,
                cost_usd REAL,
                created_at TEXT NOT NULL,
                started_at TEXT,
                completed_at TEXT,
                "group" TEXT,
                depends_on INTEGER REFERENCES tasks(id),
                spec TEXT,
                create_review INTEGER DEFAULT 0,
                same_branch INTEGER DEFAULT 0,
                task_type_hint TEXT,
                output_content TEXT
            )
        """)

        # Insert a test task
        now = datetime.now(UTC).isoformat()
        conn.execute(
            "INSERT INTO tasks (prompt, status, created_at) VALUES (?, ?, ?)",
            ("Old task", "failed", now),
        )
        conn.commit()
        conn.close()

        # Open with SqliteTaskStore - auto-migrates up to v24, then manual v25
        with pytest.raises(ManualMigrationRequired):
            SqliteTaskStore(db_path)
        _run_v25_v26_v27_migrations(db_path, "gza")
        store = SqliteTaskStore(db_path)

        # Check schema version
        conn = sqlite3.connect(db_path)
        cur = conn.execute("SELECT version FROM schema_version")
        version = cur.fetchone()[0]
        conn.close()
        assert version == SCHEMA_VERSION

        # Verify old task can be retrieved (with NULL session_id)
        task = store.get("gza-1")
        assert task is not None
        assert task.session_id is None

        # Create new task with session_id
        new_task = store.add(prompt="New task")
        new_task.session_id = "new-session-456"
        store.update(new_task)

        retrieved = store.get(new_task.id)
        assert retrieved.session_id == "new-session-456"


class TestNumTurnsFields:
    """Tests for num_turns_reported and num_turns_computed fields."""

    def test_num_steps_reported_stored_and_retrieved(self, tmp_path: Path):
        """Step metrics should be stored and retrieved correctly."""
        from gza.db import TaskStats

        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        task = store.add(prompt="Test task")
        stats = TaskStats(
            num_steps_reported=12,
            num_steps_computed=10,
            num_turns_reported=6,
            num_turns_computed=5,
        )
        store.mark_completed(task, has_commits=False, stats=stats)

        retrieved = store.get(task.id)
        assert retrieved is not None
        assert retrieved.num_steps_reported == 12
        assert retrieved.num_steps_computed == 10
        assert retrieved.num_turns_reported == 6
        assert retrieved.num_turns_computed == 5

    def test_num_turns_reported_stored_and_retrieved(self, tmp_path: Path):
        """Test that num_turns_reported is stored and retrieved correctly."""
        from gza.db import TaskStats

        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        task = store.add(prompt="Test task")
        stats = TaskStats(
            duration_seconds=42.0,
            num_turns_reported=10,
            num_turns_computed=8,
            cost_usd=0.15,
        )
        store.mark_completed(task, has_commits=False, stats=stats)

        retrieved = store.get(task.id)
        assert retrieved is not None
        assert retrieved.num_turns_reported == 10
        assert retrieved.num_turns_computed == 8

    def test_num_turns_fields_default_to_none(self, tmp_path: Path):
        """Test that num_turns fields default to None when not set."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        task = store.add(prompt="Test task")

        retrieved = store.get(task.id)
        assert retrieved is not None
        assert retrieved.num_turns_reported is None
        assert retrieved.num_turns_computed is None

    def test_get_stats_aggregates_num_turns_reported(self, tmp_path: Path):
        """Test that get_stats sums num_turns_reported correctly."""
        from gza.db import TaskStats

        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        task1 = store.add(prompt="Task 1")
        store.mark_completed(task1, has_commits=False, stats=TaskStats(num_turns_reported=5))

        task2 = store.add(prompt="Task 2")
        store.mark_completed(task2, has_commits=False, stats=TaskStats(num_turns_reported=7))

        stats = store.get_stats()
        assert stats["total_turns"] == 12

    def test_get_stats_aggregates_num_steps_reported(self, tmp_path: Path):
        """Test that get_stats sums num_steps_reported correctly."""
        from gza.db import TaskStats

        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        task1 = store.add(prompt="Task 1")
        store.mark_completed(task1, has_commits=False, stats=TaskStats(num_steps_reported=8))

        task2 = store.add(prompt="Task 2")
        store.mark_completed(task2, has_commits=False, stats=TaskStats(num_steps_reported=9))

        stats = store.get_stats()
        assert stats["total_steps"] == 17

    def test_get_stats_aggregates_step_fallback_chain(self, tmp_path: Path):
        """Test that get_stats uses steps -> computed steps -> legacy turns fallback."""
        from gza.db import TaskStats

        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        reported = store.add(prompt="Reported steps")
        store.mark_completed(reported, has_commits=False, stats=TaskStats(num_steps_reported=8))

        computed_only = store.add(prompt="Computed steps")
        store.mark_completed(computed_only, has_commits=False, stats=TaskStats(num_steps_computed=5))

        turns_only = store.add(prompt="Legacy turns")
        store.mark_completed(turns_only, has_commits=False, stats=TaskStats(num_turns_reported=3))

        stats = store.get_stats()
        assert stats["total_steps"] == 16

    def test_migration_v7_to_v8_adds_columns(self, tmp_path: Path):
        """Test that migration from v7 to v8 adds num_turns_reported and num_turns_computed."""
        import sqlite3
        from datetime import datetime

        db_path = tmp_path / "test.db"

        # Create a v7 database manually (without the new columns)
        conn = sqlite3.connect(db_path)
        conn.execute("CREATE TABLE schema_version (version INTEGER PRIMARY KEY)")
        conn.execute("INSERT INTO schema_version (version) VALUES (7)")
        conn.execute("""
            CREATE TABLE tasks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                prompt TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending',
                task_type TEXT NOT NULL DEFAULT 'task',
                task_id TEXT,
                branch TEXT,
                log_file TEXT,
                report_file TEXT,
                based_on INTEGER REFERENCES tasks(id),
                has_commits INTEGER,
                duration_seconds REAL,
                num_turns INTEGER,
                cost_usd REAL,
                created_at TEXT NOT NULL,
                started_at TEXT,
                completed_at TEXT,
                "group" TEXT,
                depends_on INTEGER REFERENCES tasks(id),
                spec TEXT,
                create_review INTEGER DEFAULT 0,
                same_branch INTEGER DEFAULT 0,
                task_type_hint TEXT,
                output_content TEXT,
                session_id TEXT,
                pr_number INTEGER,
                model TEXT,
                provider TEXT
            )
        """)

        now = datetime.now(UTC).isoformat()
        conn.execute(
            "INSERT INTO tasks (prompt, status, created_at, num_turns) VALUES (?, ?, ?, ?)",
            ("Old task with turns", "completed", now, 15),
        )
        conn.commit()
        conn.close()

        # Open with SqliteTaskStore to trigger auto-migrations, then manual v25
        with pytest.raises(ManualMigrationRequired):
            SqliteTaskStore(db_path)
        _run_v25_v26_v27_migrations(db_path, "gza")
        store = SqliteTaskStore(db_path)

        # Check schema version updated
        conn = sqlite3.connect(db_path)
        cur = conn.execute("SELECT version FROM schema_version")
        version = cur.fetchone()[0]
        conn.close()
        assert version == SCHEMA_VERSION

        # Verify old task migrated: num_turns_reported populated from num_turns
        task = store.get("gza-1")
        assert task is not None
        assert task.num_turns_reported == 15
        assert task.num_turns_computed is None

        # Verify new tasks can store both fields
        new_task = store.add(prompt="New task")
        from gza.db import TaskStats
        store.mark_completed(new_task, has_commits=False, stats=TaskStats(
            num_turns_reported=3,
            num_turns_computed=2,
        ))
        retrieved = store.get(new_task.id)
        assert retrieved.num_turns_reported == 3
        assert retrieved.num_turns_computed == 2


class TestTokenCountFields:
    """Tests for input_tokens and output_tokens fields."""

    def test_token_counts_stored_and_retrieved(self, tmp_path: Path):
        """Test that input_tokens and output_tokens are stored and retrieved correctly."""
        from gza.db import TaskStats

        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        task = store.add(prompt="Test task")
        stats = TaskStats(
            duration_seconds=30.0,
            num_turns_reported=5,
            cost_usd=0.10,
            input_tokens=12345,
            output_tokens=6789,
        )
        store.mark_completed(task, has_commits=False, stats=stats)

        retrieved = store.get(task.id)
        assert retrieved is not None
        assert retrieved.input_tokens == 12345
        assert retrieved.output_tokens == 6789

    def test_token_counts_default_to_none(self, tmp_path: Path):
        """Test that token count fields default to None when not set."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        task = store.add(prompt="Test task")
        retrieved = store.get(task.id)
        assert retrieved is not None
        assert retrieved.input_tokens is None
        assert retrieved.output_tokens is None

    def test_token_counts_persisted_on_failure(self, tmp_path: Path):
        """Test that token counts are persisted when a task fails."""
        from gza.db import TaskStats

        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        task = store.add(prompt="Test task")
        stats = TaskStats(input_tokens=500, output_tokens=200)
        store.mark_failed(task, log_file="logs/test.log", stats=stats)

        retrieved = store.get(task.id)
        assert retrieved is not None
        assert retrieved.status == "failed"
        assert retrieved.input_tokens == 500
        assert retrieved.output_tokens == 200

    def test_token_counts_persisted_on_unmerged(self, tmp_path: Path):
        """Test that token counts are persisted when a task is marked unmerged."""
        from gza.db import TaskStats

        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        task = store.add(prompt="Test task")
        stats = TaskStats(input_tokens=300, output_tokens=100)
        store.mark_unmerged(task, branch="test/branch", stats=stats)

        retrieved = store.get(task.id)
        assert retrieved is not None
        assert retrieved.status == "unmerged"
        assert retrieved.input_tokens == 300
        assert retrieved.output_tokens == 100

    def test_get_stats_aggregates_token_counts(self, tmp_path: Path):
        """Test that get_stats sums input_tokens and output_tokens correctly."""
        from gza.db import TaskStats

        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        task1 = store.add(prompt="Task 1")
        store.mark_completed(task1, has_commits=False, stats=TaskStats(
            input_tokens=1000, output_tokens=500,
        ))

        task2 = store.add(prompt="Task 2")
        store.mark_completed(task2, has_commits=False, stats=TaskStats(
            input_tokens=2000, output_tokens=800,
        ))

        # Task with no token counts (should be treated as 0)
        task3 = store.add(prompt="Task 3")
        store.mark_completed(task3, has_commits=False, stats=TaskStats())

        stats = store.get_stats()
        assert stats["total_input_tokens"] == 3000
        assert stats["total_output_tokens"] == 1300

    def test_get_stats_token_counts_zero_when_no_data(self, tmp_path: Path):
        """Test that get_stats returns 0 for token counts when no tasks have them."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        stats = store.get_stats()
        assert stats["total_input_tokens"] == 0
        assert stats["total_output_tokens"] == 0

    def test_migration_v8_to_v9_adds_token_columns(self, tmp_path: Path):
        """Test that migration from v8 to v9 adds input_tokens and output_tokens columns."""
        import sqlite3
        from datetime import datetime

        db_path = tmp_path / "test.db"

        # Create a v8 database manually (without the token count columns)
        conn = sqlite3.connect(db_path)
        conn.execute("CREATE TABLE schema_version (version INTEGER PRIMARY KEY)")
        conn.execute("INSERT INTO schema_version (version) VALUES (8)")
        conn.execute("""
            CREATE TABLE tasks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                prompt TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending',
                task_type TEXT NOT NULL DEFAULT 'task',
                task_id TEXT,
                branch TEXT,
                log_file TEXT,
                report_file TEXT,
                based_on INTEGER REFERENCES tasks(id),
                has_commits INTEGER,
                duration_seconds REAL,
                num_turns INTEGER,
                num_turns_reported INTEGER,
                num_turns_computed INTEGER,
                cost_usd REAL,
                created_at TEXT NOT NULL,
                started_at TEXT,
                completed_at TEXT,
                "group" TEXT,
                depends_on INTEGER REFERENCES tasks(id),
                spec TEXT,
                create_review INTEGER DEFAULT 0,
                same_branch INTEGER DEFAULT 0,
                task_type_hint TEXT,
                output_content TEXT,
                session_id TEXT,
                pr_number INTEGER,
                model TEXT,
                provider TEXT
            )
        """)

        now = datetime.now(UTC).isoformat()
        conn.execute(
            "INSERT INTO tasks (prompt, status, created_at, cost_usd) VALUES (?, ?, ?, ?)",
            ("Old task", "completed", now, 0.05),
        )
        conn.commit()
        conn.close()

        # Open with SqliteTaskStore to trigger auto-migrations, then manual v25
        with pytest.raises(ManualMigrationRequired):
            SqliteTaskStore(db_path)
        _run_v25_v26_v27_migrations(db_path, "gza")
        store = SqliteTaskStore(db_path)

        # Check schema version updated
        conn = sqlite3.connect(db_path)
        cur = conn.execute("SELECT version FROM schema_version")
        version = cur.fetchone()[0]
        conn.close()
        assert version == SCHEMA_VERSION

        # Verify old task can be retrieved with NULL token counts
        task = store.get("gza-1")
        assert task is not None
        assert task.input_tokens is None
        assert task.output_tokens is None

        # Verify new tasks can store token counts
        from gza.db import TaskStats
        new_task = store.add(prompt="New task")
        store.mark_completed(new_task, has_commits=False, stats=TaskStats(
            input_tokens=10000,
            output_tokens=5000,
            cost_usd=0.10,
        ))
        retrieved = store.get(new_task.id)
        assert retrieved.input_tokens == 10000
        assert retrieved.output_tokens == 5000


class TestGetReviewsForTask:
    """Tests for get_reviews_for_task method."""

    def test_get_reviews_for_task_returns_matching_reviews(self, tmp_path: Path):
        """Test that get_reviews_for_task returns reviews linked to the given task."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        # Create an implementation task
        impl_task = store.add("Add feature", task_type="implement")

        # Create review tasks linked via both canonical and legacy relations.
        review1 = store.add("First review", task_type="review", based_on=impl_task.id)
        review2 = store.add("Second review", task_type="review", depends_on=impl_task.id)

        # Create unrelated review
        other_impl = store.add("Other feature", task_type="implement")
        other_review = store.add("Other review", task_type="review", based_on=other_impl.id)

        # Get reviews for impl_task
        reviews = store.get_reviews_for_task(impl_task.id)

        assert len(reviews) == 2
        review_ids = [r.id for r in reviews]
        assert review1.id in review_ids
        assert review2.id in review_ids
        assert other_review.id not in review_ids

    def test_get_reviews_for_task_ordered_by_completed_at_desc(self, tmp_path: Path):
        """Test that reviews are returned in descending order by completed_at.

        The most recently *completed* review should be first, regardless of
        creation order. Incomplete reviews (completed_at IS NULL) sort last.
        """
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        impl_task = store.add("Add feature", task_type="implement")

        # Create reviews in order
        review1 = store.add("First review", task_type="review", depends_on=impl_task.id)
        review2 = store.add("Second review", task_type="review", depends_on=impl_task.id)
        review3 = store.add("Third review", task_type="review", depends_on=impl_task.id)

        # Complete them in reverse order: review3 first (oldest completed_at),
        # review1 last (newest completed_at). This is opposite of creation order
        # so we can confirm the sort is by completed_at, not created_at.
        t1 = datetime(2026, 1, 1, 10, 0, 0, tzinfo=UTC)
        t2 = datetime(2026, 1, 1, 11, 0, 0, tzinfo=UTC)
        t3 = datetime(2026, 1, 1, 12, 0, 0, tzinfo=UTC)

        review3.completed_at = t1  # completed first (oldest)
        review3.status = "completed"
        store.update(review3)

        review2.completed_at = t2
        review2.status = "completed"
        store.update(review2)

        review1.completed_at = t3  # completed last (newest)
        review1.status = "completed"
        store.update(review1)

        reviews = store.get_reviews_for_task(impl_task.id)

        # Most recently completed should be first
        assert reviews[0].id == review1.id  # completed_at = t3 (latest)
        assert reviews[1].id == review2.id  # completed_at = t2
        assert reviews[2].id == review3.id  # completed_at = t1 (earliest)

    def test_get_reviews_for_task_incomplete_reviews_sort_last(self, tmp_path: Path):
        """Test that reviews without completed_at sort after completed reviews."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        impl_task = store.add("Add feature", task_type="implement")

        completed_review = store.add("Completed review", task_type="review", depends_on=impl_task.id)
        incomplete_review = store.add("In-progress review", task_type="review", depends_on=impl_task.id)

        # Complete only the first review
        completed_review.completed_at = datetime(2026, 1, 1, 10, 0, 0, tzinfo=UTC)
        completed_review.status = "completed"
        store.update(completed_review)
        # incomplete_review has completed_at = NULL

        reviews = store.get_reviews_for_task(impl_task.id)

        assert len(reviews) == 2
        # Completed review comes first, incomplete (NULL completed_at) comes last
        assert reviews[0].id == completed_review.id
        assert reviews[1].id == incomplete_review.id

    def test_get_reviews_for_task_prefers_based_on_over_conflicting_depends_on(self, tmp_path: Path):
        """A review with both links belongs to its canonical based_on implementation."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        canonical_impl = store.add("Canonical impl", task_type="implement")
        legacy_impl = store.add("Legacy impl", task_type="implement")

        review = store.add(
            "Imported review",
            task_type="review",
            based_on=canonical_impl.id,
            depends_on=legacy_impl.id,
        )

        canonical_reviews = store.get_reviews_for_task(canonical_impl.id)
        legacy_reviews = store.get_reviews_for_task(legacy_impl.id)

        assert [task.id for task in canonical_reviews] == [review.id]
        assert legacy_reviews == []

    def test_get_hydrates_legacy_naive_timestamps_as_utc_aware(self, tmp_path: Path):
        """Legacy rows without an offset should load as UTC-aware datetimes."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        task = store.add("Legacy timestamp task")
        assert task.id is not None

        conn = sqlite3.connect(db_path)
        conn.execute(
            "UPDATE tasks SET created_at = ?, completed_at = ? WHERE id = ?",
            ("2026-01-01T10:00:00", "2026-01-01T11:00:00", task.id),
        )
        conn.commit()
        conn.close()

        reloaded = store.get(task.id)

        assert reloaded is not None
        assert reloaded.created_at == datetime(2026, 1, 1, 10, 0, 0, tzinfo=UTC)
        assert reloaded.completed_at == datetime(2026, 1, 1, 11, 0, 0, tzinfo=UTC)

    def test_update_and_run_writers_normalize_naive_datetimes_to_canonical_utc(self, tmp_path: Path) -> None:
        """Write paths should persist naive datetimes as UTC-aware ISO strings."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path, prefix="gza")

        task = store.add("Normalize naive timestamps")
        assert task.id is not None
        task.started_at = datetime(2026, 1, 2, 9, 0, 0)
        task.completed_at = datetime(2026, 1, 2, 10, 0, 0)
        task.pr_last_synced_at = datetime(2026, 1, 2, 11, 0, 0)
        task.sync_last_synced_at = datetime(2026, 1, 2, 12, 0, 0)
        task.merged_at = datetime(2026, 1, 2, 13, 0, 0)
        task.review_cleared_at = datetime(2026, 1, 2, 14, 0, 0)
        store.update(task)

        step_ref = store.emit_step(
            task.id,
            "step",
            provider="codex",
            started_at=datetime(2026, 1, 2, 15, 0, 0),
        )
        store.emit_substep(
            step_ref,
            "tool_call",
            {"ok": True},
            source="assistant",
            timestamp=datetime(2026, 1, 2, 15, 1, 0),
        )
        store.finalize_step(step_ref, "completed", completed_at=datetime(2026, 1, 2, 15, 2, 0))

        unit = store.create_merge_unit(
            source_branch="feature/timestamps",
            target_branch="main",
            owner_task_id=task.id,
            state="merged",
            merged_at=datetime(2026, 1, 2, 16, 0, 0),
            pr_last_synced_at=datetime(2026, 1, 2, 16, 1, 0),
            sync_last_synced_at=datetime(2026, 1, 2, 16, 2, 0),
        )
        store.attach_task_to_merge_unit(task.id, unit.id, "owner")

        with sqlite3.connect(db_path) as conn:
            task_row = conn.execute(
                """
                SELECT started_at, completed_at, pr_last_synced_at, sync_last_synced_at, merged_at, review_cleared_at
                FROM tasks
                WHERE id = ?
                """,
                (task.id,),
            ).fetchone()
            assert task_row == (
                "2026-01-02T09:00:00+00:00",
                "2026-01-02T10:00:00+00:00",
                "2026-01-02T11:00:00+00:00",
                "2026-01-02T12:00:00+00:00",
                "2026-01-02T13:00:00+00:00",
                "2026-01-02T14:00:00+00:00",
            )
            step_row = conn.execute(
                "SELECT started_at, completed_at FROM run_steps WHERE run_id = ?",
                (task.id,),
            ).fetchone()
            assert step_row == ("2026-01-02T15:00:00+00:00", "2026-01-02T15:02:00+00:00")
            substep_row = conn.execute(
                "SELECT timestamp FROM run_substeps WHERE run_id = ?",
                (task.id,),
            ).fetchone()
            assert substep_row == ("2026-01-02T15:01:00+00:00",)
            unit_row = conn.execute(
                """
                SELECT merged_at, pr_last_synced_at, sync_last_synced_at
                FROM merge_units
                WHERE id = ?
                """,
                (unit.id,),
            ).fetchone()
            assert unit_row == (
                "2026-01-02T16:00:00+00:00",
                "2026-01-02T16:01:00+00:00",
                "2026-01-02T16:02:00+00:00",
            )

    def test_auto_migration_v43_to_v44_backfills_all_persisted_timestamp_tables(self, tmp_path: Path) -> None:
        """Opening a v43 DB should rewrite legacy naive timestamp strings to canonical UTC-aware text."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path, prefix="gza")

        task = store.add("Backfill timestamp migration")
        assert task.id is not None
        step_ref = store.emit_step(
            task.id,
            "step",
            provider="codex",
            started_at=datetime(2026, 1, 3, 10, 0, tzinfo=UTC),
        )
        store.emit_substep(
            step_ref,
            "tool_call",
            {"ok": True},
            source="assistant",
            timestamp=datetime(2026, 1, 3, 10, 1, tzinfo=UTC),
        )
        store.finalize_step(step_ref, "completed", completed_at=datetime(2026, 1, 3, 10, 2, tzinfo=UTC))
        comment = store.add_comment(task.id, "comment")
        unit = store.create_merge_unit(
            source_branch="feature/backfill",
            target_branch="main",
            owner_task_id=task.id,
            state="merged",
            merged_at=datetime(2026, 1, 3, 11, 2, tzinfo=UTC),
            pr_last_synced_at=datetime(2026, 1, 3, 11, 3, tzinfo=UTC),
            sync_last_synced_at=datetime(2026, 1, 3, 11, 4, tzinfo=UTC),
        )
        store.attach_task_to_merge_unit(task.id, unit.id, "owner")

        with sqlite3.connect(db_path) as conn:
            conn.execute(
                """
                UPDATE projects
                SET created_at = ?, last_seen_at = ?
                WHERE id = ?
                """,
                ("2026-01-03T08:00:00", "2026-01-03T08:01:00", "default"),
            )
            conn.execute(
                """
                UPDATE tasks
                SET created_at = ?,
                    started_at = ?,
                    completed_at = ?,
                    pr_last_synced_at = ?,
                    sync_last_synced_at = ?,
                    urgent_bumped_at = ?,
                    merged_at = ?,
                    review_cleared_at = ?
                WHERE id = ?
                """,
                (
                    "2026-01-03T09:00:00",
                    "2026-01-03T09:01:00",
                    "2026-01-03T09:02:00",
                    "2026-01-03T09:03:00",
                    "2026-01-03T09:04:00",
                    "2026-01-03T09:05:00",
                    "2026-01-03T09:06:00",
                    "2026-01-03T09:07:00",
                    task.id,
                ),
            )
            conn.execute(
                "UPDATE run_steps SET started_at = ?, completed_at = ? WHERE run_id = ?",
                ("2026-01-03T10:00:00", "2026-01-03T10:02:00", task.id),
            )
            conn.execute(
                "UPDATE run_substeps SET timestamp = ? WHERE run_id = ?",
                ("2026-01-03T10:01:00", task.id),
            )
            conn.execute(
                "UPDATE task_comments SET created_at = ?, resolved_at = ? WHERE id = ?",
                ("2026-01-03T10:03:00", "2026-01-03T10:04:00", comment.id),
            )
            conn.execute(
                """
                UPDATE merge_units
                SET created_at = ?,
                    updated_at = ?,
                    merged_at = ?,
                    pr_last_synced_at = ?,
                    sync_last_synced_at = ?
                WHERE id = ?
                """,
                (
                    "2026-01-03T11:00:00",
                    "2026-01-03T11:01:00",
                    "2026-01-03T11:02:00",
                    "2026-01-03T11:03:00",
                    "2026-01-03T11:04:00",
                    unit.id,
                ),
            )
            conn.execute(
                """
                UPDATE merge_unit_tasks
                SET attached_at = ?
                WHERE merge_unit_id = ? AND task_id = ?
                """,
                ("2026-01-03T11:05:00", unit.id, task.id),
            )
            conn.execute("UPDATE schema_version SET version = 43")
            conn.commit()

        migrated = SqliteTaskStore(db_path, prefix="gza")
        reloaded = migrated.get(task.id)
        assert reloaded is not None
        assert reloaded.created_at == datetime(2026, 1, 3, 9, 0, tzinfo=UTC)
        assert reloaded.started_at == datetime(2026, 1, 3, 9, 1, tzinfo=UTC)
        assert reloaded.completed_at == datetime(2026, 1, 3, 9, 2, tzinfo=UTC)
        assert reloaded.pr_last_synced_at == datetime(2026, 1, 3, 9, 3, tzinfo=UTC)
        assert reloaded.sync_last_synced_at == datetime(2026, 1, 3, 9, 4, tzinfo=UTC)
        assert reloaded.merged_at == datetime(2026, 1, 3, 9, 6, tzinfo=UTC)
        assert reloaded.review_cleared_at == datetime(2026, 1, 3, 9, 7, tzinfo=UTC)

        with sqlite3.connect(db_path) as conn:
            version = conn.execute("SELECT version FROM schema_version").fetchone()[0]
            project_row = conn.execute(
                "SELECT created_at, last_seen_at FROM projects WHERE id = ?",
                ("default",),
            ).fetchone()
            task_row = conn.execute(
                """
                SELECT created_at, started_at, completed_at, pr_last_synced_at, sync_last_synced_at,
                       urgent_bumped_at, merged_at, review_cleared_at
                FROM tasks
                WHERE id = ?
                """,
                (task.id,),
            ).fetchone()
            step_row = conn.execute(
                "SELECT started_at, completed_at FROM run_steps WHERE run_id = ?",
                (task.id,),
            ).fetchone()
            substep_row = conn.execute(
                "SELECT timestamp FROM run_substeps WHERE run_id = ?",
                (task.id,),
            ).fetchone()
            comment_row = conn.execute(
                "SELECT created_at, resolved_at FROM task_comments WHERE id = ?",
                (comment.id,),
            ).fetchone()
            unit_row = conn.execute(
                """
                SELECT created_at, updated_at, merged_at, pr_last_synced_at, sync_last_synced_at
                FROM merge_units
                WHERE id = ?
                """,
                (unit.id,),
            ).fetchone()
            attachment_row = conn.execute(
                """
                SELECT attached_at
                FROM merge_unit_tasks
                WHERE merge_unit_id = ? AND task_id = ?
                """,
                (unit.id, task.id),
            ).fetchone()

        assert version == SCHEMA_VERSION
        assert project_row is not None
        assert project_row[0] == "2026-01-03T08:00:00+00:00"
        assert project_row[1].endswith("+00:00")
        assert task_row == (
            "2026-01-03T09:00:00+00:00",
            "2026-01-03T09:01:00+00:00",
            "2026-01-03T09:02:00+00:00",
            "2026-01-03T09:03:00+00:00",
            "2026-01-03T09:04:00+00:00",
            "2026-01-03T09:05:00+00:00",
            "2026-01-03T09:06:00+00:00",
            "2026-01-03T09:07:00+00:00",
        )
        assert step_row == ("2026-01-03T10:00:00+00:00", "2026-01-03T10:02:00+00:00")
        assert substep_row == ("2026-01-03T10:01:00+00:00",)
        assert comment_row == ("2026-01-03T10:03:00+00:00", "2026-01-03T10:04:00+00:00")
        assert unit_row == (
            "2026-01-03T11:00:00+00:00",
            "2026-01-03T11:01:00+00:00",
            "2026-01-03T11:02:00+00:00",
            "2026-01-03T11:03:00+00:00",
            "2026-01-03T11:04:00+00:00",
        )
        assert attachment_row == ("2026-01-03T11:05:00+00:00",)

    def test_get_reviews_for_task_returns_empty_when_no_reviews(self, tmp_path: Path):
        """Test that an empty list is returned when no reviews exist."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        impl_task = store.add("Add feature", task_type="implement")

        reviews = store.get_reviews_for_task(impl_task.id)

        assert reviews == []

    def test_get_reviews_for_task_excludes_non_review_dependents(self, tmp_path: Path):
        """Test that only review tasks are returned, not other types that depend on the task."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        impl_task = store.add("Add feature", task_type="implement")

        # Create a review task
        review = store.add("Review", task_type="review", depends_on=impl_task.id)

        # Create an improve task that also depends on the implementation
        store.add("Improve", task_type="improve", depends_on=impl_task.id)

        reviews = store.get_reviews_for_task(impl_task.id)

        assert len(reviews) == 1
        assert reviews[0].id == review.id


class TestGetImproveTasksByRoot:
    """Tests for get_improve_tasks_by_root — must walk retry/resume chains."""

    def test_direct_improves_are_returned(self, tmp_path: Path):
        """A first-generation improve (based_on=impl) is returned."""
        store = SqliteTaskStore(tmp_path / "test.db")
        impl = store.add("Impl", task_type="implement")
        review = store.add("Review", task_type="review", depends_on=impl.id)
        improve = store.add("Improve", task_type="improve", based_on=impl.id, depends_on=review.id)

        results = store.get_improve_tasks_by_root(impl.id)
        assert [t.id for t in results] == [improve.id]

    def test_chained_retry_resume_improves_are_returned(self, tmp_path: Path):
        """Retries/resumes whose based_on points at the previous improve are included."""
        store = SqliteTaskStore(tmp_path / "test.db")
        impl = store.add("Impl", task_type="implement")
        review = store.add("Review", task_type="review", depends_on=impl.id)
        improve1 = store.add("Improve 1", task_type="improve", based_on=impl.id, depends_on=review.id)
        improve2 = store.add("Improve 2 (retry)", task_type="improve", based_on=improve1.id, depends_on=review.id)
        improve3 = store.add("Improve 3 (resume)", task_type="improve", based_on=improve2.id, depends_on=review.id)

        results = store.get_improve_tasks_by_root(impl.id)
        returned_ids = {t.id for t in results}
        assert returned_ids == {improve1.id, improve2.id, improve3.id}

    def test_unrelated_improves_are_excluded(self, tmp_path: Path):
        """An improve rooted at a different impl is not returned."""
        store = SqliteTaskStore(tmp_path / "test.db")
        impl_a = store.add("Impl A", task_type="implement")
        impl_b = store.add("Impl B", task_type="implement")
        review_a = store.add("Review A", task_type="review", depends_on=impl_a.id)
        review_b = store.add("Review B", task_type="review", depends_on=impl_b.id)
        improve_a = store.add("Improve A", task_type="improve", based_on=impl_a.id, depends_on=review_a.id)
        store.add("Improve B", task_type="improve", based_on=impl_b.id, depends_on=review_b.id)

        results = store.get_improve_tasks_by_root(impl_a.id)
        assert [t.id for t in results] == [improve_a.id]


class TestGetFixTasksByRoot:
    """Tests for get_fix_tasks_by_root transitive traversal."""

    def test_direct_and_chained_fixes_are_returned(self, tmp_path: Path):
        store = SqliteTaskStore(tmp_path / "test.db")
        impl = store.add("Impl", task_type="implement")
        review = store.add("Review", task_type="review", depends_on=impl.id)
        improve = store.add("Improve", task_type="improve", based_on=impl.id, depends_on=review.id)
        fix1 = store.add("Fix 1", task_type="fix", based_on=improve.id, depends_on=review.id)
        fix2 = store.add("Fix 2", task_type="fix", based_on=fix1.id, depends_on=review.id)

        results = store.get_fix_tasks_by_root(impl.id)
        assert {task.id for task in results} == {fix1.id, fix2.id}


class TestTaskComments:
    """Tests for task comment storage and resolution helpers."""

    def test_add_get_and_resolve_comments(self, tmp_path: Path):
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        task = store.add("Task with comments", task_type="implement")
        assert task.id is not None

        first = store.add_comment(task.id, "Address API edge case.", source="direct", author="alice")
        second = store.add_comment(task.id, "nit: rename helper", source="github")

        all_comments = store.get_comments(task.id)
        assert [c.id for c in all_comments] == [first.id, second.id]
        assert all_comments[0].author == "alice"
        assert all_comments[1].author is None
        assert all_comments[0].resolved_at is None

        unresolved = store.get_comments(task.id, unresolved_only=True)
        assert len(unresolved) == 2

        store.resolve_comments(task.id)
        unresolved_after = store.get_comments(task.id, unresolved_only=True)
        assert unresolved_after == []
        resolved = store.get_comments(task.id)
        assert all(comment.resolved_at is not None for comment in resolved)

    def test_add_comment_rejects_unknown_source(self, tmp_path: Path):
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        task = store.add("Task with bad comment source")
        assert task.id is not None

        with pytest.raises(ValueError, match="Unknown comment source"):
            store.add_comment(task.id, "Invalid source", source="email")

    def test_add_comment_rejects_empty_content(self, tmp_path: Path):
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        task = store.add("Task with empty comment")
        assert task.id is not None

        with pytest.raises(ValueError, match="cannot be empty"):
            store.add_comment(task.id, "   ")

    def test_add_comment_rejects_unknown_task_id(self, tmp_path: Path):
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        with pytest.raises(KeyError, match="Task gza-9999 not found"):
            store.add_comment("gza-9999", "orphan?")

    def test_get_and_resolve_comments_can_be_scoped_by_created_at(self, tmp_path: Path):
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        task = store.add("Task with scoped comments", task_type="implement")
        assert task.id is not None

        store.add_comment(task.id, "Old comment", source="direct")
        snapshot = datetime.now(UTC)
        store.add_comment(task.id, "New comment", source="direct")

        scoped_unresolved = store.get_comments(
            task.id,
            unresolved_only=True,
            created_on_or_before=snapshot,
        )
        assert [comment.content for comment in scoped_unresolved] == ["Old comment"]

        store.resolve_comments(task.id, created_on_or_before=snapshot)
        unresolved_after = store.get_comments(task.id, unresolved_only=True)
        assert [comment.content for comment in unresolved_after] == ["New comment"]

    def test_add_comment_makes_created_at_monotonic_when_clock_repeats(self, tmp_path: Path):
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        task = store.add("Task with repeated comment timestamps", task_type="implement")
        assert task.id is not None

        repeated_now = datetime(2026, 1, 2, 3, 4, 5, 678901, tzinfo=UTC)
        with patch("gza.db.datetime", wraps=datetime) as mock_datetime:
            mock_datetime.now.return_value = repeated_now
            first = store.add_comment(task.id, "First comment", source="direct")
            second = store.add_comment(task.id, "Second comment", source="direct")

        assert first.created_at is not None
        assert second.created_at is not None
        assert second.created_at > first.created_at

        scoped = store.get_comments(task.id, created_on_or_before=first.created_at)
        assert [comment.content for comment in scoped] == ["First comment"]


class TestMergeStatus:
    """Tests for merge_status field and related functionality."""

    def test_merge_status_defaults_to_none(self, tmp_path: Path):
        """New tasks have merge_status=None by default."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        task = store.add(prompt="Test task")
        assert task.merge_status is None

        retrieved = store.get(task.id)
        assert retrieved is not None
        assert retrieved.merge_status is None

    def test_mark_completed_with_commits_sets_unmerged(self, tmp_path: Path):
        """mark_completed with has_commits=True sets merge_status='unmerged'."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        task = store.add(prompt="Test task")
        store.mark_completed(
            task,
            has_commits=True,
            branch="feature/test",
            head_sha="abc123",
            base_sha="def456",
        )

        retrieved = store.get(task.id)
        assert retrieved is not None
        assert retrieved.merge_status == "unmerged"
        assert retrieved.has_commits is True
        unit = store.resolve_merge_unit_for_task(task.id)
        assert unit is not None
        assert unit.source_branch == "feature/test"
        assert unit.target_branch == "main"
        assert unit.state == "unmerged"
        assert unit.head_sha == "abc123"
        assert unit.base_sha == "def456"

    def test_refresh_merge_unit_head_preserves_omitted_sha_and_allows_explicit_clear(self, tmp_path: Path) -> None:
        """Merge-unit SHA updates should be patch-like unless callers explicitly clear a field."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        task = store.add(prompt="Test task")
        store.mark_completed(
            task,
            has_commits=True,
            branch="feature/test",
            head_sha="abc123",
            base_sha="def456",
        )
        assert task.id is not None
        unit = store.resolve_merge_unit_for_task(task.id)
        assert unit is not None

        store.refresh_merge_unit_head(unit.id, "head456", DB_UNSET)
        refreshed = store.get_merge_unit(unit.id)
        assert refreshed is not None
        assert refreshed.head_sha == "head456"
        assert refreshed.base_sha == "def456"

        store.refresh_merge_unit_head(unit.id, DB_UNSET, None)
        cleared = store.get_merge_unit(unit.id)
        assert cleared is not None
        assert cleared.head_sha == "head456"
        assert cleared.base_sha is None

    def test_mark_completed_explore_with_commits_owns_unit_and_is_unmerged(self, tmp_path: Path) -> None:
        """Explore tasks with commits should own merge state and appear in unmerged views."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        task = store.add(prompt="Explore merge behavior", task_type="explore")
        store.mark_completed(task, has_commits=True, branch="feature/explore-merge")

        retrieved = store.get(task.id)
        assert retrieved is not None
        assert retrieved.merge_status == "unmerged"
        assert retrieved.has_commits is True

        unit = store.resolve_merge_unit_for_task(task.id)
        assert unit is not None
        assert unit.owner_task_id == task.id
        assert unit.source_branch == "feature/explore-merge"
        assert unit.state == "unmerged"

        assert [candidate.id for candidate in store.get_unmerged()] == [task.id]

    def test_mark_completed_same_branch_improve_keeps_merge_status_on_owner_only(self, tmp_path: Path):
        """Completed same-branch improve rows should not own merge state."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        impl = store.add(prompt="Implement parent", task_type="implement")
        improve = store.add(prompt="Improve parent", task_type="improve", based_on=impl.id, same_branch=True)
        store.mark_completed(improve, has_commits=True, branch="feature/test")

        retrieved = store.get(improve.id)
        assert retrieved is not None
        assert retrieved.merge_status is None
        assert retrieved.has_commits is True

    def test_mark_completed_without_commits_leaves_merge_status_none(self, tmp_path: Path):
        """mark_completed with has_commits=False leaves merge_status as None."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        task = store.add(prompt="Test task")
        store.mark_completed(task, has_commits=False)

        retrieved = store.get(task.id)
        assert retrieved is not None
        assert retrieved.merge_status is None
        assert retrieved.has_commits is False

    def test_set_merge_status_updates_field(self, tmp_path: Path):
        """set_merge_status correctly updates the merge_status field."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        task = store.add(prompt="Test task")
        store.mark_completed(task, has_commits=True, branch="feature/test")

        # Verify initial state
        retrieved = store.get(task.id)
        assert retrieved.merge_status == "unmerged"

        # Update to merged
        store.set_merge_status(task.id, "merged")

        retrieved = store.get(task.id)
        assert retrieved.merge_status == "merged"

    def test_set_merge_status_to_none(self, tmp_path: Path):
        """set_merge_status can set merge_status back to None."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        task = store.add(prompt="Test task")
        store.mark_completed(task, has_commits=True, branch="feature/test")
        store.set_merge_status(task.id, None)

        retrieved = store.get(task.id)
        assert retrieved.merge_status is None

    def test_set_merge_status_to_none_does_not_flip_unit_to_stale(self, tmp_path: Path) -> None:
        """Legacy None clears the task row without mutating canonical unit state."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        task = store.add(prompt="Test task")
        store.mark_completed(task, has_commits=True, branch="feature/test")
        assert task.id is not None

        before = store.resolve_merge_unit_for_task(task.id)
        assert before is not None
        assert before.state == "unmerged"

        store.set_merge_status(task.id, None)

        retrieved = store.get(task.id)
        assert retrieved is not None
        assert retrieved.merge_status is None

        after = store.resolve_merge_unit_for_task(task.id)
        assert after is not None
        assert after.state == "unmerged"

    def test_get_unmerged_queries_by_merge_status(self, tmp_path: Path):
        """get_unmerged returns tasks with merge_status='unmerged'."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        # Task with commits (will have merge_status='unmerged')
        task1 = store.add(prompt="Task with commits")
        store.mark_completed(task1, has_commits=True, branch="feature/task1")

        # Task without commits
        task2 = store.add(prompt="Task without commits")
        store.mark_completed(task2, has_commits=False)

        # Task merged (set merge_status to 'merged')
        task3 = store.add(prompt="Merged task")
        store.mark_completed(task3, has_commits=True, branch="feature/task3")
        store.set_merge_status(task3.id, "merged")

        unmerged = store.get_unmerged()
        unmerged_ids = [t.id for t in unmerged]

        assert task1.id in unmerged_ids
        assert task2.id not in unmerged_ids
        assert task3.id not in unmerged_ids

    def test_get_unmerged_excludes_merged_tasks(self, tmp_path: Path):
        """get_unmerged does not return tasks with merge_status='merged'."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        task = store.add(prompt="Test task")
        store.mark_completed(task, has_commits=True, branch="feature/test")
        store.set_merge_status(task.id, "merged")

        unmerged = store.get_unmerged()
        assert len(unmerged) == 0

    def test_get_unmerged_excludes_improve_tasks(self, tmp_path: Path):
        """get_unmerged does not return improve tasks (they use same_branch=True)."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        # Regular unmerged task
        impl_task = store.add(prompt="Implement feature", task_type="implement")
        store.mark_completed(impl_task, has_commits=True, branch="feature/impl")

        # Improve task with same_branch=True and based_on (commits to impl branch)
        improve_task = store.add(
            prompt="Improve implementation", task_type="improve", same_branch=True,
            based_on=impl_task.id,
        )
        store.mark_completed(improve_task, has_commits=True, branch="feature/impl")

        unmerged = store.get_unmerged()
        unmerged_ids = [t.id for t in unmerged]

        assert impl_task.id in unmerged_ids
        assert improve_task.id not in unmerged_ids

    def test_get_unmerged_excludes_fix_tasks(self, tmp_path: Path):
        """get_unmerged does not return same-branch fix tasks."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        impl_task = store.add(prompt="Implement feature", task_type="implement")
        store.mark_completed(impl_task, has_commits=True, branch="feature/impl")

        fix_task = store.add(
            prompt="Fix implementation",
            task_type="fix",
            same_branch=True,
            based_on=impl_task.id,
        )
        store.mark_completed(fix_task, has_commits=True, branch="feature/impl")

        unmerged = store.get_unmerged()
        unmerged_ids = [t.id for t in unmerged]

        assert impl_task.id in unmerged_ids
        assert fix_task.id not in unmerged_ids

    def test_needs_merge_status_migration_ignores_same_branch_improve_rows(self, tmp_path: Path):
        """Same-branch improve rows may validly keep merge_status=None after completion."""
        from gza.db import needs_merge_status_migration

        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        impl_task = store.add(prompt="Implement feature", task_type="implement")
        store.mark_completed(impl_task, has_commits=True, branch="feature/impl")

        improve_task = store.add(
            prompt="Improve implementation",
            task_type="improve",
            same_branch=True,
            based_on=impl_task.id,
        )
        store.mark_completed(improve_task, has_commits=True, branch="feature/impl")

        refreshed_improve = store.get(improve_task.id)
        assert refreshed_improve is not None
        assert refreshed_improve.merge_status is None
        assert needs_merge_status_migration(store) is False

    def test_needs_merge_status_migration_is_disabled_when_merge_units_are_available(self, tmp_path: Path):
        """Merge-unit-backed stores no longer report legacy merge-status migration work."""
        from gza.db import needs_merge_status_migration

        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        impl_task = store.add(prompt="Legacy implement", task_type="implement")
        store.mark_completed(impl_task, has_commits=True, branch="feature/impl")
        assert impl_task.id is not None
        store.set_merge_status(impl_task.id, None)

        assert needs_merge_status_migration(store) is False

    def test_same_branch_followups_share_one_merge_unit(self, tmp_path: Path) -> None:
        """Same-branch improve/fix/review rows attach to the existing merge unit."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        impl = store.add(prompt="Implement feature", task_type="implement")
        store.mark_completed(impl, has_commits=True, branch="feature/impl")
        review = store.add("Review feature", task_type="review", depends_on=impl.id, based_on=impl.id)
        review.status = "completed"
        review.branch = "feature/impl"
        review.completed_at = datetime.now(UTC)
        store.update(review)
        improve = store.add("Improve feature", task_type="improve", based_on=impl.id, same_branch=True)
        store.mark_completed(improve, has_commits=True, branch="feature/impl")

        impl_unit = store.resolve_merge_unit_for_task(impl.id)
        review_unit = store.resolve_merge_unit_for_task(review.id)
        improve_unit = store.resolve_merge_unit_for_task(improve.id)
        assert impl_unit is not None
        assert review_unit is not None
        assert improve_unit is not None
        assert impl_unit.id == review_unit.id == improve_unit.id
        attached_ids = {task.id for task in store.list_tasks_for_merge_unit(impl_unit.id)}
        assert attached_ids == {impl.id, review.id, improve.id}

    def test_mark_completed_without_explicit_target_raises_when_project_default_branch_fails(
        self,
        tmp_path: Path,
    ) -> None:
        """Project-backed write paths must not silently persist merge truth under main."""
        project_root = tmp_path / "project"
        project_root.mkdir()
        store = SqliteTaskStore(tmp_path / "test.db", project_root=project_root)
        task = store.add(prompt="Implement feature", task_type="implement")

        with patch("gza.git.Git.default_branch", side_effect=RuntimeError("git failure")):
            with pytest.raises(MergeTargetResolutionError, match="Could not determine default merge target"):
                store.mark_completed(task, has_commits=True, branch="feature/strict-default")

        assert task.id is not None
        assert store.resolve_merge_unit_for_task(task.id) is None
        assert store.resolve_merge_unit_for_task(task.id) is None

    def test_get_unmerged_backfills_legacy_actionable_row_when_merge_units_exist(self, tmp_path: Path) -> None:
        """Legacy unmerged rows should remain visible until lazy merge-unit backfill attaches them."""
        store = SqliteTaskStore(tmp_path / "test.db")

        task = store.add(prompt="Legacy feature", task_type="implement")
        task.status = "completed"
        task.completed_at = datetime.now(UTC)
        task.branch = "feature/legacy-unmerged"
        task.has_commits = True
        task.merge_status = "unmerged"
        store.update(task)

        assert task.id is not None
        assert store.supports_merge_units() is True
        assert store.resolve_merge_unit_for_task(task.id) is None

        unmerged = store.get_unmerged()

        assert [candidate.id for candidate in unmerged] == [task.id]
        unit = store.resolve_merge_unit_for_task(task.id)
        assert unit is not None
        assert unit.state == "unmerged"
        assert {member.id for member in store.list_tasks_for_merge_unit(unit.id)} == {task.id}

    def test_get_unmerged_prefers_actionable_merge_unit_member_over_failed_owner(self, tmp_path: Path) -> None:
        """Unit-backed reads should surface the mergeable member, not a failed historical owner."""
        store = SqliteTaskStore(tmp_path / "test.db")

        failed = store.add(prompt="Failed implementation", task_type="implement")
        assert failed.id is not None
        failed.status = "failed"
        failed.completed_at = datetime.now(UTC)
        failed.branch = "feature/recovered-work"
        failed.has_commits = True
        failed.merge_status = "unmerged"
        store.update(failed)

        recovery = store.add(prompt="Completed retry", task_type="implement", based_on=failed.id)
        store.mark_completed(recovery, has_commits=True, branch="feature/recovered-work")
        assert recovery.id is not None

        unit = store.resolve_merge_unit_for_task(recovery.id)
        assert unit is not None
        assert unit.owner_task_id == failed.id
        assert store._legacy_merge_status_owner_for_unit(unit).id == failed.id

        assert [task.id for task in store.get_unmerged()] == [recovery.id]
        representative = store.resolve_merge_unit_representative_task(unit, require_actionable=True)
        assert representative is not None
        assert representative.id == recovery.id

    def test_default_target_branch_apis_use_store_default_merge_target_not_main(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Store-level merge-unit writes and reads should honor the configured default target."""
        store = SqliteTaskStore(tmp_path / "test.db")
        monkeypatch.setattr(store, "default_merge_target", lambda *, strict=False: "trunk")

        task = store.add(prompt="Implement feature", task_type="implement")
        store.mark_completed(task, has_commits=True, branch="feature/trunk-target")

        assert task.id is not None
        trunk_unit = store.resolve_merge_unit_for_task(task.id)
        assert trunk_unit is not None
        assert trunk_unit.target_branch == "trunk"
        assert [candidate.id for candidate in store.get_unmerged()] == [task.id]

        store.set_merge_status(task.id, "merged")

        refreshed_trunk_unit = store.resolve_merge_unit_for_task(task.id)
        assert refreshed_trunk_unit is not None
        assert refreshed_trunk_unit.state == "merged"
        assert refreshed_trunk_unit.target_branch == "trunk"
        assert store.get_unmerged() == []

    def test_set_merge_status_without_target_updates_canonical_unit(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Compatibility writes without a target must resolve through the canonical unit."""
        store = SqliteTaskStore(tmp_path / "test.db")
        monkeypatch.setattr(store, "default_merge_target", lambda *, strict=False: "main")

        task = store.add(prompt="Implement feature", task_type="implement")
        store.mark_completed(task, has_commits=True, branch="feature/multi-target")

        assert task.id is not None
        main_unit = store.resolve_merge_unit_for_task(task.id)
        assert main_unit is not None

        store.set_merge_unit_state(main_unit.id, "unmerged")

        store.set_merge_status(task.id, "merged")

        refreshed_main_unit = store.resolve_merge_unit_for_task(task.id)
        refreshed_task = store.get(task.id)
        assert refreshed_main_unit is not None
        assert refreshed_task is not None
        assert refreshed_main_unit.state == "merged"
        assert refreshed_task.merge_status == "merged"

    def test_mark_completed_without_explicit_target_raises_when_project_default_branch_fails(
        self,
        tmp_path: Path,
    ) -> None:
        """Project-backed write paths must not silently persist merge truth under main."""
        project_root = tmp_path / "project"
        project_root.mkdir()
        store = SqliteTaskStore(tmp_path / "test.db", project_root=project_root)
        task = store.add(prompt="Implement feature", task_type="implement")

        with patch("gza.git.Git.default_branch", side_effect=RuntimeError("git failure")):
            with pytest.raises(MergeTargetResolutionError, match="Could not determine default merge target"):
                store.mark_completed(task, has_commits=True, branch="feature/strict-default")

        assert task.id is not None
        assert store.resolve_merge_unit_for_task(task.id) is None
        assert store.resolve_merge_unit_for_task(task.id) is None

    def test_get_unmerged_backfills_legacy_actionable_row_when_merge_units_exist(self, tmp_path: Path) -> None:
        """Legacy unmerged rows should remain visible until lazy merge-unit backfill attaches them."""
        store = SqliteTaskStore(tmp_path / "test.db")

        task = store.add(prompt="Legacy feature", task_type="implement")
        task.status = "completed"
        task.completed_at = datetime.now(UTC)
        task.branch = "feature/legacy-unmerged"
        task.has_commits = True
        task.merge_status = "unmerged"
        store.update(task)

        assert task.id is not None
        assert store.supports_merge_units() is True
        assert store.resolve_merge_unit_for_task(task.id) is None

        unmerged = store.get_unmerged()

        assert [candidate.id for candidate in unmerged] == [task.id]
        unit = store.resolve_merge_unit_for_task(task.id)
        assert unit is not None
        assert unit.state == "unmerged"
        assert {member.id for member in store.list_tasks_for_merge_unit(unit.id)} == {task.id}

    def test_get_unmerged_prefers_actionable_merge_unit_member_over_failed_owner(self, tmp_path: Path) -> None:
        """Unit-backed reads should surface the mergeable member, not a failed historical owner."""
        store = SqliteTaskStore(tmp_path / "test.db")

        failed = store.add(prompt="Failed implementation", task_type="implement")
        assert failed.id is not None
        failed.status = "failed"
        failed.completed_at = datetime.now(UTC)
        failed.branch = "feature/recovered-work"
        failed.has_commits = True
        failed.merge_status = "unmerged"
        store.update(failed)

        recovery = store.add(prompt="Completed retry", task_type="implement", based_on=failed.id)
        store.mark_completed(recovery, has_commits=True, branch="feature/recovered-work")
        assert recovery.id is not None

        unit = store.resolve_merge_unit_for_task(recovery.id)
        assert unit is not None
        assert unit.owner_task_id == failed.id
        assert store._legacy_merge_status_owner_for_unit(unit).id == failed.id

        assert [task.id for task in store.get_unmerged()] == [recovery.id]
        representative = store.resolve_merge_unit_representative_task(unit, require_actionable=True)
        assert representative is not None
        assert representative.id == recovery.id

    def test_merge_unit_backfill_attaches_existing_branchless_reviews_with_review_role(self, tmp_path: Path) -> None:
        """Backfilling an implementation unit should attach existing branchless reviews."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        impl = store.add(prompt="Legacy implement", task_type="implement")
        impl.status = "completed"
        impl.completed_at = datetime.now(UTC)
        impl.branch = "feature/review-backfill"
        impl.has_commits = True
        impl.merge_status = "unmerged"
        store.update(impl)

        review = store.add("Review feature", task_type="review", depends_on=impl.id, based_on=impl.id)
        review.status = "completed"
        review.completed_at = datetime.now(UTC)
        store.update(review)

        unit = store.get_or_create_merge_unit_for_task(impl)
        assert unit is not None
        assert review.id is not None
        assert store.resolve_merge_unit_for_task(review.id).id == unit.id

        conn = sqlite3.connect(db_path)
        role_row = conn.execute(
            """
            SELECT role
            FROM merge_unit_tasks
            WHERE project_id = ? AND merge_unit_id = ? AND task_id = ?
            """,
            ("default", unit.id, review.id),
        ).fetchone()
        conn.close()
        assert role_row is not None
        assert role_row[0] == "review"

    def test_reused_branch_creates_new_merge_unit_for_unrelated_work(self, tmp_path: Path) -> None:
        """Unrelated later work on a reused branch must not reopen the historical unit."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        original = store.add(prompt="Original feature", task_type="implement")
        store.mark_completed(original, has_commits=True, branch="feature/reused")
        assert original.id is not None
        original_unit = store.resolve_merge_unit_for_task(original.id)
        assert original_unit is not None
        store.set_merge_unit_state(original_unit.id, "merged")

        merged_original = store.get(original.id)
        assert merged_original is not None
        assert merged_original.merge_status == "merged"
        original_merged_at = merged_original.merged_at

        unrelated = store.add(prompt="Unrelated feature", task_type="implement")
        store.mark_completed(unrelated, has_commits=True, branch="feature/reused")
        assert unrelated.id is not None
        unrelated_unit = store.resolve_merge_unit_for_task(unrelated.id)
        assert unrelated_unit is not None

        assert unrelated_unit.id != original_unit.id
        assert store.get_merge_unit(original_unit.id).state == "merged"
        assert {task.id for task in store.list_tasks_for_merge_unit(original_unit.id)} == {original.id}
        assert {task.id for task in store.list_tasks_for_merge_unit(unrelated_unit.id)} == {unrelated.id}

        refreshed_original = store.get(original.id)
        assert refreshed_original is not None
        assert refreshed_original.merge_status == "merged"
        assert refreshed_original.merged_at == original_merged_at

    def test_set_merge_unit_state_rejects_non_owner_merged_by_task_id(self, tmp_path: Path) -> None:
        """Merged provenance must always be attributed to the merge-unit owner."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        impl = store.add(prompt="Implement feature", task_type="implement")
        store.mark_completed(impl, has_commits=True, branch="feature/remerge")
        assert impl.id is not None
        impl_unit = store.resolve_merge_unit_for_task(impl.id)
        assert impl_unit is not None

        improve = store.add(
            prompt="Improve feature",
            task_type="improve",
            based_on=impl.id,
            same_branch=True,
        )
        store.mark_completed(improve, has_commits=True, branch="feature/remerge")
        assert improve.id is not None

        with pytest.raises(ValueError, match="merged_by_task_id must equal merge-unit owner"):
            store.set_merge_unit_state(impl_unit.id, "merged", merged_by_task_id=improve.id)

    def test_set_merge_unit_state_clears_provenance_for_unmerged_state(self, tmp_path: Path) -> None:
        """Unmerged states must not retain merged provenance on the unit or owner projection."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        impl = store.add(prompt="Implement feature", task_type="implement")
        store.mark_completed(impl, has_commits=True, branch="feature/remerge")
        assert impl.id is not None
        impl_unit = store.resolve_merge_unit_for_task(impl.id)
        assert impl_unit is not None

        store.set_merge_unit_state(impl_unit.id, "merged")
        store.set_merge_unit_state(impl_unit.id, "unmerged")

        unmerged_unit = store.get_merge_unit(impl_unit.id)
        assert unmerged_unit is not None
        assert unmerged_unit.state == "unmerged"
        assert unmerged_unit.merged_at is None
        assert unmerged_unit.merged_by_task_id is None

        unmerged_impl = store.get(impl.id)
        assert unmerged_impl is not None
        assert unmerged_impl.merge_status == "unmerged"
        assert unmerged_impl.merged_at is None

    def test_set_merge_unit_state_empty_clears_provenance_and_hides_actionable_listing(
        self,
        tmp_path: Path,
    ) -> None:
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        impl = store.add(prompt="Implement feature", task_type="implement")
        store.mark_completed(impl, has_commits=True, branch="feature/empty-state")
        assert impl.id is not None
        impl_unit = store.resolve_merge_unit_for_task(impl.id)
        assert impl_unit is not None

        store.set_merge_unit_state(impl_unit.id, "merged")
        store.set_merge_unit_state(impl_unit.id, "empty")

        empty_unit = store.get_merge_unit(impl_unit.id)
        assert empty_unit is not None
        assert empty_unit.state == "empty"
        assert empty_unit.merged_at is None
        assert empty_unit.merged_by_task_id is None
        assert store.get_unmerged_merge_units() == []

        empty_impl = store.get(impl.id)
        assert empty_impl is not None
        assert empty_impl.merge_status is None
        assert empty_impl.merged_at is None

    def test_set_merge_unit_state_rejects_explicit_provenance_for_non_merged_states(
        self,
        tmp_path: Path,
    ) -> None:
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        impl = store.add(prompt="Implement feature", task_type="implement")
        store.mark_completed(impl, has_commits=True, branch="feature/non-merged-provenance")
        assert impl.id is not None
        impl_unit = store.resolve_merge_unit_for_task(impl.id)
        assert impl_unit is not None

        with pytest.raises(ValueError, match="cannot retain merged_by_task_id provenance"):
            store.set_merge_unit_state(impl_unit.id, "empty", merged_by_task_id=impl.id)

        with pytest.raises(ValueError, match="cannot retain merged_at provenance"):
            store.set_merge_unit_state(impl_unit.id, "blocked", merged_at=datetime.now(UTC))

        with pytest.raises(ValueError, match="cannot retain merge_source provenance"):
            store.set_merge_unit_state(impl_unit.id, "unmerged", merge_source="manual")

    def test_set_merge_unit_state_sets_merged_state_and_provenance_together(self, tmp_path: Path) -> None:
        """Merged writes should stamp owner provenance and merged_at in one state change."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        impl = store.add(prompt="Implement feature", task_type="implement")
        store.mark_completed(impl, has_commits=True, branch="feature/remerge")
        assert impl.id is not None
        impl_unit = store.resolve_merge_unit_for_task(impl.id)
        assert impl_unit is not None

        store.set_merge_unit_state(impl_unit.id, "merged", merge_source="manual")

        merged_unit = store.get_merge_unit(impl_unit.id)
        assert merged_unit is not None
        assert merged_unit.state == "merged"
        assert merged_unit.merged_at is not None
        assert merged_unit.merged_by_task_id == impl.id
        assert merged_unit.merge_source == "manual"

        merged_impl = store.get(impl.id)
        assert merged_impl is not None
        assert merged_impl.merge_status == "merged"
        assert merged_impl.merged_at == merged_unit.merged_at

    def test_list_merged_units_filters_by_source_and_window(self, tmp_path: Path) -> None:
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        manual = store.add(prompt="Manual merge", task_type="implement")
        store.mark_completed(manual, has_commits=True, branch="feature/manual")
        assert manual.id is not None
        manual_unit = store.resolve_merge_unit_for_task(manual.id)
        assert manual_unit is not None
        manual_time = datetime(2026, 6, 2, 12, 0, tzinfo=UTC)
        store.set_merge_unit_state(
            manual_unit.id,
            "merged",
            merge_source="manual",
            merged_at=manual_time,
        )

        advance = store.add(prompt="Advance merge", task_type="implement")
        store.mark_completed(advance, has_commits=True, branch="feature/advance")
        assert advance.id is not None
        advance_unit = store.resolve_merge_unit_for_task(advance.id)
        assert advance_unit is not None
        advance_time = datetime(2026, 5, 20, 12, 0, tzinfo=UTC)
        store.set_merge_unit_state(
            advance_unit.id,
            "merged",
            merge_source="advance",
            merged_at=advance_time,
        )

        units = store.list_merged_units(
            source="manual",
            after=datetime(2026, 6, 1, tzinfo=UTC),
            before=datetime(2026, 6, 3, tzinfo=UTC),
        )
        assert [unit.id for unit in units] == [manual_unit.id]

    def test_set_merge_unit_state_preserves_unrelated_task_fields(self, tmp_path: Path) -> None:
        """Dual-write merge projection should not rewrite unrelated task columns."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        impl = store.add(prompt="Implement feature", task_type="implement")
        impl.slug = "20260531-merge-projection"
        impl.output_content = "kept"
        store.mark_completed(impl, has_commits=True, branch="feature/remerge")
        store.update(impl)
        assert impl.id is not None

        impl_unit = store.resolve_merge_unit_for_task(impl.id)
        assert impl_unit is not None

        synced_at = datetime.now(UTC).replace(microsecond=0)
        store.set_merge_unit_state(
            impl_unit.id,
            "merged",
            merged_by_task_id=impl.id,
            pr_number=42,
            pr_state="open",
            pr_last_synced_at=synced_at,
            sync_last_synced_at=synced_at,
            diff_stats=(3, 10, 2),
        )

        refreshed = store.get(impl.id)
        assert refreshed is not None
        assert refreshed.prompt == "Implement feature"
        assert refreshed.status == "completed"
        assert refreshed.slug == "20260531-merge-projection"
        assert refreshed.output_content == "kept"
        assert refreshed.merge_status == "merged"
        assert refreshed.pr_number == 42
        assert refreshed.pr_state == "open"

    def test_set_merge_unit_state_public_db_unset_preserves_existing_optional_fields(
        self, tmp_path: Path
    ) -> None:
        """Public DB_UNSET should leave merge-unit optional fields untouched."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        impl = store.add(prompt="Implement feature", task_type="implement")
        store.mark_completed(impl, has_commits=True, branch="feature/db-unset")
        assert impl.id is not None
        impl_unit = store.resolve_merge_unit_for_task(impl.id)
        assert impl_unit is not None

        synced_at = datetime.now(UTC).replace(microsecond=0)
        store.set_merge_unit_state(
            impl_unit.id,
            "merged",
            merged_by_task_id=impl.id,
            pr_number=42,
            pr_state="open",
            pr_last_synced_at=synced_at,
            sync_last_synced_at=synced_at,
        )

        store.set_merge_unit_state(
            impl_unit.id,
            "merged",
            merged_by_task_id=DB_UNSET,
            pr_number=DB_UNSET,
            pr_state=DB_UNSET,
            pr_last_synced_at=DB_UNSET,
            sync_last_synced_at=DB_UNSET,
        )

        refreshed_unit = store.get_merge_unit(impl_unit.id)
        assert refreshed_unit is not None
        assert refreshed_unit.merged_by_task_id == impl.id
        assert refreshed_unit.pr_number == 42
        assert refreshed_unit.pr_state == "open"
        assert refreshed_unit.pr_last_synced_at == synced_at
        assert refreshed_unit.sync_last_synced_at == synced_at

    def test_repair_inconsistent_unmerged_merge_units_is_idempotent(self, tmp_path: Path) -> None:
        """Startup cleanup and manual reruns should clear stale merged provenance once."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        impl = store.add(prompt="Implement feature", task_type="implement")
        store.mark_completed(impl, has_commits=True, branch="feature/repair-one")
        assert impl.id is not None
        impl_unit = store.resolve_merge_unit_for_task(impl.id)
        assert impl_unit is not None

        review_impl = store.add(prompt="Review target", task_type="implement")
        store.mark_completed(review_impl, has_commits=True, branch="feature/repair-two")
        assert review_impl.id is not None
        review_unit = store.resolve_merge_unit_for_task(review_impl.id)
        assert review_unit is not None

        now_iso = datetime.now(UTC).isoformat()
        with store._connect() as conn:
            conn.execute(
                """
                UPDATE merge_units
                SET state = 'unmerged',
                    merged_at = ?,
                    merged_by_task_id = ?
                WHERE project_id = ? AND id = ?
                """,
                (now_iso, impl.id, store._project_id, impl_unit.id),
            )
            conn.execute(
                """
                UPDATE merge_units
                SET state = 'unmerged',
                    merged_at = ?,
                    merged_by_task_id = NULL
                WHERE project_id = ? AND id = ?
                """,
                (now_iso, store._project_id, review_unit.id),
            )

        repaired = store.repair_inconsistent_unmerged_merge_units()
        repaired_again = store.repair_inconsistent_unmerged_merge_units()

        assert repaired == 2
        assert repaired_again == 0
        refreshed_impl_unit = store.get_merge_unit(impl_unit.id)
        refreshed_review_unit = store.get_merge_unit(review_unit.id)
        assert refreshed_impl_unit is not None
        assert refreshed_review_unit is not None
        assert refreshed_impl_unit.merged_at is None
        assert refreshed_impl_unit.merged_by_task_id is None
        assert refreshed_review_unit.merged_at is None
        assert refreshed_review_unit.merged_by_task_id is None

    def test_store_open_repairs_inconsistent_unmerged_merge_units(self, tmp_path: Path) -> None:
        """DB open should clear corrupt unmerged merge provenance before normal use."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        first = store.add(prompt="Repair first", task_type="implement")
        store.mark_completed(first, has_commits=True, branch="feature/open-repair-one")
        assert first.id is not None
        first_unit = store.resolve_merge_unit_for_task(first.id)
        assert first_unit is not None

        second = store.add(prompt="Repair second", task_type="implement")
        store.mark_completed(second, has_commits=True, branch="feature/open-repair-two")
        assert second.id is not None
        second_unit = store.resolve_merge_unit_for_task(second.id)
        assert second_unit is not None

        now_iso = datetime.now(UTC).isoformat()
        with store._connect() as conn:
            conn.execute(
                """
                UPDATE merge_units
                SET state = 'unmerged',
                    merged_at = ?,
                    merged_by_task_id = ?
                WHERE project_id = ? AND id = ?
                """,
                (now_iso, first.id, store._project_id, first_unit.id),
            )
            conn.execute(
                """
                UPDATE merge_units
                SET state = 'unmerged',
                    merged_at = NULL,
                    merged_by_task_id = ?
                WHERE project_id = ? AND id = ?
                """,
                (second.id, store._project_id, second_unit.id),
            )

        reopened = SqliteTaskStore(db_path)
        repaired_first = reopened.get_merge_unit(first_unit.id)
        repaired_second = reopened.get_merge_unit(second_unit.id)
        assert repaired_first is not None
        assert repaired_second is not None
        assert repaired_first.state == "unmerged"
        assert repaired_first.merged_at is None
        assert repaired_first.merged_by_task_id is None
        assert repaired_second.state == "unmerged"
        assert repaired_second.merged_at is None
        assert repaired_second.merged_by_task_id is None

    def test_store_open_repairs_inconsistent_empty_merge_unit_provenance(self, tmp_path: Path) -> None:
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        impl = store.add(prompt="Repair empty provenance", task_type="implement")
        store.mark_completed(impl, has_commits=True, branch="feature/open-repair-empty")
        assert impl.id is not None
        impl_unit = store.resolve_merge_unit_for_task(impl.id)
        assert impl_unit is not None

        now_iso = datetime.now(UTC).isoformat()
        with store._connect() as conn:
            conn.execute(
                """
                UPDATE merge_units
                SET state = 'empty',
                    merged_at = ?,
                    merged_by_task_id = ?
                WHERE project_id = ? AND id = ?
                """,
                (now_iso, impl.id, store._project_id, impl_unit.id),
            )

        reopened = SqliteTaskStore(db_path)
        repaired_unit = reopened.get_merge_unit(impl_unit.id)
        assert repaired_unit is not None
        assert repaired_unit.state == "empty"
        assert repaired_unit.merged_at is None
        assert repaired_unit.merged_by_task_id is None

    def test_same_branch_improve_reuses_related_merged_unit(self, tmp_path: Path) -> None:
        """A same-lineage same-branch improve task should reopen the existing unit."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        impl = store.add(prompt="Implement feature", task_type="implement")
        store.mark_completed(impl, has_commits=True, branch="feature/reused")
        assert impl.id is not None
        impl_unit = store.resolve_merge_unit_for_task(impl.id)
        assert impl_unit is not None
        store.set_merge_unit_state(impl_unit.id, "merged")

        improve = store.add(
            prompt="Improve feature",
            task_type="improve",
            based_on=impl.id,
            same_branch=True,
        )
        store.mark_completed(improve, has_commits=True, branch="feature/reused")
        assert improve.id is not None
        improve_unit = store.resolve_merge_unit_for_task(improve.id)
        assert improve_unit is not None

        assert improve_unit.id == impl_unit.id
        assert store.get_merge_unit(impl_unit.id).state == "unmerged"
        assert {task.id for task in store.list_tasks_for_merge_unit(impl_unit.id)} == {impl.id, improve.id}

    def test_migrate_merge_status_logs_when_remote_probe_fails(self, tmp_path: Path, caplog: pytest.LogCaptureFixture):
        """Migration logs a warning and defaults safely when origin inspection fails."""
        from gza.db import migrate_merge_status
        from gza.git import Git

        class FakeGit(Git):
            def __init__(self) -> None:
                pass

            def default_branch(self) -> str:
                return "main"

            def branch_exists(self, branch: str) -> bool:
                return True

            def _run(self, *args, **kwargs):
                raise RuntimeError("git failure")

            def is_merged(self, source: str, into: str) -> bool:
                return False

        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)
        task = store.add(prompt="Task with commits")
        store.mark_completed(task, has_commits=True, branch="feature/test")
        store.set_merge_status(task.id, None)

        with caplog.at_level("WARNING"):
            migrate_merge_status(store, FakeGit())

        updated = store.get(task.id)
        assert updated is not None
        assert updated.merge_status == "unmerged"
        unit = store.resolve_merge_unit_for_task(task.id)
        assert unit is not None
        assert unit.state == "unmerged"
        assert "Could not inspect origin while backfilling merge status" in caplog.text

    def test_migrate_merge_status_deleted_local_branch_with_remote_survivor_stays_unmerged(self, tmp_path: Path):
        """Migration should use shared remote-aware merge truth for deleted local branches."""
        from gza.db import migrate_merge_status
        from gza.git import Git

        class FakeGit(Git):
            def __init__(self) -> None:
                pass

            def default_branch(self) -> str:
                return "main"

            def remote_exists(self, remote: str) -> bool:
                return remote == "origin"

            def fetch(self, remote: str = "origin") -> None:
                assert remote == "origin"

            def ref_exists(self, ref: str) -> bool:
                return ref in {"origin/main", "origin/feature/remote-survivor"}

            def branch_exists(self, branch: str) -> bool:
                return False

            def is_merged(self, source: str, into: str) -> bool:
                return False

        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        task = store.add(prompt="Legacy deleted local branch")
        store.mark_completed(task, has_commits=True, branch="feature/remote-survivor")
        store.set_merge_status(task.id, None)

        migrate_merge_status(store, FakeGit())

        updated = store.get(task.id)
        assert updated is not None
        assert updated.merge_status == "unmerged"

    def test_migration_v9_to_v10_adds_merge_status_column(self, tmp_path: Path):
        """Migration from v9 to v10 adds merge_status column."""
        import sqlite3

        db_path = tmp_path / "test.db"

        # Create a v9 database manually (without merge_status column)
        conn = sqlite3.connect(db_path)
        conn.execute("CREATE TABLE schema_version (version INTEGER PRIMARY KEY)")
        conn.execute("INSERT INTO schema_version (version) VALUES (9)")
        conn.execute("""
            CREATE TABLE tasks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                prompt TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending',
                task_type TEXT NOT NULL DEFAULT 'task',
                task_id TEXT,
                branch TEXT,
                log_file TEXT,
                report_file TEXT,
                based_on INTEGER REFERENCES tasks(id),
                has_commits INTEGER,
                duration_seconds REAL,
                num_turns INTEGER,
                num_turns_reported INTEGER,
                num_turns_computed INTEGER,
                cost_usd REAL,
                created_at TEXT NOT NULL,
                started_at TEXT,
                completed_at TEXT,
                "group" TEXT,
                depends_on INTEGER REFERENCES tasks(id),
                spec TEXT,
                create_review INTEGER DEFAULT 0,
                same_branch INTEGER DEFAULT 0,
                task_type_hint TEXT,
                output_content TEXT,
                session_id TEXT,
                pr_number INTEGER,
                model TEXT,
                provider TEXT,
                input_tokens INTEGER,
                output_tokens INTEGER
            )
        """)

        now = datetime.now(UTC).isoformat()
        conn.execute(
            "INSERT INTO tasks (prompt, status, created_at) VALUES (?, ?, ?)",
            ("Old task", "completed", now),
        )
        conn.commit()
        conn.close()

        # Open with SqliteTaskStore to trigger auto-migrations, then manual v25
        with pytest.raises(ManualMigrationRequired):
            SqliteTaskStore(db_path)
        _run_v25_v26_v27_migrations(db_path, "gza")
        store = SqliteTaskStore(db_path)

        # Check schema version updated
        conn = sqlite3.connect(db_path)
        cur = conn.execute("SELECT version FROM schema_version")
        version = cur.fetchone()[0]
        conn.close()
        assert version == SCHEMA_VERSION

        # Verify old task can be retrieved with NULL merge_status
        task = store.get("gza-1")
        assert task is not None
        assert task.merge_status is None

        # Verify new tasks can store merge_status
        new_task = store.add(prompt="New task")
        store.mark_completed(new_task, has_commits=True, branch="feature/test")
        retrieved = store.get(new_task.id)
        assert retrieved.merge_status == "unmerged"

    def test_merge_status_persists_through_update(self, tmp_path: Path):
        """merge_status is persisted correctly through the update method."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        task = store.add(prompt="Test task")
        task.merge_status = "merged"
        task.status = "completed"
        from datetime import datetime
        task.completed_at = datetime.now(UTC)
        store.update(task)

        retrieved = store.get(task.id)
        assert retrieved is not None
        assert retrieved.merge_status == "merged"


class TestEditPromptDefaultContent:
    """Tests for edit_prompt default content generation."""

    def test_edit_prompt_provides_default_for_implement_with_based_on(self, tmp_path: Path, monkeypatch):
        """Test that edit_prompt provides a default prompt for implement tasks with based_on."""
        from gza.db import edit_prompt

        # Mock subprocess.run to capture what would be written to the editor
        editor_content = []

        def mock_run(cmd):
            # Read the temporary file that was passed to the editor
            temp_file = cmd[1]
            with open(temp_file) as f:
                editor_content.append(f.read())
            # Return success
            class Result:
                returncode = 0
            return Result()

        monkeypatch.setattr("gza.db._launch_editor", mock_run)

        # Call edit_prompt with implement type and based_on
        # Note: This will still try to open editor, but our mock will capture the content
        # We need to also write back to the file so it doesn't return None
        def mock_run_with_write(cmd):
            temp_file = cmd[1]
            with open(temp_file) as f:
                content = f.read()
                editor_content.append(content)
            # Verify the default prompt is present
            assert "Implement plan from task gza-16" in content
            # Return success without modifying the file
            class Result:
                returncode = 0
            return Result()

        monkeypatch.setattr("gza.db._launch_editor", mock_run_with_write)

        result = edit_prompt(
            initial_content="",
            task_type="implement",
            based_on="gza-16",
        )

        # Verify the default prompt was included in the editor
        assert len(editor_content) == 1
        assert "Implement plan from task gza-16" in editor_content[0]

        # Verify the result includes the default
        assert result == "Implement plan from task gza-16"

    def test_edit_prompt_includes_slug_when_provided(self, tmp_path: Path, monkeypatch):
        """Test that edit_prompt includes the slug in the default prompt when provided."""
        from gza.db import edit_prompt

        editor_content = []

        def mock_run_with_write(cmd):
            temp_file = cmd[1]
            with open(temp_file) as f:
                content = f.read()
                editor_content.append(content)
            class Result:
                returncode = 0
            return Result()

        monkeypatch.setattr("gza.db._launch_editor", mock_run_with_write)

        result = edit_prompt(
            initial_content="",
            task_type="implement",
            based_on="gza-16",
            based_on_slug="design-feature-x",
        )

        assert len(editor_content) == 1
        assert "Implement plan from task gza-16: design-feature-x" in editor_content[0]
        assert result == "Implement plan from task gza-16: design-feature-x"

    def test_edit_prompt_no_default_for_other_task_types(self, tmp_path: Path, monkeypatch):
        """Test that edit_prompt does not provide default for non-implement tasks with based_on."""
        from gza.db import edit_prompt

        editor_content = []

        def mock_run(cmd):
            temp_file = cmd[1]
            with open(temp_file) as f:
                content = f.read()
                editor_content.append(content)
            # Don't write anything back (simulate empty editor)
            class Result:
                returncode = 0
            return Result()

        monkeypatch.setattr("gza.db._launch_editor", mock_run)

        result = edit_prompt(
            initial_content="",
            task_type="plan",  # Not implement
            based_on="gza-16",
        )

        # Verify no default prompt was added for plan type
        assert len(editor_content) == 1
        assert "Implement plan from task gza-16" not in editor_content[0]

        # Verify empty result since editor was "empty"
        assert result is None

    def test_edit_prompt_no_default_for_implement_without_based_on(self, tmp_path: Path, monkeypatch):
        """Test that edit_prompt does not provide default for implement tasks without based_on."""
        from gza.db import edit_prompt

        editor_content = []

        def mock_run(cmd):
            temp_file = cmd[1]
            with open(temp_file) as f:
                content = f.read()
                editor_content.append(content)
            class Result:
                returncode = 0
            return Result()

        monkeypatch.setattr("gza.db._launch_editor", mock_run)

        result = edit_prompt(
            initial_content="",
            task_type="implement",
            based_on=None,  # No based_on
        )

        # Verify no default prompt was added
        assert len(editor_content) == 1
        assert "Implement plan from task gza-" not in editor_content[0]
        assert result is None

    def test_edit_prompt_preserves_custom_initial_content(self, tmp_path: Path, monkeypatch):
        """Test that edit_prompt does not override custom initial_content."""
        from gza.db import edit_prompt

        editor_content = []
        custom_content = "Custom implementation task"

        def mock_run(cmd):
            temp_file = cmd[1]
            with open(temp_file) as f:
                content = f.read()
                editor_content.append(content)
            class Result:
                returncode = 0
            return Result()

        monkeypatch.setattr("gza.db._launch_editor", mock_run)

        result = edit_prompt(
            initial_content=custom_content,
            task_type="implement",
            based_on="gza-16",
        )

        # Verify custom content is present, not the default
        assert len(editor_content) == 1
        assert custom_content in editor_content[0]
        # The default should NOT be added when initial_content is provided
        # (it's already in the content area, not overwritten)
        assert result == custom_content

    def test_add_task_interactive_includes_slug_from_based_on(self, tmp_path: Path, monkeypatch):
        """Test that add_task_interactive looks up the slug from the based_on task."""
        from gza.db import SqliteTaskStore, add_task_interactive

        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        # Create a plan task with a known task_id containing a slug
        plan_task = store.add(prompt="Design feature X", task_type="plan")
        plan_task.slug = "20260223-design-feature-x"
        store.update(plan_task)

        editor_content = []

        def mock_run(cmd):
            temp_file = cmd[1]
            with open(temp_file) as f:
                content = f.read()
                editor_content.append(content)
            class Result:
                returncode = 0
            return Result()

        monkeypatch.setattr("gza.db._launch_editor", mock_run)

        add_task_interactive(store, task_type="implement", based_on=plan_task.id)

        assert len(editor_content) == 1
        assert "Implement plan from task " in editor_content[0]
        assert "design-feature-x" in editor_content[0]


class TestFailureReasonTracking:
    """Tests for failure_reason field and extract_failure_reason function."""

    def test_failure_reason_defaults_to_none_for_pending_task(self, tmp_path: Path):
        """New tasks have failure_reason=None."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        task = store.add(prompt="Test task")
        assert task.failure_reason is None
        assert task.completion_reason is None

        retrieved = store.get(task.id)
        assert retrieved is not None
        assert retrieved.failure_reason is None
        assert retrieved.completion_reason is None

    def test_based_on_tasks_persist_explicit_recovery_origin(self, tmp_path: Path) -> None:
        """based_on tasks should persist explicit recovery provenance when provided."""
        store = SqliteTaskStore(tmp_path / "test.db")
        parent = store.add(prompt="Parent task")
        assert parent.id is not None

        child = store.add(
            prompt="Manual follow-up",
            based_on=parent.id,
            recovery_origin="manual",
            trigger_source="manual",
        )
        assert child.recovery_origin == "manual"
        assert child.trigger_source == "manual"

        reloaded = store.get(child.id)
        assert reloaded is not None
        assert reloaded.recovery_origin == "manual"
        assert reloaded.trigger_source == "manual"

    def test_mark_failed_sets_unknown_by_default(self, tmp_path: Path):
        """mark_failed sets failure_reason='UNKNOWN' when not specified."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        task = store.add(prompt="Test task")
        store.mark_failed(task, log_file="logs/test.log")

        retrieved = store.get(task.id)
        assert retrieved is not None
        assert retrieved.status == "failed"
        assert retrieved.failure_reason == "UNKNOWN"

    def test_mark_failed_stores_provided_failure_reason(self, tmp_path: Path):
        """mark_failed stores a specified failure_reason."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        task = store.add(prompt="Test task")
        store.mark_failed(task, log_file="logs/test.log", failure_reason="MAX_TURNS")

        retrieved = store.get(task.id)
        assert retrieved is not None
        assert retrieved.failure_reason == "MAX_TURNS"

    def test_mark_failed_stores_test_failure_reason(self, tmp_path: Path):
        """mark_failed stores TEST_FAILURE reason."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        task = store.add(prompt="Test task")
        store.mark_failed(task, log_file="logs/test.log", failure_reason="TEST_FAILURE")

        retrieved = store.get(task.id)
        assert retrieved is not None
        assert retrieved.failure_reason == "TEST_FAILURE"

    def test_mark_completed_stores_completion_reason(self, tmp_path: Path):
        """mark_completed persists completion_reason and clears failure_reason."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        task = store.add(prompt="Test task")
        task.failure_reason = "MAX_TURNS"
        store.mark_completed(task, has_commits=False, completion_reason="EXTRACTION_ALREADY_MERGED")

        retrieved = store.get(task.id)
        assert retrieved is not None
        assert retrieved.status == "completed"
        assert retrieved.failure_reason is None
        assert retrieved.completion_reason == "EXTRACTION_ALREADY_MERGED"

    def test_mark_completed_stores_changed_diff(self, tmp_path: Path):
        """mark_completed persists changed_diff for completed rebase tasks."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        task = store.add(prompt="Rebase task", task_type="rebase")
        store.mark_completed(task, has_commits=False, changed_diff=False)

        retrieved = store.get(task.id)
        assert retrieved is not None
        assert retrieved.changed_diff is False

        store.mark_completed(task, has_commits=False, changed_diff=True)
        retrieved = store.get(task.id)
        assert retrieved is not None
        assert retrieved.changed_diff is True

    def test_mark_failed_clears_completion_reason(self, tmp_path: Path):
        """mark_failed clears any prior completion_reason."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        task = store.add(prompt="Test task")
        store.mark_completed(task, has_commits=False, completion_reason="EXTRACTION_ALREADY_MERGED")
        store.mark_failed(task, failure_reason="TEST_FAILURE")

        retrieved = store.get(task.id)
        assert retrieved is not None
        assert retrieved.status == "failed"
        assert retrieved.failure_reason == "TEST_FAILURE"
        assert retrieved.completion_reason is None

    def test_mark_failed_with_commits_creates_unmerged_merge_unit(self, tmp_path: Path) -> None:
        """Failed tasks with commits should write merge-unit truth immediately."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        task = store.add(prompt="Test task", task_type="implement")
        store.mark_failed(
            task,
            log_file="logs/test.log",
            branch="feature/test",
            has_commits=True,
            failure_reason="TEST_FAILURE",
            head_sha="abc123",
            base_sha="def456",
        )

        retrieved = store.get(task.id)
        assert retrieved is not None
        assert retrieved.status == "failed"
        assert retrieved.has_commits is True
        assert retrieved.merge_status == "unmerged"

        unit = store.resolve_merge_unit_for_task(task.id)
        assert unit is not None
        assert unit.source_branch == "feature/test"
        assert unit.target_branch == "main"
        assert unit.state == "unmerged"
        assert unit.head_sha == "abc123"
        assert unit.base_sha == "def456"

    def test_get_resumable_failed_tasks_excludes_test_failure(self, tmp_path: Path):
        """Auto-resume query includes MAX_* failures only, not TEST_FAILURE."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        resumable = store.add(prompt="Resumable task")
        resumable.status = "failed"
        resumable.failure_reason = "MAX_TURNS"
        resumable.session_id = "sess-resume"
        resumable.completed_at = datetime.now(UTC)
        store.update(resumable)

        non_resumable = store.add(prompt="Test failure task")
        non_resumable.status = "failed"
        non_resumable.failure_reason = "TEST_FAILURE"
        non_resumable.session_id = "sess-test"
        non_resumable.completed_at = datetime.now(UTC)
        store.update(non_resumable)

        resumable_ids = {task.id for task in store.get_resumable_failed_tasks()}
        assert resumable.id in resumable_ids
        assert non_resumable.id not in resumable_ids

    def test_extract_failure_reason_returns_unknown_for_missing_file(self, tmp_path: Path):
        """extract_failure_reason returns UNKNOWN when log file doesn't exist."""
        from gza.db import extract_failure_reason

        result = extract_failure_reason(tmp_path / "nonexistent.log")
        assert result == "UNKNOWN"

    def test_extract_failure_reason_returns_unknown_for_empty_file(self, tmp_path: Path):
        """extract_failure_reason returns UNKNOWN for empty log file."""
        from gza.db import extract_failure_reason

        log_file = tmp_path / "empty.log"
        log_file.write_text("")

        result = extract_failure_reason(log_file)
        assert result == "UNKNOWN"

    def test_extract_failure_reason_detects_max_turns(self, tmp_path: Path):
        """extract_failure_reason detects MAX_TURNS marker."""
        from gza.db import extract_failure_reason

        log_file = tmp_path / "test.log"
        log_file.write_text("Some output\n[GZA_FAILURE:MAX_TURNS]\nEnd of output")

        result = extract_failure_reason(log_file)
        assert result == "MAX_TURNS"

    def test_extract_failure_reason_detects_test_failure(self, tmp_path: Path):
        """extract_failure_reason detects TEST_FAILURE marker."""
        from gza.db import extract_failure_reason

        log_file = tmp_path / "test.log"
        log_file.write_text("Some output\n[GZA_FAILURE:TEST_FAILURE]\nFinal message")

        result = extract_failure_reason(log_file)
        assert result == "TEST_FAILURE"

    def test_extract_failure_reason_detects_agent_forfeit(self, tmp_path: Path):
        """extract_failure_reason detects AGENT_FORFEIT marker."""
        from gza.db import extract_failure_reason

        log_file = tmp_path / "test.log"
        log_file.write_text("Some output\n[GZA_FAILURE:AGENT_FORFEIT]\nFinal message")

        result = extract_failure_reason(log_file)
        assert result == "AGENT_FORFEIT"

    def test_extract_failure_reason_detects_max_steps(self, tmp_path: Path):
        """extract_failure_reason detects MAX_STEPS marker."""
        from gza.db import extract_failure_reason

        log_file = tmp_path / "test.log"
        log_file.write_text("Some output\n[GZA_FAILURE:MAX_STEPS]\nEnd of output")

        result = extract_failure_reason(log_file)
        assert result == "MAX_STEPS"

    def test_extract_failure_reason_detects_prerequisite_unmerged(self, tmp_path: Path):
        """extract_failure_reason detects PREREQUISITE_UNMERGED marker."""
        from gza.db import extract_failure_reason

        log_file = tmp_path / "test.log"
        log_file.write_text("Some output\n[GZA_FAILURE:PREREQUISITE_UNMERGED]\nEnd of output")

        result = extract_failure_reason(log_file)
        assert result == "PREREQUISITE_UNMERGED"

    def test_extract_failure_reason_detects_config_error(self, tmp_path: Path):
        """extract_failure_reason detects CONFIG_ERROR marker."""
        from gza.db import extract_failure_reason

        log_file = tmp_path / "test.log"
        log_file.write_text("Some output\n[GZA_FAILURE:CONFIG_ERROR]\nEnd of output")

        result = extract_failure_reason(log_file)
        assert result == "CONFIG_ERROR"

    def test_extract_failure_reason_detects_provider_unavailable(self, tmp_path: Path):
        """extract_failure_reason detects PROVIDER_UNAVAILABLE marker."""
        from gza.db import extract_failure_reason

        log_file = tmp_path / "test.log"
        log_file.write_text("Some output\n[GZA_FAILURE:PROVIDER_UNAVAILABLE]\nEnd of output")

        result = extract_failure_reason(log_file)
        assert result == "PROVIDER_UNAVAILABLE"

    def test_extract_failure_reason_returns_last_match(self, tmp_path: Path):
        """extract_failure_reason returns the last matching marker."""
        from gza.db import extract_failure_reason

        log_file = tmp_path / "test.log"
        log_file.write_text(
            "First attempt\n[GZA_FAILURE:MAX_TURNS]\n"
            "Retry attempt\n[GZA_FAILURE:TEST_FAILURE]\nFinal"
        )

        result = extract_failure_reason(log_file)
        assert result == "TEST_FAILURE"

    def test_extract_failure_reason_ignores_unknown_categories(self, tmp_path: Path):
        """extract_failure_reason ignores markers with unknown categories."""
        from gza.db import extract_failure_reason

        log_file = tmp_path / "test.log"
        log_file.write_text("Output\n[GZA_FAILURE:SOME_UNKNOWN_REASON]\nEnd")

        result = extract_failure_reason(log_file)
        assert result == "UNKNOWN"

    def test_extract_failure_reason_unknown_falls_back_to_known_if_mixed(self, tmp_path: Path):
        """extract_failure_reason uses last known marker even if unknown ones follow."""
        from gza.db import extract_failure_reason

        log_file = tmp_path / "test.log"
        log_file.write_text(
            "Output\n[GZA_FAILURE:TEST_FAILURE]\n[GZA_FAILURE:BOGUS_REASON]\nEnd"
        )

        # BOGUS_REASON is not known, so last *known* match is TEST_FAILURE
        result = extract_failure_reason(log_file)
        assert result == "TEST_FAILURE"

    def test_extract_failure_reason_returns_unknown_without_marker(self, tmp_path: Path):
        """extract_failure_reason returns UNKNOWN when no marker is found."""
        from gza.db import extract_failure_reason

        log_file = tmp_path / "test.log"
        log_file.write_text("Task ran for a while but didn't write any failure marker\n")

        result = extract_failure_reason(log_file)
        assert result == "UNKNOWN"

    def test_failure_reason_persisted_through_update(self, tmp_path: Path):
        """failure_reason is correctly persisted through the update method."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        task = store.add(prompt="Test task")
        task.failure_reason = "MAX_TURNS"
        task.status = "failed"
        task.completed_at = datetime.now(UTC)
        store.update(task)

        retrieved = store.get(task.id)
        assert retrieved is not None
        assert retrieved.failure_reason == "MAX_TURNS"

    def test_migration_v10_to_v11_adds_failure_reason_column(self, tmp_path: Path):
        """Migration from v10 to v11 adds failure_reason column and backfills failed tasks."""
        import sqlite3

        db_path = tmp_path / "test.db"

        # Create a v10 database manually (without failure_reason column)
        conn = sqlite3.connect(db_path)
        conn.execute("CREATE TABLE schema_version (version INTEGER PRIMARY KEY)")
        conn.execute("INSERT INTO schema_version (version) VALUES (10)")
        conn.execute("""
            CREATE TABLE tasks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                prompt TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending',
                task_type TEXT NOT NULL DEFAULT 'task',
                task_id TEXT,
                branch TEXT,
                log_file TEXT,
                report_file TEXT,
                based_on INTEGER REFERENCES tasks(id),
                has_commits INTEGER,
                duration_seconds REAL,
                num_turns INTEGER,
                num_turns_reported INTEGER,
                num_turns_computed INTEGER,
                cost_usd REAL,
                created_at TEXT NOT NULL,
                started_at TEXT,
                completed_at TEXT,
                "group" TEXT,
                depends_on INTEGER REFERENCES tasks(id),
                spec TEXT,
                create_review INTEGER DEFAULT 0,
                same_branch INTEGER DEFAULT 0,
                task_type_hint TEXT,
                output_content TEXT,
                session_id TEXT,
                pr_number INTEGER,
                model TEXT,
                provider TEXT,
                input_tokens INTEGER,
                output_tokens INTEGER,
                merge_status TEXT
            )
        """)

        now = datetime.now(UTC).isoformat()
        conn.execute(
            "INSERT INTO tasks (prompt, status, created_at) VALUES (?, ?, ?)",
            ("Failed task", "failed", now),
        )
        conn.execute(
            "INSERT INTO tasks (prompt, status, created_at) VALUES (?, ?, ?)",
            ("Pending task", "pending", now),
        )
        conn.commit()
        conn.close()

        # Open with SqliteTaskStore to trigger auto-migrations, then manual v25
        with pytest.raises(ManualMigrationRequired):
            SqliteTaskStore(db_path)
        _run_v25_v26_v27_migrations(db_path, "gza")
        store = SqliteTaskStore(db_path)

        # Check schema version updated
        conn = sqlite3.connect(db_path)
        cur = conn.execute("SELECT version FROM schema_version")
        version = cur.fetchone()[0]
        conn.close()
        assert version == SCHEMA_VERSION

        # Verify existing failed task was backfilled with 'UNKNOWN'
        failed_task = store.get("gza-1")
        assert failed_task is not None
        assert failed_task.status == "failed"
        assert failed_task.failure_reason == "UNKNOWN"

        # Verify pending task was NOT backfilled
        pending_task = store.get("gza-2")
        assert pending_task is not None
        assert pending_task.status == "pending"
        assert pending_task.failure_reason is None

    def test_migration_v39_to_v40_adds_completion_reason_column(self, tmp_path: Path):
        """Migration from v39 to v40 adds completion_reason column."""
        import sqlite3

        db_path = tmp_path / "test.db"
        conn = sqlite3.connect(db_path)
        conn.execute("CREATE TABLE schema_version (version INTEGER PRIMARY KEY)")
        conn.execute("INSERT INTO schema_version (version) VALUES (39)")
        conn.execute(
            """
            CREATE TABLE tasks (
                project_id TEXT NOT NULL,
                id TEXT NOT NULL,
                prompt TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending',
                task_type TEXT NOT NULL DEFAULT 'implement',
                slug TEXT,
                branch TEXT,
                log_file TEXT,
                report_file TEXT,
                based_on TEXT,
                has_commits INTEGER,
                duration_seconds REAL,
                num_steps_reported INTEGER,
                num_steps_computed INTEGER,
                num_turns INTEGER,
                num_turns_reported INTEGER,
                num_turns_computed INTEGER,
                attach_count INTEGER,
                attach_duration_seconds REAL,
                cost_usd REAL,
                created_at TEXT NOT NULL,
                started_at TEXT,
                running_pid INTEGER,
                completed_at TEXT,
                "group" TEXT,
                depends_on TEXT,
                spec TEXT,
                create_review INTEGER DEFAULT 0,
                create_pr INTEGER DEFAULT 0,
                same_branch INTEGER DEFAULT 0,
                task_type_hint TEXT,
                output_content TEXT,
                session_id TEXT,
                pr_number INTEGER,
                pr_state TEXT,
                pr_last_synced_at TEXT,
                sync_last_synced_at TEXT,
                model TEXT,
                provider TEXT,
                provider_is_explicit INTEGER DEFAULT 0,
                urgent INTEGER DEFAULT 0,
                urgent_bumped_at TEXT,
                queue_position INTEGER,
                input_tokens INTEGER,
                output_tokens INTEGER,
                merge_status TEXT,
                merged_at TEXT,
                failure_reason TEXT,
                skip_learnings INTEGER DEFAULT 0,
                diff_files_changed INTEGER,
                diff_lines_added INTEGER,
                diff_lines_removed INTEGER,
                review_cleared_at TEXT,
                review_score INTEGER,
                log_schema_version INTEGER DEFAULT 1,
                execution_mode TEXT,
                base_branch TEXT,
                PRIMARY KEY(project_id, id)
            )
            """
        )
        conn.execute(
            """
            INSERT INTO tasks (project_id, id, prompt, status, created_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            ("default", "testproject-1", "Completed task", "completed", datetime.now(UTC).isoformat()),
        )
        conn.commit()
        conn.close()

        store = SqliteTaskStore(db_path, prefix="testproject")
        retrieved = store.get("testproject-1")
        assert retrieved is not None
        assert retrieved.completion_reason is None

        with sqlite3.connect(db_path) as conn2:
            columns = {row[1] for row in conn2.execute("PRAGMA table_info(tasks)").fetchall()}
            version = conn2.execute("SELECT version FROM schema_version").fetchone()[0]

        assert "completion_reason" in columns
        assert version == SCHEMA_VERSION

    def test_migration_v40_to_v41_adds_recovery_origin_column(self, tmp_path: Path):
        """Migration from v40 to v41 adds recovery_origin column."""
        import sqlite3

        db_path = tmp_path / "test.db"
        conn = sqlite3.connect(db_path)
        conn.execute("CREATE TABLE schema_version (version INTEGER PRIMARY KEY)")
        conn.execute("INSERT INTO schema_version (version) VALUES (40)")
        conn.execute(
            """
            CREATE TABLE tasks (
                project_id TEXT NOT NULL,
                id TEXT NOT NULL,
                prompt TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending',
                task_type TEXT NOT NULL DEFAULT 'implement',
                slug TEXT,
                branch TEXT,
                log_file TEXT,
                report_file TEXT,
                based_on TEXT,
                has_commits INTEGER,
                duration_seconds REAL,
                num_steps_reported INTEGER,
                num_steps_computed INTEGER,
                num_turns INTEGER,
                num_turns_reported INTEGER,
                num_turns_computed INTEGER,
                attach_count INTEGER,
                attach_duration_seconds REAL,
                cost_usd REAL,
                created_at TEXT NOT NULL,
                started_at TEXT,
                running_pid INTEGER,
                completed_at TEXT,
                "group" TEXT,
                depends_on TEXT,
                spec TEXT,
                create_review INTEGER DEFAULT 0,
                create_pr INTEGER DEFAULT 0,
                same_branch INTEGER DEFAULT 0,
                task_type_hint TEXT,
                output_content TEXT,
                session_id TEXT,
                pr_number INTEGER,
                pr_state TEXT,
                pr_last_synced_at TEXT,
                sync_last_synced_at TEXT,
                model TEXT,
                provider TEXT,
                provider_is_explicit INTEGER DEFAULT 0,
                urgent INTEGER DEFAULT 0,
                urgent_bumped_at TEXT,
                queue_position INTEGER,
                input_tokens INTEGER,
                output_tokens INTEGER,
                merge_status TEXT,
                merged_at TEXT,
                failure_reason TEXT,
                completion_reason TEXT,
                skip_learnings INTEGER DEFAULT 0,
                diff_files_changed INTEGER,
                diff_lines_added INTEGER,
                diff_lines_removed INTEGER,
                review_cleared_at TEXT,
                review_score INTEGER,
                log_schema_version INTEGER DEFAULT 1,
                execution_mode TEXT,
                base_branch TEXT,
                PRIMARY KEY(project_id, id)
            )
            """
        )
        conn.execute(
            """
            INSERT INTO tasks (project_id, id, prompt, status, created_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            ("default", "testproject-1", "Task before recovery origin", "pending", datetime.now(UTC).isoformat()),
        )
        conn.commit()
        conn.close()

        store = SqliteTaskStore(db_path, prefix="testproject")
        retrieved = store.get("testproject-1")
        assert retrieved is not None
        assert retrieved.recovery_origin is None

        with sqlite3.connect(db_path) as conn2:
            columns = {row[1] for row in conn2.execute("PRAGMA table_info(tasks)").fetchall()}
            version = conn2.execute("SELECT version FROM schema_version").fetchone()[0]

        assert "recovery_origin" in columns
        assert version == SCHEMA_VERSION

    def test_migration_v44_to_v45_adds_trigger_source_column(self, tmp_path: Path):
        """Migration from v44 to v45 adds trigger_source column without backfilling rows."""
        import sqlite3

        db_path = tmp_path / "test.db"
        conn = sqlite3.connect(db_path)
        conn.execute("CREATE TABLE schema_version (version INTEGER PRIMARY KEY)")
        conn.execute("INSERT INTO schema_version (version) VALUES (44)")
        conn.execute(
            """
            CREATE TABLE tasks (
                project_id TEXT NOT NULL,
                id TEXT NOT NULL,
                prompt TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending',
                task_type TEXT NOT NULL DEFAULT 'implement',
                slug TEXT,
                branch TEXT,
                log_file TEXT,
                report_file TEXT,
                based_on TEXT,
                has_commits INTEGER,
                duration_seconds REAL,
                num_steps_reported INTEGER,
                num_steps_computed INTEGER,
                num_turns INTEGER,
                num_turns_reported INTEGER,
                num_turns_computed INTEGER,
                attach_count INTEGER,
                attach_duration_seconds REAL,
                cost_usd REAL,
                created_at TEXT NOT NULL,
                started_at TEXT,
                running_pid INTEGER,
                completed_at TEXT,
                "group" TEXT,
                depends_on TEXT,
                spec TEXT,
                create_review INTEGER DEFAULT 0,
                create_pr INTEGER DEFAULT 0,
                same_branch INTEGER DEFAULT 0,
                task_type_hint TEXT,
                output_content TEXT,
                session_id TEXT,
                pr_number INTEGER,
                pr_state TEXT,
                pr_last_synced_at TEXT,
                sync_last_synced_at TEXT,
                model TEXT,
                provider TEXT,
                provider_is_explicit INTEGER DEFAULT 0,
                urgent INTEGER DEFAULT 0,
                urgent_bumped_at TEXT,
                queue_position INTEGER,
                recovery_origin TEXT,
                input_tokens INTEGER,
                output_tokens INTEGER,
                merge_status TEXT,
                merged_at TEXT,
                failure_reason TEXT,
                completion_reason TEXT,
                skip_learnings INTEGER DEFAULT 0,
                diff_files_changed INTEGER,
                diff_lines_added INTEGER,
                diff_lines_removed INTEGER,
                changed_diff INTEGER,
                review_cleared_at TEXT,
                review_score INTEGER,
                log_schema_version INTEGER DEFAULT 1,
                execution_mode TEXT,
                base_branch TEXT,
                PRIMARY KEY(project_id, id)
            )
            """
        )
        conn.execute(
            """
            INSERT INTO tasks (project_id, id, prompt, status, created_at, recovery_origin)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            ("default", "testproject-1", "Task before trigger source", "pending", datetime.now(UTC).isoformat(), "retry"),
        )
        conn.commit()
        conn.close()

        store = SqliteTaskStore(db_path, prefix="testproject")
        migrated = store.get("testproject-1")
        assert migrated is not None
        assert migrated.recovery_origin == "retry"
        assert migrated.trigger_source is None

        fresh_store = SqliteTaskStore(tmp_path / "fresh.db", prefix="testproject")
        created = fresh_store.add("Task after trigger source", trigger_source="manual")
        assert created.trigger_source == "manual"
        reloaded_created = fresh_store.get(created.id)
        assert reloaded_created is not None
        assert reloaded_created.trigger_source == "manual"

        with sqlite3.connect(db_path) as conn2:
            columns = {row[1] for row in conn2.execute("PRAGMA table_info(tasks)").fetchall()}
            version = conn2.execute("SELECT version FROM schema_version").fetchone()[0]

        assert "trigger_source" in columns
        assert version == SCHEMA_VERSION

    def test_migration_v42_to_v43_adds_changed_diff_column(self, tmp_path: Path):
        """Migration from v42 to v43 adds changed_diff column."""
        import sqlite3

        db_path = tmp_path / "test.db"
        conn = sqlite3.connect(db_path)
        conn.execute("CREATE TABLE schema_version (version INTEGER PRIMARY KEY)")
        conn.execute("INSERT INTO schema_version (version) VALUES (42)")
        conn.execute(
            """
            CREATE TABLE tasks (
                project_id TEXT NOT NULL,
                id TEXT NOT NULL,
                prompt TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending',
                task_type TEXT NOT NULL DEFAULT 'implement',
                slug TEXT,
                branch TEXT,
                log_file TEXT,
                report_file TEXT,
                based_on TEXT,
                has_commits INTEGER,
                duration_seconds REAL,
                num_steps_reported INTEGER,
                num_steps_computed INTEGER,
                num_turns INTEGER,
                num_turns_reported INTEGER,
                num_turns_computed INTEGER,
                attach_count INTEGER,
                attach_duration_seconds REAL,
                cost_usd REAL,
                created_at TEXT NOT NULL,
                started_at TEXT,
                running_pid INTEGER,
                completed_at TEXT,
                "group" TEXT,
                depends_on TEXT,
                spec TEXT,
                create_review INTEGER DEFAULT 0,
                create_pr INTEGER DEFAULT 0,
                same_branch INTEGER DEFAULT 0,
                task_type_hint TEXT,
                output_content TEXT,
                session_id TEXT,
                pr_number INTEGER,
                pr_state TEXT,
                pr_last_synced_at TEXT,
                sync_last_synced_at TEXT,
                model TEXT,
                provider TEXT,
                provider_is_explicit INTEGER DEFAULT 0,
                urgent INTEGER DEFAULT 0,
                urgent_bumped_at TEXT,
                queue_position INTEGER,
                recovery_origin TEXT,
                input_tokens INTEGER,
                output_tokens INTEGER,
                merge_status TEXT,
                merged_at TEXT,
                failure_reason TEXT,
                completion_reason TEXT,
                skip_learnings INTEGER DEFAULT 0,
                diff_files_changed INTEGER,
                diff_lines_added INTEGER,
                diff_lines_removed INTEGER,
                review_cleared_at TEXT,
                review_score INTEGER,
                log_schema_version INTEGER DEFAULT 1,
                execution_mode TEXT,
                base_branch TEXT,
                PRIMARY KEY(project_id, id)
            )
            """
        )
        conn.execute(
            """
            INSERT INTO tasks (project_id, id, prompt, status, task_type, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            ("default", "testproject-1", "Completed rebase", "completed", "rebase", datetime.now(UTC).isoformat()),
        )
        conn.commit()
        conn.close()

        store = SqliteTaskStore(db_path, prefix="testproject")
        retrieved = store.get("testproject-1")
        assert retrieved is not None
        assert retrieved.changed_diff is None

        with sqlite3.connect(db_path) as conn2:
            columns = {row[1] for row in conn2.execute("PRAGMA table_info(tasks)").fetchall()}
            version = conn2.execute("SELECT version FROM schema_version").fetchone()[0]

        assert "changed_diff" in columns
        assert version == SCHEMA_VERSION

    def test_known_failure_reasons_set(self):
        """KNOWN_FAILURE_REASONS contains expected values."""
        from gza.db import KNOWN_FAILURE_REASONS

        assert "AGENT_FORFEIT" in KNOWN_FAILURE_REASONS
        assert "BRANCH_UNPUSHABLE" in KNOWN_FAILURE_REASONS
        assert "CONFIG_ERROR" in KNOWN_FAILURE_REASONS
        assert "INFRASTRUCTURE_ERROR" in KNOWN_FAILURE_REASONS
        assert "MAX_STEPS" in KNOWN_FAILURE_REASONS
        assert "MAX_TURNS" in KNOWN_FAILURE_REASONS
        assert "PR_REQUIRED" in KNOWN_FAILURE_REASONS
        assert "PREREQUISITE_UNMERGED" in KNOWN_FAILURE_REASONS
        assert "PROVIDER_EMPTY_TURN" in KNOWN_FAILURE_REASONS
        assert "PROVIDER_UNAVAILABLE" in KNOWN_FAILURE_REASONS
        assert "RETRYABLE_PROVIDER_ERROR" in KNOWN_FAILURE_REASONS
        assert "TEST_FAILURE" in KNOWN_FAILURE_REASONS
        assert "UNKNOWN" in KNOWN_FAILURE_REASONS


class TestDiffStats:
    """Tests for diff stats columns (schema v12)."""

    def test_diff_stats_null_by_default(self, tmp_path: Path):
        """New tasks have NULL diff stats."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)
        task = store.add(prompt="Test task")
        retrieved = store.get(task.id)
        assert retrieved is not None
        assert retrieved.diff_files_changed is None
        assert retrieved.diff_lines_added is None
        assert retrieved.diff_lines_removed is None

    def test_mark_completed_persists_diff_stats(self, tmp_path: Path):
        """mark_completed stores diff stats when provided."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)
        task = store.add(prompt="Test task")
        store.mark_completed(
            task,
            has_commits=True,
            diff_files_changed=5,
            diff_lines_added=120,
            diff_lines_removed=34,
        )
        retrieved = store.get(task.id)
        assert retrieved is not None
        assert retrieved.diff_files_changed == 5
        assert retrieved.diff_lines_added == 120
        assert retrieved.diff_lines_removed == 34
        assert retrieved.merge_status == "unmerged"

    def test_mark_completed_without_diff_stats_leaves_null(self, tmp_path: Path):
        """mark_completed without diff stats leaves them as NULL."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)
        task = store.add(prompt="Test task")
        store.mark_completed(task, has_commits=False)
        retrieved = store.get(task.id)
        assert retrieved is not None
        assert retrieved.diff_files_changed is None
        assert retrieved.diff_lines_added is None
        assert retrieved.diff_lines_removed is None

    def test_update_diff_stats(self, tmp_path: Path):
        """update_diff_stats sets diff columns without touching other fields."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)
        task = store.add(prompt="Test task")
        store.mark_completed(task, has_commits=True)

        assert task.id is not None
        store.update_diff_stats(task.id, files_changed=3, lines_added=50, lines_removed=10)
        retrieved = store.get(task.id)
        assert retrieved is not None
        assert retrieved.diff_files_changed == 3
        assert retrieved.diff_lines_added == 50
        assert retrieved.diff_lines_removed == 10
        # Other fields unchanged
        assert retrieved.status == "completed"
        assert retrieved.has_commits is True

    def test_update_diff_stats_with_none(self, tmp_path: Path):
        """update_diff_stats can reset stats to NULL."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)
        task = store.add(prompt="Test task")
        store.mark_completed(
            task,
            has_commits=True,
            diff_files_changed=5,
            diff_lines_added=10,
            diff_lines_removed=2,
        )
        assert task.id is not None
        store.update_diff_stats(task.id, files_changed=None, lines_added=None, lines_removed=None)
        retrieved = store.get(task.id)
        assert retrieved is not None
        assert retrieved.diff_files_changed is None
        assert retrieved.diff_lines_added is None
        assert retrieved.diff_lines_removed is None

    def test_migration_v11_to_v12_adds_diff_columns(self, tmp_path: Path):
        """Migration from v11 to v12 adds diff stat columns."""
        import sqlite3
        from datetime import datetime

        db_path = tmp_path / "test.db"

        # Create a v11 database (without diff stat columns)
        conn = sqlite3.connect(db_path)
        conn.execute("CREATE TABLE schema_version (version INTEGER PRIMARY KEY)")
        conn.execute("INSERT INTO schema_version (version) VALUES (11)")
        conn.execute("""
            CREATE TABLE tasks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                prompt TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending',
                task_type TEXT NOT NULL DEFAULT 'task',
                task_id TEXT,
                branch TEXT,
                log_file TEXT,
                report_file TEXT,
                based_on INTEGER REFERENCES tasks(id),
                has_commits INTEGER,
                duration_seconds REAL,
                num_turns INTEGER,
                num_turns_reported INTEGER,
                num_turns_computed INTEGER,
                cost_usd REAL,
                created_at TEXT NOT NULL,
                started_at TEXT,
                completed_at TEXT,
                "group" TEXT,
                depends_on INTEGER REFERENCES tasks(id),
                spec TEXT,
                create_review INTEGER DEFAULT 0,
                same_branch INTEGER DEFAULT 0,
                task_type_hint TEXT,
                output_content TEXT,
                session_id TEXT,
                pr_number INTEGER,
                model TEXT,
                provider TEXT,
                input_tokens INTEGER,
                output_tokens INTEGER,
                merge_status TEXT,
                failure_reason TEXT
            )
        """)
        now = datetime.now(UTC).isoformat()
        conn.execute(
            "INSERT INTO tasks (prompt, status, created_at) VALUES (?, ?, ?)",
            ("Existing task", "completed", now),
        )
        conn.commit()
        conn.close()

        # Open with SqliteTaskStore to trigger auto-migrations, then manual v25
        with pytest.raises(ManualMigrationRequired):
            SqliteTaskStore(db_path)
        _run_v25_v26_v27_migrations(db_path, "gza")
        store = SqliteTaskStore(db_path)

        # Check schema version updated
        conn = sqlite3.connect(db_path)
        cur = conn.execute("SELECT version FROM schema_version")
        version = cur.fetchone()[0]
        conn.close()
        assert version == SCHEMA_VERSION

        # Verify existing task has NULL diff stats
        task = store.get("gza-1")
        assert task is not None
        assert task.diff_files_changed is None
        assert task.diff_lines_added is None
        assert task.diff_lines_removed is None


class TestReviewClearedAt:
    """Tests for review_cleared_at field and clear_review_state (schema v14)."""

    def test_migration_v13_to_v14_adds_review_cleared_at_column(self, tmp_path: Path):
        """Migration from v13 to v14 adds review_cleared_at column."""
        import sqlite3

        db_path = tmp_path / "test.db"

        # Create a v13 database manually (without review_cleared_at column)
        conn = sqlite3.connect(db_path)
        conn.execute("CREATE TABLE schema_version (version INTEGER PRIMARY KEY)")
        conn.execute("INSERT INTO schema_version (version) VALUES (13)")
        conn.execute("""
            CREATE TABLE tasks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                prompt TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending',
                task_type TEXT NOT NULL DEFAULT 'task',
                task_id TEXT,
                branch TEXT,
                log_file TEXT,
                report_file TEXT,
                based_on INTEGER REFERENCES tasks(id),
                has_commits INTEGER,
                duration_seconds REAL,
                num_turns INTEGER,
                num_turns_reported INTEGER,
                num_turns_computed INTEGER,
                cost_usd REAL,
                created_at TEXT NOT NULL,
                started_at TEXT,
                completed_at TEXT,
                "group" TEXT,
                depends_on INTEGER REFERENCES tasks(id),
                spec TEXT,
                create_review INTEGER DEFAULT 0,
                same_branch INTEGER DEFAULT 0,
                task_type_hint TEXT,
                output_content TEXT,
                session_id TEXT,
                pr_number INTEGER,
                model TEXT,
                provider TEXT,
                input_tokens INTEGER,
                output_tokens INTEGER,
                merge_status TEXT,
                failure_reason TEXT,
                skip_learnings INTEGER DEFAULT 0,
                diff_files_changed INTEGER,
                diff_lines_added INTEGER,
                diff_lines_removed INTEGER
            )
        """)
        now = datetime.now(UTC).isoformat()
        conn.execute(
            "INSERT INTO tasks (prompt, status, created_at) VALUES (?, ?, ?)",
            ("Existing task", "completed", now),
        )
        conn.commit()
        conn.close()

        # Open with SqliteTaskStore to trigger auto-migrations, then manual v25
        with pytest.raises(ManualMigrationRequired):
            SqliteTaskStore(db_path)
        _run_v25_v26_v27_migrations(db_path, "gza")
        store = SqliteTaskStore(db_path)

        # Verify schema version updated
        conn = sqlite3.connect(db_path)
        cur = conn.execute("SELECT version FROM schema_version")
        version = cur.fetchone()[0]
        conn.close()
        assert version == SCHEMA_VERSION

        # Verify existing task can be retrieved with NULL review_cleared_at
        task = store.get("gza-1")
        assert task is not None
        assert task.review_cleared_at is None

        # Verify the column is readable and writable on new tasks
        new_task = store.add(prompt="New task", task_type="implement")
        assert new_task.id is not None
        assert new_task.review_cleared_at is None

        store.clear_review_state(new_task.id)
        updated = store.get(new_task.id)
        assert updated is not None
        assert updated.review_cleared_at is not None

    def test_clear_review_state_on_nonexistent_task_is_graceful(self, tmp_path: Path):
        """clear_review_state does not raise when task_id does not exist."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        # Should not raise any exception
        store.clear_review_state("gza-nonexistent")

    def test_invalidate_review_state_clears_review_cleared_at(self, tmp_path: Path):
        """invalidate_review_state sets review_cleared_at to NULL."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        task = store.add(prompt="Task to invalidate", task_type="implement")
        assert task.id is not None

        # First set review_cleared_at
        store.clear_review_state(task.id)
        updated = store.get(task.id)
        assert updated is not None
        assert updated.review_cleared_at is not None

        # Now invalidate it
        store.invalidate_review_state(task.id)
        invalidated = store.get(task.id)
        assert invalidated is not None
        assert invalidated.review_cleared_at is None

    def test_invalidate_review_state_on_nonexistent_task_is_graceful(self, tmp_path: Path):
        """invalidate_review_state does not raise when task_id does not exist."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        # Should not raise any exception
        store.invalidate_review_state(99999)


class TestConvenienceFunctions:
    """Tests for module-level convenience functions get_task, get_task_log_path,
    get_task_report_path, and get_baseline_stats."""

    @pytest.fixture(autouse=True)
    def _write_config(self, tmp_path: Path) -> None:
        db_path = tmp_path / ".gza" / "gza.db"
        (tmp_path / "gza.yaml").write_text(
            "project_name: test\n"
            "project_id: default\n"
            f"db_path: {db_path}\n",
            encoding="utf-8",
        )

    def test_get_task_returns_dict(self, tmp_path: Path, monkeypatch):
        """get_task returns a dict with all task fields."""
        from gza.db import get_task

        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True)
        store = SqliteTaskStore(db_path)
        task = store.add(prompt="Test task for get_task", task_type="implement")

        monkeypatch.chdir(tmp_path)

        result = get_task(task.id)

        assert isinstance(result, dict)
        assert result["id"] == task.id
        assert result["prompt"] == "Test task for get_task"
        assert result["status"] == "pending"
        assert result["task_type"] == "implement"
        assert result["changed_diff"] is None
        # Datetime fields should be ISO strings or None
        assert isinstance(result["created_at"], str)
        assert result["started_at"] is None
        assert result["completed_at"] is None

    def test_get_task_all_fields_json_serializable(self, tmp_path: Path, monkeypatch):
        """get_task result is fully JSON-serializable."""
        import json

        from gza.db import TaskStats, get_task

        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True)
        store = SqliteTaskStore(db_path)
        task = store.add(
            prompt="Full task",
            task_type="implement",
            group="test-group",
            spec="specs/test.md",
        )
        store.mark_completed(
            task,
            has_commits=True,
            branch="feature/test",
            log_file=".gza/logs/test.log",
            report_file=".gza/reports/test.md",
            stats=TaskStats(
                duration_seconds=42.0,
                num_turns_reported=5,
                num_turns_computed=4,
                cost_usd=0.10,
                input_tokens=1000,
                output_tokens=500,
            ),
        )

        monkeypatch.chdir(tmp_path)

        result = get_task(task.id)
        # Should not raise
        serialized = json.dumps(result)
        assert serialized  # non-empty

    def test_get_task_raises_for_missing_task(self, tmp_path: Path, monkeypatch):
        """get_task raises ValueError when task does not exist."""
        from gza.db import get_task

        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True)
        SqliteTaskStore(db_path)  # create empty DB

        monkeypatch.chdir(tmp_path)

        with pytest.raises(KeyError, match="Task 999 not found"):
            get_task(999)

    def test_get_task_log_path_returns_log_file(self, tmp_path: Path, monkeypatch):
        """get_task_log_path returns the log_file field."""
        from gza.db import get_task_log_path

        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True)
        store = SqliteTaskStore(db_path)
        task = store.add(prompt="Task with log")
        store.mark_failed(task, log_file=".gza/logs/task-1.log")

        monkeypatch.chdir(tmp_path)

        result = get_task_log_path(task.id)
        assert result == ".gza/logs/task-1.log"

    def test_get_task_log_path_returns_none_when_not_set(self, tmp_path: Path, monkeypatch):
        """get_task_log_path returns None when log_file is not set."""
        from gza.db import get_task_log_path

        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True)
        store = SqliteTaskStore(db_path)
        task = store.add(prompt="Task without log")

        monkeypatch.chdir(tmp_path)

        result = get_task_log_path(task.id)
        assert result is None

    def test_get_task_log_path_returns_none_for_missing_task(self, tmp_path: Path, monkeypatch):
        """get_task_log_path returns None when task does not exist."""
        from gza.db import get_task_log_path

        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True)
        SqliteTaskStore(db_path)

        monkeypatch.chdir(tmp_path)

        result = get_task_log_path(999)
        assert result is None

    def test_get_task_report_path_returns_report_file(self, tmp_path: Path, monkeypatch):
        """get_task_report_path returns the report_file field."""
        from gza.db import get_task_report_path

        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True)
        store = SqliteTaskStore(db_path)
        task = store.add(prompt="Task with report")
        store.mark_completed(task, report_file=".gza/reports/task-1.md", has_commits=False)

        monkeypatch.chdir(tmp_path)

        result = get_task_report_path(task.id)
        assert result == ".gza/reports/task-1.md"

    def test_get_task_report_path_returns_none_when_not_set(self, tmp_path: Path, monkeypatch):
        """get_task_report_path returns None when report_file is not set."""
        from gza.db import get_task_report_path

        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True)
        store = SqliteTaskStore(db_path)
        task = store.add(prompt="Task without report")

        monkeypatch.chdir(tmp_path)

        result = get_task_report_path(task.id)
        assert result is None

    def test_get_task_report_path_returns_none_for_missing_task(self, tmp_path: Path, monkeypatch):
        """get_task_report_path returns None when task does not exist."""
        from gza.db import get_task_report_path

        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True)
        SqliteTaskStore(db_path)

        monkeypatch.chdir(tmp_path)

        result = get_task_report_path(999)
        assert result is None

    def test_get_baseline_stats_returns_averages(self, tmp_path: Path, monkeypatch):
        """get_baseline_stats returns avg_turns, avg_duration, avg_cost."""
        from gza.db import TaskStats, get_baseline_stats

        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True)
        store = SqliteTaskStore(db_path)

        # Add completed tasks with known stats
        for i in range(3):
            task = store.add(prompt=f"Task {i}")
            store.mark_completed(
                task,
                has_commits=False,
                stats=TaskStats(
                    duration_seconds=float(10 * (i + 1)),  # 10, 20, 30
                    num_turns_reported=i + 1,              # 1, 2, 3
                    cost_usd=0.01 * (i + 1),               # 0.01, 0.02, 0.03
                ),
            )

        monkeypatch.chdir(tmp_path)

        result = get_baseline_stats()

        assert isinstance(result, dict)
        assert "avg_turns" in result
        assert "avg_duration" in result
        assert "avg_cost" in result
        # avg_turns = (1+2+3)/3 = 2.0
        assert result["avg_turns"] == 2.0
        # avg_duration = (10+20+30)/3 = 20.0
        assert result["avg_duration"] == 20.0
        # avg_cost = (0.01+0.02+0.03)/3 = 0.02
        assert result["avg_cost"] is not None

    def test_get_baseline_stats_respects_limit(self, tmp_path: Path, monkeypatch):
        """get_baseline_stats only includes the last N tasks."""
        from datetime import datetime

        from gza.db import get_baseline_stats

        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True)
        store = SqliteTaskStore(db_path)

        # Add 5 completed tasks with differing costs
        for i in range(5):
            task = store.add(prompt=f"Task {i}")
            task.completed_at = datetime(2026, 1, i + 1, tzinfo=UTC)
            task.status = "completed"
            task.cost_usd = float(i + 1)  # 1.0, 2.0, 3.0, 4.0, 5.0
            task.num_turns_reported = i + 1
            task.duration_seconds = float(i + 1)
            store.update(task)

        monkeypatch.chdir(tmp_path)

        # limit=2 should only use the 2 most recent tasks (cost 4.0 and 5.0)
        result = get_baseline_stats(limit=2)
        assert result["avg_cost"] == round((4.0 + 5.0) / 2, 4)

    def test_get_baseline_stats_returns_none_when_no_completed_tasks(self, tmp_path: Path, monkeypatch):
        """get_baseline_stats returns None values when no completed tasks exist."""
        from gza.db import get_baseline_stats

        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True)
        SqliteTaskStore(db_path)  # empty DB

        monkeypatch.chdir(tmp_path)

        result = get_baseline_stats()
        assert result["avg_turns"] is None
        assert result["avg_duration"] is None
        assert result["avg_cost"] is None

    def test_get_task_datetime_fields_serialized_as_iso_strings(self, tmp_path: Path, monkeypatch):
        """get_task returns datetime fields as ISO-format strings, not datetime objects."""
        from gza.db import TaskStats, get_task

        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True)
        store = SqliteTaskStore(db_path)
        task = store.add(prompt="Task with dates")
        store.mark_completed(task, has_commits=False, stats=TaskStats(duration_seconds=10.0))

        monkeypatch.chdir(tmp_path)

        result = get_task(task.id)
        # Datetimes must be strings so JSON serialization works
        assert isinstance(result["created_at"], str)
        assert isinstance(result["completed_at"], str)
        assert result["started_at"] is None  # never set started_at


class TestRetryChainDependencyResolution:
    """Tests for auto-resolving blocked tasks when a retry of their dependency succeeds."""

    def _make_store(self, tmp_path: Path) -> SqliteTaskStore:
        return SqliteTaskStore(tmp_path / "test.db")

    def _fail(self, store: SqliteTaskStore, task: Task) -> Task:
        store.mark_failed(task, failure_reason="UNKNOWN")
        result = store.get(task.id)
        assert result is not None
        return result

    def _complete(self, store: SqliteTaskStore, task: Task) -> Task:
        store.mark_completed(task, has_commits=False)
        result = store.get(task.id)
        assert result is not None
        return result

    def _complete_implement_with_branch(
        self,
        store: SqliteTaskStore,
        task: Task,
        *,
        branch: str,
        merge_state: str = "empty",
    ) -> Task:
        store.mark_completed(task, has_commits=True, branch=branch)
        assert task.id is not None
        unit = store.resolve_merge_unit_for_task(task.id)
        assert unit is not None
        store.set_merge_unit_state(unit.id, merge_state)
        result = store.get(task.id)
        assert result is not None
        return result

    # --- is_task_blocked ---

    def test_no_dependency_not_blocked(self, tmp_path: Path):
        """Regression: task with no dependency is never blocked."""
        store = self._make_store(tmp_path)
        task = store.add("Independent task")
        is_blocked, blocking_id, blocking_status = store.is_task_blocked(task)
        assert is_blocked is False
        assert blocking_id is None
        assert blocking_status is None

    def test_completed_dependency_not_blocked(self, tmp_path: Path):
        """Regression: task with a completed dependency is not blocked."""
        store = self._make_store(tmp_path)
        dep = store.add("Dependency", task_type="plan")
        self._complete(store, dep)
        downstream = store.add("Downstream", depends_on=dep.id)
        is_blocked, _, _ = store.is_task_blocked(downstream)
        assert is_blocked is False

    def test_completed_unmerged_implement_dependency_stays_blocked(self, tmp_path: Path):
        """Completed code prerequisites remain blocked until merge dependency is satisfied."""
        store = self._make_store(tmp_path)
        dep = store.add("Dependency", task_type="implement")
        self._complete_implement_with_branch(store, dep, branch="feature/dep-unmerged", merge_state="unmerged")
        downstream = store.add("Downstream", task_type="implement", depends_on=dep.id)

        is_blocked, blocking_id, blocking_status = store.is_task_blocked(downstream)
        assert is_blocked is True
        assert blocking_id == dep.id
        assert blocking_status == "completed"
        assert store.get_next_pending() is None
        assert store.get_pending_pickup() == []

    def test_completed_unmerged_implement_dependency_does_not_block_review(self, tmp_path: Path):
        """Non-code downstream tasks only require completed prerequisites, not merged code."""
        store = self._make_store(tmp_path)
        dep = store.add("Dependency", task_type="implement")
        self._complete_implement_with_branch(store, dep, branch="feature/dep-unmerged-review", merge_state="unmerged")
        review = store.add("Review dependency output", task_type="review", depends_on=dep.id)

        is_blocked, blocking_id, blocking_status = store.is_task_blocked(review)
        assert is_blocked is False
        assert blocking_id is None
        assert blocking_status is None
        assert store.get_next_pending() is not None
        assert store.get_next_pending().id == review.id

    def test_failed_dep_no_retry_still_blocked(self, tmp_path: Path):
        """Task blocked by a failed dep with no retry stays blocked."""
        store = self._make_store(tmp_path)
        dep = store.add("Dependency")
        self._fail(store, dep)
        downstream = store.add("Downstream", depends_on=dep.id)
        is_blocked, blocking_id, blocking_status = store.is_task_blocked(downstream)
        assert is_blocked is True
        assert blocking_id == dep.id
        assert blocking_status == "failed"

    def test_failed_dep_with_successful_retry_unblocks(self, tmp_path: Path):
        """Task blocked by failed dep is unblocked when a direct retry succeeds."""
        store = self._make_store(tmp_path)
        dep = store.add("Dependency", task_type="plan")
        self._fail(store, dep)
        retry = store.add("Retry of dep", task_type="plan", based_on=dep.id)
        self._complete(store, retry)

        downstream = store.add("Downstream", depends_on=dep.id)
        is_blocked, _, _ = store.is_task_blocked(downstream)
        assert is_blocked is False

    def test_failed_dep_with_failed_retry_still_blocked(self, tmp_path: Path):
        """Task stays blocked when the retry also failed."""
        store = self._make_store(tmp_path)
        dep = store.add("Dependency")
        self._fail(store, dep)
        retry = store.add("Retry of dep", based_on=dep.id)
        self._fail(store, retry)

        downstream = store.add("Downstream", depends_on=dep.id)
        is_blocked, blocking_id, _ = store.is_task_blocked(downstream)
        assert is_blocked is True
        assert blocking_id == dep.id

    def test_missing_dependency_is_blocked_everywhere(self, tmp_path: Path) -> None:
        """Missing dependency rows must hold pickup, blocked-state checks, and claims."""
        store = self._make_store(tmp_path)
        downstream = store.add("Downstream", task_type="implement", depends_on="gza-999999")

        is_blocked, blocking_id, blocking_status = store.is_task_blocked(downstream)
        assert is_blocked is True
        assert blocking_id == "gza-999999"
        assert blocking_status == "missing"
        assert store.get_pending_pickup() == []
        assert store.count_blocked_tasks() == 1

        assert downstream.id is not None
        claim = store.try_mark_in_progress(downstream.id, 12345)
        assert claim.task is None
        assert claim.refusal_reason == "blocked"
        assert claim.readiness_reason == "missing"
        assert claim.blocking_task_id == "gza-999999"
        assert claim.blocking_task_status == "missing"

    def test_retry_chain_failed_failed_completed_unblocks(self, tmp_path: Path):
        """dep(failed) → retry1(failed) → retry2(completed): downstream unblocked."""
        store = self._make_store(tmp_path)
        dep = store.add("Original dep", task_type="plan")
        self._fail(store, dep)
        retry1 = store.add("First retry", task_type="plan", based_on=dep.id)
        self._fail(store, retry1)
        retry2 = store.add("Second retry", task_type="plan", based_on=retry1.id)
        self._complete(store, retry2)

        downstream = store.add("Downstream", depends_on=dep.id)
        is_blocked, _, _ = store.is_task_blocked(downstream)
        assert is_blocked is False

    def test_resolve_dependency_completion_returns_completed_retry(self, tmp_path: Path):
        """resolve_dependency_completion should resolve to completed retry descendant."""
        store = self._make_store(tmp_path)
        dep = store.add("Original dep", task_type="plan")
        self._fail(store, dep)
        retry = store.add("Retry dep", task_type="plan", based_on=dep.id)
        self._complete(store, retry)
        downstream = store.add("Downstream", depends_on=dep.id)

        resolved = store.resolve_dependency_completion(downstream)
        assert resolved is not None
        assert resolved.id == retry.id

    def test_recovered_dependency_uses_canonical_lineage_merge_unit(self, tmp_path: Path) -> None:
        """Resolved retry completions must still follow the original dependency merge unit."""
        store = self._make_store(tmp_path)
        dep = store.add("Original dependency", task_type="implement")
        store.mark_completed(dep, has_commits=True, branch="feature/original-dependency")
        assert dep.id is not None
        unit = store.resolve_merge_unit_for_task(dep.id)
        assert unit is not None
        store.set_merge_unit_state(unit.id, "unmerged")

        dep = store.get(dep.id)
        assert dep is not None
        store.mark_failed(dep, failure_reason="UNKNOWN")

        retry = store.add("Recovered dependency", task_type="implement", based_on=dep.id)
        store.mark_completed(retry, has_commits=True, branch="feature/original-dependency-recovered")
        assert retry.id is not None

        downstream = store.add("Downstream", task_type="implement", depends_on=dep.id)
        readiness = store.get_dependency_readiness(downstream)
        assert readiness.ready is False
        assert readiness.blocking_merge_unit_id == unit.id
        assert readiness.blocking_merge_state == "unmerged"
        assert readiness.blocking_merge_unit_owner_task_id == dep.id
        assert store.get_pending_pickup() == []

        store.set_merge_unit_state(unit.id, "merged")

        readiness = store.get_dependency_readiness(downstream)
        assert readiness.ready is True
        assert [task.id for task in store.get_pending_pickup()] == [downstream.id]

    def test_dropped_dep_with_successful_retry_unblocks(self, tmp_path: Path):
        """Dropped dependency remains blocking unless a retry descendant completes."""
        store = self._make_store(tmp_path)
        dep = store.add("Dependency", task_type="plan")
        dep.status = "dropped"
        dep.completed_at = datetime.now(UTC)
        store.update(dep)
        retry = store.add("Retry of dropped dep", task_type="plan", based_on=dep.id)
        self._complete(store, retry)

        downstream = store.add("Downstream", depends_on=dep.id)
        is_blocked, _, _ = store.is_task_blocked(downstream)
        assert is_blocked is False

    # --- get_next_pending ---

    def test_get_next_pending_skips_task_blocked_by_failed_dep(self, tmp_path: Path):
        """get_next_pending does not return a task whose dep is failed with no retry."""
        store = self._make_store(tmp_path)
        dep = store.add("Dependency")
        self._fail(store, dep)
        _downstream = store.add("Downstream", depends_on=dep.id)

        next_task = store.get_next_pending()
        # dep is failed, downstream is blocked — nothing runnable
        assert next_task is None

    def test_get_next_pending_returns_task_unblocked_by_successful_retry(self, tmp_path: Path):
        """get_next_pending returns downstream once its dep's retry succeeds."""
        store = self._make_store(tmp_path)
        dep = store.add("Dependency", task_type="plan")
        self._fail(store, dep)
        retry = store.add("Retry", task_type="plan", based_on=dep.id)
        self._complete(store, retry)
        downstream = store.add("Downstream", depends_on=dep.id)

        next_task = store.get_next_pending()
        assert next_task is not None
        assert next_task.id == downstream.id

    def test_get_next_pending_handles_retry_chain(self, tmp_path: Path):
        """get_next_pending unblocks downstream after multi-hop retry chain succeeds."""
        store = self._make_store(tmp_path)
        dep = store.add("Original dep", task_type="plan")
        self._fail(store, dep)
        retry1 = store.add("Retry 1", task_type="plan", based_on=dep.id)
        self._fail(store, retry1)
        retry2 = store.add("Retry 2", task_type="plan", based_on=retry1.id)
        self._complete(store, retry2)
        downstream = store.add("Downstream", depends_on=dep.id)

        next_task = store.get_next_pending()
        assert next_task is not None
        assert next_task.id == downstream.id

    def test_get_next_pending_no_dep_always_runnable(self, tmp_path: Path):
        """Regression: independent tasks are always returned by get_next_pending."""
        store = self._make_store(tmp_path)
        task = store.add("Independent task")

        next_task = store.get_next_pending()
        assert next_task is not None
        assert next_task.id == task.id

    def test_get_next_pending_completed_dep_unblocks(self, tmp_path: Path):
        """Regression: get_next_pending returns downstream when dep is completed."""
        store = self._make_store(tmp_path)
        dep = store.add("Dep", task_type="plan")
        self._complete(store, dep)
        downstream = store.add("Downstream", depends_on=dep.id)

        next_task = store.get_next_pending()
        assert next_task is not None
        assert next_task.id == downstream.id

    def test_completed_held_plan_dependency_blocks_until_released(self, tmp_path: Path) -> None:
        """Completed held plans keep dependents out of readiness and pickup until released."""
        store = self._make_store(tmp_path)
        plan = store.add("Held plan", task_type="plan", auto_implement=False)
        self._complete(store, plan)
        downstream = store.add("Downstream", task_type="implement", depends_on=plan.id)

        readiness = store.get_dependency_readiness(downstream)
        assert readiness.ready is False
        assert readiness.reason == "plan_awaiting_review"
        assert readiness.blocking_task_id == plan.id
        assert store.get_pending_pickup() == []

        refreshed_plan = store.get(plan.id)
        assert refreshed_plan is not None
        refreshed_plan.auto_implement = True
        store.update(refreshed_plan)

        readiness = store.get_dependency_readiness(downstream)
        assert readiness.ready is True
        assert [task.id for task in store.get_pending_pickup()] == [downstream.id]

    def test_dropped_held_plan_dependency_stays_blocked_even_with_completed_retry(self, tmp_path: Path) -> None:
        """Retry-chain completion must not bypass a directly held plan that was dropped."""
        store = self._make_store(tmp_path)
        plan = store.add("Dropped held plan", task_type="plan", auto_implement=False)
        assert plan.id is not None
        plan.status = "dropped"
        plan.completed_at = datetime.now(UTC)
        store.update(plan)

        retry = store.add("Completed retry", task_type="plan", based_on=plan.id)
        self._complete(store, retry)

        downstream = store.add("Blocked downstream", task_type="implement", depends_on=plan.id)
        readiness = store.get_dependency_readiness(downstream)
        assert readiness.ready is False
        assert readiness.reason == "dropped"
        assert readiness.blocking_task_id == plan.id
        assert store.get_pending_pickup() == []

    def test_completed_empty_implement_dependency_blocks_by_default(self, tmp_path: Path):
        """Completed empty implement prerequisites stay blocked by default."""
        store = self._make_store(tmp_path)
        dep = store.add("Dep", task_type="implement")
        self._complete_implement_with_branch(store, dep, branch="feature/dep-empty-default")
        downstream = store.add("Downstream", task_type="implement", depends_on=dep.id)

        assert store.resolve_dependency_completion(downstream) is not None
        assert store.get_next_pending() is None
        assert store.get_pending_pickup() == []

        is_blocked, blocking_id, blocking_status = store.is_task_blocked(downstream)
        assert is_blocked is True
        assert blocking_id == dep.id
        assert blocking_status == "completed"

        assert store.count_blocked_tasks() == 1

    def test_completed_empty_implement_dependency_is_runnable_when_policy_enabled(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Toggling the empty-prereq policy must flip pickup and blocked semantics together."""
        store = self._make_store(tmp_path)
        dep = store.add("Dep", task_type="implement")
        self._complete_implement_with_branch(store, dep, branch="feature/dep-empty-toggle")
        downstream = store.add("Downstream", task_type="implement", depends_on=dep.id)

        monkeypatch.setattr(
            dependency_preconditions_module,
            "empty_prereq_satisfies_dependency",
            lambda _store, _prereq, _dependent: True,
        )

        next_task = store.get_next_pending()
        assert next_task is not None
        assert next_task.id == downstream.id
        assert [task.id for task in store.get_pending_pickup()] == [downstream.id]

        is_blocked, blocking_id, blocking_status = store.is_task_blocked(downstream)
        assert is_blocked is False
        assert blocking_id is None
        assert blocking_status is None

        assert store.count_blocked_tasks() == 0

    # --- count_blocked_tasks ---

    def test_count_blocked_excludes_unblocked_by_retry(self, tmp_path: Path):
        """count_blocked_tasks does not count tasks unblocked by a successful retry."""
        store = self._make_store(tmp_path)
        dep = store.add("Dep", task_type="plan")
        self._fail(store, dep)
        retry = store.add("Retry", task_type="plan", based_on=dep.id)
        self._complete(store, retry)
        _downstream = store.add("Downstream", depends_on=dep.id)

        count = store.count_blocked_tasks()
        assert count == 0

    def test_count_blocked_includes_tasks_with_failed_retry(self, tmp_path: Path):
        """count_blocked_tasks counts tasks whose dep's retry also failed."""
        store = self._make_store(tmp_path)
        dep = store.add("Dep")
        self._fail(store, dep)
        retry = store.add("Retry", based_on=dep.id)
        self._fail(store, retry)
        _downstream = store.add("Downstream", depends_on=dep.id)

        count = store.count_blocked_tasks()
        assert count == 1


class TestStepColumnsMigration:
    """Tests for step-metric columns migration (v14 -> v15)."""

    def test_migration_v14_to_v15_adds_step_columns_and_backfills(self, tmp_path: Path):
        """v14 databases should gain step columns and copy turn values into them."""
        import sqlite3
        from datetime import datetime

        db_path = tmp_path / "test.db"
        conn = sqlite3.connect(db_path)
        conn.execute("CREATE TABLE schema_version (version INTEGER PRIMARY KEY)")
        conn.execute("INSERT INTO schema_version (version) VALUES (14)")
        conn.execute(
            """
            CREATE TABLE tasks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                prompt TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending',
                task_type TEXT NOT NULL DEFAULT 'task',
                task_id TEXT,
                branch TEXT,
                log_file TEXT,
                report_file TEXT,
                based_on INTEGER REFERENCES tasks(id),
                has_commits INTEGER,
                duration_seconds REAL,
                num_turns INTEGER,
                num_turns_reported INTEGER,
                num_turns_computed INTEGER,
                cost_usd REAL,
                created_at TEXT NOT NULL,
                started_at TEXT,
                completed_at TEXT,
                "group" TEXT,
                depends_on INTEGER REFERENCES tasks(id),
                spec TEXT,
                create_review INTEGER DEFAULT 0,
                same_branch INTEGER DEFAULT 0,
                task_type_hint TEXT,
                output_content TEXT,
                session_id TEXT,
                pr_number INTEGER,
                model TEXT,
                provider TEXT,
                input_tokens INTEGER,
                output_tokens INTEGER,
                merge_status TEXT,
                failure_reason TEXT,
                skip_learnings INTEGER DEFAULT 0,
                diff_files_changed INTEGER,
                diff_lines_added INTEGER,
                diff_lines_removed INTEGER,
                review_cleared_at TEXT
            )
            """
        )
        now = datetime.now(UTC).isoformat()
        conn.execute(
            "INSERT INTO tasks (prompt, status, created_at, num_turns_reported, num_turns_computed) VALUES (?, ?, ?, ?, ?)",
            ("Legacy task", "completed", now, 4, 3),
        )
        conn.commit()
        conn.close()

        with pytest.raises(ManualMigrationRequired):
            SqliteTaskStore(db_path)
        _run_v25_v26_v27_migrations(db_path, "gza")
        store = SqliteTaskStore(db_path)

        conn = sqlite3.connect(db_path)
        cur = conn.execute("SELECT version FROM schema_version")
        version = cur.fetchone()[0]
        conn.close()
        assert version == SCHEMA_VERSION

        migrated = store.get("gza-1")
        assert migrated is not None
        assert migrated.num_steps_reported == 4
        assert migrated.num_steps_computed == 3


class TestRunStepPersistence:
    """Tests for run_steps/run_substeps schema and writer APIs."""

    def test_migration_v15_to_v16_adds_run_step_tables(self, tmp_path: Path):
        """v15 databases should be migrated to include run_steps/run_substeps tables."""
        import sqlite3

        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)
        del store

        conn = sqlite3.connect(db_path)
        conn.execute("DROP TABLE run_substeps")
        conn.execute("DROP TABLE run_steps")
        conn.execute("UPDATE schema_version SET version = 15")
        conn.commit()
        conn.close()

        # Auto-migrations v16+ re-add run_steps/run_substeps; v25 is manual
        with pytest.raises(ManualMigrationRequired):
            SqliteTaskStore(db_path)
        _run_v25_v26_v27_migrations(db_path, "gza")
        SqliteTaskStore(db_path)

        conn = sqlite3.connect(db_path)
        cur = conn.execute("SELECT version FROM schema_version")
        version = cur.fetchone()[0]
        assert version == SCHEMA_VERSION

        cur = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name IN ('run_steps', 'run_substeps')"
        )
        tables = {row[0] for row in cur.fetchall()}
        conn.close()
        assert tables == {"run_steps", "run_substeps"}

    def test_emit_step_emit_substep_finalize_step_persists_records(self, tmp_path: Path):
        """Writer APIs should persist ordered step/substep data with compatibility metadata."""
        store = SqliteTaskStore(tmp_path / "test.db")
        task = store.add("Task for step persistence")
        assert task.id is not None

        step_ref = store.emit_step(
            task.id,
            "I am running tests",
            provider="codex",
            legacy_turn_id="T1",
            legacy_event_id="T1.1",
        )
        first = store.emit_substep(
            step_ref,
            "tool_call",
            {"tool": "Bash", "command": "rg -n test"},
            source="provider",
            call_id="call-1",
            legacy_turn_id="T1",
            legacy_event_id="T1.2",
        )
        second = store.emit_substep(
            step_ref,
            "tool_output",
            {"exit_code": 0, "stdout": "ok"},
            source="tool",
            call_id="call-1",
            legacy_turn_id="T1",
            legacy_event_id="T1.3",
        )
        store.finalize_step(step_ref, "completed", "Tests finished")

        steps = store.get_run_steps(task.id)
        assert len(steps) == 1
        step = steps[0]
        assert step.step_index == 1
        assert step.step_id == "S1"
        assert step.provider == "codex"
        assert step.message_text == "I am running tests"
        assert step.outcome == "completed"
        assert step.summary == "Tests finished"
        assert step.completed_at is not None
        assert step.legacy_turn_id == "T1"
        assert step.legacy_event_id == "T1.1"

        substeps = store.get_run_substeps(step_ref)
        assert len(substeps) == 2
        assert first.substep_id == "S1.1"
        assert second.substep_id == "S1.2"
        assert substeps[0].substep_index == 1
        assert substeps[0].type == "tool_call"
        assert substeps[0].payload == {"tool": "Bash", "command": "rg -n test"}
        assert substeps[0].legacy_turn_id == "T1"
        assert substeps[0].legacy_event_id == "T1.2"
        assert substeps[1].substep_index == 2
        assert substeps[1].type == "tool_output"
        assert substeps[1].payload == {"exit_code": 0, "stdout": "ok"}
        assert substeps[1].legacy_turn_id == "T1"
        assert substeps[1].legacy_event_id == "T1.3"

    def test_step_and_substep_indices_are_scoped(self, tmp_path: Path):
        """Step indices should increment per run and substeps should increment per parent step."""
        store = SqliteTaskStore(tmp_path / "test.db")
        run_a = store.add("Run A")
        run_b = store.add("Run B")
        assert run_a.id is not None
        assert run_b.id is not None

        step_a1 = store.emit_step(run_a.id, "A1", provider="claude")
        step_a2 = store.emit_step(run_a.id, "A2", provider="claude")
        step_b1 = store.emit_step(run_b.id, "B1", provider="claude")

        sub_a1 = store.emit_substep(step_a1, "status_update", {"msg": "one"}, source="runner")
        sub_a2 = store.emit_substep(step_a1, "status_update", {"msg": "two"}, source="runner")
        sub_b1 = store.emit_substep(step_a2, "status_update", {"msg": "three"}, source="runner")

        assert step_a1.step_id == "S1"
        assert step_a2.step_id == "S2"
        assert step_b1.step_id == "S1"
        assert sub_a1.substep_id == "S1.1"
        assert sub_a2.substep_id == "S1.2"
        assert sub_b1.substep_id == "S2.1"

    def test_emit_substep_rejects_invalid_step_ref(self, tmp_path: Path):
        """emit_substep should fail for unknown step references."""
        store = SqliteTaskStore(tmp_path / "test.db")
        task = store.add("Task")
        assert task.id is not None

        invalid = StepRef(id=999, run_id=task.id, step_index=1, step_id="S1")
        with pytest.raises(ValueError, match="Unknown step reference"):
            store.emit_substep(invalid, "tool_call", {"tool": "Bash"}, source="provider")

    def test_emit_substep_rejects_tampered_step_ref(self, tmp_path: Path):
        """emit_substep should reject mismatched StepRef metadata."""
        store = SqliteTaskStore(tmp_path / "test.db")
        task = store.add("Task")
        assert task.id is not None
        step_ref = store.emit_step(task.id, "hello", provider="claude")

        tampered = StepRef(
            id=step_ref.id,
            run_id=step_ref.run_id,
            step_index=999,
            step_id=step_ref.step_id,
        )
        with pytest.raises(ValueError, match="Step reference index mismatch"):
            store.emit_substep(tampered, "tool_call", {"tool": "Bash"}, source="provider")

    def test_finalize_step_rejects_tampered_step_ref(self, tmp_path: Path):
        """finalize_step should reject mismatched StepRef metadata."""
        store = SqliteTaskStore(tmp_path / "test.db")
        task = store.add("Task")
        assert task.id is not None
        step_ref = store.emit_step(task.id, "hello", provider="claude")

        tampered = StepRef(
            id=step_ref.id,
            run_id=step_ref.run_id,
            step_index=step_ref.step_index,
            step_id="S999",
        )
        with pytest.raises(ValueError, match="Step reference label mismatch"):
            store.finalize_step(tampered, "completed")

    def test_get_run_substeps_rejects_tampered_step_ref(self, tmp_path: Path):
        """get_run_substeps should reject mismatched StepRef metadata."""
        store = SqliteTaskStore(tmp_path / "test.db")
        task = store.add("Task")
        assert task.id is not None
        step_ref = store.emit_step(task.id, "hello", provider="claude")
        store.emit_substep(step_ref, "tool_call", {"tool": "Bash"}, source="provider")

        tampered = StepRef(
            id=step_ref.id,
            run_id=step_ref.run_id,
            step_index=999,
            step_id=step_ref.step_id,
        )
        with pytest.raises(ValueError, match="Step reference index mismatch"):
            store.get_run_substeps(tampered)

    def test_count_steps_returns_correct_count_and_zero_for_empty(self, tmp_path: Path):
        """count_steps returns N for a task with N run_steps rows and 0 when none exist."""
        store = SqliteTaskStore(tmp_path / "test.db")
        task_a = store.add("Task with steps")
        task_b = store.add("Task without steps")
        assert task_a.id is not None
        assert task_b.id is not None

        # Emit 4 steps for task_a.
        for i in range(4):
            store.emit_step(task_a.id, f"Step {i + 1}", provider="claude")

        assert store.count_steps(task_a.id) == 4
        assert store.count_steps(task_b.id) == 0

    def test_migration_v15_to_v16_is_idempotent(self, tmp_path: Path):
        """Running v15->v16 migration twice should not duplicate indexes/tables."""
        import sqlite3

        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)
        del store

        conn = sqlite3.connect(db_path)
        conn.execute("DROP TABLE run_substeps")
        conn.execute("DROP TABLE run_steps")
        conn.execute("UPDATE schema_version SET version = 15")
        conn.commit()
        conn.close()

        # Auto-migrations v16+ re-add run_steps/run_substeps; v25 is manual
        with pytest.raises(ManualMigrationRequired):
            SqliteTaskStore(db_path)
        _run_v25_v26_v27_migrations(db_path, "gza")
        SqliteTaskStore(db_path)
        SqliteTaskStore(db_path)  # Second open should be idempotent

        conn = sqlite3.connect(db_path)
        cur = conn.execute(
            """
            SELECT name FROM sqlite_master
            WHERE type='index'
              AND name IN (
                'idx_run_steps_project_run_id',
                'idx_run_steps_project_step_index',
                'idx_run_substeps_project_run_id',
                'idx_run_substeps_project_step_id'
              )
            """
        )
        indexes = sorted(row[0] for row in cur.fetchall())
        conn.close()
        assert indexes == [
            "idx_run_steps_project_run_id",
            "idx_run_steps_project_step_index",
            "idx_run_substeps_project_run_id",
            "idx_run_substeps_project_step_id",
        ]

    def test_new_tasks_default_log_schema_version_1(self, tmp_path: Path):
        """New tasks should default to legacy log schema marker until step logs are persisted."""
        store = SqliteTaskStore(tmp_path / "test.db")
        task = store.add("Task")
        assert task.log_schema_version == 1

        reloaded = store.get(task.id)
        assert reloaded is not None
        assert reloaded.log_schema_version == 1

    def test_migration_v16_to_v17_adds_log_schema_version(self, tmp_path: Path):
        """v16 databases should gain log_schema_version with default value 1."""
        import sqlite3

        db_path = tmp_path / "test.db"
        conn = sqlite3.connect(db_path)
        conn.execute("CREATE TABLE schema_version (version INTEGER PRIMARY KEY)")
        conn.execute("INSERT INTO schema_version (version) VALUES (16)")
        conn.execute(
            """
            CREATE TABLE tasks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                prompt TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending',
                task_type TEXT NOT NULL DEFAULT 'task',
                task_id TEXT,
                branch TEXT,
                log_file TEXT,
                report_file TEXT,
                based_on INTEGER REFERENCES tasks(id),
                has_commits INTEGER,
                duration_seconds REAL,
                num_steps_reported INTEGER,
                num_steps_computed INTEGER,
                num_turns INTEGER,
                num_turns_reported INTEGER,
                num_turns_computed INTEGER,
                cost_usd REAL,
                created_at TEXT NOT NULL,
                started_at TEXT,
                completed_at TEXT,
                "group" TEXT,
                depends_on INTEGER REFERENCES tasks(id),
                spec TEXT,
                create_review INTEGER DEFAULT 0,
                same_branch INTEGER DEFAULT 0,
                task_type_hint TEXT,
                output_content TEXT,
                session_id TEXT,
                pr_number INTEGER,
                model TEXT,
                provider TEXT,
                input_tokens INTEGER,
                output_tokens INTEGER,
                merge_status TEXT,
                failure_reason TEXT,
                skip_learnings INTEGER DEFAULT 0,
                diff_files_changed INTEGER,
                diff_lines_added INTEGER,
                diff_lines_removed INTEGER,
                review_cleared_at TEXT
            )
            """
        )
        now = datetime.now(UTC).isoformat()
        conn.execute("INSERT INTO tasks (prompt, status, created_at) VALUES (?, ?, ?)", ("legacy", "pending", now))
        conn.commit()
        conn.close()

        with pytest.raises(ManualMigrationRequired):
            SqliteTaskStore(db_path)
        _run_v25_v26_v27_migrations(db_path, "gza")
        SqliteTaskStore(db_path)

        conn = sqlite3.connect(db_path)
        version = conn.execute("SELECT version FROM schema_version").fetchone()[0]
        value = conn.execute("SELECT log_schema_version FROM tasks WHERE id = 'gza-1'").fetchone()[0]
        conn.close()

        assert version == SCHEMA_VERSION
        assert value == 1

    def test_set_log_schema_version_updates_task(self, tmp_path: Path):
        """set_log_schema_version should persist explicit schema marker values."""
        store = SqliteTaskStore(tmp_path / "test.db")
        task = store.add("Task")
        assert task.id is not None

        store.set_log_schema_version(task.id, 2)
        updated = store.get(task.id)
        assert updated is not None
        assert updated.log_schema_version == 2

    def test_new_tasks_default_execution_mode_none(self, tmp_path: Path):
        """New tasks should default to no execution provenance until execution begins."""
        store = SqliteTaskStore(tmp_path / "test.db")
        task = store.add("Task")
        assert task.execution_mode is None

        reloaded = store.get(task.id)
        assert reloaded is not None
        assert reloaded.execution_mode is None

    def test_set_execution_mode_updates_task(self, tmp_path: Path):
        """set_execution_mode should persist recognized execution provenance values."""
        store = SqliteTaskStore(tmp_path / "test.db")
        task = store.add("Task")
        assert task.id is not None

        store.set_execution_mode(task.id, "skill_inline")
        updated = store.get(task.id)
        assert updated is not None
        assert updated.execution_mode == "skill_inline"


def test_get_impl_based_on_ids_returns_targeted_set(tmp_path: Path):
    """get_impl_based_on_ids returns source IDs referenced by implement tasks."""
    store = SqliteTaskStore(tmp_path / "test.db")

    plan1 = store.add("Plan 1", task_type="plan")
    plan2 = store.add("Plan 2", task_type="plan")
    plan3 = store.add("Plan 3", task_type="plan")
    assert plan1.id is not None and plan2.id is not None and plan3.id is not None

    # plan1 is implemented via based_on and plan2 via depends_on.
    store.add("Impl 1", task_type="implement", based_on=plan1.id)
    store.add("Impl 2", task_type="implement", depends_on=plan2.id)
    # A review task with based_on should NOT be included
    store.add("Review of plan2", task_type="review", based_on=plan2.id)
    # A plain task with no based_on
    store.add("Task no based_on", task_type="implement")

    result = store.get_impl_based_on_ids()

    assert result == {plan1.id, plan2.id}

def test_get_impl_based_on_ids_empty_db(tmp_path: Path):
    """get_impl_based_on_ids returns empty set when no implement tasks exist."""
    store = SqliteTaskStore(tmp_path / "test.db")
    assert store.get_impl_based_on_ids() == set()


class TestComputePercentiles:
    """Tests for _compute_percentiles helper."""

    def test_empty_list_returns_none(self):
        from gza.db import _compute_percentiles
        assert _compute_percentiles([]) is None

    def test_single_value(self):
        from gza.db import _compute_percentiles
        result = _compute_percentiles([5.0])
        assert result is not None
        assert result["min"] == 5.0
        assert result["max"] == 5.0
        assert result["avg"] == 5.0
        assert result["median"] == 5.0
        assert result["count"] == 1

    def test_odd_count_median(self):
        from gza.db import _compute_percentiles
        result = _compute_percentiles([1.0, 3.0, 5.0])
        assert result is not None
        assert result["median"] == 3.0

    def test_even_count_median(self):
        from gza.db import _compute_percentiles
        result = _compute_percentiles([1.0, 2.0, 3.0, 4.0])
        assert result is not None
        assert result["median"] == 2.5

    def test_p90_with_large_sample(self):
        from gza.db import _compute_percentiles
        values = list(range(1, 11))  # 1..10
        result = _compute_percentiles([float(v) for v in values])
        assert result is not None
        # p90_idx = max(0, int(0.9 * 10) - 1) = 8, sorted[8] = 9
        assert result["p90"] == 9.0


class TestGetHistorySinceParam:
    """Tests for the since parameter of SqliteTaskStore.get_history()."""

    def test_since_excludes_old_tasks(self, tmp_path: Path):
        store = SqliteTaskStore(tmp_path / "test.db")
        now = datetime.now(UTC)

        # Old task (10 days ago)
        old = store.add("old task")
        old.status = "completed"
        old.completed_at = now - timedelta(days=10)
        store.update(old)

        # Recent task (1 day ago)
        recent = store.add("recent task")
        recent.status = "completed"
        recent.completed_at = now - timedelta(days=1)
        store.update(recent)

        cutoff = now - timedelta(days=5)
        results = store.get_history(limit=None, since=cutoff)
        prompts = [t.prompt for t in results]
        assert "recent task" in prompts
        assert "old task" not in prompts

    def test_since_includes_tasks_exactly_at_cutoff(self, tmp_path: Path):
        store = SqliteTaskStore(tmp_path / "test.db")
        now = datetime.now(UTC)
        cutoff = now - timedelta(days=5)

        task = store.add("boundary task")
        task.status = "completed"
        # Set completed_at to exactly the cutoff time
        task.completed_at = cutoff
        store.update(task)

        results = store.get_history(limit=None, since=cutoff)
        prompts = [t.prompt for t in results]
        assert "boundary task" in prompts

    def test_since_none_returns_all(self, tmp_path: Path):
        store = SqliteTaskStore(tmp_path / "test.db")
        now = datetime.now(UTC)

        for i in range(3):
            t = store.add(f"task {i}")
            t.status = "completed"
            t.completed_at = now - timedelta(days=i * 10)
            store.update(t)

        results = store.get_history(limit=None, since=None)
        assert len(results) == 3


class TestGetHistoryInternalFiltering:
    """Tests for default internal-task filtering in get_history()."""

    def test_excludes_internal_tasks_by_default(self, tmp_path: Path):
        store = SqliteTaskStore(tmp_path / "test.db")
        now = datetime.now(UTC)

        impl = store.add("Implement task", task_type="implement")
        impl.status = "completed"
        impl.completed_at = now
        store.update(impl)

        internal = store.add("Internal task", task_type="internal")
        internal.status = "completed"
        internal.completed_at = now
        store.update(internal)

        results = store.get_history(limit=None)
        prompts = [t.prompt for t in results]
        assert "Implement task" in prompts
        assert "Internal task" not in prompts

    def test_includes_internal_tasks_when_task_type_requested(self, tmp_path: Path):
        store = SqliteTaskStore(tmp_path / "test.db")
        now = datetime.now(UTC)

        internal = store.add("Internal task", task_type="internal")
        internal.status = "completed"
        internal.completed_at = now
        store.update(internal)

        results = store.get_history(limit=None, task_type="internal")
        assert len(results) == 1
        assert results[0].task_type == "internal"
        assert results[0].prompt == "Internal task"


class TestGetHistoryUnmergedStatus:
    """Tests for get_history(status='unmerged') matching both current and legacy data."""

    def test_unmerged_status_matches_merge_status_and_legacy_status(self, tmp_path: Path):
        store = SqliteTaskStore(tmp_path / "test.db")
        now = datetime.now(UTC)

        # Task with current merge_status='unmerged'
        t1 = store.add("Current unmerged", task_type="implement")
        t1.status = "completed"
        t1.completed_at = now
        t1.merge_status = "unmerged"
        t1.has_commits = True
        store.update(t1)

        # Simulate legacy task with status='unmerged' (no merge_status field)
        t2 = store.add("Legacy unmerged", task_type="implement")
        with store._connect() as conn:
            conn.execute(
                "UPDATE tasks SET status='unmerged', merge_status=NULL WHERE id=?",
                (t2.id,),
            )

        results = store.get_history(limit=None, status="unmerged")
        result_ids = {t.id for t in results}
        assert t1.id in result_ids, "Should match task with merge_status='unmerged'"
        assert t2.id in result_ids, "Should match legacy task with status='unmerged'"

    def test_unmerged_status_excludes_merged_tasks(self, tmp_path: Path):
        store = SqliteTaskStore(tmp_path / "test.db")
        now = datetime.now(UTC)

        merged = store.add("Merged task", task_type="implement")
        merged.status = "completed"
        merged.completed_at = now
        merged.merge_status = "merged"
        store.update(merged)

        results = store.get_history(limit=None, status="unmerged")
        assert all(t.id != merged.id for t in results)


class TestSearchByPrompt:
    """Tests for SqliteTaskStore.search()."""

    def test_search_matches_prompt_substring_across_statuses(self, tmp_path: Path):
        store = SqliteTaskStore(tmp_path / "test.db")
        now = datetime.now(UTC)

        pending = store.add("alpha pending task")

        failed_internal = store.add("Alpha internal task", task_type="internal")
        failed_internal.status = "failed"
        failed_internal.completed_at = now
        store.update(failed_internal)

        other = store.add("beta unrelated task")

        completed = store.add("alpha completed task")
        completed.status = "completed"
        completed.completed_at = now
        store.update(completed)

        in_progress = store.add("alpha in progress task")
        store.mark_in_progress(in_progress)

        results = store.search("alpha")
        assert [t.id for t in results] == [
            in_progress.id,
            completed.id,
            failed_internal.id,
            pending.id,
        ]
        assert all(t.id != other.id for t in results)

        statuses = {t.status for t in results}
        assert {"pending", "in_progress", "completed", "failed"} <= statuses
        assert any(t.task_type == "internal" for t in results)

    def test_search_is_case_insensitive_for_ascii_and_returns_empty_on_no_match(self, tmp_path: Path):
        store = SqliteTaskStore(tmp_path / "test.db")
        store.add("alpha one")
        store.add("Alpha two")

        assert len(store.search("ALPHA")) == 2
        assert store.search("nope") == []


class TestGetBasedOnChildren:
    """Tests for SqliteTaskStore.get_based_on_children()."""

    def test_returns_direct_children(self, tmp_path: Path):
        store = SqliteTaskStore(tmp_path / "test.db")
        parent = store.add("parent task")
        child1 = store.add("child 1", based_on=parent.id)
        child2 = store.add("child 2", based_on=parent.id)

        children = store.get_based_on_children(parent.id)
        child_ids = {c.id for c in children}
        assert child1.id in child_ids
        assert child2.id in child_ids

    def test_returns_empty_when_no_children(self, tmp_path: Path):
        store = SqliteTaskStore(tmp_path / "test.db")
        task = store.add("standalone task")

        children = store.get_based_on_children(task.id)
        assert children == []

    def test_does_not_return_grandchildren(self, tmp_path: Path):
        """get_based_on_children returns only direct children, not transitive."""
        store = SqliteTaskStore(tmp_path / "test.db")
        grandparent = store.add("grandparent")
        parent = store.add("parent", based_on=grandparent.id)
        store.add("child", based_on=parent.id)

        children = store.get_based_on_children(grandparent.id)
        assert len(children) == 1
        assert children[0].id == parent.id

    def test_ordered_by_id_ascending(self, tmp_path: Path):
        store = SqliteTaskStore(tmp_path / "test.db")
        parent = store.add("parent")
        c1 = store.add("first child", based_on=parent.id)
        c2 = store.add("second child", based_on=parent.id)
        c3 = store.add("third child", based_on=parent.id)

        children = store.get_based_on_children(parent.id)
        ids = [c.id for c in children]
        assert ids == [c1.id, c2.id, c3.id]

    def test_chronological_order_across_decimal_width_boundary(self, tmp_path: Path):
        """Children spanning seq=9→10 remain in numeric creation order."""
        store = SqliteTaskStore(tmp_path / "test.db")

        # Advance the sequence counter so parent lands at seq=9
        for _ in range(8):
            store.add("filler")

        parent = store.add("parent")  # seq=9
        assert parent.id is not None
        prefix = parent.id.rsplit("-", 1)[0]
        assert parent.id == f"{prefix}-9", f"expected seq-9 parent, got {parent.id}"

        # Create children: seq 10..36.
        children = [store.add(f"child {i}", based_on=parent.id) for i in range(27)]

        assert children[0].id == f"{prefix}-10", f"expected {prefix}-10, got {children[0].id}"
        assert children[26].id == f"{prefix}-36", f"expected {prefix}-36, got {children[26].id}"

        result = store.get_based_on_children(parent.id)
        assert [t.id for t in result] == [t.id for t in children], (
            "get_based_on_children returned wrong order across decimal width boundary"
        )

    def test_identical_timestamps_do_not_invert_via_id_lexicographic_sort(self, tmp_path: Path):
        """With identical created_at, ordering must not invert at seq=9→10."""
        store = SqliteTaskStore(tmp_path / "test.db")

        # Advance sequence so parent is seq=8 and children are seq=9,10.
        for _ in range(7):
            store.add("filler")

        parent = store.add("parent")  # seq=8
        assert parent.id is not None
        prefix = parent.id.rsplit("-", 1)[0]

        earlier = store.add("earlier child", based_on=parent.id)  # seq=9
        later = store.add("later child", based_on=parent.id)      # seq=10

        assert earlier.id == f"{prefix}-9", f"expected {prefix}-9, got {earlier.id}"
        assert later.id == f"{prefix}-10", f"expected {prefix}-10, got {later.id}"

        # Force identical created_at on both children — this triggers the tie-breaker scenario
        shared_ts = earlier.created_at
        later.created_at = shared_ts
        store.update(later)

        result = store.get_based_on_children(parent.id)
        result_ids = [t.id for t in result]
        # Insertion order: earlier ({prefix}-9) before later ({prefix}-10)
        assert result_ids == [earlier.id, later.id], (
            f"expected [{earlier.id}, {later.id}] but got {result_ids} — "
            "id ASC lexicographic tie-break may have incorrectly sorted the results"
        )


class TestGetHistoryOrderByBase36Boundary:
    """Regression tests for get_history ORDER BY correctness with TEXT IDs."""

    def test_history_orders_by_created_at_when_completed_at_tied(self, tmp_path: Path):
        """get_history uses created_at DESC as tie-breaker when completed_at values match.

        ``{prefix}-10`` must sort after ``{prefix}-9`` in descending order because
        it was created later, even though plain string ordering would place ``9``
        ahead of ``10``.
        """
        store = SqliteTaskStore(tmp_path / "test.db")

        # Advance sequence to 8 so next two tasks land at seq 9 and 10.
        for _ in range(8):
            store.add("filler")

        task_9 = store.add("task at boundary 9")
        task_10 = store.add("task at boundary 10")

        assert task_9.id is not None and task_10.id is not None
        prefix = task_9.id.rsplit("-", 1)[0]
        assert task_9.id == f"{prefix}-9", f"expected {prefix}-9, got {task_9.id}"
        assert task_10.id == f"{prefix}-10", f"expected {prefix}-10, got {task_10.id}"

        # Complete both with the SAME completed_at so created_at is the tie-breaker
        same_time = datetime(2026, 1, 1, 12, 0, 0, tzinfo=UTC)
        task_9.completed_at = same_time
        task_9.status = "completed"
        store.update(task_9)
        task_10.completed_at = same_time
        task_10.status = "completed"
        store.update(task_10)

        history = store.get_history(limit=None)
        our_tasks = [t for t in history if t.id in {task_9.id, task_10.id}]

        assert len(our_tasks) == 2
        # task_10 was created after task_9, so with DESC it should appear first.
        assert our_tasks[0].id == task_10.id, (
            f"Expected {task_10.id} (created later) first in DESC order, "
            f"got {our_tasks[0].id}. Buggy ORDER BY id DESC can place '9' before '10'."
        )
        assert our_tasks[1].id == task_9.id


class TestGetLineageChildren:
    """Tests for SqliteTaskStore.get_lineage_children()."""

    def test_returns_children_from_both_relationship_columns(self, tmp_path: Path):
        store = SqliteTaskStore(tmp_path / "test.db")
        root = store.add("root", task_type="implement")
        based_child = store.add("based child", task_type="implement", based_on=root.id)
        depends_child = store.add("depends child", task_type="review", depends_on=root.id)

        children = store.get_lineage_children(root.id)
        child_ids = {child.id for child in children}
        assert based_child.id in child_ids
        assert depends_child.id in child_ids

    def test_returns_empty_when_no_lineage_children(self, tmp_path: Path):
        store = SqliteTaskStore(tmp_path / "test.db")
        task = store.add("standalone")
        assert store.get_lineage_children(task.id) == []


class TestMigrationV19ToV20:
    """Tests for database migration v19 → v20 (task → implement default type)."""

    def test_migration_converts_task_type_to_implement(self, tmp_path: Path):
        """Migration v19->v20 updates existing rows with task_type='task' to 'implement'."""
        import sqlite3

        from gza.db import SCHEMA_VERSION

        db_path = tmp_path / "test.db"

        # Manually create a v19 database with a 'task' type row
        conn = sqlite3.connect(db_path)
        conn.execute("CREATE TABLE schema_version (version INTEGER PRIMARY KEY)")
        conn.execute("INSERT INTO schema_version (version) VALUES (19)")
        conn.execute("""
            CREATE TABLE tasks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                prompt TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending',
                task_type TEXT NOT NULL DEFAULT 'task',
                task_id TEXT,
                branch TEXT,
                log_file TEXT,
                report_file TEXT,
                based_on INTEGER,
                has_commits INTEGER,
                duration_seconds REAL,
                num_steps_reported INTEGER,
                num_steps_computed INTEGER,
                num_turns_reported INTEGER,
                num_turns_computed INTEGER,
                cost_usd REAL,
                input_tokens INTEGER,
                output_tokens INTEGER,
                created_at TEXT,
                started_at TEXT,
                completed_at TEXT,
                "group" TEXT,
                depends_on INTEGER,
                spec TEXT,
                create_review INTEGER NOT NULL DEFAULT 0,
                same_branch INTEGER NOT NULL DEFAULT 0,
                task_type_hint TEXT,
                output_content TEXT,
                session_id TEXT,
                pr_number INTEGER,
                model TEXT,
                provider TEXT,
                merge_status TEXT,
                failure_reason TEXT,
                skip_learnings INTEGER NOT NULL DEFAULT 0,
                diff_files_changed INTEGER,
                diff_lines_added INTEGER,
                diff_lines_removed INTEGER,
                review_cleared_at TEXT,
                log_schema_version INTEGER NOT NULL DEFAULT 1,
                cycle_id INTEGER,
                cycle_iteration_index INTEGER,
                cycle_role TEXT
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS task_cycles (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                implementation_task_id INTEGER NOT NULL,
                status TEXT NOT NULL,
                max_iterations INTEGER NOT NULL DEFAULT 3,
                started_at TEXT NOT NULL,
                ended_at TEXT,
                stop_reason TEXT
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS task_cycle_iterations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                cycle_id INTEGER NOT NULL,
                iteration_index INTEGER NOT NULL,
                review_task_id INTEGER,
                review_verdict TEXT,
                improve_task_id INTEGER,
                state TEXT NOT NULL,
                started_at TEXT NOT NULL,
                ended_at TEXT
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS run_steps (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id TEXT NOT NULL,
                step_type TEXT NOT NULL,
                payload TEXT,
                timestamp TEXT NOT NULL,
                legacy_turn_id INTEGER,
                legacy_event_id INTEGER
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_tasks_type_based_on ON tasks(task_type, based_on)")
        conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS uq_task_cycle_iterations_cycle_iter ON task_cycle_iterations(cycle_id, iteration_index)")
        conn.execute(
            "INSERT INTO tasks (prompt, task_type, created_at) VALUES (?, ?, ?)",
            ("Old task", "task", "2024-01-01T00:00:00+00:00")
        )
        conn.commit()
        conn.close()

        # Open the store — auto-migrations, then manual v25
        with pytest.raises(ManualMigrationRequired):
            SqliteTaskStore(db_path)
        _run_v25_v26_v27_migrations(db_path, "gza")
        store = SqliteTaskStore(db_path)
        tasks = store.get_all()

        assert len(tasks) == 1
        assert tasks[0].task_type == "implement"

        # Verify schema version was bumped
        conn2 = sqlite3.connect(db_path)
        row = conn2.execute("SELECT version FROM schema_version").fetchone()
        conn2.close()
        assert row[0] == SCHEMA_VERSION


class TestMigrationV21ToV22:
    """Tests for database migration v21 → v22 (learn → internal)."""

    def test_migration_converts_learn_task_type_to_internal(self, tmp_path: Path):
        """Migration v21->v22 updates existing rows with task_type='learn' to 'internal'."""
        import sqlite3

        from gza.db import SCHEMA_VERSION

        db_path = tmp_path / "test.db"

        conn = sqlite3.connect(db_path)
        conn.execute("CREATE TABLE schema_version (version INTEGER PRIMARY KEY)")
        conn.execute("INSERT INTO schema_version (version) VALUES (21)")
        conn.execute("""
            CREATE TABLE tasks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                prompt TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending',
                task_type TEXT NOT NULL DEFAULT 'implement',
                task_id TEXT,
                branch TEXT,
                log_file TEXT,
                report_file TEXT,
                based_on INTEGER,
                has_commits INTEGER,
                duration_seconds REAL,
                num_steps_reported INTEGER,
                num_steps_computed INTEGER,
                num_turns_reported INTEGER,
                num_turns_computed INTEGER,
                cost_usd REAL,
                input_tokens INTEGER,
                output_tokens INTEGER,
                created_at TEXT,
                started_at TEXT,
                completed_at TEXT,
                "group" TEXT,
                depends_on INTEGER,
                spec TEXT,
                create_review INTEGER NOT NULL DEFAULT 0,
                same_branch INTEGER NOT NULL DEFAULT 0,
                task_type_hint TEXT,
                output_content TEXT,
                session_id TEXT,
                pr_number INTEGER,
                model TEXT,
                provider TEXT,
                provider_is_explicit INTEGER NOT NULL DEFAULT 0,
                merge_status TEXT,
                failure_reason TEXT,
                skip_learnings INTEGER NOT NULL DEFAULT 0,
                diff_files_changed INTEGER,
                diff_lines_added INTEGER,
                diff_lines_removed INTEGER,
                review_cleared_at TEXT,
                log_schema_version INTEGER NOT NULL DEFAULT 1,
                cycle_id INTEGER,
                cycle_iteration_index INTEGER,
                cycle_role TEXT
            )
        """)
        conn.execute(
            "INSERT INTO tasks (prompt, task_type, created_at) VALUES (?, ?, ?)",
            ("Learn prompt", "learn", "2024-01-01T00:00:00+00:00"),
        )
        conn.commit()
        conn.close()

        with pytest.raises(ManualMigrationRequired):
            SqliteTaskStore(db_path)
        _run_v25_v26_v27_migrations(db_path, "gza")
        store = SqliteTaskStore(db_path)
        tasks = store.get_all()

        assert len(tasks) == 1
        assert tasks[0].task_type == "internal"

        conn2 = sqlite3.connect(db_path)
        row = conn2.execute("SELECT version FROM schema_version").fetchone()
        conn2.close()
        assert row[0] == SCHEMA_VERSION


class TestResolveTaskId:
    """Unit tests for strict full-ID task resolution."""

    PREFIX = "gza"

    @pytest.mark.parametrize(
        "arg, expected",
        [
            # Full prefixed IDs are returned as-is.
            ("gza-1", "gza-1"),
            ("gza-000001", "gza-000001"),
            ("gza-10", "gza-10"),
            # Whitespace padding — stripped before processing
            (" gza-10 ", "gza-10"),
        ],
    )
    def test_resolve(self, arg: str, expected: str) -> None:
        assert resolve_task_id(arg, self.PREFIX) == expected

    def test_empty_string_raises_value_error(self) -> None:
        with pytest.raises(ValueError, match="Invalid task ID"):
            resolve_task_id("", self.PREFIX)

    def test_whitespace_only_raises_value_error(self) -> None:
        with pytest.raises(ValueError, match="Invalid task ID"):
            resolve_task_id("   ", self.PREFIX)

    def test_error_message_uses_decimal_example(self) -> None:
        with pytest.raises(ValueError) as exc:
            resolve_task_id("gza-abc", self.PREFIX)
        assert "gza-1234" in str(exc.value)

    @pytest.mark.parametrize(
        "arg",
        [
            "3f",
            "42",
            "0",
            "abc",
            "feature/add-branch",
            "gza-",
            "gza-3f",
            "gza-z",
            "-000001",
            "GZA-000001",
            "gza_000001",
        ],
    )
    def test_rejects_non_full_or_invalid_ids(self, arg: str) -> None:
        with pytest.raises(ValueError, match="Use a full prefixed task ID"):
            resolve_task_id(arg, self.PREFIX)

    def test_uses_project_prefix(self) -> None:
        assert resolve_task_id("myapp-000001", "myapp") == "myapp-000001"
        assert resolve_task_id("other-000001", "myapp") == "other-000001"

def _make_v24_db(db_path: "Path") -> None:
    """Create a minimal v24 database suitable for v25 migration tests.

    Uses the standard pattern: create a v21-era schema manually, then open
    SqliteTaskStore to trigger auto-migrations up to v24 (which raises
    ManualMigrationRequired — the expected gate before the manual v25 step).
    """
    import sqlite3

    conn = sqlite3.connect(db_path)
    conn.execute("CREATE TABLE schema_version (version INTEGER PRIMARY KEY)")
    conn.execute("INSERT INTO schema_version (version) VALUES (21)")
    conn.execute("""
        CREATE TABLE tasks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            prompt TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending',
            task_type TEXT NOT NULL DEFAULT 'implement',
            task_id TEXT,
            branch TEXT,
            log_file TEXT,
            report_file TEXT,
            based_on INTEGER,
            has_commits INTEGER,
            duration_seconds REAL,
            num_steps_reported INTEGER,
            num_steps_computed INTEGER,
            num_turns_reported INTEGER,
            num_turns_computed INTEGER,
            cost_usd REAL,
            input_tokens INTEGER,
            output_tokens INTEGER,
            created_at TEXT,
            started_at TEXT,
            completed_at TEXT,
            "group" TEXT,
            depends_on INTEGER,
            spec TEXT,
            create_review INTEGER NOT NULL DEFAULT 0,
            same_branch INTEGER NOT NULL DEFAULT 0,
            task_type_hint TEXT,
            output_content TEXT,
            session_id TEXT,
            pr_number INTEGER,
            model TEXT,
            provider TEXT,
            provider_is_explicit INTEGER NOT NULL DEFAULT 0,
            merge_status TEXT,
            failure_reason TEXT,
            skip_learnings INTEGER NOT NULL DEFAULT 0,
            diff_files_changed INTEGER,
            diff_lines_added INTEGER,
            diff_lines_removed INTEGER,
            review_cleared_at TEXT,
            log_schema_version INTEGER NOT NULL DEFAULT 1,
            cycle_id INTEGER,
            cycle_iteration_index INTEGER,
            cycle_role TEXT
        )
    """)
    conn.commit()
    conn.close()
    # Auto-migrate to v24 (raises ManualMigrationRequired — expected)
    import pytest
    with pytest.raises(ManualMigrationRequired):
        SqliteTaskStore(db_path)


def _run_v25_v26_v27_migrations(db_path: Path, prefix: str = "gza") -> None:
    """Run the manual migration chain required for legacy DB fixtures."""
    run_v25_migration(db_path, prefix)
    run_v26_migration(db_path)
    run_v27_migration(db_path)


def _make_v29_db_without_urgent_bumped_at(db_path: Path) -> None:
    """Create a minimal v29 DB where tasks.urgent exists but tasks.urgent_bumped_at does not."""
    import sqlite3

    conn = sqlite3.connect(db_path)
    conn.execute("CREATE TABLE schema_version (version INTEGER PRIMARY KEY)")
    conn.execute("INSERT INTO schema_version (version) VALUES (29)")
    conn.execute(
        """
        CREATE TABLE tasks (
            id TEXT PRIMARY KEY,
            prompt TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending',
            task_type TEXT NOT NULL DEFAULT 'implement',
            created_at TEXT NOT NULL,
            urgent INTEGER DEFAULT 0
        )
        """
    )
    conn.commit()
    conn.close()


def _make_v35_db_with_legacy_key_shapes(db_path: Path) -> None:
    """Create a v35 DB with legacy non-project-scoped keys/fks/uniques."""
    conn = sqlite3.connect(db_path)
    conn.execute("CREATE TABLE schema_version (version INTEGER PRIMARY KEY)")
    conn.execute("INSERT INTO schema_version (version) VALUES (35)")
    conn.execute(
        """
        CREATE TABLE project_sequences (
            prefix TEXT PRIMARY KEY,
            next_seq INTEGER NOT NULL DEFAULT 1
        )
        """
    )
    conn.execute("INSERT INTO project_sequences(prefix, next_seq) VALUES ('gza', 2)")
    conn.execute(
        """
        CREATE TABLE tasks (
            id TEXT PRIMARY KEY,
            prompt TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending',
            task_type TEXT NOT NULL DEFAULT 'implement',
            task_id TEXT,
            branch TEXT,
            log_file TEXT,
            report_file TEXT,
            based_on TEXT,
            has_commits INTEGER,
            duration_seconds REAL,
            num_steps_reported INTEGER,
            num_steps_computed INTEGER,
            num_turns INTEGER,
            num_turns_reported INTEGER,
            num_turns_computed INTEGER,
            attach_count INTEGER,
            attach_duration_seconds REAL,
            cost_usd REAL,
            created_at TEXT NOT NULL,
            started_at TEXT,
            running_pid INTEGER,
            completed_at TEXT,
            "group" TEXT,
            depends_on TEXT,
            spec TEXT,
            create_review INTEGER DEFAULT 0,
            same_branch INTEGER DEFAULT 0,
            task_type_hint TEXT,
            output_content TEXT,
            session_id TEXT,
            pr_number INTEGER,
            model TEXT,
            provider TEXT,
            provider_is_explicit INTEGER DEFAULT 0,
            urgent INTEGER DEFAULT 0,
            urgent_bumped_at TEXT,
            queue_position INTEGER,
            input_tokens INTEGER,
            output_tokens INTEGER,
            merge_status TEXT,
            merged_at TEXT,
            failure_reason TEXT,
            skip_learnings INTEGER DEFAULT 0,
            diff_files_changed INTEGER,
            diff_lines_added INTEGER,
            diff_lines_removed INTEGER,
            review_cleared_at TEXT,
            review_score INTEGER,
            log_schema_version INTEGER DEFAULT 1,
            execution_mode TEXT,
            base_branch TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE task_tags (
            task_id TEXT NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
            tag TEXT NOT NULL,
            PRIMARY KEY(task_id, tag)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE run_steps (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id TEXT NOT NULL,
            step_index INTEGER NOT NULL,
            step_id TEXT NOT NULL,
            provider TEXT NOT NULL,
            message_role TEXT NOT NULL,
            message_text TEXT,
            started_at TEXT NOT NULL,
            completed_at TEXT,
            outcome TEXT,
            summary TEXT,
            legacy_turn_id TEXT,
            legacy_event_id TEXT,
            UNIQUE(run_id, step_index),
            UNIQUE(run_id, step_id),
            FOREIGN KEY(run_id) REFERENCES tasks(id) ON DELETE CASCADE
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE run_substeps (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id TEXT NOT NULL,
            step_id INTEGER NOT NULL REFERENCES run_steps(id) ON DELETE CASCADE,
            substep_index INTEGER NOT NULL,
            substep_id TEXT NOT NULL,
            type TEXT NOT NULL,
            source TEXT NOT NULL,
            call_id TEXT,
            payload_json TEXT NOT NULL,
            timestamp TEXT NOT NULL,
            legacy_turn_id TEXT,
            legacy_event_id TEXT,
            UNIQUE(step_id, substep_index),
            UNIQUE(step_id, substep_id),
            FOREIGN KEY(run_id) REFERENCES tasks(id) ON DELETE CASCADE
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE task_comments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            task_id TEXT NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
            content TEXT NOT NULL,
            source TEXT NOT NULL,
            author TEXT,
            created_at TEXT NOT NULL,
            resolved_at TEXT
        )
        """
    )
    conn.execute(
        """
        INSERT INTO tasks (id, prompt, status, task_type, task_id, created_at)
        VALUES ('gza-1', 'legacy task', 'pending', 'implement', '20260427-gza-legacy-task', '2024-01-01T00:00:00+00:00')
        """
    )
    conn.execute("INSERT INTO task_tags(task_id, tag) VALUES ('gza-1', 'legacy')")
    conn.execute(
        """
        INSERT INTO run_steps (run_id, step_index, step_id, provider, message_role, message_text, started_at)
        VALUES ('gza-1', 1, 'S1', 'codex', 'assistant', 'legacy step', '2024-01-01T00:00:00+00:00')
        """
    )
    conn.execute(
        """
        INSERT INTO run_substeps (run_id, step_id, substep_index, substep_id, type, source, payload_json, timestamp)
        VALUES ('gza-1', 1, 1, 'S1.1', 'tool_call', 'assistant', '{}', '2024-01-01T00:00:00+00:00')
        """
    )
    conn.execute(
        """
        INSERT INTO task_comments(task_id, content, source, created_at)
        VALUES ('gza-1', 'legacy comment', 'direct', '2024-01-01T00:00:00+00:00')
        """
    )
    conn.commit()
    conn.close()


def _drop_tasks_column(db_path: Path, column_name: str) -> None:
    """Rebuild the tasks table without a specific column."""
    import sqlite3

    def _quote(column: str) -> str:
        return f'"{column}"' if column in ("group",) else column

    conn = sqlite3.connect(db_path)
    conn.execute("ALTER TABLE tasks RENAME TO tasks_old")
    cols = [row[1] for row in conn.execute("PRAGMA table_info(tasks_old)")]
    kept_cols = [c for c in cols if c != column_name]
    cols_str = ", ".join(_quote(c) for c in kept_cols)
    col_defs = []
    pragma_rows = list(conn.execute("PRAGMA table_info(tasks_old)"))
    pk_cols = [(row[5], row[1]) for row in pragma_rows if row[1] != column_name and row[5]]
    has_composite_pk = len(pk_cols) > 1
    for row in pragma_rows:
        if row[1] == column_name:
            continue
        name, typ, notnull, dflt, pk = row[1], row[2], row[3], row[4], row[5]
        quoted_name = f'"{name}"' if name in ("group",) else name
        parts = [quoted_name, typ]
        if pk and not has_composite_pk:
            parts.append("PRIMARY KEY")
        if notnull and not pk:
            parts.append("NOT NULL")
        if dflt is not None:
            parts.append(f"DEFAULT {dflt}")
        col_defs.append(" ".join(parts))
    if has_composite_pk:
        ordered_pk = ", ".join(_quote(name) for _, name in sorted(pk_cols, key=lambda item: item[0]))
        col_defs.append(f"PRIMARY KEY({ordered_pk})")
    conn.execute(f"CREATE TABLE tasks ({', '.join(col_defs)})")
    conn.execute(f"INSERT INTO tasks ({cols_str}) SELECT {cols_str} FROM tasks_old")
    conn.execute("DROP TABLE tasks_old")
    conn.commit()
    conn.close()


def _drop_task_comments_column(db_path: Path, column_name: str) -> None:
    """Rebuild task_comments table without a specific column."""
    import sqlite3

    conn = sqlite3.connect(db_path)
    conn.execute("ALTER TABLE task_comments RENAME TO task_comments_old")
    cols = [row[1] for row in conn.execute("PRAGMA table_info(task_comments_old)")]
    kept_cols = [c for c in cols if c != column_name]
    cols_str = ", ".join(kept_cols)
    col_defs = []
    for row in conn.execute("PRAGMA table_info(task_comments_old)"):
        if row[1] == column_name:
            continue
        name, typ, notnull, dflt, pk = row[1], row[2], row[3], row[4], row[5]
        parts = [name, typ]
        if pk:
            parts.append("PRIMARY KEY")
        if notnull and not pk:
            parts.append("NOT NULL")
        if dflt is not None:
            parts.append(f"DEFAULT {dflt}")
        col_defs.append(" ".join(parts))
    conn.execute(f"CREATE TABLE task_comments ({', '.join(col_defs)})")
    conn.execute(f"INSERT INTO task_comments ({cols_str}) SELECT {cols_str} FROM task_comments_old")
    conn.execute("DROP TABLE task_comments_old")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_task_comments_task_created ON task_comments(task_id, created_at ASC)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_task_comments_task_unresolved ON task_comments(task_id, resolved_at)")
    conn.commit()
    conn.close()


def _drop_merge_units_column(db_path: Path, column_name: str) -> None:
    """Rebuild merge_units without a specific column."""
    import sqlite3

    conn = sqlite3.connect(db_path)
    conn.execute("ALTER TABLE merge_units RENAME TO merge_units_old")
    cols = [row[1] for row in conn.execute("PRAGMA table_info(merge_units_old)")]
    kept_cols = [c for c in cols if c != column_name]
    cols_str = ", ".join(kept_cols)
    col_defs = []
    pragma_rows = list(conn.execute("PRAGMA table_info(merge_units_old)"))
    pk_cols = [(row[5], row[1]) for row in pragma_rows if row[1] != column_name and row[5]]
    has_composite_pk = len(pk_cols) > 1
    for row in pragma_rows:
        if row[1] == column_name:
            continue
        name, typ, notnull, dflt, pk = row[1], row[2], row[3], row[4], row[5]
        parts = [name, typ]
        if pk and not has_composite_pk:
            parts.append("PRIMARY KEY")
        if notnull and not pk:
            parts.append("NOT NULL")
        if dflt is not None:
            parts.append(f"DEFAULT {dflt}")
        col_defs.append(" ".join(parts))
    if has_composite_pk:
        ordered_pk = ", ".join(name for _, name in sorted(pk_cols, key=lambda item: item[0]))
        col_defs.append(f"PRIMARY KEY({ordered_pk})")
    conn.execute(f"CREATE TABLE merge_units ({', '.join(col_defs)})")
    conn.execute(f"INSERT INTO merge_units ({cols_str}) SELECT {cols_str} FROM merge_units_old")
    conn.execute("DROP TABLE merge_units_old")
    conn.commit()
    conn.close()


def _drop_run_steps_column(db_path: Path, column_name: str) -> None:
    """Rebuild run_steps without a specific column."""
    import sqlite3

    conn = sqlite3.connect(db_path)
    conn.execute("ALTER TABLE run_steps RENAME TO run_steps_old")
    cols = [row[1] for row in conn.execute("PRAGMA table_info(run_steps_old)")]
    kept_cols = [c for c in cols if c != column_name]
    cols_str = ", ".join(kept_cols)
    col_defs = []
    for row in conn.execute("PRAGMA table_info(run_steps_old)"):
        if row[1] == column_name:
            continue
        name, typ, notnull, dflt, pk = row[1], row[2], row[3], row[4], row[5]
        parts = [name, typ]
        if pk:
            parts.append("PRIMARY KEY")
        if notnull and not pk:
            parts.append("NOT NULL")
        if dflt is not None:
            parts.append(f"DEFAULT {dflt}")
        col_defs.append(" ".join(parts))
    conn.execute(f"CREATE TABLE run_steps ({', '.join(col_defs)})")
    conn.execute(f"INSERT INTO run_steps ({cols_str}) SELECT {cols_str} FROM run_steps_old")
    conn.execute("DROP TABLE run_steps_old")
    conn.commit()
    conn.close()


def _drop_task_artifacts_column(db_path: Path, column_name: str) -> None:
    """Rebuild task_artifacts without a specific column."""
    import sqlite3

    conn = sqlite3.connect(db_path)
    conn.execute("ALTER TABLE task_artifacts RENAME TO task_artifacts_old")
    cols = [row[1] for row in conn.execute("PRAGMA table_info(task_artifacts_old)")]
    kept_cols = [c for c in cols if c != column_name]
    cols_str = ", ".join(kept_cols)
    col_defs = []
    for row in conn.execute("PRAGMA table_info(task_artifacts_old)"):
        if row[1] == column_name:
            continue
        name, typ, notnull, dflt, pk = row[1], row[2], row[3], row[4], row[5]
        parts = [name, typ]
        if pk:
            parts.append("PRIMARY KEY")
        if notnull and not pk:
            parts.append("NOT NULL")
        if dflt is not None:
            parts.append(f"DEFAULT {dflt}")
        col_defs.append(" ".join(parts))
    conn.execute(f"CREATE TABLE task_artifacts ({', '.join(col_defs)})")
    conn.execute(f"INSERT INTO task_artifacts ({cols_str}) SELECT {cols_str} FROM task_artifacts_old")
    conn.execute("DROP TABLE task_artifacts_old")
    conn.commit()
    conn.close()


class TestMigrationUtilityFunctions:
    """Tests for migration utilities and manual migration chaining."""

    def test_check_migration_status_on_v24_db(self, tmp_path: Path) -> None:
        """check_migration_status on a v24 DB reports pending manual/auto migration chains."""
        db_path = tmp_path / "test.db"
        _make_v24_db(db_path)

        status = check_migration_status(db_path)

        assert status["current_version"] == 24
        assert status["target_version"] == SCHEMA_VERSION
        assert status["pending_auto"] == list(range(28, SCHEMA_VERSION + 1))
        assert status["pending_manual"] == [25, 26, 27]

    def test_check_migration_status_after_v25_migration(self, tmp_path: Path) -> None:
        """After manual migrations, auto migrations remain pending until SqliteTaskStore runs."""
        db_path = tmp_path / "test.db"
        _make_v24_db(db_path)
        _run_v25_v26_v27_migrations(db_path, "gza")

        status = check_migration_status(db_path)

        assert status["current_version"] == 27
        assert status["pending_auto"] == list(range(28, SCHEMA_VERSION + 1))
        assert status["pending_manual"] == []

        # Constructing SqliteTaskStore triggers remaining auto-migrations.
        SqliteTaskStore(db_path, prefix="gza")
        status_after = check_migration_status(db_path)
        assert status_after["current_version"] == SCHEMA_VERSION


@pytest.mark.timeout(4, method="signal")
class TestSharedDbIsolationAndImportGating:
    def test_explicit_project_id_is_honored_unchanged(self, tmp_path: Path) -> None:
        from gza.config import Config

        project_dir = tmp_path / "project"
        project_dir.mkdir(parents=True, exist_ok=True)
        shared_db = tmp_path / "shared" / "gza.db"
        (project_dir / "gza.yaml").write_text(
            "project_name: tarantino-ui\n"
            "project_id: existinghash123\n"
            f"db_path: {shared_db}\n",
            encoding="utf-8",
        )

        config = Config.load(project_dir)
        assert config.project_id == "existinghash123"

    def test_missing_project_id_in_shared_mode_requires_explicit_legacy_id_with_remediation(
        self, tmp_path: Path
    ) -> None:
        from gza.config import Config, ConfigError

        shared_db = tmp_path / "shared" / "gza.db"
        project_a = tmp_path / "project-a"
        project_a.mkdir(parents=True, exist_ok=True)
        (project_a / "gza.yaml").write_text(f"project_name: demo\ndb_path: {shared_db}\n", encoding="utf-8")

        expected_legacy_id = _legacy_project_id(project_a, "demo")
        with pytest.raises(ConfigError) as exc_info:
            Config.load(project_a)

        message = str(exc_info.value)
        assert "'project_id' is required when shared DB mode is active" in message
        assert f"project_id: {expected_legacy_id}" in message
        assert "uv run gza migrate --import-local-db --yes" in message

        is_valid, errors, _warnings = Config.validate(project_a)
        assert is_valid is False
        assert any(f"project_id: {expected_legacy_id}" in err for err in errors)

    def test_config_load_missing_project_id_readonly_file_is_non_mutating(
        self,
        tmp_path: Path,
    ) -> None:
        from gza.config import Config, ConfigError

        config_path = tmp_path / "gza.yaml"
        shared_db = tmp_path / "shared" / "gza.db"
        original = (
            "project_name: readonly-project\n"
            f"db_path: {shared_db}\n"
        )
        config_path.write_text(original, encoding="utf-8")
        config_path.chmod(0o444)
        try:
            with pytest.raises(ConfigError, match="'project_id' is required when shared DB mode is active"):
                Config.load(tmp_path)
        finally:
            config_path.chmod(0o644)

        assert config_path.read_text(encoding="utf-8") == original

    def test_read_only_db_can_be_opened_for_reads(self, tmp_path: Path) -> None:
        db_path = tmp_path / "readonly.db"
        store = SqliteTaskStore(db_path, prefix="gza", project_id="projreadonly1")
        created = store.add("readonly open")
        assert created.id is not None

        db_path.chmod(0o444)
        try:
            reopened = SqliteTaskStore(db_path, prefix="gza", project_id="projreadonly1")
            fetched = reopened.get(created.id)
        finally:
            db_path.chmod(0o644)

        assert fetched is not None
        assert fetched.prompt == "readonly open"

    def test_convenience_helpers_surface_config_error_without_silent_local_fallback(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        from gza.config import ConfigError
        from gza.db import get_task

        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        store = SqliteTaskStore(db_path, prefix="gza")
        task = store.add("legacy local task")
        monkeypatch.chdir(tmp_path)

        with pytest.raises(ConfigError, match="Configuration file not found"):
            get_task(task.id)

    def test_marker_check_verifies_fingerprint_when_local_db_metadata_unchanged(
        self,
        tmp_path: Path,
    ) -> None:
        from gza.config import Config
        from gza.db import _db_fingerprint

        project_dir = tmp_path / "project"
        project_dir.mkdir(parents=True, exist_ok=True)
        shared_db = tmp_path / "shared" / "gza.db"
        (project_dir / "gza.yaml").write_text(
            "project_name: demo\n"
            "project_id: demomarker01\n"
            "project_prefix: demo\n"
            f"db_path: {shared_db}\n",
            encoding="utf-8",
        )

        local_db = project_dir / ".gza" / "gza.db"
        local_db.parent.mkdir(parents=True, exist_ok=True)
        legacy_store = SqliteTaskStore(local_db, prefix="demo")
        legacy_store.add("legacy task")

        config = Config.load(project_dir)
        result = import_legacy_local_db(config)
        assert result["status"] == "imported"

        with patch("gza.db._db_fingerprint", wraps=_db_fingerprint) as fingerprint:
            reopened = SqliteTaskStore.from_config(config)
        assert reopened is not None
        fingerprint.assert_called_once_with(local_db)

    def test_marker_check_verifies_fingerprint_when_local_db_mtime_changes(
        self,
        tmp_path: Path,
    ) -> None:
        from gza import db as db_module
        from gza.config import Config

        project_dir = tmp_path / "project"
        project_dir.mkdir(parents=True, exist_ok=True)
        shared_db = tmp_path / "shared" / "gza.db"
        (project_dir / "gza.yaml").write_text(
            "project_name: demo\n"
            "project_id: demomarker02\n"
            "project_prefix: demo\n"
            f"db_path: {shared_db}\n",
            encoding="utf-8",
        )

        local_db = project_dir / ".gza" / "gza.db"
        local_db.parent.mkdir(parents=True, exist_ok=True)
        legacy_store = SqliteTaskStore(local_db, prefix="demo")
        legacy_store.add("legacy task")

        config = Config.load(project_dir)
        result = import_legacy_local_db(config)
        assert result["status"] == "imported"

        file_stat = local_db.stat()
        os.utime(local_db, ns=(file_stat.st_atime_ns, file_stat.st_mtime_ns + 1_000_000_000))

        with patch("gza.db._db_fingerprint", wraps=db_module._db_fingerprint) as fingerprint:
            reopened = SqliteTaskStore.from_config(config)
        assert reopened is not None
        fingerprint.assert_called_once_with(local_db)

    def test_marker_check_verifies_fingerprint_when_local_db_ctime_changes(
        self,
        tmp_path: Path,
    ) -> None:
        from gza import db as db_module
        from gza.config import Config

        project_dir = tmp_path / "project"
        project_dir.mkdir(parents=True, exist_ok=True)
        shared_db = tmp_path / "shared" / "gza.db"
        (project_dir / "gza.yaml").write_text(
            "project_name: demo\n"
            "project_id: demomarkerctime01\n"
            "project_prefix: demo\n"
            f"db_path: {shared_db}\n",
            encoding="utf-8",
        )

        local_db = project_dir / ".gza" / "gza.db"
        local_db.parent.mkdir(parents=True, exist_ok=True)
        legacy_store = SqliteTaskStore(local_db, prefix="demo")
        legacy_store.add("legacy task")

        config = Config.load(project_dir)
        result = import_legacy_local_db(config)
        assert result["status"] == "imported"

        before = local_db.stat()
        os.chmod(local_db, before.st_mode ^ stat.S_IXUSR)
        after = local_db.stat()
        if after.st_ctime_ns == before.st_ctime_ns:
            pytest.skip("filesystem did not update ctime on chmod")

        with patch("gza.db._db_fingerprint", wraps=db_module._db_fingerprint) as fingerprint:
            reopened = SqliteTaskStore.from_config(config)
        assert reopened is not None
        fingerprint.assert_called_once_with(local_db)

    def test_marker_check_metadata_equality_still_requires_fingerprint_match(
        self,
        tmp_path: Path,
    ) -> None:
        from gza import db as db_module
        from gza.config import Config, ConfigError

        project_dir = tmp_path / "project"
        project_dir.mkdir(parents=True, exist_ok=True)
        shared_db = tmp_path / "shared" / "gza.db"
        (project_dir / "gza.yaml").write_text(
            "project_name: demo\n"
            "project_id: demomarker03\n"
            "project_prefix: demo\n"
            f"db_path: {shared_db}\n",
            encoding="utf-8",
        )

        local_db = project_dir / ".gza" / "gza.db"
        local_db.parent.mkdir(parents=True, exist_ok=True)
        legacy_store = SqliteTaskStore(local_db, prefix="demo")
        legacy_store.add("legacy task")

        config = Config.load(project_dir)
        result = import_legacy_local_db(config)
        assert result["status"] == "imported"

        marker_path = project_dir / ".gza" / "shared-db-import.json"
        marker = json.loads(marker_path.read_text(encoding="utf-8"))
        marker_metadata = (
            marker["local_db_size"],
            marker["local_db_mtime_ns"],
            marker["local_db_ctime_ns"],
        )

        with open(local_db, "ab") as handle:
            handle.write(b"\nforced-drift\n")

        with (
            patch("gza.db._db_metadata", return_value=marker_metadata),
            patch("gza.db._db_fingerprint", wraps=db_module._db_fingerprint) as fingerprint,
        ):
            with pytest.raises(ConfigError) as exc_info:
                SqliteTaskStore.from_config(config)
        fingerprint.assert_called_once_with(local_db)
        message = str(exc_info.value)
        assert "Legacy local DB detected" in message
        assert "uv run gza migrate --import-local-db --yes" in message
        assert "db_path: .gza/gza.db" in message

    def test_import_local_db_dry_run_pre_v37_missing_create_pr_defaults_false(self, tmp_path: Path) -> None:
        from gza.config import Config

        project_dir = tmp_path / "project"
        project_dir.mkdir(parents=True, exist_ok=True)
        shared_db = tmp_path / "shared" / "gza.db"
        (project_dir / "gza.yaml").write_text(
            "project_name: demo\n"
            "project_id: demoimportdryrun04\n"
            "project_prefix: demo\n"
            f"db_path: {shared_db}\n",
            encoding="utf-8",
        )

        local_db = project_dir / ".gza" / "gza.db"
        local_db.parent.mkdir(parents=True, exist_ok=True)
        legacy_store = SqliteTaskStore(local_db, prefix="demo")
        legacy_task = legacy_store.add("legacy task before v37")
        _drop_tasks_column(local_db, "create_pr")

        config = Config.load(project_dir)
        result = import_legacy_local_db(config, dry_run=True)

        assert result["status"] == "dry_run"
        assert result["local_task_count"] == 1
        assert result["shared_existing_task_count"] == 0
        with sqlite3.connect(local_db) as conn:
            row = conn.execute("SELECT id FROM tasks").fetchone()
        assert row is not None
        assert row[0] == legacy_task.id

    def test_import_local_db_dry_run_pre_v40_missing_completion_reason_defaults_null(
        self,
        tmp_path: Path,
    ) -> None:
        from gza.config import Config

        project_dir = tmp_path / "project"
        project_dir.mkdir(parents=True, exist_ok=True)
        shared_db = tmp_path / "shared" / "gza.db"
        (project_dir / "gza.yaml").write_text(
            "project_name: demo\n"
            "project_id: demoimportdryrun05\n"
            "project_prefix: demo\n"
            f"db_path: {shared_db}\n",
            encoding="utf-8",
        )

        local_db = project_dir / ".gza" / "gza.db"
        local_db.parent.mkdir(parents=True, exist_ok=True)
        legacy_store = SqliteTaskStore(local_db, prefix="demo")
        legacy_task = legacy_store.add("legacy task before v40")
        _drop_tasks_column(local_db, "completion_reason")

        config = Config.load(project_dir)
        result = import_legacy_local_db(config, dry_run=True)

        assert result["status"] == "dry_run"
        assert result["local_task_count"] == 1
        with sqlite3.connect(local_db) as conn:
            row = conn.execute("SELECT id FROM tasks").fetchone()
        assert row is not None
        assert row[0] == legacy_task.id

    def test_import_local_db_pre_v37_missing_create_pr_imports_with_false(self, tmp_path: Path) -> None:
        from gza.config import Config

        project_dir = tmp_path / "project"
        project_dir.mkdir(parents=True, exist_ok=True)
        shared_db = tmp_path / "shared" / "gza.db"
        (project_dir / "gza.yaml").write_text(
            "project_name: demo\n"
            "project_id: demoimport03\n"
            "project_prefix: demo\n"
            f"db_path: {shared_db}\n",
            encoding="utf-8",
        )

        local_db = project_dir / ".gza" / "gza.db"
        local_db.parent.mkdir(parents=True, exist_ok=True)
        legacy_store = SqliteTaskStore(local_db, prefix="demo")
        legacy_task = legacy_store.add("legacy task before v37")
        _drop_tasks_column(local_db, "create_pr")

        config = Config.load(project_dir)
        result = import_legacy_local_db(config)
        assert result["status"] == "imported"

        shared_store = SqliteTaskStore.from_config(config)
        imported = shared_store.get(legacy_task.id)
        assert imported is not None
        assert imported.create_pr is False

    def test_import_local_db_pre_v40_missing_completion_reason_imports_with_null(
        self,
        tmp_path: Path,
    ) -> None:
        from gza.config import Config

        project_dir = tmp_path / "project"
        project_dir.mkdir(parents=True, exist_ok=True)
        shared_db = tmp_path / "shared" / "gza.db"
        (project_dir / "gza.yaml").write_text(
            "project_name: demo\n"
            "project_id: demoimport04\n"
            "project_prefix: demo\n"
            f"db_path: {shared_db}\n",
            encoding="utf-8",
        )

        local_db = project_dir / ".gza" / "gza.db"
        local_db.parent.mkdir(parents=True, exist_ok=True)
        legacy_store = SqliteTaskStore(local_db, prefix="demo")
        legacy_task = legacy_store.add("legacy task before v40")
        _drop_tasks_column(local_db, "completion_reason")

        config = Config.load(project_dir)
        result = import_legacy_local_db(config)
        assert result["status"] == "imported"

        shared_store = SqliteTaskStore.from_config(config)
        imported = shared_store.get(legacy_task.id)
        assert imported is not None
        assert imported.completion_reason is None

    def test_import_local_db_then_add_task_continues_sequence(self, tmp_path: Path) -> None:
        from gza.config import Config

        project_dir = tmp_path / "project"
        project_dir.mkdir(parents=True, exist_ok=True)
        shared_db = tmp_path / "shared" / "gza.db"
        (project_dir / "gza.yaml").write_text(
            "project_name: demo\n"
            "project_id: demoproject01\n"
            "project_prefix: demo\n"
            f"db_path: {shared_db}\n",
            encoding="utf-8",
        )

        local_db = project_dir / ".gza" / "gza.db"
        local_db.parent.mkdir(parents=True, exist_ok=True)
        legacy_store = SqliteTaskStore(local_db, prefix="demo")
        legacy_store.add("legacy-1")
        legacy_store.add("legacy-2")

        config = Config.load(project_dir)
        result = import_legacy_local_db(config)
        assert result["status"] == "imported"

        shared_store = SqliteTaskStore.from_config(config)
        created = shared_store.add("post-import")
        assert created.id == "demo-3"

    def test_import_local_db_preserves_higher_existing_shared_sequence(self, tmp_path: Path) -> None:
        from gza.config import Config

        project_dir = tmp_path / "project"
        project_dir.mkdir(parents=True, exist_ok=True)
        shared_db = tmp_path / "shared" / "gza.db"
        (project_dir / "gza.yaml").write_text(
            "project_name: demo\n"
            "project_id: demoproject02\n"
            "project_prefix: demo\n"
            f"db_path: {shared_db}\n",
            encoding="utf-8",
        )

        config = Config.load(project_dir)
        shared_store = SqliteTaskStore.from_config(config)
        with sqlite3.connect(shared_db) as conn:
            conn.execute(
                """
                INSERT INTO project_sequences(project_id, prefix, next_seq)
                VALUES (?, ?, ?)
                ON CONFLICT(project_id) DO UPDATE SET next_seq = excluded.next_seq
                """,
                (config.project_id, config.project_prefix, 50),
            )

        local_db = project_dir / ".gza" / "gza.db"
        local_db.parent.mkdir(parents=True, exist_ok=True)
        legacy_store = SqliteTaskStore(local_db, prefix="demo")
        legacy_store.add("legacy-1")

        result = import_legacy_local_db(config)
        assert result["status"] == "imported"

        created = shared_store.add("post-import")
        assert created.id == "demo-51"

    def test_import_local_db_run_substeps_link_to_same_project_run_steps(self, tmp_path: Path) -> None:
        from gza.config import Config

        shared_db = tmp_path / "shared" / "gza.db"
        other_store = SqliteTaskStore(shared_db, prefix="other", project_id="other1")
        other_task = other_store.add("other task")
        other_step = other_store.emit_step(other_task.id, "other step", provider="codex")

        project_dir = tmp_path / "project"
        project_dir.mkdir(parents=True, exist_ok=True)
        (project_dir / "gza.yaml").write_text(
            "project_name: demo\n"
            "project_id: demoimport01\n"
            "project_prefix: demo\n"
            f"db_path: {shared_db}\n",
            encoding="utf-8",
        )

        local_db = project_dir / ".gza" / "gza.db"
        local_db.parent.mkdir(parents=True, exist_ok=True)
        legacy_store = SqliteTaskStore(local_db, prefix="demo")
        task = legacy_store.add("legacy task")
        step = legacy_store.emit_step(task.id, "legacy step", provider="codex")
        legacy_store.emit_substep(step, "tool_call", {"ok": True}, source="assistant")

        config = Config.load(project_dir)
        result = import_legacy_local_db(config)
        assert result["status"] == "imported"

        shared_store = SqliteTaskStore.from_config(config)
        imported_steps = shared_store.get_run_steps(task.id)
        assert len(imported_steps) == 1
        imported_step = imported_steps[0]
        assert imported_step.id != other_step.id

        substeps = shared_store.get_run_substeps(
            StepRef(
                id=imported_step.id,
                run_id=imported_step.run_id,
                step_index=imported_step.step_index,
                step_id=imported_step.step_id,
            )
        )
        assert len(substeps) == 1
        assert substeps[0].step_id == imported_step.id
        assert substeps[0].step_id != other_step.id

    def test_import_local_db_preserves_step_substep_graph(self, tmp_path: Path) -> None:
        from gza.config import Config

        shared_db = tmp_path / "shared" / "gza.db"
        other_store = SqliteTaskStore(shared_db, prefix="other", project_id="other2")
        other_task = other_store.add("other task")
        other_step = other_store.emit_step(other_task.id, "other step", provider="codex")
        other_store.emit_substep(other_step, "tool_call", {"other": True}, source="assistant")

        project_dir = tmp_path / "project"
        project_dir.mkdir(parents=True, exist_ok=True)
        (project_dir / "gza.yaml").write_text(
            "project_name: demo\n"
            "project_id: demoimport02\n"
            "project_prefix: demo\n"
            f"db_path: {shared_db}\n",
            encoding="utf-8",
        )

        local_db = project_dir / ".gza" / "gza.db"
        local_db.parent.mkdir(parents=True, exist_ok=True)
        legacy_store = SqliteTaskStore(local_db, prefix="demo")
        task = legacy_store.add("legacy task")
        step = legacy_store.emit_step(task.id, "legacy step", provider="codex")
        legacy_store.emit_substep(step, "tool_call", {"ok": True}, source="assistant")

        config = Config.load(project_dir)
        result = import_legacy_local_db(config)
        assert result["status"] == "imported"

        with sqlite3.connect(shared_db) as conn:
            mismatches = conn.execute(
                """
                SELECT COUNT(*)
                FROM run_substeps sub
                JOIN run_steps step ON sub.step_id = step.id
                WHERE sub.project_id != step.project_id
                """
            ).fetchone()[0]
            demo_links = conn.execute(
                """
                SELECT COUNT(*)
                FROM run_substeps sub
                JOIN run_steps step ON sub.step_id = step.id
                WHERE sub.project_id = ? AND step.project_id = ?
                """,
                (config.project_id, config.project_id),
            ).fetchone()[0]
        assert mismatches == 0
        assert demo_links == 1

    def test_import_local_db_normalizes_naive_timestamps_without_false_conflicts(self, tmp_path: Path) -> None:
        from gza.config import Config

        project_dir = tmp_path / "project"
        project_dir.mkdir(parents=True, exist_ok=True)
        shared_db = tmp_path / "shared" / "gza.db"
        (project_dir / "gza.yaml").write_text(
            "project_name: demo\n"
            "project_id: demoimporttimestamps01\n"
            "project_prefix: demo\n"
            f"db_path: {shared_db}\n",
            encoding="utf-8",
        )

        config = Config.load(project_dir)
        shared_store = SqliteTaskStore.from_config(config)
        shared_task = shared_store.add("same task")
        shared_step = shared_store.emit_step(shared_task.id, "same step", provider="codex")
        shared_store.emit_substep(shared_step, "tool_call", {"ok": True}, source="assistant")
        shared_store.finalize_step(shared_step, "completed", completed_at=datetime(2026, 1, 4, 10, 2, tzinfo=UTC))
        shared_comment = shared_store.add_comment(shared_task.id, "same comment")

        local_db = project_dir / ".gza" / "gza.db"
        local_db.parent.mkdir(parents=True, exist_ok=True)
        local_store = SqliteTaskStore(local_db, prefix="demo")
        local_task = local_store.add("same task")
        local_step = local_store.emit_step(local_task.id, "same step", provider="codex")
        local_store.emit_substep(local_step, "tool_call", {"ok": True}, source="assistant")
        local_store.finalize_step(local_step, "completed", completed_at=datetime(2026, 1, 4, 10, 2, tzinfo=UTC))
        local_comment = local_store.add_comment(local_task.id, "same comment")

        assert shared_task.id == local_task.id

        with sqlite3.connect(shared_db) as conn:
            conn.execute(
                "UPDATE tasks SET created_at = ?, completed_at = ? WHERE project_id = ? AND id = ?",
                (
                    "2026-01-04T10:00:00+00:00",
                    "2026-01-04T10:05:00+00:00",
                    config.project_id,
                    shared_task.id,
                ),
            )
            conn.execute(
                "UPDATE run_steps SET started_at = ?, completed_at = ? WHERE project_id = ? AND run_id = ?",
                (
                    "2026-01-04T10:01:00+00:00",
                    "2026-01-04T10:02:00+00:00",
                    config.project_id,
                    shared_task.id,
                ),
            )
            conn.execute(
                "UPDATE run_substeps SET timestamp = ? WHERE project_id = ? AND run_id = ?",
                ("2026-01-04T10:01:30+00:00", config.project_id, shared_task.id),
            )
            conn.execute(
                "UPDATE task_comments SET created_at = ?, resolved_at = ? WHERE project_id = ? AND id = ?",
                (
                    "2026-01-04T10:03:00+00:00",
                    "2026-01-04T10:04:00+00:00",
                    config.project_id,
                    shared_comment.id,
                ),
            )
            conn.commit()

        with sqlite3.connect(local_db) as conn:
            conn.execute(
                "UPDATE tasks SET created_at = ?, completed_at = ? WHERE id = ?",
                ("2026-01-04T10:00:00", "2026-01-04T10:05:00", local_task.id),
            )
            conn.execute(
                "UPDATE run_steps SET started_at = ?, completed_at = ? WHERE run_id = ?",
                ("2026-01-04T10:01:00", "2026-01-04T10:02:00", local_task.id),
            )
            conn.execute(
                "UPDATE run_substeps SET timestamp = ? WHERE run_id = ?",
                ("2026-01-04T10:01:30", local_task.id),
            )
            conn.execute(
                "UPDATE task_comments SET created_at = ?, resolved_at = ? WHERE id = ?",
                ("2026-01-04T10:03:00", "2026-01-04T10:04:00", local_comment.id),
            )
            conn.commit()

        result = import_legacy_local_db(config)
        assert result["status"] == "imported"
        assert result["tasks_imported"] == 0

        refreshed = SqliteTaskStore.from_config(config)
        comments = refreshed.get_comments(shared_task.id)
        steps = refreshed.get_run_steps(shared_task.id)
        assert len(comments) == 1
        assert len(steps) == 1
        substeps = refreshed.get_run_substeps(
            StepRef(
                id=steps[0].id,
                run_id=steps[0].run_id,
                step_index=steps[0].step_index,
                step_id=steps[0].step_id,
            )
        )
        assert len(substeps) == 1

        with sqlite3.connect(shared_db) as conn:
            task_row = conn.execute(
                "SELECT created_at, completed_at FROM tasks WHERE project_id = ? AND id = ?",
                (config.project_id, shared_task.id),
            ).fetchone()
            step_row = conn.execute(
                "SELECT started_at, completed_at FROM run_steps WHERE project_id = ? AND run_id = ?",
                (config.project_id, shared_task.id),
            ).fetchone()
            substep_row = conn.execute(
                "SELECT timestamp FROM run_substeps WHERE project_id = ? AND run_id = ?",
                (config.project_id, shared_task.id),
            ).fetchone()
            comment_row = conn.execute(
                "SELECT created_at, resolved_at FROM task_comments WHERE project_id = ? AND id = ?",
                (config.project_id, shared_comment.id),
            ).fetchone()

        assert task_row == ("2026-01-04T10:00:00+00:00", "2026-01-04T10:05:00+00:00")
        assert step_row == ("2026-01-04T10:01:00+00:00", "2026-01-04T10:02:00+00:00")
        assert substep_row == ("2026-01-04T10:01:30+00:00",)
        assert comment_row == ("2026-01-04T10:03:00+00:00", "2026-01-04T10:04:00+00:00")

    def test_bootstrap_missing_shared_project_id_persists_legacy_identity_for_import(self, tmp_path: Path) -> None:
        from gza.config import Config, bootstrap_missing_shared_project_id

        project_dir = tmp_path / "project"
        project_dir.mkdir(parents=True, exist_ok=True)
        shared_db = tmp_path / "shared" / "gza.db"
        (project_dir / "gza.yaml").write_text(
            "project_name: demo\n"
            f"db_path: {shared_db}\n",
            encoding="utf-8",
        )

        project_id, updated = bootstrap_missing_shared_project_id(project_dir)
        assert project_id == _legacy_project_id(project_dir, "demo")
        assert updated is True

        config = Config.load(project_dir)
        assert config.project_id == project_id
        assert f"project_id: {project_id}" in (project_dir / "gza.yaml").read_text(encoding="utf-8")

    def test_bootstrap_missing_shared_project_id_respects_local_db_override(self, tmp_path: Path) -> None:
        from gza.config import Config, bootstrap_missing_shared_project_id

        project_dir = tmp_path / "project"
        project_dir.mkdir(parents=True, exist_ok=True)
        shared_db = tmp_path / "shared" / "gza.db"
        (project_dir / "gza.yaml").write_text("project_name: demo\n", encoding="utf-8")
        (project_dir / "gza.local.yaml").write_text(
            f"db_path: {shared_db}\n",
            encoding="utf-8",
        )

        project_id, updated = bootstrap_missing_shared_project_id(project_dir)
        assert project_id == _legacy_project_id(project_dir, "demo")
        assert updated is True

        config = Config.load(project_dir)
        assert config.project_id == project_id
        assert config.db_path == shared_db.resolve()
        assert f"project_id: {project_id}" in (project_dir / "gza.yaml").read_text(encoding="utf-8")

    def test_shared_mode_rejects_project_id_default(self, tmp_path: Path) -> None:
        from gza.config import Config, ConfigError

        project_dir = tmp_path / "project"
        project_dir.mkdir(parents=True, exist_ok=True)
        shared_db = tmp_path / "shared" / "gza.db"
        (project_dir / "gza.yaml").write_text(
            "project_name: demo\n"
            "project_id: default\n"
            f"db_path: {shared_db}\n",
            encoding="utf-8",
        )

        with pytest.raises(ConfigError, match="only valid with local DB mode"):
            Config.load(project_dir)

        is_valid, errors, _warnings = Config.validate(project_dir)
        assert is_valid is False
        assert any("only valid with local DB mode" in err for err in errors)

    def test_local_db_mode_allows_project_id_default(self, tmp_path: Path) -> None:
        from gza.config import Config

        project_dir = tmp_path / "project"
        project_dir.mkdir(parents=True, exist_ok=True)
        (project_dir / "gza.yaml").write_text(
            "project_name: demo\n"
            "project_id: default\n"
            "db_path: .gza/gza.db\n",
            encoding="utf-8",
        )

        config = Config.load(project_dir)
        assert config.project_id == "default"

        is_valid, errors, _warnings = Config.validate(project_dir)
        assert is_valid is True
        assert errors == []

    def test_v35_to_v36_migration_rebuilds_project_scoped_keys(self, tmp_path: Path) -> None:
        """Auto-migrating v35 must rebuild keys/fks/uniques to isolate projects."""
        db_path = tmp_path / "test.db"
        _make_v35_db_with_legacy_key_shapes(db_path)

        store_alpha = SqliteTaskStore(db_path, prefix="gza", project_id="alpha")
        legacy_alpha_task = store_alpha.get("gza-1")
        assert legacy_alpha_task is not None
        step_alpha = store_alpha.emit_step("gza-1", "alpha step", provider="codex")
        substep_alpha = store_alpha.emit_substep(step_alpha, "tool_call", {"alpha": True}, source="assistant")
        assert substep_alpha.substep_id.endswith(".1")

        store_beta = SqliteTaskStore(db_path, prefix="gza", project_id="beta")
        created_beta = store_beta.add("beta task")
        assert created_beta.id == "gza-1"
        step_beta = store_beta.emit_step("gza-1", "beta step", provider="codex")
        substep_beta = store_beta.emit_substep(step_beta, "tool_call", {"beta": True}, source="assistant")
        assert substep_beta.substep_id.endswith(".1")

        conn = sqlite3.connect(db_path)
        version = conn.execute("SELECT version FROM schema_version").fetchone()[0]
        task_pk = tuple(
            row[1]
            for row in sorted(
                conn.execute("PRAGMA table_info(tasks)").fetchall(),
                key=lambda row: row[5],
            )
            if row[5] > 0
        )
        run_steps_unique_indexes = {
            tuple(col[2] for col in conn.execute(f"PRAGMA index_info('{idx[1]}')").fetchall())
            for idx in conn.execute("PRAGMA index_list(run_steps)").fetchall()
            if idx[2] == 1
        }
        run_substeps_unique_indexes = {
            tuple(col[2] for col in conn.execute(f"PRAGMA index_info('{idx[1]}')").fetchall())
            for idx in conn.execute("PRAGMA index_list(run_substeps)").fetchall()
            if idx[2] == 1
        }
        conn.close()

        assert version == SCHEMA_VERSION
        assert task_pk == ("project_id", "id")
        assert ("project_id", "run_id", "step_index") in run_steps_unique_indexes
        assert ("project_id", "run_id", "step_id") in run_steps_unique_indexes
        assert ("project_id", "step_id", "substep_index") in run_substeps_unique_indexes
        assert ("project_id", "step_id", "substep_id") in run_substeps_unique_indexes

    def test_open_read_only_v35_db_raises_schema_integrity_error_not_operational_error(
        self, tmp_path: Path
    ) -> None:
        """Read-only v35 DB should fail with controlled SchemaIntegrityError during v36 auto-migration."""
        db_path = tmp_path / "readonly-v35.db"
        _make_v35_db_with_legacy_key_shapes(db_path)

        db_path.chmod(0o444)
        try:
            with pytest.raises(
                SchemaIntegrityError,
                match=r"Cannot auto-migrate schema v35->v36 on a read-only database",
            ):
                SqliteTaskStore(db_path, prefix="gza", project_id="alpha")
        finally:
            db_path.chmod(0o644)

    def test_v35_to_v36_writable_disk_io_error_is_not_rewritten_as_read_only_schema_error(
        self, tmp_path: Path
    ) -> None:
        """Writable v35 auto-migration should preserve real disk I/O failures."""
        db_path = tmp_path / "disk-io-v35.db"
        _make_v35_db_with_legacy_key_shapes(db_path)

        with patch(
            "gza.db._run_v35_to_v36_migration",
            side_effect=sqlite3.OperationalError("disk I/O error"),
        ):
            with pytest.raises(sqlite3.OperationalError, match=r"disk I/O error"):
                SqliteTaskStore(db_path, prefix="gza", project_id="alpha")

    def test_v35_to_v36_migration_keeps_active_prefix_sequence_continuity(self, tmp_path: Path) -> None:
        """v35→v36 migration must keep next ID above existing active-prefix task suffixes."""
        db_path = tmp_path / "test.db"
        _make_v35_db_with_legacy_key_shapes(db_path)

        with sqlite3.connect(db_path) as conn:
            conn.execute("DELETE FROM project_sequences")
            conn.execute("INSERT INTO project_sequences(prefix, next_seq) VALUES ('old', 1)")
            conn.execute("INSERT INTO project_sequences(prefix, next_seq) VALUES ('gza', 5)")
            conn.execute(
                """
                INSERT INTO tasks (id, prompt, status, task_type, task_id, created_at)
                VALUES ('gza-5', 'legacy task 5', 'pending', 'implement', '20260427-gza-legacy-task-5', '2024-01-01T00:00:00+00:00')
                """
            )
            conn.commit()

        store = SqliteTaskStore(db_path, prefix="gza", project_id="alpha")
        created = store.add("post-migration task")
        assert created.id == "gza-6"

        with sqlite3.connect(db_path) as conn:
            conn.row_factory = sqlite3.Row
            seq = conn.execute(
                "SELECT next_seq FROM project_sequences WHERE project_id = ?",
                ("alpha",),
            ).fetchone()

        assert seq is not None
        assert int(seq["next_seq"]) == 6

    def test_preview_v25_migration_shows_samples(self, tmp_path: Path) -> None:
        """preview_v25_migration returns correct task_count and sample ID conversions."""
        import sqlite3

        db_path = tmp_path / "test.db"
        _make_v24_db(db_path)

        # Insert a few tasks directly via sqlite so we have integer IDs 1, 2, 3
        conn = sqlite3.connect(db_path)
        for i in range(3):
            conn.execute(
                "INSERT INTO tasks (prompt, created_at) VALUES (?, ?)",
                (f"Task {i + 1}", "2024-01-01T00:00:00+00:00"),
            )
        conn.commit()
        conn.close()

        preview = preview_v25_migration(db_path, "gza")

        assert preview["task_count"] == 3
        assert ("gza-000001", "gza-000001") not in preview["samples"]
        assert preview["samples"][0] == (1, "gza-000001")
        assert preview["samples"][1] == (2, "gza-000002")
        assert preview["samples"][2] == (3, "gza-000003")
        # Only 3 tasks, all consumed by the first-samples path → no random tail to sample
        assert preview["random_samples"] == []
        assert preview["first_post_migration_id"] == "gza-000004"

    def test_preview_v25_migration_includes_random_tail_samples(self, tmp_path: Path) -> None:
        """preview_v25_migration returns up to N random samples from IDs beyond the first N."""
        import sqlite3

        db_path = tmp_path / "test.db"
        _make_v24_db(db_path)

        # Insert 50 tasks so there's a meaningful tail beyond the first 10
        conn = sqlite3.connect(db_path)
        for i in range(50):
            conn.execute(
                "INSERT INTO tasks (prompt, created_at) VALUES (?, ?)",
                (f"Task {i + 1}", "2024-01-01T00:00:00+00:00"),
            )
        conn.commit()
        conn.close()

        preview = preview_v25_migration(db_path, "gza", sample_limit=10, random_sample_limit=10)

        assert preview["task_count"] == 50
        assert len(preview["samples"]) == 10
        # First samples are sequential from id=1
        assert preview["samples"][0] == (1, "gza-000001")
        assert preview["samples"][9] == (10, "gza-00000a")

        # Random samples: exactly 10, all drawn from IDs > 10, all well-formed
        assert len(preview["random_samples"]) == 10
        for old_id, new_id in preview["random_samples"]:
            assert old_id > 10, f"random sample id {old_id} should be > first_limit (10)"
            assert old_id <= 50
            # v25 still previews canonical base36 IDs.
            suffix = new_id.split("-", 1)[1]
            assert int(suffix, 36) == old_id
        # No overlap between first and random samples
        first_ids = {old for old, _ in preview["samples"]}
        random_ids = {old for old, _ in preview["random_samples"]}
        assert first_ids.isdisjoint(random_ids)

    def test_preview_v25_migration_on_already_migrated_db(self, tmp_path: Path) -> None:
        """preview_v25_migration on a v26 DB returns empty samples."""
        db_path = tmp_path / "test.db"
        _make_v24_db(db_path)
        _run_v25_v26_v27_migrations(db_path, "gza")

        preview = preview_v25_migration(db_path, "gza")

        # Already migrated: no integer IDs to convert
        assert preview["samples"] == []
        assert preview["random_samples"] == []
        assert preview["first_post_migration_id"] == ""

    def test_run_v25_migration_idempotent(self, tmp_path: Path) -> None:
        """Calling run_v25_migration twice on the same DB does not raise and leaves DB valid."""
        db_path = tmp_path / "test.db"
        _make_v24_db(db_path)

        run_v25_migration(db_path, "gza")
        # Second call must not raise
        run_v25_migration(db_path, "gza")

        status = check_migration_status(db_path)
        assert status["current_version"] == 25
        assert status["pending_manual"] == [26, 27]

    def test_migration_preserves_fk_references(self, tmp_path: Path) -> None:
        """Manual migrations preserve based_on and depends_on FK values."""
        import sqlite3

        db_path = tmp_path / "test.db"
        _make_v24_db(db_path)

        # Insert two tasks: task 1 is standalone, task 2 has based_on=1 and depends_on=1
        conn = sqlite3.connect(db_path)
        conn.execute(
            "INSERT INTO tasks (id, prompt, created_at) VALUES (1, 'parent', '2024-01-01T00:00:00+00:00')"
        )
        conn.execute(
            "INSERT INTO tasks (id, prompt, based_on, depends_on, created_at) VALUES (2, 'child', 1, 1, '2024-01-01T00:00:00+00:00')"
        )
        conn.commit()
        conn.close()

        _run_v25_v26_v27_migrations(db_path, "gza")
        store = SqliteTaskStore(db_path)

        parent = store.get("gza-1")
        child = store.get("gza-2")
        assert parent is not None
        assert child is not None
        # FK columns must stay consistent after v25->v26 rewrite.
        assert child.based_on == "gza-1"
        assert child.depends_on == "gza-1"

    def test_run_v26_migration_rewrites_base36_to_decimal(self, tmp_path: Path) -> None:
        db_path = tmp_path / "test.db"
        _make_v24_db(db_path)

        import sqlite3
        conn = sqlite3.connect(db_path)
        conn.execute(
            "INSERT INTO tasks (id, prompt, created_at) VALUES (1, 'parent', '2024-01-01T00:00:00+00:00')"
        )
        conn.execute(
            "INSERT INTO tasks (id, prompt, based_on, depends_on, created_at) VALUES (2, 'child', 1, 1, '2024-01-01T00:00:00+00:00')"
        )
        conn.commit()
        conn.close()

        run_v25_migration(db_path, "gza")
        run_v26_migration(db_path)
        run_v27_migration(db_path)
        store = SqliteTaskStore(db_path)

        parent = store.get("gza-1")
        child = store.get("gza-2")
        assert parent is not None
        assert child is not None
        assert child.based_on == "gza-1"
        assert child.depends_on == "gza-1"

    def test_run_v26_migration_rewrites_slug_lineage_segments(self, tmp_path: Path) -> None:
        import sqlite3

        db_path = tmp_path / "test.db"
        _make_v24_db(db_path)

        conn = sqlite3.connect(db_path)
        conn.execute(
            "INSERT INTO tasks (id, prompt, task_id, created_at) VALUES (?, ?, ?, ?)",
            (10, "add feature", "20260410-00000a-impl-add-feature", "2024-01-01T00:00:00+00:00"),
        )
        conn.commit()
        conn.close()

        run_v25_migration(db_path, "gza")
        run_v26_migration(db_path)
        run_v27_migration(db_path)

        store = SqliteTaskStore(db_path, prefix="gza")
        impl_task = store.get("gza-10")
        assert impl_task is not None
        assert impl_task.slug == "20260410-10-impl-add-feature"

        prompt = build_auto_review_prompt(
            impl_task,
            known_task_id_suffixes={"10"},
        )
        assert prompt == "review add-feature"

        review_task = store.add("review task", task_type="review", depends_on=impl_task.id)
        slug_override = _compute_slug_override(review_task, store)
        assert slug_override is not None
        assert "00000a" not in slug_override
        assert slug_override == "add-feature"

    def test_run_v26_migration_preserves_semantic_slug_prefixes(self, tmp_path: Path) -> None:
        import sqlite3

        db_path = tmp_path / "test.db"
        _make_v24_db(db_path)

        conn = sqlite3.connect(db_path)
        conn.execute(
            "INSERT INTO tasks (id, prompt, task_id, created_at) VALUES (?, ?, ?, ?)",
            (10, "semantic slug", "20260410-10-impl-rollout", "2024-01-01T00:00:00+00:00"),
        )
        conn.commit()
        conn.close()

        run_v25_migration(db_path, "gza")

        # Seed a legacy v25-style suffix token that can collide with semantic slug text.
        # Old migration logic rewrote any leading "<token>-impl-..." segment globally.
        conn = sqlite3.connect(db_path)
        conn.execute(
            "INSERT INTO tasks (id, prompt, created_at) VALUES (?, ?, ?)",
            ("aux-10", "legacy token holder", "2024-01-01T00:00:00+00:00"),
        )
        conn.commit()
        conn.close()

        run_v26_migration(db_path)
        run_v27_migration(db_path)

        store = SqliteTaskStore(db_path, prefix="gza")
        task = store.get("gza-10")
        assert task is not None
        assert task.slug == "20260410-10-impl-rollout"

    def test_run_v26_migration_preserves_monotonic_project_sequences(self, tmp_path: Path) -> None:
        import sqlite3

        db_path = tmp_path / "test.db"
        _make_v24_db(db_path)

        conn = sqlite3.connect(db_path)
        conn.execute(
            "INSERT INTO tasks (id, prompt, created_at) VALUES (?, ?, ?)",
            (1, "first", "2024-01-01T00:00:00+00:00"),
        )
        conn.execute(
            "INSERT INTO tasks (id, prompt, created_at) VALUES (?, ?, ?)",
            (2, "second", "2024-01-01T00:00:00+00:00"),
        )
        conn.commit()
        conn.close()

        run_v25_migration(db_path, "gza")

        conn = sqlite3.connect(db_path)
        conn.execute("UPDATE project_sequences SET next_seq = 50 WHERE prefix = 'gza'")
        conn.commit()
        conn.close()

        run_v26_migration(db_path)
        run_v27_migration(db_path)

        conn = sqlite3.connect(db_path)
        row = conn.execute(
            "SELECT next_seq FROM project_sequences WHERE prefix = 'gza'"
        ).fetchone()
        conn.close()
        assert row is not None
        assert row[0] == 50

        store = SqliteTaskStore(db_path, prefix="gza")
        created = store.add("post-migration task")
        assert created.id == "gza-51"

    def test_run_v26_migration_idempotent_on_v26_db(self, tmp_path: Path) -> None:
        db_path = tmp_path / "test.db"
        _make_v24_db(db_path)
        run_v25_migration(db_path, "gza")
        run_v26_migration(db_path)

        run_v26_migration(db_path)
        status = check_migration_status(db_path)
        assert status["current_version"] == 26
        assert status["pending_manual"] == [27]

    def test_run_v26_migration_rejects_v24_db(self, tmp_path: Path) -> None:
        db_path = tmp_path / "test.db"
        _make_v24_db(db_path)

        with pytest.raises(RuntimeError, match="requires DB at v25"):
            run_v26_migration(db_path)

    def test_preview_v26_migration_shows_samples(self, tmp_path: Path) -> None:
        db_path = tmp_path / "test.db"
        _make_v24_db(db_path)
        run_v25_migration(db_path, "gza")

        preview = preview_v26_migration(db_path)
        assert preview["task_count"] >= 0
        # For empty task sets there are no samples; when present, all convert to decimal suffixes.
        for old_id, new_id in preview["samples"] + preview["random_samples"]:
            assert "-" in old_id and "-" in new_id
            assert new_id.rsplit("-", 1)[-1].isdigit()

    def test_auto_migration_v27_to_v28_adds_attach_columns(self, tmp_path: Path) -> None:
        """Opening a v27 DB with SqliteTaskStore auto-migrates to v28, adding attach columns.

        Note: the v27 manual migration's CREATE TABLE already includes the columns,
        so this test verifies the version bump and that record_attach_session works.
        For pre-v27 DBs that somehow reached v27 without the columns, the ALTER TABLE
        in the v28 migration adds them (OperationalError for duplicates is silently ignored).
        """
        import sqlite3

        db_path = tmp_path / "test.db"
        _make_v24_db(db_path)
        _run_v25_v26_v27_migrations(db_path, "gza")

        # Verify DB is at v27
        conn = sqlite3.connect(db_path)
        version = conn.execute("SELECT version FROM schema_version").fetchone()[0]
        conn.close()
        assert version == 27

        # SqliteTaskStore auto-migrates to latest schema.
        store = SqliteTaskStore(db_path, prefix="gza")

        conn = sqlite3.connect(db_path)
        version = conn.execute("SELECT version FROM schema_version").fetchone()[0]
        columns = {row[1] for row in conn.execute("PRAGMA table_info(tasks)")}
        conn.close()

        assert version == SCHEMA_VERSION
        assert "attach_count" in columns
        assert "attach_duration_seconds" in columns
        assert "urgent" in columns

        # store.update() should succeed
        task = store.get("gza-1")
        if task is None:
            task = store.add("test v28 migration")
        store.update(task)

        # record_attach_session should work on migrated DB
        store.record_attach_session(task, 42.5)
        refreshed = store.get(task.id)
        assert refreshed is not None
        assert refreshed.attach_count == 1
        assert refreshed.attach_duration_seconds == 42.5

    def test_auto_migration_v27_to_v28_adds_columns_when_missing(self, tmp_path: Path) -> None:
        """If a v27 DB lacks attach columns (e.g. old v27 CREATE TABLE), v28 migration adds them."""
        import sqlite3

        db_path = tmp_path / "test.db"
        _make_v24_db(db_path)
        _run_v25_v26_v27_migrations(db_path, "gza")

        # Simulate a v27 DB where attach columns are missing by dropping them
        def _quote(column: str) -> str:
            return f'"{column}"' if column in ("group",) else column

        conn = sqlite3.connect(db_path)
        # SQLite doesn't support DROP COLUMN easily; recreate without the columns
        conn.execute("ALTER TABLE tasks RENAME TO tasks_old")
        # Get existing columns minus attach ones
        cols = [row[1] for row in conn.execute("PRAGMA table_info(tasks_old)")]
        kept_cols = [c for c in cols if c not in ("attach_count", "attach_duration_seconds")]
        cols_str = ", ".join(_quote(c) for c in kept_cols)
        # Recreate with same columns minus attach
        col_defs = []
        for row in conn.execute("PRAGMA table_info(tasks_old)"):
            if row[1] in ("attach_count", "attach_duration_seconds"):
                continue
            name, typ, notnull, dflt, pk = row[1], row[2], row[3], row[4], row[5]
            quoted_name = f'"{name}"' if name in ("group",) else name
            parts = [quoted_name, typ]
            if pk:
                parts.append("PRIMARY KEY")
            if notnull and not pk:
                parts.append("NOT NULL")
            if dflt is not None:
                parts.append(f"DEFAULT {dflt}")
            col_defs.append(" ".join(parts))
        conn.execute(f"CREATE TABLE tasks ({', '.join(col_defs)})")
        conn.execute(f"INSERT INTO tasks ({cols_str}) SELECT {cols_str} FROM tasks_old")
        conn.execute("DROP TABLE tasks_old")
        conn.commit()

        # Confirm attach columns are missing
        columns = {row[1] for row in conn.execute("PRAGMA table_info(tasks)")}
        assert "attach_count" not in columns
        conn.close()

        # SqliteTaskStore auto-migrates: ALTER TABLE ADD COLUMN succeeds
        store = SqliteTaskStore(db_path, prefix="gza")

        conn = sqlite3.connect(db_path)
        version = conn.execute("SELECT version FROM schema_version").fetchone()[0]
        columns = {row[1] for row in conn.execute("PRAGMA table_info(tasks)")}
        conn.close()

        assert version == SCHEMA_VERSION
        assert "attach_count" in columns
        assert "attach_duration_seconds" in columns
        assert "urgent" in columns

        task = store.add("test missing columns")
        store.record_attach_session(task, 10.0)
        refreshed = store.get(task.id)
        assert refreshed is not None
        assert refreshed.attach_count == 1

    def test_auto_migration_v29_to_v31_adds_provenance_columns(self, tmp_path: Path) -> None:
        """Opening a v29 DB should migrate through v31 and create provenance columns."""
        import sqlite3

        db_path = tmp_path / "test.db"
        _make_v29_db_without_urgent_bumped_at(db_path)

        SqliteTaskStore(db_path, prefix="gza")

        conn = sqlite3.connect(db_path)
        version = conn.execute("SELECT version FROM schema_version").fetchone()[0]
        columns = {row[1] for row in conn.execute("PRAGMA table_info(tasks)")}
        conn.close()

        assert version == SCHEMA_VERSION
        assert "urgent_bumped_at" in columns
        assert "execution_mode" in columns

    def test_auto_migration_v31_to_v32_adds_task_comments_table(self, tmp_path: Path) -> None:
        """Opening a v31 DB should migrate to v32 and create task_comments."""
        import sqlite3

        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path, prefix="gza")
        task = store.add("Task before downgrade")
        assert task.id is not None

        conn = sqlite3.connect(db_path)
        conn.execute("UPDATE schema_version SET version = 31")
        conn.execute("DROP TABLE task_comments")
        conn.commit()
        conn.close()

        SqliteTaskStore(db_path, prefix="gza")

        conn = sqlite3.connect(db_path)
        version = conn.execute("SELECT version FROM schema_version").fetchone()[0]
        tables = {
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='task_comments'"
            ).fetchall()
        }
        conn.close()

        assert version == SCHEMA_VERSION
        assert "task_comments" in tables

    def test_auto_migration_v32_to_v33_adds_review_score_column(self, tmp_path: Path) -> None:
        """Opening a v32 DB should migrate to v33 and create tasks.review_score."""
        import sqlite3

        db_path = tmp_path / "test.db"
        SqliteTaskStore(db_path, prefix="gza")

        conn = sqlite3.connect(db_path)
        conn.execute("UPDATE schema_version SET version = 32")
        conn.commit()
        conn.close()

        SqliteTaskStore(db_path, prefix="gza")

        conn = sqlite3.connect(db_path)
        version = conn.execute("SELECT version FROM schema_version").fetchone()[0]
        columns = {row[1] for row in conn.execute("PRAGMA table_info(tasks)")}
        conn.close()

        assert version == SCHEMA_VERSION
        assert "review_score" in columns

    def test_auto_migration_v33_to_v34_adds_queue_position_column(self, tmp_path: Path) -> None:
        """Opening a v33 DB should migrate to v34 and create tasks.queue_position."""
        import sqlite3

        db_path = tmp_path / "test.db"
        SqliteTaskStore(db_path, prefix="gza")

        conn = sqlite3.connect(db_path)
        conn.execute("UPDATE schema_version SET version = 33")
        conn.commit()
        conn.close()

        SqliteTaskStore(db_path, prefix="gza")

        conn = sqlite3.connect(db_path)
        version = conn.execute("SELECT version FROM schema_version").fetchone()[0]
        columns = {row[1] for row in conn.execute("PRAGMA table_info(tasks)")}
        conn.close()

        assert version == SCHEMA_VERSION
        assert "queue_position" in columns

    def test_open_current_v32_db_repairs_missing_task_comments_table(self, tmp_path: Path) -> None:
        """Opening an already-v32 DB should repair missing comment artifacts."""
        import sqlite3

        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path, prefix="gza")
        task = store.add("Task before table damage")
        assert task.id is not None

        conn = sqlite3.connect(db_path)
        conn.execute("UPDATE schema_version SET version = 32")
        conn.execute("DROP TABLE task_comments")
        conn.commit()
        conn.close()

        SqliteTaskStore(db_path, prefix="gza")

        conn = sqlite3.connect(db_path)
        version = conn.execute("SELECT version FROM schema_version").fetchone()[0]
        tables = {
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='task_comments'"
            ).fetchall()
        }
        conn.close()

        assert version == SCHEMA_VERSION
        assert "task_comments" in tables

    def test_open_current_v32_db_repairs_missing_execution_mode_column(self, tmp_path: Path) -> None:
        """Opening an already-v32 DB should repair missing tasks.execution_mode."""
        import sqlite3

        db_path = tmp_path / "test.db"
        SqliteTaskStore(db_path, prefix="gza")

        _drop_tasks_column(db_path, "execution_mode")

        SqliteTaskStore(db_path, prefix="gza")

        conn = sqlite3.connect(db_path)
        columns = {row[1] for row in conn.execute("PRAGMA table_info(tasks)")}
        conn.close()

        assert "execution_mode" in columns

    def test_open_current_v32_db_repairs_missing_urgent_bumped_at_column(self, tmp_path: Path) -> None:
        """Opening an already-v32 DB should repair missing tasks.urgent_bumped_at."""
        import sqlite3

        db_path = tmp_path / "test.db"
        SqliteTaskStore(db_path, prefix="gza")

        _drop_tasks_column(db_path, "urgent_bumped_at")

        SqliteTaskStore(db_path, prefix="gza")

        conn = sqlite3.connect(db_path)
        columns = {row[1] for row in conn.execute("PRAGMA table_info(tasks)")}
        conn.close()

        assert "urgent_bumped_at" in columns

    def test_open_current_db_repairs_missing_create_pr_column(self, tmp_path: Path) -> None:
        """Opening a current DB should repair missing tasks.create_pr and restore writes."""
        import sqlite3

        db_path = tmp_path / "test.db"
        SqliteTaskStore(db_path, prefix="gza")

        _drop_tasks_column(db_path, "create_pr")

        repaired_store = SqliteTaskStore(db_path, prefix="gza")
        created = repaired_store.add("Task after create_pr repair", create_pr=True)

        conn = sqlite3.connect(db_path)
        columns = {row[1] for row in conn.execute("PRAGMA table_info(tasks)")}
        conn.close()

        assert "create_pr" in columns
        reloaded = repaired_store.get(created.id)
        assert reloaded is not None
        assert reloaded.create_pr is True

    def test_open_current_db_repairs_missing_pr_state_column(self, tmp_path: Path) -> None:
        """Opening a current DB should repair missing tasks.pr_state and preserve PR cache writes."""
        import sqlite3

        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path, prefix="gza")
        task = store.add("Task before pr_state repair")
        _drop_tasks_column(db_path, "pr_state")

        repaired_store = SqliteTaskStore(db_path, prefix="gza")
        task = repaired_store.get(task.id)
        assert task is not None
        task.pr_state = "open"
        repaired_store.update(task)

        conn = sqlite3.connect(db_path)
        columns = {row[1] for row in conn.execute("PRAGMA table_info(tasks)")}
        conn.close()

        assert "pr_state" in columns
        reloaded = repaired_store.get(task.id)
        assert reloaded is not None
        assert reloaded.pr_state == "open"

    def test_open_current_db_repairs_missing_pr_last_synced_at_column(self, tmp_path: Path) -> None:
        """Opening a current DB should repair missing tasks.pr_last_synced_at and preserve timestamps."""
        import sqlite3

        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path, prefix="gza")
        task = store.add("Task before pr_last_synced_at repair")
        _drop_tasks_column(db_path, "pr_last_synced_at")

        repaired_store = SqliteTaskStore(db_path, prefix="gza")
        task = repaired_store.get(task.id)
        assert task is not None
        task.pr_last_synced_at = datetime.now(UTC)
        repaired_store.update(task)

        conn = sqlite3.connect(db_path)
        columns = {row[1] for row in conn.execute("PRAGMA table_info(tasks)")}
        conn.close()

        assert "pr_last_synced_at" in columns
        reloaded = repaired_store.get(task.id)
        assert reloaded is not None
        assert reloaded.pr_last_synced_at is not None

    def test_open_current_db_repairs_missing_sync_last_synced_at_column(self, tmp_path: Path) -> None:
        """Opening a current DB should repair missing tasks.sync_last_synced_at and preserve timestamps."""
        import sqlite3

        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path, prefix="gza")
        task = store.add("Task before sync_last_synced_at repair")
        _drop_tasks_column(db_path, "sync_last_synced_at")

        repaired_store = SqliteTaskStore(db_path, prefix="gza")
        task = repaired_store.get(task.id)
        assert task is not None
        task.sync_last_synced_at = datetime.now(UTC)
        repaired_store.update(task)

        conn = sqlite3.connect(db_path)
        columns = {row[1] for row in conn.execute("PRAGMA table_info(tasks)")}
        conn.close()

        assert "sync_last_synced_at" in columns
        reloaded = repaired_store.get(task.id)
        assert reloaded is not None
        assert reloaded.sync_last_synced_at is not None

    def test_open_current_v32_db_repairs_missing_task_comments_source_column(self, tmp_path: Path) -> None:
        """Opening an already-v32 DB should repair missing task_comments.source."""
        import sqlite3

        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path, prefix="gza")
        task = store.add("Task before task_comments source damage")
        store.add_comment(task.id, "Existing comment before repair", source="direct")

        _drop_task_comments_column(db_path, "source")

        repaired_store = SqliteTaskStore(db_path, prefix="gza")
        repaired_store.add_comment(task.id, "Comment after repair", source="github")

        conn = sqlite3.connect(db_path)
        columns = {row[1] for row in conn.execute("PRAGMA table_info(task_comments)")}
        conn.close()

        assert "source" in columns
        comments = repaired_store.get_comments(task.id)
        assert len(comments) == 2
        assert comments[0].source == "direct"
        assert comments[1].source == "github"

    def test_open_current_v32_db_missing_execution_mode_column_fails_on_read_only_db(
        self, tmp_path: Path
    ) -> None:
        """Read-only v32 damage should fail early with SchemaIntegrityError for execution_mode."""
        db_path = tmp_path / "test.db"
        SqliteTaskStore(db_path, prefix="gza")
        _drop_tasks_column(db_path, "execution_mode")

        db_path.chmod(0o444)
        try:
            with pytest.raises(
                SchemaIntegrityError,
                match=r"required column tasks\.execution_mode",
            ):
                SqliteTaskStore(db_path, prefix="gza")
        finally:
            db_path.chmod(0o644)

    def test_open_current_v32_db_missing_urgent_bumped_at_column_fails_on_read_only_db(
        self, tmp_path: Path
    ) -> None:
        """Read-only v32 damage should fail early with SchemaIntegrityError for urgent_bumped_at."""
        db_path = tmp_path / "test.db"
        SqliteTaskStore(db_path, prefix="gza")
        _drop_tasks_column(db_path, "urgent_bumped_at")

        db_path.chmod(0o444)
        try:
            with pytest.raises(
                SchemaIntegrityError,
                match=r"required column tasks\.urgent_bumped_at",
            ):
                SqliteTaskStore(db_path, prefix="gza")
        finally:
            db_path.chmod(0o644)

    def test_open_current_db_missing_create_pr_column_fails_on_read_only_db(
        self, tmp_path: Path
    ) -> None:
        """Read-only current-schema damage should fail early with SchemaIntegrityError for create_pr."""
        db_path = tmp_path / "test.db"
        SqliteTaskStore(db_path, prefix="gza")
        _drop_tasks_column(db_path, "create_pr")

        db_path.chmod(0o444)
        try:
            with pytest.raises(
                SchemaIntegrityError,
                match=r"required column tasks\.create_pr",
            ):
                SqliteTaskStore(db_path, prefix="gza")
        finally:
            db_path.chmod(0o644)

    def test_open_current_v32_db_missing_task_comments_source_fails_on_read_only_db(
        self, tmp_path: Path
    ) -> None:
        """Read-only v32 damage should fail early with SchemaIntegrityError for task_comments.source."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path, prefix="gza")
        task = store.add("Task before read-only source damage")
        store.add_comment(task.id, "Existing comment", source="direct")
        _drop_task_comments_column(db_path, "source")

        db_path.chmod(0o444)
        try:
            with pytest.raises(
                SchemaIntegrityError,
                match=r"required column task_comments\.source",
            ):
                SqliteTaskStore(db_path, prefix="gza")
        finally:
            db_path.chmod(0o644)

    def test_query_only_open_current_db_missing_execution_mode_warns_but_reads(
        self, tmp_path: Path
    ) -> None:
        """Query-only open should warn instead of repairing missing execution_mode on a frozen DB."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path, prefix="gza")
        task = store.add("Task with query-only execution-mode damage")
        assert task.id is not None

        _drop_tasks_column(db_path, "execution_mode")

        db_path.chmod(0o444)
        try:
            query_store = SqliteTaskStore(db_path, prefix="gza", open_mode="query_only")
            reloaded = query_store.get(task.id)
        finally:
            db_path.chmod(0o644)

        assert reloaded is not None
        assert reloaded.prompt == "Task with query-only execution-mode damage"
        assert any("tasks.execution_mode" in warning for warning in query_store.startup_warnings())

    def test_query_only_open_pre_v40_db_missing_completion_reason_reads_with_null(
        self, tmp_path: Path
    ) -> None:
        """Query-only open should read v39 snapshots without forcing the v40 completion_reason migration."""
        import sqlite3

        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path, prefix="gza")
        task = store.add("Task before v40 completion_reason")
        task.status = "completed"
        task.completed_at = datetime.now(UTC)
        store.update(task)

        _drop_tasks_column(db_path, "completion_reason")
        conn = sqlite3.connect(db_path)
        conn.execute("UPDATE schema_version SET version = 39")
        conn.commit()
        conn.close()

        db_path.chmod(0o444)
        try:
            query_store = SqliteTaskStore(db_path, prefix="gza", open_mode="query_only")
            reloaded = query_store.get(task.id)
            history = query_store.get_history(limit=None)
        finally:
            db_path.chmod(0o644)

        assert reloaded is not None
        assert reloaded.prompt == "Task before v40 completion_reason"
        assert reloaded.completion_reason is None
        assert [row.id for row in history] == [task.id]
        assert history[0].completion_reason is None
        assert any("tasks.completion_reason" in warning for warning in query_store.startup_warnings())

    def test_query_only_open_pre_v41_db_missing_recovery_origin_reads_with_null(
        self, tmp_path: Path
    ) -> None:
        """Query-only open should read v40 snapshots without forcing the v41 recovery_origin migration."""
        import sqlite3

        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path, prefix="gza")
        task = store.add("Task before v41 recovery_origin")
        assert task.id is not None
        task.status = "completed"
        task.completed_at = datetime.now(UTC)
        store.update(task)

        _drop_tasks_column(db_path, "recovery_origin")
        with sqlite3.connect(db_path) as conn:
            conn.execute("UPDATE schema_version SET version = 40")
            conn.commit()

        db_path.chmod(0o444)
        try:
            query_store = SqliteTaskStore(db_path, prefix="gza", open_mode="query_only")
            reloaded = query_store.get(task.id)
            history = query_store.get_history(limit=None)
        finally:
            db_path.chmod(0o644)

        assert reloaded is not None
        assert reloaded.prompt == "Task before v41 recovery_origin"
        assert reloaded.recovery_origin is None
        assert [row.id for row in history] == [task.id]
        assert history[0].recovery_origin is None
        assert any("tasks.recovery_origin" in warning for warning in query_store.startup_warnings())

    def test_query_only_open_pre_v45_db_missing_trigger_source_reads_with_null(
        self, tmp_path: Path
    ) -> None:
        """Query-only open should read v44 snapshots without forcing the v45 trigger_source migration."""
        import sqlite3

        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path, prefix="gza")
        task = store.add("Task before v45 trigger_source", trigger_source="manual")
        assert task.id is not None
        task.status = "completed"
        task.completed_at = datetime.now(UTC)
        store.update(task)

        _drop_tasks_column(db_path, "trigger_source")
        with sqlite3.connect(db_path) as conn:
            conn.execute("UPDATE schema_version SET version = 44")
            conn.commit()

        db_path.chmod(0o444)
        try:
            query_store = SqliteTaskStore(db_path, prefix="gza", open_mode="query_only")
            reloaded = query_store.get(task.id)
            history = query_store.get_history(limit=None)
        finally:
            db_path.chmod(0o644)

        assert reloaded is not None
        assert reloaded.prompt == "Task before v45 trigger_source"
        assert reloaded.trigger_source is None
        assert [row.id for row in history] == [task.id]
        assert history[0].trigger_source is None
        assert any("tasks.trigger_source" in warning for warning in query_store.startup_warnings())

    def test_query_only_open_pre_v47_db_missing_review_scope_reads_with_null(
        self, tmp_path: Path
    ) -> None:
        """Query-only open should read v46 snapshots without forcing the v47 review_scope migration."""
        import sqlite3

        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path, prefix="gza")
        task = store.add(
            "Task before v47 review_scope",
            task_type="implement",
            review_scope="slice F-A1 + F-A2: authoritative metadata",
        )
        assert task.id is not None
        task.status = "completed"
        task.completed_at = datetime.now(UTC)
        store.update(task)

        _drop_tasks_column(db_path, "review_scope")
        with sqlite3.connect(db_path) as conn:
            conn.execute("UPDATE schema_version SET version = 46")
            conn.commit()

        db_path.chmod(0o444)
        try:
            query_store = SqliteTaskStore(db_path, prefix="gza", open_mode="query_only")
            reloaded = query_store.get(task.id)
            history = query_store.get_history(limit=None)
        finally:
            db_path.chmod(0o644)

        assert reloaded is not None
        assert reloaded.prompt == "Task before v47 review_scope"
        assert reloaded.review_scope is None
        assert [row.id for row in history] == [task.id]
        assert history[0].review_scope is None
        assert any("tasks.review_scope" in warning for warning in query_store.startup_warnings())

    def test_auto_migration_v47_to_v49_adds_merge_source_and_preserves_existing_rows(
        self, tmp_path: Path
    ) -> None:
        import sqlite3

        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path, prefix="gza")
        task = store.add("Task before v48 merge source", model="claude-sonnet-4-6")
        store.mark_completed(task, has_commits=True, branch="feature/pre-v48")
        assert task.id is not None
        unit = store.resolve_merge_unit_for_task(task.id)
        assert unit is not None

        _drop_tasks_column(db_path, "model_is_explicit")
        _drop_merge_units_column(db_path, "merge_source")
        with sqlite3.connect(db_path) as conn:
            conn.execute("UPDATE schema_version SET version = 47")
            conn.commit()

        migrated_store = SqliteTaskStore(db_path, prefix="gza")
        reloaded = migrated_store.get(task.id)
        reloaded_unit = migrated_store.resolve_merge_unit_for_task(task.id)
        assert reloaded is not None
        assert reloaded_unit is not None
        assert reloaded.model == "claude-sonnet-4-6"
        assert reloaded.model_is_explicit is False
        assert reloaded_unit.merge_source is None

        with sqlite3.connect(db_path) as conn:
            task_columns = {row[1] for row in conn.execute("PRAGMA table_info(tasks)").fetchall()}
            unit_columns = {row[1] for row in conn.execute("PRAGMA table_info(merge_units)").fetchall()}
            version = conn.execute("SELECT version FROM schema_version").fetchone()[0]
            stored_model_explicit = conn.execute(
                "SELECT model_is_explicit FROM tasks WHERE project_id = ? AND id = ?",
                ("default", task.id),
            ).fetchone()[0]
            stored_merge_source = conn.execute(
                "SELECT merge_source FROM merge_units WHERE project_id = ? AND id = ?",
                ("default", unit.id),
            ).fetchone()[0]

        assert "model_is_explicit" in task_columns
        assert "merge_source" in unit_columns
        assert version == SCHEMA_VERSION
        assert stored_model_explicit == 0
        assert stored_merge_source is None

    def test_current_v48_db_missing_merge_source_repairs_and_supports_provenance_io(
        self, tmp_path: Path
    ) -> None:
        import sqlite3

        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path, prefix="gza")
        task = store.add("Task on current v48 store", model="claude-sonnet-4-6")
        store.mark_completed(task, has_commits=True, branch="feature/current-v48")
        assert task.id is not None
        unit = store.resolve_merge_unit_for_task(task.id)
        assert unit is not None

        _drop_merge_units_column(db_path, "merge_source")
        with sqlite3.connect(db_path) as conn:
            conn.execute("UPDATE schema_version SET version = 48")
            conn.commit()

        repaired_store = SqliteTaskStore(db_path, prefix="gza")
        repaired_store.set_merge_unit_state(unit.id, "merged", merge_source="manual")
        merged_units = repaired_store.list_merged_units(source="manual")

        with sqlite3.connect(db_path) as conn:
            unit_columns = {row[1] for row in conn.execute("PRAGMA table_info(merge_units)").fetchall()}
            index_names = {row[1] for row in conn.execute("PRAGMA index_list(merge_units)").fetchall()}
            version = conn.execute("SELECT version FROM schema_version").fetchone()[0]
            stored_merge_source = conn.execute(
                "SELECT merge_source FROM merge_units WHERE project_id = ? AND id = ?",
                ("default", unit.id),
            ).fetchone()[0]

        assert "merge_source" in unit_columns
        assert "idx_merge_units_project_state_source" in index_names
        assert version == SCHEMA_VERSION
        assert stored_merge_source == "manual"
        assert [merged.id for merged in merged_units] == [unit.id]
        assert merged_units[0].merge_source == "manual"

    def test_auto_migration_v49_to_v50_adds_task_artifacts_and_store_accessors(
        self, tmp_path: Path
    ) -> None:
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path, prefix="gza")
        task = store.add("Task before v50 artifacts")
        assert task.id is not None

        with sqlite3.connect(db_path) as conn:
            conn.execute("DROP TABLE task_artifacts")
            conn.execute("UPDATE schema_version SET version = 49")
            conn.commit()

        migrated_store = SqliteTaskStore(db_path, prefix="gza")
        stored = migrated_store.add_artifact(
            task.id,
            kind="verify_command_output",
            label="verify",
            path=".gza/artifacts/gza-1/verify.txt",
            byte_size=7,
            sha256="abc123",
            created_at=datetime(2026, 1, 5, 12, 30, 0),
            producer="review_verify",
            command="./bin/tests",
            status="failed",
            exit_status="1",
            head_sha="deadbeef",
            metadata={"scope": "pkg"},
        )

        listed = migrated_store.list_artifacts(task.id)
        fetched = migrated_store.get_artifact(stored.id, task_id=task.id)

        with sqlite3.connect(db_path) as conn:
            columns = {row[1] for row in conn.execute("PRAGMA table_info(task_artifacts)").fetchall()}
            index_names = {row[1] for row in conn.execute("PRAGMA index_list(task_artifacts)").fetchall()}
            version = conn.execute("SELECT version FROM schema_version").fetchone()[0]
            stored_created_at = conn.execute(
                "SELECT created_at FROM task_artifacts WHERE project_id = ? AND id = ?",
                ("default", stored.id),
            ).fetchone()[0]

        assert "created_at" in columns
        assert "idx_task_artifacts_project_task_created" in index_names
        assert "idx_task_artifacts_project_task_kind_created" in index_names
        assert version == SCHEMA_VERSION
        assert stored.created_at == datetime(2026, 1, 5, 12, 30, 0, tzinfo=UTC)
        assert stored_created_at == "2026-01-05T12:30:00+00:00"
        assert [artifact.id for artifact in listed] == [stored.id]
        assert fetched == stored
        assert stored.metadata == {"scope": "pkg"}

    def test_current_v50_db_missing_task_artifact_column_and_indexes_repairs_in_place(
        self, tmp_path: Path
    ) -> None:
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path, prefix="gza")
        task = store.add("Task on current v50 artifact store")
        assert task.id is not None
        artifact = store.add_artifact(
            task.id,
            kind="verify_command_output",
            label="verify",
            path=".gza/artifacts/gza-1/verify.txt",
            byte_size=12,
            sha256="feedface",
            created_at=datetime(2026, 2, 1, 9, 0, tzinfo=UTC),
            producer="review_verify",
        )

        _drop_task_artifacts_column(db_path, "metadata_json")
        with sqlite3.connect(db_path) as conn:
            conn.execute("DROP INDEX IF EXISTS idx_task_artifacts_project_task_created")
            conn.execute("DROP INDEX IF EXISTS idx_task_artifacts_project_task_kind_created")
            conn.execute("UPDATE schema_version SET version = 50")
            conn.commit()

        repaired_store = SqliteTaskStore(db_path, prefix="gza")
        repaired = repaired_store.get_artifact(artifact.id, task_id=task.id)

        with sqlite3.connect(db_path) as conn:
            columns = {row[1] for row in conn.execute("PRAGMA table_info(task_artifacts)").fetchall()}
            index_names = {row[1] for row in conn.execute("PRAGMA index_list(task_artifacts)").fetchall()}
            version = conn.execute("SELECT version FROM schema_version").fetchone()[0]

        assert repaired is not None
        assert repaired.id == artifact.id
        assert repaired.path == artifact.path
        assert repaired.kind == artifact.kind
        assert repaired.byte_size == artifact.byte_size
        assert repaired.sha256 == artifact.sha256
        assert "metadata_json" in columns
        assert "idx_task_artifacts_project_task_created" in index_names
        assert "idx_task_artifacts_project_task_kind_created" in index_names
        assert version == SCHEMA_VERSION

    def test_list_artifacts_filters_by_kind_and_orders_newest_first(self, tmp_path: Path) -> None:
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path, prefix="gza")
        task = store.add("Task with multiple artifacts")
        assert task.id is not None

        older = store.add_artifact(
            task.id,
            kind="verify_command_output",
            label="verify",
            path=".gza/artifacts/gza-1/verify-old.txt",
            byte_size=3,
            sha256="111",
            created_at=datetime(2026, 3, 1, 8, 0, 0),
        )
        different_kind = store.add_artifact(
            task.id,
            kind="build_output",
            label="build",
            path=".gza/artifacts/gza-1/build.txt",
            byte_size=4,
            sha256="222",
            created_at=datetime(2026, 3, 1, 9, 0, 0),
        )
        newer = store.add_artifact(
            task.id,
            kind="verify_command_output",
            label="verify",
            path=".gza/artifacts/gza-1/verify-new.txt",
            byte_size=5,
            sha256="333",
            created_at=datetime(2026, 3, 1, 10, 0, 0),
        )

        all_artifacts = store.list_artifacts(task.id)
        verify_only = store.list_artifacts(task.id, kind="verify_command_output")

        assert [artifact.id for artifact in all_artifacts] == [newer.id, different_kind.id, older.id]
        assert [artifact.id for artifact in verify_only] == [newer.id, older.id]
        assert older.created_at == datetime(2026, 3, 1, 8, 0, 0, tzinfo=UTC)

    def test_add_artifact_accepts_live_and_archive_paths_and_rejects_escaped_paths(self, tmp_path: Path) -> None:
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path, prefix="gza")
        task = store.add("Task with validated artifact paths")
        assert task.id is not None

        live = store.add_artifact(
            task.id,
            kind="verify_command_output",
            label="verify",
            path=".gza/artifacts/gza-1/verify.txt",
            byte_size=3,
            sha256="111",
        )
        archived = store.add_artifact(
            task.id,
            kind="verify_command_output",
            label="verify",
            path=".gza/archives/artifacts/gza-1/verify.txt",
            byte_size=4,
            sha256="222",
        )

        assert live.path == ".gza/artifacts/gza-1/verify.txt"
        assert archived.path == ".gza/archives/artifacts/gza-1/verify.txt"

        with pytest.raises(ValueError, match="artifact path"):
            store.add_artifact(
                task.id,
                kind="verify_command_output",
                label="verify",
                path="../outside.txt",
                byte_size=5,
                sha256="333",
            )

    def test_query_only_open_pre_v49_db_missing_merge_source_warns_and_degrades_source_filter(
        self, tmp_path: Path
    ) -> None:
        import sqlite3

        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path, prefix="gza")
        task = store.add("Task before v49 merge source", model="claude-sonnet-4-6")
        store.mark_completed(task, has_commits=True, branch="feature/pre-v49-query-only")
        assert task.id is not None
        unit = store.resolve_merge_unit_for_task(task.id)
        assert unit is not None
        store.set_merge_unit_state(unit.id, "merged")

        _drop_merge_units_column(db_path, "merge_source")
        with sqlite3.connect(db_path) as conn:
            conn.execute("UPDATE schema_version SET version = 48")
            conn.commit()

        db_path.chmod(0o444)
        try:
            query_store = SqliteTaskStore(db_path, prefix="gza", open_mode="query_only")
            merged_units = query_store.list_merged_units()
            filtered_units = query_store.list_merged_units(source="manual")
        finally:
            db_path.chmod(0o644)

        assert [merged.id for merged in merged_units] == [unit.id]
        assert merged_units[0].merge_source is None
        assert filtered_units == []
        assert any("merge_units.merge_source" in warning for warning in query_store.startup_warnings())

    def test_query_only_open_pre_v43_db_missing_changed_diff_reads_with_null(
        self, tmp_path: Path
    ) -> None:
        """Query-only open should read v42 snapshots without forcing the v43 changed_diff migration."""
        import sqlite3

        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path, prefix="gza")
        task = store.add("Rebase before v43 changed_diff", task_type="rebase")
        assert task.id is not None
        task.status = "completed"
        task.completed_at = datetime.now(UTC)
        store.update(task)

        _drop_tasks_column(db_path, "changed_diff")
        with sqlite3.connect(db_path) as conn:
            conn.execute("UPDATE schema_version SET version = 42")
            conn.commit()

        db_path.chmod(0o444)
        try:
            query_store = SqliteTaskStore(db_path, prefix="gza", open_mode="query_only")
            reloaded = query_store.get(task.id)
            history = query_store.get_history(limit=None)
        finally:
            db_path.chmod(0o644)

        assert reloaded is not None
        assert reloaded.prompt == "Rebase before v43 changed_diff"
        assert reloaded.changed_diff is None
        assert [row.id for row in history] == [task.id]
        assert history[0].changed_diff is None
        assert any("tasks.changed_diff" in warning for warning in query_store.startup_warnings())

    def test_query_only_open_pre_v45_db_missing_auto_implement_defaults_enabled(
        self, tmp_path: Path
    ) -> None:
        """Query-only open should read v44 snapshots without forcing the v45 auto_implement migration."""
        import sqlite3

        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path, prefix="gza")
        task = store.add("Plan before v45 auto_implement", task_type="plan")
        assert task.id is not None
        task.status = "completed"
        task.completed_at = datetime.now(UTC)
        store.update(task)

        _drop_tasks_column(db_path, "auto_implement")
        with sqlite3.connect(db_path) as conn:
            conn.execute("UPDATE schema_version SET version = 44")
            conn.commit()

        db_path.chmod(0o444)
        try:
            query_store = SqliteTaskStore(db_path, prefix="gza", open_mode="query_only")
            reloaded = query_store.get(task.id)
            history = query_store.get_history(limit=None)
        finally:
            db_path.chmod(0o644)

        assert reloaded is not None
        assert reloaded.prompt == "Plan before v45 auto_implement"
        assert reloaded.auto_implement is None
        assert [row.id for row in history] == [task.id]
        assert history[0].auto_implement is None
        assert any("tasks.auto_implement" in warning for warning in query_store.startup_warnings())

    def test_query_only_open_current_db_missing_create_pr_fails_closed(
        self, tmp_path: Path
    ) -> None:
        """Query-only open should fail closed when the core tasks.create_pr column is missing."""
        db_path = tmp_path / "test.db"
        SqliteTaskStore(db_path, prefix="gza")
        _drop_tasks_column(db_path, "create_pr")

        db_path.chmod(0o444)
        try:
            with pytest.raises(
                SchemaIntegrityError,
                match=r"required column tasks\.create_pr",
            ):
                SqliteTaskStore(db_path, prefix="gza", open_mode="query_only")
        finally:
            db_path.chmod(0o644)

    def test_query_only_open_current_db_missing_pr_state_fails_closed(
        self, tmp_path: Path
    ) -> None:
        """Query-only open should fail closed when tasks.pr_state is missing."""
        db_path = tmp_path / "test.db"
        SqliteTaskStore(db_path, prefix="gza")
        _drop_tasks_column(db_path, "pr_state")

        db_path.chmod(0o444)
        try:
            with pytest.raises(
                SchemaIntegrityError,
                match=r"required column tasks\.pr_state",
            ):
                SqliteTaskStore(db_path, prefix="gza", open_mode="query_only")
        finally:
            db_path.chmod(0o644)
    def test_query_only_open_current_db_missing_task_comments_source_warns_and_reads_comments(
        self, tmp_path: Path
    ) -> None:
        """Query-only open should keep comments readable when task_comments.source is missing."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path, prefix="gza")
        task = store.add("Task with query-only comment damage")
        assert task.id is not None
        store.add_comment(task.id, "Existing comment", source="direct")

        _drop_task_comments_column(db_path, "source")

        db_path.chmod(0o444)
        try:
            query_store = SqliteTaskStore(db_path, prefix="gza", open_mode="query_only")
            comments = query_store.get_comments(task.id)
        finally:
            db_path.chmod(0o644)

        assert len(comments) == 1
        assert comments[0].content == "Existing comment"
        assert comments[0].source == "direct"
        assert any("task_comments" in warning for warning in query_store.startup_warnings())

    def test_query_only_open_current_db_missing_task_comments_id_warns_and_degrades_comments(
        self, tmp_path: Path
    ) -> None:
        """Query-only open should degrade comments when task_comments.id is missing."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path, prefix="gza")
        task = store.add("Task with query-only comment id damage")
        assert task.id is not None
        store.add_comment(task.id, "Existing comment", source="direct")

        _drop_task_comments_column(db_path, "id")

        db_path.chmod(0o444)
        try:
            query_store = SqliteTaskStore(db_path, prefix="gza", open_mode="query_only")
            comments = query_store.get_comments(task.id)
        finally:
            db_path.chmod(0o644)

        assert comments == []
        assert any("task_comments" in warning for warning in query_store.startup_warnings())

    def test_query_only_open_current_db_missing_run_steps_project_id_warns_and_degrades_counts(
        self, tmp_path: Path
    ) -> None:
        """Query-only open should warn and skip step counts when run_steps.project_id is missing."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path, prefix="gza")
        task = store.add("Task with query-only run-steps damage")
        assert task.id is not None
        store.emit_step(task.id, "Existing step", provider="claude")

        _drop_run_steps_column(db_path, "project_id")

        db_path.chmod(0o444)
        try:
            query_store = SqliteTaskStore(db_path, prefix="gza", open_mode="query_only")
            count = query_store.count_steps(task.id)
        finally:
            db_path.chmod(0o644)

        assert count == 0
        assert any("run_steps.project_id" in warning for warning in query_store.startup_warnings())
    def test_auto_migration_v30_failure_does_not_advance_schema_version(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Migration failures must not stamp schema_version forward when v30 SQL fails."""
        import sqlite3

        import gza.db as db_module

        db_path = tmp_path / "test.db"
        _make_v29_db_without_urgent_bumped_at(db_path)

        broken_migrations = [
            (version, "ALTER TABLE tasks ADD COLUMN ;" if version == 30 else sql)
            for version, sql in db_module._MIGRATIONS
        ]
        monkeypatch.setattr(db_module, "_MIGRATIONS", broken_migrations)

        with pytest.raises(sqlite3.OperationalError):
            SqliteTaskStore(db_path, prefix="gza")

        conn = sqlite3.connect(db_path)
        version = conn.execute("SELECT version FROM schema_version").fetchone()[0]
        columns = {row[1] for row in conn.execute("PRAGMA table_info(tasks)")}
        conn.close()

        assert version == 29
        assert "urgent_bumped_at" not in columns

    def test_auto_migration_v32_validation_requires_task_comments_source(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """v32 auto-migration must fail if task_comments.source is missing and keep schema_version at v31."""
        import sqlite3

        import gza.db as db_module

        db_path = tmp_path / "test.db"
        conn = sqlite3.connect(db_path)
        conn.execute("CREATE TABLE schema_version (version INTEGER PRIMARY KEY)")
        conn.execute("INSERT INTO schema_version (version) VALUES (31)")
        conn.execute(
            """
            CREATE TABLE tasks (
                id TEXT PRIMARY KEY,
                prompt TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending',
                task_type TEXT NOT NULL DEFAULT 'implement',
                created_at TEXT NOT NULL
            )
            """
        )
        conn.commit()
        conn.close()

        broken_migrations = [
            (
                version,
                """
                CREATE TABLE IF NOT EXISTS task_comments (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    task_id TEXT NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
                    content TEXT NOT NULL,
                    author TEXT,
                    created_at TEXT NOT NULL,
                    resolved_at TEXT
                );
                """ if version == 32 else sql,
            )
            for version, sql in db_module._MIGRATIONS
        ]
        monkeypatch.setattr(db_module, "_MIGRATIONS", broken_migrations)

        with pytest.raises(
            RuntimeError,
            match=r"Auto-migration to v32 incomplete: missing required column task_comments\.source",
        ):
            SqliteTaskStore(db_path, prefix="gza")

        conn = sqlite3.connect(db_path)
        version = conn.execute("SELECT version FROM schema_version").fetchone()[0]
        conn.close()
        assert version == 31

    def test_run_v27_migration_drops_cycle_schema_and_preserves_task_data(self, tmp_path: Path) -> None:
        import sqlite3

        db_path = tmp_path / "test.db"
        _make_v24_db(db_path)

        conn = sqlite3.connect(db_path)
        conn.execute(
            "INSERT INTO tasks (id, prompt, created_at) VALUES (1, 'parent', '2024-01-01T00:00:00+00:00')"
        )
        conn.execute(
            "INSERT INTO tasks (id, prompt, based_on, depends_on, created_at, cycle_id, cycle_iteration_index, cycle_role) "
            "VALUES (2, 'child', 1, 1, '2024-01-01T00:00:00+00:00', 9, 0, 'review')"
        )
        conn.commit()
        conn.close()

        run_v25_migration(db_path, "gza")
        run_v26_migration(db_path)

        conn = sqlite3.connect(db_path)
        before_count = conn.execute("SELECT COUNT(*) FROM tasks").fetchone()[0]
        conn.close()

        run_v27_migration(db_path)

        conn = sqlite3.connect(db_path)
        tables = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        indexes = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='index'")}
        columns = {row[1] for row in conn.execute("PRAGMA table_info(tasks)")}
        after_count = conn.execute("SELECT COUNT(*) FROM tasks").fetchone()[0]
        conn.close()

        assert "task_cycles" not in tables
        assert "task_cycle_iterations" not in tables
        assert "idx_tasks_cycle_id" not in indexes
        assert "cycle_id" not in columns
        assert "cycle_iteration_index" not in columns
        assert "cycle_role" not in columns
        assert before_count == after_count

        store = SqliteTaskStore(db_path, prefix="gza")
        child = store.get("gza-2")
        assert child is not None
        assert child.based_on == "gza-1"
        assert child.depends_on == "gza-1"

    def test_run_v27_migration_defaults_missing_legacy_create_pr_to_false(self, tmp_path: Path) -> None:
        import sqlite3

        db_path = tmp_path / "test.db"
        _make_v24_db(db_path)

        conn = sqlite3.connect(db_path)
        conn.execute(
            "INSERT INTO tasks (id, prompt, created_at) VALUES (1, 'parent', '2024-01-01T00:00:00+00:00')"
        )
        conn.commit()
        conn.close()

        run_v25_migration(db_path, "gza")
        run_v26_migration(db_path)
        _drop_tasks_column(db_path, "create_pr")

        run_v27_migration(db_path)

        conn = sqlite3.connect(db_path)
        columns = {row[1] for row in conn.execute("PRAGMA table_info(tasks)")}
        version = conn.execute("SELECT version FROM schema_version").fetchone()[0]
        conn.close()

        assert version == 27
        assert "create_pr" in columns

        store = SqliteTaskStore(db_path, prefix="gza")
        migrated = store.get("gza-1")
        assert migrated is not None
        assert migrated.create_pr is False

    def test_run_v27_migration_defaults_missing_legacy_attach_columns_to_null(self, tmp_path: Path) -> None:
        import sqlite3

        db_path = tmp_path / "test.db"
        _make_v24_db(db_path)

        conn = sqlite3.connect(db_path)
        conn.execute(
            "INSERT INTO tasks (id, prompt, created_at) VALUES (1, 'parent', '2024-01-01T00:00:00+00:00')"
        )
        conn.commit()
        conn.close()

        run_v25_migration(db_path, "gza")
        run_v26_migration(db_path)
        _drop_tasks_column(db_path, "attach_count")
        _drop_tasks_column(db_path, "attach_duration_seconds")

        run_v27_migration(db_path)

        conn = sqlite3.connect(db_path)
        columns = {
            row[1]: row[3]
            for row in conn.execute("PRAGMA table_info(tasks)")
        }
        version = conn.execute("SELECT version FROM schema_version").fetchone()[0]
        row = conn.execute(
            "SELECT attach_count, attach_duration_seconds FROM tasks WHERE id = ?",
            ("gza-1",),
        ).fetchone()
        conn.close()

        assert version == 27
        assert "attach_count" in columns
        assert "attach_duration_seconds" in columns
        assert columns["attach_count"] == 0
        assert columns["attach_duration_seconds"] == 0
        assert row == (None, None)

    def test_run_v27_migration_preserves_legacy_attach_values_when_columns_exist(self, tmp_path: Path) -> None:
        import sqlite3

        db_path = tmp_path / "test.db"
        _make_v24_db(db_path)

        conn = sqlite3.connect(db_path)
        conn.execute(
            "INSERT INTO tasks (id, prompt, created_at) VALUES (1, 'parent', '2024-01-01T00:00:00+00:00')"
        )
        conn.commit()
        conn.close()

        run_v25_migration(db_path, "gza")
        run_v26_migration(db_path)

        conn = sqlite3.connect(db_path)
        conn.execute(
            "UPDATE tasks SET attach_count = ?, attach_duration_seconds = ? WHERE id = ?",
            (3, 12.5, "gza-1"),
        )
        conn.commit()
        conn.close()

        run_v27_migration(db_path)

        conn = sqlite3.connect(db_path)
        version = conn.execute("SELECT version FROM schema_version").fetchone()[0]
        row = conn.execute(
            "SELECT attach_count, attach_duration_seconds FROM tasks WHERE id = ?",
            ("gza-1",),
        ).fetchone()
        conn.close()

        assert version == 27
        assert row == (3, 12.5)

class TestSyncCandidates:
    """Tests for bounded sync candidate selection."""

    def test_get_sync_candidates_includes_unmerged_open_pr_and_recent_pr_intent(self, tmp_path: Path) -> None:
        store = SqliteTaskStore(tmp_path / "test.db", prefix="gza")
        now = datetime.now(UTC)
        old = now - timedelta(days=90)

        unmerged = store.add("Unmerged task", task_type="implement")
        unmerged.status = "completed"
        unmerged.completed_at = now
        unmerged.branch = "feature/unmerged"
        unmerged.has_commits = True
        unmerged.merge_status = "unmerged"
        store.update(unmerged)

        open_pr = store.add("Open PR task", task_type="implement")
        open_pr.status = "completed"
        open_pr.completed_at = old
        open_pr.branch = "feature/open-pr"
        open_pr.has_commits = True
        open_pr.merge_status = "merged"
        open_pr.pr_number = 12
        open_pr.pr_state = "open"
        store.update(open_pr)

        recent_pr_intent = store.add("Recent PR intent", task_type="implement")
        recent_pr_intent.status = "completed"
        recent_pr_intent.completed_at = now
        recent_pr_intent.branch = "feature/recent-pr"
        recent_pr_intent.has_commits = True
        recent_pr_intent.merge_status = "merged"
        recent_pr_intent.create_pr = True
        store.update(recent_pr_intent)

        stale_closed = store.add("Stale closed PR", task_type="implement")
        stale_closed.status = "completed"
        stale_closed.completed_at = old
        stale_closed.branch = "feature/stale-closed"
        stale_closed.has_commits = True
        stale_closed.merge_status = "merged"
        stale_closed.pr_number = 44
        stale_closed.pr_state = "closed"
        store.update(stale_closed)

        candidate_ids = {task.id for task in store.get_sync_candidates(recent_days=30)}

        assert unmerged.id in candidate_ids
        assert open_pr.id in candidate_ids
        assert recent_pr_intent.id in candidate_ids
        assert stale_closed.id not in candidate_ids

    def test_get_sync_candidates_does_not_keep_old_merged_task_recent_after_resync(self, tmp_path: Path) -> None:
        store = SqliteTaskStore(tmp_path / "test.db", prefix="gza")
        old = datetime.now(UTC) - timedelta(days=60)

        task = store.add("Old merged task", task_type="implement")
        task.status = "completed"
        task.completed_at = old
        task.branch = "feature/old-merged"
        task.has_commits = True
        task.merge_status = "merged"
        task.merged_at = old
        task.pr_number = 77
        task.pr_state = "closed"
        store.update(task)

        git = Mock()
        git.default_branch.return_value = "main"
        git.branch_exists.return_value = True
        git.is_merged.return_value = True

        results, partial = sync_branch_cohorts(
            store,
            git,
            [BranchCohort(branch="feature/old-merged", tasks=(task,))],
            include_git=True,
            include_pr=False,
            dry_run=False,
            fetch_remote=False,
        )

        assert partial is False
        assert "marked merged" in results[0].actions

        refreshed = store.get(task.id)
        assert refreshed is not None
        assert refreshed.merged_at == old
        candidate_ids = {candidate.id for candidate in store.get_sync_candidates(recent_days=30)}
        assert task.id not in candidate_ids

    def test_get_sync_candidates_skips_recently_synced_branch_until_cooldown_expires(self, tmp_path: Path) -> None:
        store = SqliteTaskStore(tmp_path / "test.db", prefix="gza")
        now = datetime.now(UTC)

        task = store.add("Recently synced task", task_type="implement")
        task.status = "completed"
        task.completed_at = now - timedelta(days=1)
        task.branch = "feature/recent-sync"
        task.has_commits = True
        task.merge_status = "unmerged"
        task.sync_last_synced_at = now
        store.update(task)

        cached_candidate_ids = {
            candidate.id for candidate in store.get_sync_candidates(recent_days=30, cooldown_seconds=300)
        }
        uncached_candidate_ids = {
            candidate.id for candidate in store.get_sync_candidates(recent_days=30, cooldown_seconds=0)
        }

        assert task.id not in cached_candidate_ids
        assert task.id in uncached_candidate_ids

    def test_get_sync_candidates_unions_merge_units_and_legacy_rows_during_migration(self, tmp_path: Path) -> None:
        store = SqliteTaskStore(tmp_path / "test.db", prefix="gza")
        now = datetime.now(UTC)

        unit_task = store.add("Unit-backed task", task_type="implement")
        unit_task.status = "completed"
        unit_task.completed_at = now
        unit_task.branch = "feature/unit-backed"
        unit_task.has_commits = True
        unit_task.merge_status = "unmerged"
        store.update(unit_task)
        unit = store.get_or_create_merge_unit_for_task(unit_task)
        assert unit is not None

        legacy_task = store.add("Legacy task", task_type="implement")
        legacy_task.status = "completed"
        legacy_task.completed_at = now - timedelta(hours=1)
        legacy_task.branch = "feature/legacy-only"
        legacy_task.has_commits = True
        legacy_task.merge_status = "unmerged"
        store.update(legacy_task)

        candidate_ids = {task.id for task in store.get_sync_candidates(recent_days=30)}

        assert candidate_ids == {unit_task.id, legacy_task.id}
