"""Tests for database operations and task chaining."""

import tempfile
from pathlib import Path
from datetime import datetime, timedelta, timezone

import pytest

from gza.db import SCHEMA_VERSION, SqliteTaskStore, StepRef, Task


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
            create_review=True,
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
        assert retrieved.create_review is True
        assert retrieved.same_branch is True

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
        task1.completed_at = datetime.now(timezone.utc)
        store.update(task1)

        # Now task2 should be available
        next_task = store.get_next_pending()
        assert next_task is not None
        # Could be task2 or task3 depending on order
        assert next_task.id in (task2.id, task3.id)

    def test_get_in_progress_returns_only_in_progress_tasks(self, tmp_path: Path):
        """Test get_in_progress returns only in-progress tasks."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        pending = store.add("Pending task")
        in_progress = store.add("In-progress task")
        completed = store.add("Completed task")

        store.mark_in_progress(in_progress)
        completed.status = "completed"
        completed.completed_at = datetime.now(timezone.utc)
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
        task1 = store.add("First task")
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
        task1.completed_at = datetime.now(timezone.utc)
        store.update(task1)

        # task2 should no longer be blocked
        is_blocked, blocking_id, blocking_status = store.is_task_blocked(task2)
        assert is_blocked is False

    def test_count_blocked_tasks(self, tmp_path: Path):
        """Test count_blocked_tasks returns correct count."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        # Create some blocked and unblocked tasks
        task1 = store.add("First task")
        task2 = store.add("Second task", depends_on=task1.id)
        task3 = store.add("Third task", depends_on=task1.id)
        task4 = store.add("Independent task")

        # Should have 2 blocked tasks
        count = store.count_blocked_tasks()
        assert count == 2

        # Complete task1
        task1.status = "completed"
        task1.completed_at = datetime.now(timezone.utc)
        store.update(task1)

        # Should have 0 blocked tasks
        count = store.count_blocked_tasks()
        assert count == 0

    def test_get_groups(self, tmp_path: Path):
        """Test get_groups returns correct group statistics."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        # Create tasks in different groups
        task1 = store.add("Task 1", group="group-a")
        task2 = store.add("Task 2", group="group-a")
        task3 = store.add("Task 3", group="group-b")
        task4 = store.add("Task 4")  # No group

        # Mark one as completed
        task1.status = "completed"
        task1.completed_at = datetime.now(timezone.utc)
        store.update(task1)

        groups = store.get_groups()

        assert "group-a" in groups
        assert "group-b" in groups
        assert groups["group-a"]["completed"] == 1
        assert groups["group-a"]["pending"] == 1
        assert groups["group-b"]["pending"] == 1

    def test_get_by_group(self, tmp_path: Path):
        """Test get_by_group returns tasks in correct order."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        # Create tasks in a group
        task1 = store.add("First", group="test-group")
        task2 = store.add("Second", group="test-group")
        task3 = store.add("Third", group="other-group")

        tasks = store.get_by_group("test-group")
        assert len(tasks) == 2
        assert tasks[0].id == task1.id
        assert tasks[1].id == task2.id

    def test_update_task_with_new_fields(self, tmp_path: Path):
        """Test updating a task with new fields."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        # Create a task
        task = store.add("Test task")
        assert task.group is None
        assert task.create_review is False

        # Update with new fields
        task.group = "new-group"
        task.create_review = True
        task.same_branch = True
        store.update(task)

        # Retrieve and verify
        retrieved = store.get(task.id)
        assert retrieved.group == "new-group"
        assert retrieved.create_review is True
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
        task.completed_at = datetime.now(timezone.utc)
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

        # Open with SqliteTaskStore to trigger migration
        store = SqliteTaskStore(db_path)

        # Verify migration worked
        task = store.get(1)
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
        task = Task(id=1, prompt="Test", task_type="explore")
        assert task.is_explore() is True

        task = Task(id=1, prompt="Test", task_type="implement")
        assert task.is_explore() is False

    def test_is_blocked(self):
        """Test is_blocked method."""
        task = Task(id=1, prompt="Test", depends_on=5)
        assert task.is_blocked() is True

        task = Task(id=1, prompt="Test", depends_on=None)
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
        now = datetime.now(timezone.utc).isoformat()
        conn.execute(
            "INSERT INTO tasks (prompt, task_type, created_at) VALUES (?, ?, ?)",
            ("Old task", "plan", now),
        )
        conn.commit()
        conn.close()

        # Open with SqliteTaskStore - should auto-migrate to v9
        store = SqliteTaskStore(db_path)

        # Check schema version
        conn = sqlite3.connect(db_path)
        cur = conn.execute("SELECT version FROM schema_version")
        version = cur.fetchone()[0]
        conn.close()
        assert version == SCHEMA_VERSION

        # Verify old task can be retrieved (with NULL output_content)
        task = store.get(1)
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
        now = datetime.now(timezone.utc).isoformat()
        conn.execute(
            "INSERT INTO tasks (prompt, status, created_at) VALUES (?, ?, ?)",
            ("Old task", "failed", now),
        )
        conn.commit()
        conn.close()

        # Open with SqliteTaskStore - should auto-migrate to v9
        store = SqliteTaskStore(db_path)

        # Check schema version
        conn = sqlite3.connect(db_path)
        cur = conn.execute("SELECT version FROM schema_version")
        version = cur.fetchone()[0]
        conn.close()
        assert version == SCHEMA_VERSION

        # Verify old task can be retrieved (with NULL session_id)
        task = store.get(1)
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
        from datetime import datetime, timezone

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

        now = datetime.now(timezone.utc).isoformat()
        conn.execute(
            "INSERT INTO tasks (prompt, status, created_at, num_turns) VALUES (?, ?, ?, ?)",
            ("Old task with turns", "completed", now, 15),
        )
        conn.commit()
        conn.close()

        # Open with SqliteTaskStore to trigger migration
        store = SqliteTaskStore(db_path)

        # Check schema version updated to 9
        conn = sqlite3.connect(db_path)
        cur = conn.execute("SELECT version FROM schema_version")
        version = cur.fetchone()[0]
        conn.close()
        assert version == SCHEMA_VERSION

        # Verify old task migrated: num_turns_reported populated from num_turns
        task = store.get(1)
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
        from datetime import datetime, timezone

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

        now = datetime.now(timezone.utc).isoformat()
        conn.execute(
            "INSERT INTO tasks (prompt, status, created_at, cost_usd) VALUES (?, ?, ?, ?)",
            ("Old task", "completed", now, 0.05),
        )
        conn.commit()
        conn.close()

        # Open with SqliteTaskStore to trigger migration
        store = SqliteTaskStore(db_path)

        # Check schema version updated to 9
        conn = sqlite3.connect(db_path)
        cur = conn.execute("SELECT version FROM schema_version")
        version = cur.fetchone()[0]
        conn.close()
        assert version == SCHEMA_VERSION

        # Verify old task can be retrieved with NULL token counts
        task = store.get(1)
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
        """Test that get_reviews_for_task returns reviews that depend on the given task."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        # Create an implementation task
        impl_task = store.add("Add feature", task_type="implement")

        # Create review tasks that depend on it
        review1 = store.add("First review", task_type="review", depends_on=impl_task.id)
        review2 = store.add("Second review", task_type="review", depends_on=impl_task.id)

        # Create unrelated review
        other_impl = store.add("Other feature", task_type="implement")
        other_review = store.add("Other review", task_type="review", depends_on=other_impl.id)

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
        t1 = datetime(2026, 1, 1, 10, 0, 0, tzinfo=timezone.utc)
        t2 = datetime(2026, 1, 1, 11, 0, 0, tzinfo=timezone.utc)
        t3 = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)

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
        completed_review.completed_at = datetime(2026, 1, 1, 10, 0, 0, tzinfo=timezone.utc)
        completed_review.status = "completed"
        store.update(completed_review)
        # incomplete_review has completed_at = NULL

        reviews = store.get_reviews_for_task(impl_task.id)

        assert len(reviews) == 2
        # Completed review comes first, incomplete (NULL completed_at) comes last
        assert reviews[0].id == completed_review.id
        assert reviews[1].id == incomplete_review.id

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
        improve = store.add("Improve", task_type="improve", depends_on=impl_task.id)

        reviews = store.get_reviews_for_task(impl_task.id)

        assert len(reviews) == 1
        assert reviews[0].id == review.id


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
        store.mark_completed(task, has_commits=True, branch="feature/test")

        retrieved = store.get(task.id)
        assert retrieved is not None
        assert retrieved.merge_status == "unmerged"
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

    def test_migrate_merge_status_logs_when_git_check_fails(self, tmp_path: Path, caplog: pytest.LogCaptureFixture):
        """Migration logs a warning and defaults to unmerged when git checks fail."""
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

            def is_merged(self, source: str, target: str) -> bool:
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
        assert "Could not determine merge status" in caplog.text

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

        now = datetime.now(timezone.utc).isoformat()
        conn.execute(
            "INSERT INTO tasks (prompt, status, created_at) VALUES (?, ?, ?)",
            ("Old task", "completed", now),
        )
        conn.commit()
        conn.close()

        # Open with SqliteTaskStore to trigger migration
        store = SqliteTaskStore(db_path)

        # Check schema version updated to 12
        conn = sqlite3.connect(db_path)
        cur = conn.execute("SELECT version FROM schema_version")
        version = cur.fetchone()[0]
        conn.close()
        assert version == SCHEMA_VERSION

        # Verify old task can be retrieved with NULL merge_status
        task = store.get(1)
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
        from datetime import datetime, timezone
        task.completed_at = datetime.now(timezone.utc)
        store.update(task)

        retrieved = store.get(task.id)
        assert retrieved is not None
        assert retrieved.merge_status == "merged"


class TestEditPromptDefaultContent:
    """Tests for edit_prompt default content generation."""

    def test_edit_prompt_provides_default_for_implement_with_based_on(self, tmp_path: Path, monkeypatch):
        """Test that edit_prompt provides a default prompt for implement tasks with based_on."""
        from gza.db import edit_prompt
        import subprocess

        # Mock subprocess.run to capture what would be written to the editor
        editor_content = []

        def mock_run(cmd):
            # Read the temporary file that was passed to the editor
            temp_file = cmd[1]
            with open(temp_file, 'r') as f:
                editor_content.append(f.read())
            # Return success
            class Result:
                returncode = 0
            return Result()

        monkeypatch.setattr(subprocess, 'run', mock_run)

        # Call edit_prompt with implement type and based_on
        # Note: This will still try to open editor, but our mock will capture the content
        # We need to also write back to the file so it doesn't return None
        def mock_run_with_write(cmd):
            temp_file = cmd[1]
            with open(temp_file, 'r') as f:
                content = f.read()
                editor_content.append(content)
            # Verify the default prompt is present
            assert "Implement plan from task #42" in content
            # Return success without modifying the file
            class Result:
                returncode = 0
            return Result()

        monkeypatch.setattr(subprocess, 'run', mock_run_with_write)

        result = edit_prompt(
            initial_content="",
            task_type="implement",
            based_on=42,
        )

        # Verify the default prompt was included in the editor
        assert len(editor_content) == 1
        assert "Implement plan from task #42" in editor_content[0]

        # Verify the result includes the default
        assert result == "Implement plan from task #42"

    def test_edit_prompt_includes_slug_when_provided(self, tmp_path: Path, monkeypatch):
        """Test that edit_prompt includes the slug in the default prompt when provided."""
        from gza.db import edit_prompt
        import subprocess

        editor_content = []

        def mock_run_with_write(cmd):
            temp_file = cmd[1]
            with open(temp_file, 'r') as f:
                content = f.read()
                editor_content.append(content)
            class Result:
                returncode = 0
            return Result()

        monkeypatch.setattr(subprocess, 'run', mock_run_with_write)

        result = edit_prompt(
            initial_content="",
            task_type="implement",
            based_on=42,
            based_on_slug="design-feature-x",
        )

        assert len(editor_content) == 1
        assert "Implement plan from task #42: design-feature-x" in editor_content[0]
        assert result == "Implement plan from task #42: design-feature-x"

    def test_edit_prompt_no_default_for_other_task_types(self, tmp_path: Path, monkeypatch):
        """Test that edit_prompt does not provide default for non-implement tasks with based_on."""
        from gza.db import edit_prompt
        import subprocess

        editor_content = []

        def mock_run(cmd):
            temp_file = cmd[1]
            with open(temp_file, 'r') as f:
                content = f.read()
                editor_content.append(content)
            # Don't write anything back (simulate empty editor)
            class Result:
                returncode = 0
            return Result()

        monkeypatch.setattr(subprocess, 'run', mock_run)

        result = edit_prompt(
            initial_content="",
            task_type="plan",  # Not implement
            based_on=42,
        )

        # Verify no default prompt was added for plan type
        assert len(editor_content) == 1
        assert "Implement plan from task #42" not in editor_content[0]

        # Verify empty result since editor was "empty"
        assert result is None

    def test_edit_prompt_no_default_for_implement_without_based_on(self, tmp_path: Path, monkeypatch):
        """Test that edit_prompt does not provide default for implement tasks without based_on."""
        from gza.db import edit_prompt
        import subprocess

        editor_content = []

        def mock_run(cmd):
            temp_file = cmd[1]
            with open(temp_file, 'r') as f:
                content = f.read()
                editor_content.append(content)
            class Result:
                returncode = 0
            return Result()

        monkeypatch.setattr(subprocess, 'run', mock_run)

        result = edit_prompt(
            initial_content="",
            task_type="implement",
            based_on=None,  # No based_on
        )

        # Verify no default prompt was added
        assert len(editor_content) == 1
        assert "Implement plan from task #" not in editor_content[0]
        assert result is None

    def test_edit_prompt_preserves_custom_initial_content(self, tmp_path: Path, monkeypatch):
        """Test that edit_prompt does not override custom initial_content."""
        from gza.db import edit_prompt
        import subprocess

        editor_content = []
        custom_content = "Custom implementation task"

        def mock_run(cmd):
            temp_file = cmd[1]
            with open(temp_file, 'r') as f:
                content = f.read()
                editor_content.append(content)
            class Result:
                returncode = 0
            return Result()

        monkeypatch.setattr(subprocess, 'run', mock_run)

        result = edit_prompt(
            initial_content=custom_content,
            task_type="implement",
            based_on=42,
        )

        # Verify custom content is present, not the default
        assert len(editor_content) == 1
        assert custom_content in editor_content[0]
        # The default should NOT be added when initial_content is provided
        # (it's already in the content area, not overwritten)
        assert result == custom_content

    def test_add_task_interactive_includes_slug_from_based_on(self, tmp_path: Path, monkeypatch):
        """Test that add_task_interactive looks up the slug from the based_on task."""
        from gza.db import add_task_interactive, SqliteTaskStore
        import subprocess

        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        # Create a plan task with a known task_id containing a slug
        plan_task = store.add(prompt="Design feature X", task_type="plan")
        plan_task.task_id = "20260223-design-feature-x"
        store.update(plan_task)

        editor_content = []

        def mock_run(cmd):
            temp_file = cmd[1]
            with open(temp_file, 'r') as f:
                content = f.read()
                editor_content.append(content)
            class Result:
                returncode = 0
            return Result()

        monkeypatch.setattr(subprocess, 'run', mock_run)

        add_task_interactive(store, task_type="implement", based_on=plan_task.id)

        assert len(editor_content) == 1
        assert "Implement plan from task #" in editor_content[0]
        assert "design-feature-x" in editor_content[0]


class TestFailureReasonTracking:
    """Tests for failure_reason field and extract_failure_reason function."""

    def test_failure_reason_defaults_to_none_for_pending_task(self, tmp_path: Path):
        """New tasks have failure_reason=None."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        task = store.add(prompt="Test task")
        assert task.failure_reason is None

        retrieved = store.get(task.id)
        assert retrieved is not None
        assert retrieved.failure_reason is None

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

    def test_extract_failure_reason_detects_max_steps(self, tmp_path: Path):
        """extract_failure_reason detects MAX_STEPS marker."""
        from gza.db import extract_failure_reason

        log_file = tmp_path / "test.log"
        log_file.write_text("Some output\n[GZA_FAILURE:MAX_STEPS]\nEnd of output")

        result = extract_failure_reason(log_file)
        assert result == "MAX_STEPS"

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
        task.completed_at = datetime.now(timezone.utc)
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

        now = datetime.now(timezone.utc).isoformat()
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

        # Open with SqliteTaskStore to trigger migration
        store = SqliteTaskStore(db_path)

        # Check schema version updated to 12
        conn = sqlite3.connect(db_path)
        cur = conn.execute("SELECT version FROM schema_version")
        version = cur.fetchone()[0]
        conn.close()
        assert version == SCHEMA_VERSION

        # Verify existing failed task was backfilled with 'UNKNOWN'
        failed_task = store.get(1)
        assert failed_task is not None
        assert failed_task.status == "failed"
        assert failed_task.failure_reason == "UNKNOWN"

        # Verify pending task was NOT backfilled
        pending_task = store.get(2)
        assert pending_task is not None
        assert pending_task.status == "pending"
        assert pending_task.failure_reason is None

    def test_known_failure_reasons_set(self):
        """KNOWN_FAILURE_REASONS contains expected values."""
        from gza.db import KNOWN_FAILURE_REASONS

        assert "MAX_STEPS" in KNOWN_FAILURE_REASONS
        assert "MAX_TURNS" in KNOWN_FAILURE_REASONS
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
        from datetime import datetime, timezone

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
        now = datetime.now(timezone.utc).isoformat()
        conn.execute(
            "INSERT INTO tasks (prompt, status, created_at) VALUES (?, ?, ?)",
            ("Existing task", "completed", now),
        )
        conn.commit()
        conn.close()

        # Open with SqliteTaskStore to trigger migration
        store = SqliteTaskStore(db_path)

        # Check schema version updated to 12
        conn = sqlite3.connect(db_path)
        cur = conn.execute("SELECT version FROM schema_version")
        version = cur.fetchone()[0]
        conn.close()
        assert version == SCHEMA_VERSION

        # Verify existing task has NULL diff stats
        task = store.get(1)
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
        now = datetime.now(timezone.utc).isoformat()
        conn.execute(
            "INSERT INTO tasks (prompt, status, created_at) VALUES (?, ?, ?)",
            ("Existing task", "completed", now),
        )
        conn.commit()
        conn.close()

        # Open with SqliteTaskStore to trigger migration
        store = SqliteTaskStore(db_path)

        # Verify schema version updated to 14
        conn = sqlite3.connect(db_path)
        cur = conn.execute("SELECT version FROM schema_version")
        version = cur.fetchone()[0]
        conn.close()
        assert version == SCHEMA_VERSION

        # Verify existing task can be retrieved with NULL review_cleared_at
        task = store.get(1)
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
        store.clear_review_state(99999)


class TestConvenienceFunctions:
    """Tests for module-level convenience functions get_task, get_task_log_path,
    get_task_report_path, and get_baseline_stats."""

    def test_get_task_returns_dict(self, tmp_path: Path, monkeypatch):
        """get_task returns a dict with all task fields."""
        from gza.db import get_task, TaskStats

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
        # Datetime fields should be ISO strings or None
        assert isinstance(result["created_at"], str)
        assert result["started_at"] is None
        assert result["completed_at"] is None

    def test_get_task_all_fields_json_serializable(self, tmp_path: Path, monkeypatch):
        """get_task result is fully JSON-serializable."""
        import json
        from gza.db import get_task, TaskStats
        from datetime import datetime, timezone

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
        from gza.db import get_baseline_stats, TaskStats

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
        from gza.db import get_baseline_stats, TaskStats
        from datetime import datetime, timezone, timedelta

        db_path = tmp_path / ".gza" / "gza.db"
        db_path.parent.mkdir(parents=True)
        store = SqliteTaskStore(db_path)

        # Add 5 completed tasks with differing costs
        for i in range(5):
            task = store.add(prompt=f"Task {i}")
            task.completed_at = datetime(2026, 1, i + 1, tzinfo=timezone.utc)
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
        from gza.db import get_task, TaskStats

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
        dep = store.add("Dependency")
        self._complete(store, dep)
        downstream = store.add("Downstream", depends_on=dep.id)
        is_blocked, _, _ = store.is_task_blocked(downstream)
        assert is_blocked is False

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
        dep = store.add("Dependency")
        self._fail(store, dep)
        retry = store.add("Retry of dep", based_on=dep.id)
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

    def test_retry_chain_failed_failed_completed_unblocks(self, tmp_path: Path):
        """dep(failed) → retry1(failed) → retry2(completed): downstream unblocked."""
        store = self._make_store(tmp_path)
        dep = store.add("Original dep")
        self._fail(store, dep)
        retry1 = store.add("First retry", based_on=dep.id)
        self._fail(store, retry1)
        retry2 = store.add("Second retry", based_on=retry1.id)
        self._complete(store, retry2)

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
        dep = store.add("Dependency")
        self._fail(store, dep)
        retry = store.add("Retry", based_on=dep.id)
        self._complete(store, retry)
        downstream = store.add("Downstream", depends_on=dep.id)

        next_task = store.get_next_pending()
        assert next_task is not None
        assert next_task.id == downstream.id

    def test_get_next_pending_handles_retry_chain(self, tmp_path: Path):
        """get_next_pending unblocks downstream after multi-hop retry chain succeeds."""
        store = self._make_store(tmp_path)
        dep = store.add("Original dep")
        self._fail(store, dep)
        retry1 = store.add("Retry 1", based_on=dep.id)
        self._fail(store, retry1)
        retry2 = store.add("Retry 2", based_on=retry1.id)
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
        dep = store.add("Dep")
        self._complete(store, dep)
        downstream = store.add("Downstream", depends_on=dep.id)

        next_task = store.get_next_pending()
        assert next_task is not None
        assert next_task.id == downstream.id

    # --- count_blocked_tasks ---

    def test_count_blocked_excludes_unblocked_by_retry(self, tmp_path: Path):
        """count_blocked_tasks does not count tasks unblocked by a successful retry."""
        store = self._make_store(tmp_path)
        dep = store.add("Dep")
        self._fail(store, dep)
        retry = store.add("Retry", based_on=dep.id)
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
        from datetime import datetime, timezone

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
        now = datetime.now(timezone.utc).isoformat()
        conn.execute(
            "INSERT INTO tasks (prompt, status, created_at, num_turns_reported, num_turns_computed) VALUES (?, ?, ?, ?, ?)",
            ("Legacy task", "completed", now, 4, 3),
        )
        conn.commit()
        conn.close()

        store = SqliteTaskStore(db_path)

        conn = sqlite3.connect(db_path)
        cur = conn.execute("SELECT version FROM schema_version")
        version = cur.fetchone()[0]
        conn.close()
        assert version == SCHEMA_VERSION

        migrated = store.get(1)
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

        SqliteTaskStore(db_path)
        SqliteTaskStore(db_path)

        conn = sqlite3.connect(db_path)
        cur = conn.execute(
            """
            SELECT name FROM sqlite_master
            WHERE type='index'
              AND name IN (
                'idx_run_steps_run_id',
                'idx_run_steps_step_index',
                'idx_run_substeps_run_id',
                'idx_run_substeps_step_id'
              )
            """
        )
        indexes = sorted(row[0] for row in cur.fetchall())
        conn.close()
        assert indexes == [
            "idx_run_steps_run_id",
            "idx_run_steps_step_index",
            "idx_run_substeps_run_id",
            "idx_run_substeps_step_id",
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
        now = datetime.now(timezone.utc).isoformat()
        conn.execute("INSERT INTO tasks (prompt, status, created_at) VALUES (?, ?, ?)", ("legacy", "pending", now))
        conn.commit()
        conn.close()

        SqliteTaskStore(db_path)

        conn = sqlite3.connect(db_path)
        version = conn.execute("SELECT version FROM schema_version").fetchone()[0]
        value = conn.execute("SELECT log_schema_version FROM tasks WHERE id = 1").fetchone()[0]
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


class TestCycleOrchestratorSchema:
    """Tests for v17->v18 migration creating cycle tables and indexes."""

    def test_migration_v17_to_v18_creates_cycle_tables(self, tmp_path: Path):
        """Migration from v17 to v18 creates task_cycles and task_cycle_iterations tables."""
        import sqlite3
        from datetime import datetime, timezone

        db_path = tmp_path / "test.db"

        # Bootstrap a v17 DB manually
        conn = sqlite3.connect(db_path)
        conn.execute("""
            CREATE TABLE schema_version (version INTEGER);
        """)
        conn.execute("INSERT INTO schema_version VALUES (17)")
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
                created_at TEXT NOT NULL DEFAULT '',
                started_at TEXT,
                completed_at TEXT,
                "group" TEXT,
                depends_on INTEGER,
                spec TEXT,
                create_review INTEGER DEFAULT 0,
                same_branch INTEGER DEFAULT 0,
                task_type_hint TEXT,
                output_content TEXT,
                session_id TEXT,
                pr_number INTEGER,
                model TEXT,
                provider TEXT,
                merge_status TEXT,
                failure_reason TEXT,
                skip_learnings INTEGER DEFAULT 0,
                diff_files_changed INTEGER,
                diff_lines_added INTEGER,
                diff_lines_removed INTEGER,
                review_cleared_at TEXT,
                log_schema_version INTEGER DEFAULT 1
            )
        """)
        conn.execute("INSERT INTO tasks (id, prompt, status, created_at) VALUES (1, 'Test', 'completed', ?)",
                     (datetime.now(timezone.utc).isoformat(),))
        conn.commit()
        conn.close()

        # Open with SqliteTaskStore to trigger migration
        store = SqliteTaskStore(db_path)

        # Verify schema version updated to 18
        conn = sqlite3.connect(db_path)
        version = conn.execute("SELECT version FROM schema_version").fetchone()[0]

        # Verify new tables exist
        tables = {row[0] for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        )}
        indexes = {row[0] for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='index'"
        )}
        # Verify new task columns exist
        columns = {row[1] for row in conn.execute("PRAGMA table_info(tasks)")}
        conn.close()

        assert version == SCHEMA_VERSION
        assert "task_cycles" in tables
        assert "task_cycle_iterations" in tables
        assert "idx_task_cycles_impl_id" in indexes
        assert "idx_task_cycles_status" in indexes
        assert "idx_task_cycle_iterations_cycle_idx" in indexes
        assert "idx_tasks_cycle_id" in indexes
        assert "cycle_id" in columns
        assert "cycle_iteration_index" in columns
        assert "cycle_role" in columns

    def test_cycle_columns_on_task(self, tmp_path: Path):
        """Cycle columns on tasks table persist correctly."""
        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        task = store.add("Implement feature", task_type="implement")
        assert task.cycle_id is None
        assert task.cycle_iteration_index is None
        assert task.cycle_role is None

        task.cycle_id = 42
        task.cycle_iteration_index = 2
        task.cycle_role = "review"
        store.update(task)

        retrieved = store.get(task.id)
        assert retrieved is not None
        assert retrieved.cycle_id == 42
        assert retrieved.cycle_iteration_index == 2
        assert retrieved.cycle_role == "review"

    def test_migration_v18_to_v19_adds_indexes(self, tmp_path: Path):
        """Migration v18->v19 adds idx_tasks_type_based_on and the unique constraint."""
        import sqlite3
        from gza.db import SqliteTaskStore

        db_path = tmp_path / "test.db"
        # Bootstrap as a v18 database (full fresh schema minus the v19 additions)
        conn = sqlite3.connect(db_path)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS schema_version (version INTEGER PRIMARY KEY)
        """)
        conn.execute("INSERT INTO schema_version (version) VALUES (18)")
        # Minimal task_cycle_iterations table (no UNIQUE constraint yet)
        conn.execute("""
            CREATE TABLE task_cycle_iterations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                cycle_id INTEGER NOT NULL,
                iteration_index INTEGER NOT NULL,
                state TEXT NOT NULL DEFAULT 'review_created',
                started_at TEXT NOT NULL
            )
        """)
        # Minimal tasks table
        conn.execute("""
            CREATE TABLE tasks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                prompt TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending',
                task_type TEXT NOT NULL DEFAULT 'task',
                based_on INTEGER
            )
        """)
        conn.commit()
        conn.close()

        # Opening with SqliteTaskStore should run the v18->v19 migration
        store = SqliteTaskStore(db_path)

        conn = sqlite3.connect(db_path)
        version = conn.execute("SELECT version FROM schema_version").fetchone()[0]
        indexes = {row[1] for row in conn.execute("SELECT * FROM sqlite_master WHERE type='index'")}
        conn.close()

        assert version == SCHEMA_VERSION
        assert "idx_tasks_type_based_on" in indexes
        assert "uq_task_cycle_iterations_cycle_iter" in indexes


class TestCycleStoreAPIs:
    """Tests for cycle store APIs: start_cycle, append_cycle_iteration, etc."""

    def test_start_cycle_creates_active_cycle(self, tmp_path: Path):
        """start_cycle creates a new active cycle for an implementation task."""
        store = SqliteTaskStore(tmp_path / "test.db")
        impl = store.add("Implement feature", task_type="implement")
        assert impl.id is not None

        cycle = store.start_cycle(impl.id, max_iterations=5)

        assert cycle.id is not None
        assert cycle.implementation_task_id == impl.id
        assert cycle.status == "active"
        assert cycle.max_iterations == 5
        assert cycle.stop_reason is None
        assert cycle.ended_at is None

    def test_start_cycle_enforces_uniqueness(self, tmp_path: Path):
        """start_cycle raises ValueError if an active cycle already exists."""
        store = SqliteTaskStore(tmp_path / "test.db")
        impl = store.add("Implement feature", task_type="implement")
        assert impl.id is not None

        store.start_cycle(impl.id)
        with pytest.raises(ValueError, match="already has an active cycle"):
            store.start_cycle(impl.id)

    def test_get_active_cycle_for_impl(self, tmp_path: Path):
        """get_active_cycle_for_impl returns the active cycle."""
        store = SqliteTaskStore(tmp_path / "test.db")
        impl = store.add("Implement feature", task_type="implement")
        assert impl.id is not None

        assert store.get_active_cycle_for_impl(impl.id) is None

        cycle = store.start_cycle(impl.id)
        active = store.get_active_cycle_for_impl(impl.id)
        assert active is not None
        assert active.id == cycle.id

    def test_append_and_get_cycle_iterations(self, tmp_path: Path):
        """append_cycle_iteration creates iteration records retrievable via get_cycle_iterations."""
        store = SqliteTaskStore(tmp_path / "test.db")
        impl = store.add("Implement feature", task_type="implement")
        assert impl.id is not None
        cycle = store.start_cycle(impl.id)

        it0 = store.append_cycle_iteration(cycle.id, iteration_index=0)
        it1 = store.append_cycle_iteration(cycle.id, iteration_index=1)

        assert it0.cycle_id == cycle.id
        assert it0.iteration_index == 0
        assert it0.state == "review_created"
        assert it1.iteration_index == 1

        iters = store.get_cycle_iterations(cycle.id)
        assert len(iters) == 2
        assert iters[0].iteration_index == 0
        assert iters[1].iteration_index == 1

    def test_update_cycle_iteration_fields(self, tmp_path: Path):
        """update_cycle_iteration persists changes to verdict, state, and task IDs."""
        store = SqliteTaskStore(tmp_path / "test.db")
        impl = store.add("Implement", task_type="implement")
        assert impl.id is not None
        review = store.add("Review", task_type="review")
        improve = store.add("Improve", task_type="improve")
        assert review.id is not None and improve.id is not None

        cycle = store.start_cycle(impl.id)
        it = store.append_cycle_iteration(cycle.id, iteration_index=0)

        from datetime import datetime, timezone
        store.update_cycle_iteration(
            it.id,
            review_task_id=review.id,
            review_verdict="CHANGES_REQUESTED",
            improve_task_id=improve.id,
            state="improve_completed",
            ended_at=datetime.now(timezone.utc),
        )

        iters = store.get_cycle_iterations(cycle.id)
        assert len(iters) == 1
        updated = iters[0]
        assert updated.review_task_id == review.id
        assert updated.review_verdict == "CHANGES_REQUESTED"
        assert updated.improve_task_id == improve.id
        assert updated.state == "improve_completed"
        assert updated.ended_at is not None

    def test_close_cycle_updates_status(self, tmp_path: Path):
        """close_cycle sets status, stop_reason, and ended_at."""
        store = SqliteTaskStore(tmp_path / "test.db")
        impl = store.add("Implement", task_type="implement")
        assert impl.id is not None

        cycle = store.start_cycle(impl.id)
        assert cycle.status == "active"
        assert cycle.ended_at is None

        store.close_cycle(cycle.id, status="approved", stop_reason="approved")

        cycles = store.get_cycles_for_impl(impl.id)
        assert len(cycles) == 1
        closed = cycles[0]
        assert closed.status == "approved"
        assert closed.stop_reason == "approved"
        assert closed.ended_at is not None

    def test_get_cycles_for_impl_newest_first(self, tmp_path: Path):
        """get_cycles_for_impl returns cycles ordered newest first."""
        store = SqliteTaskStore(tmp_path / "test.db")
        impl = store.add("Implement", task_type="implement")
        assert impl.id is not None

        cycle1 = store.start_cycle(impl.id)
        store.close_cycle(cycle1.id, status="maxed_out", stop_reason="max_iterations")

        cycle2 = store.start_cycle(impl.id)

        cycles = store.get_cycles_for_impl(impl.id)
        assert len(cycles) == 2
        assert cycles[0].id == cycle2.id  # newest first
        assert cycles[1].id == cycle1.id

    def test_cycle_aggregate_stats_empty(self, tmp_path: Path):
        """get_cycle_aggregate_stats returns sensible empty result."""
        store = SqliteTaskStore(tmp_path / "test.db")
        stats = store.get_cycle_aggregate_stats()

        assert stats["total_cycles"] == 0
        assert stats["approved_cycles"] == 0
        assert stats["improves_before_approval"] is None

    def test_cycle_aggregate_stats_with_data(self, tmp_path: Path):
        """get_cycle_aggregate_stats computes correct stats from cycle data."""
        store = SqliteTaskStore(tmp_path / "test.db")

        # Cycle 1: approved after 2 improves
        impl1 = store.add("Impl 1", task_type="implement")
        assert impl1.id is not None
        cycle1 = store.start_cycle(impl1.id)
        review1 = store.add("Review 1", task_type="review")
        improve1 = store.add("Improve 1", task_type="improve")
        assert review1.id is not None and improve1.id is not None
        it1 = store.append_cycle_iteration(cycle1.id, 0)
        store.update_cycle_iteration(it1.id, review_task_id=review1.id, improve_task_id=improve1.id, state="improve_completed")
        review2 = store.add("Review 2", task_type="review")
        assert review2.id is not None
        it2 = store.append_cycle_iteration(cycle1.id, 1)
        store.update_cycle_iteration(it2.id, review_task_id=review2.id, state="terminal", review_verdict="APPROVED")
        store.close_cycle(cycle1.id, status="approved", stop_reason="approved")

        # Cycle 2: maxed_out (1 improve)
        impl2 = store.add("Impl 2", task_type="implement")
        assert impl2.id is not None
        cycle2 = store.start_cycle(impl2.id)
        review3 = store.add("Review 3", task_type="review")
        improve3 = store.add("Improve 3", task_type="improve")
        assert review3.id is not None and improve3.id is not None
        it3 = store.append_cycle_iteration(cycle2.id, 0)
        store.update_cycle_iteration(it3.id, review_task_id=review3.id, improve_task_id=improve3.id, state="improve_completed")
        review4 = store.add("Review 4", task_type="review")
        assert review4.id is not None
        it4 = store.append_cycle_iteration(cycle2.id, 1)
        store.update_cycle_iteration(it4.id, review_task_id=review4.id, state="terminal")
        store.close_cycle(cycle2.id, status="maxed_out", stop_reason="max_iterations")

        stats = store.get_cycle_aggregate_stats()
        assert stats["total_cycles"] == 2
        assert stats["approved_cycles"] == 1
        # Only cycle1 is approved: 1 improve -> improves_before_approval = 1
        assert stats["improves_before_approval"] is not None
        assert stats["improves_before_approval"]["min"] == 1.0
        # Both cycles have 2 reviews each
        assert stats["reviews_per_cycle"] is not None
        assert stats["reviews_per_cycle"]["min"] == 2.0

    def test_improves_before_approval_two_iteration_cycle(self, tmp_path: Path):
        """improves_before_approval counts improve tasks, not total iterations."""
        store = SqliteTaskStore(tmp_path / "test.db")

        # Cycle: iter 0 has review+improve (CHANGES_REQUESTED), iter 1 has review-only (APPROVED)
        impl = store.add("Impl", task_type="implement")
        assert impl.id is not None
        cycle = store.start_cycle(impl.id)

        review0 = store.add("Review 0", task_type="review")
        improve0 = store.add("Improve 0", task_type="improve")
        assert review0.id is not None and improve0.id is not None
        it0 = store.append_cycle_iteration(cycle.id, 0)
        store.update_cycle_iteration(
            it0.id,
            review_task_id=review0.id,
            improve_task_id=improve0.id,
            state="improve_completed",
            review_verdict="CHANGES_REQUESTED",
        )

        review1 = store.add("Review 1", task_type="review")
        assert review1.id is not None
        it1 = store.append_cycle_iteration(cycle.id, 1)
        store.update_cycle_iteration(
            it1.id,
            review_task_id=review1.id,
            state="terminal",
            review_verdict="APPROVED",
        )
        store.close_cycle(cycle.id, status="approved", stop_reason="approved")

        stats = store.get_cycle_aggregate_stats()
        assert stats["approved_cycles"] == 1
        # One improve ran before approval
        assert stats["improves_before_approval"] is not None
        assert stats["improves_before_approval"]["min"] == 1.0
        assert stats["improves_before_approval"]["max"] == 1.0

    def test_get_impl_based_on_ids_returns_targeted_set(self, tmp_path: Path):
        """get_impl_based_on_ids returns only based_on IDs from implement tasks."""
        store = SqliteTaskStore(tmp_path / "test.db")

        plan1 = store.add("Plan 1", task_type="plan")
        plan2 = store.add("Plan 2", task_type="plan")
        plan3 = store.add("Plan 3", task_type="plan")
        assert plan1.id is not None and plan2.id is not None and plan3.id is not None

        # Only plan1 has an implement task based on it
        store.add("Impl 1", task_type="implement", based_on=plan1.id)
        # A review task with based_on should NOT be included
        store.add("Review of plan2", task_type="review", based_on=plan2.id)
        # A plain task with no based_on
        store.add("Task no based_on", task_type="implement")

        result = store.get_impl_based_on_ids()

        assert result == {plan1.id}

    def test_get_impl_based_on_ids_empty_db(self, tmp_path: Path):
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
        now = datetime.now(timezone.utc)

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
        now = datetime.now(timezone.utc)
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
        now = datetime.now(timezone.utc)

        for i in range(3):
            t = store.add(f"task {i}")
            t.status = "completed"
            t.completed_at = now - timedelta(days=i * 10)
            store.update(t)

        results = store.get_history(limit=None, since=None)
        assert len(results) == 3


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

        # Open the store — this triggers the migration
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

        store = SqliteTaskStore(db_path)
        tasks = store.get_all()

        assert len(tasks) == 1
        assert tasks[0].task_type == "internal"

        conn2 = sqlite3.connect(db_path)
        row = conn2.execute("SELECT version FROM schema_version").fetchone()
        conn2.close()
        assert row[0] == SCHEMA_VERSION
