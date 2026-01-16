"""Tests for database operations and task chaining."""

import tempfile
from pathlib import Path
from datetime import datetime, timezone

import pytest

from gza.db import SqliteTaskStore, Task


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

        task = Task(id=1, prompt="Test", task_type="task")
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

        # Open with SqliteTaskStore - should auto-migrate to v5
        store = SqliteTaskStore(db_path)

        # Check schema version
        conn = sqlite3.connect(db_path)
        cur = conn.execute("SELECT version FROM schema_version")
        version = cur.fetchone()[0]
        conn.close()
        assert version == 5

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

        # Open with SqliteTaskStore - should auto-migrate to v5
        store = SqliteTaskStore(db_path)

        # Check schema version
        conn = sqlite3.connect(db_path)
        cur = conn.execute("SELECT version FROM schema_version")
        version = cur.fetchone()[0]
        conn.close()
        assert version == 5

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
