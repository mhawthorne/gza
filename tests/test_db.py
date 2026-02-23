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

        # Open with SqliteTaskStore - should auto-migrate to v9
        store = SqliteTaskStore(db_path)

        # Check schema version
        conn = sqlite3.connect(db_path)
        cur = conn.execute("SELECT version FROM schema_version")
        version = cur.fetchone()[0]
        conn.close()
        assert version == 13

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
        assert version == 13

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
        assert version == 13

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
        assert version == 13

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

    def test_get_reviews_for_task_ordered_by_created_at_desc(self, tmp_path: Path):
        """Test that reviews are returned in descending order by created_at."""
        import time

        db_path = tmp_path / "test.db"
        store = SqliteTaskStore(db_path)

        impl_task = store.add("Add feature", task_type="implement")

        # Create reviews with small delays to ensure different timestamps
        time.sleep(0.01)
        review1 = store.add("First review", task_type="review", depends_on=impl_task.id)
        time.sleep(0.01)
        review2 = store.add("Second review", task_type="review", depends_on=impl_task.id)
        time.sleep(0.01)
        review3 = store.add("Third review", task_type="review", depends_on=impl_task.id)

        reviews = store.get_reviews_for_task(impl_task.id)

        # Most recent should be first
        assert reviews[0].id == review3.id
        assert reviews[1].id == review2.id
        assert reviews[2].id == review1.id

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
        assert version == 13

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
            assert "Implement the plan from task #42" in content
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
        assert "Implement the plan from task #42" in editor_content[0]

        # Verify the result includes the default
        assert result == "Implement the plan from task #42"

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
        assert "Implement the plan from task #42" not in editor_content[0]

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
        assert "Implement the plan from task #" not in editor_content[0]
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
        assert version == 13

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
        assert version == 13

        # Verify existing task has NULL diff stats
        task = store.get(1)
        assert task is not None
        assert task.diff_files_changed is None
        assert task.diff_lines_added is None
        assert task.diff_lines_removed is None
