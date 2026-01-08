#!/usr/bin/env python3
"""Test script for plan persistence feature."""

import tempfile
from pathlib import Path
from theo.db import SqliteTaskStore

def test_output_content_persistence():
    """Test that output_content is stored and retrieved correctly."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.db"
        store = SqliteTaskStore(db_path)

        # Add a plan task
        task = store.add(
            prompt="Design authentication system",
            task_type="plan"
        )

        print(f"✓ Created task #{task.id}")

        # Simulate completing the task with output content
        plan_content = """# Authentication System Design

## Overview
This plan outlines the implementation of a JWT-based authentication system.

## Key Components
1. User registration endpoint
2. Login endpoint with JWT generation
3. Token refresh mechanism
4. Protected route middleware

## Implementation Steps
1. Set up user database schema
2. Implement password hashing with bcrypt
3. Create JWT token generation/validation utilities
4. Build authentication endpoints
5. Add middleware for protected routes
"""

        store.mark_completed(
            task,
            report_file=".theo/plans/test-plan.md",
            output_content=plan_content,
            has_commits=False
        )

        print(f"✓ Marked task #{task.id} as completed with output_content")

        # Retrieve task and verify output_content is stored
        retrieved = store.get(task.id)
        assert retrieved is not None, "Task should exist"
        assert retrieved.status == "completed", "Task should be completed"
        assert retrieved.output_content == plan_content, "Output content should match"

        print(f"✓ Retrieved task #{retrieved.id} with correct output_content")
        print(f"  Content length: {len(retrieved.output_content)} characters")

        return True

def test_migration_from_v3():
    """Test that migration from v3 to v4 works correctly."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.db"

        # Create a v3 database by manually creating schema without output_content
        import sqlite3
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
        from datetime import datetime, timezone
        now = datetime.now(timezone.utc).isoformat()
        conn.execute(
            "INSERT INTO tasks (prompt, task_type, created_at) VALUES (?, ?, ?)",
            ("Old task", "plan", now)
        )
        conn.commit()
        conn.close()

        print("✓ Created v3 database with test task")

        # Now open with SqliteTaskStore - should auto-migrate to v4
        store = SqliteTaskStore(db_path)

        # Check schema version
        conn = sqlite3.connect(db_path)
        cur = conn.execute("SELECT version FROM schema_version")
        version = cur.fetchone()[0]
        conn.close()

        assert version == 4, f"Schema version should be 4, got {version}"
        print(f"✓ Database migrated to v4")

        # Verify old task can be retrieved (with NULL output_content)
        task = store.get(1)
        assert task is not None, "Old task should exist"
        assert task.output_content is None, "Old task should have NULL output_content"

        print(f"✓ Old task #{task.id} still works (output_content=None)")

        # Create new task with output_content
        new_task = store.add(prompt="New task", task_type="plan")
        store.mark_completed(
            new_task,
            output_content="This is the plan content",
            has_commits=False
        )

        retrieved = store.get(new_task.id)
        assert retrieved.output_content == "This is the plan content"

        print(f"✓ New task #{new_task.id} can store output_content")

        return True

def test_get_task_output_helper():
    """Test the _get_task_output helper function."""
    from theo.runner import _get_task_output
    from theo.db import Task

    with tempfile.TemporaryDirectory() as tmpdir:
        project_dir = Path(tmpdir)

        # Test 1: Task with output_content in DB (preferred)
        task1 = Task(
            id=1,
            prompt="Test",
            output_content="Content from DB"
        )
        result = _get_task_output(task1, project_dir)
        assert result == "Content from DB", "Should prefer DB content"
        print("✓ _get_task_output prefers DB content")

        # Test 2: Task with only report_file (fallback)
        report_dir = project_dir / ".theo" / "plans"
        report_dir.mkdir(parents=True)
        report_file = report_dir / "test.md"
        report_file.write_text("Content from file")

        task2 = Task(
            id=2,
            prompt="Test",
            report_file=".theo/plans/test.md",
            output_content=None
        )
        result = _get_task_output(task2, project_dir)
        assert result == "Content from file", "Should fall back to file"
        print("✓ _get_task_output falls back to file")

        # Test 3: Task with both (should prefer DB)
        task3 = Task(
            id=3,
            prompt="Test",
            report_file=".theo/plans/test.md",
            output_content="DB wins"
        )
        result = _get_task_output(task3, project_dir)
        assert result == "DB wins", "Should prefer DB over file"
        print("✓ _get_task_output prefers DB when both exist")

        # Test 4: Task with neither
        task4 = Task(
            id=4,
            prompt="Test",
            output_content=None
        )
        result = _get_task_output(task4, project_dir)
        assert result is None, "Should return None when no content"
        print("✓ _get_task_output returns None when no content")

        return True

if __name__ == "__main__":
    print("Testing plan persistence implementation...\n")

    try:
        print("Test 1: Output content persistence")
        test_output_content_persistence()
        print()

        print("Test 2: Migration from v3 to v4")
        test_migration_from_v3()
        print()

        print("Test 3: _get_task_output helper")
        test_get_task_output_helper()
        print()

        print("=" * 50)
        print("✓ All tests passed!")
        print("=" * 50)

    except AssertionError as e:
        print(f"\n✗ Test failed: {e}")
        exit(1)
    except Exception as e:
        print(f"\n✗ Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        exit(1)
