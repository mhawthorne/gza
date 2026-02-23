"""SQLite-based task storage."""

import os
import re
import sqlite3
import subprocess
import tempfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path


# Known failure reason categories
KNOWN_FAILURE_REASONS = {"MAX_TURNS", "TEST_FAILURE", "UNKNOWN"}

_FAILURE_MARKER_RE = re.compile(r"\[GZA_FAILURE:(\w+)\]")


@dataclass
class Task:
    """A task in the database."""
    id: int | None  # None for unsaved tasks
    prompt: str
    status: str = "pending"  # pending, in_progress, completed, failed, unmerged
    task_type: str = "task"  # task, explore, plan, implement, review, improve
    task_id: str | None = None  # YYYYMMDD-slug format
    branch: str | None = None
    log_file: str | None = None
    report_file: str | None = None
    based_on: int | None = None  # Reference to parent task id
    has_commits: bool | None = None
    duration_seconds: float | None = None
    num_turns_reported: int | None = None  # Turn count reported by the provider
    num_turns_computed: int | None = None  # Turn count computed internally
    cost_usd: float | None = None
    input_tokens: int | None = None   # Total input tokens (including cache tokens)
    output_tokens: int | None = None  # Total output tokens
    created_at: datetime | None = None
    started_at: datetime | None = None
    completed_at: datetime | None = None
    # New fields for task import/chaining
    group: str | None = None  # Group name for related tasks
    depends_on: int | None = None  # Task ID this task depends on
    spec: str | None = None  # Path to spec file for context
    create_review: bool = False  # Auto-create review task on completion
    same_branch: bool = False  # Continue on depends_on task's branch instead of creating new
    task_type_hint: str | None = None  # Explicit branch type hint (e.g., "fix", "feature")
    output_content: str | None = None  # Actual content of report/plan/review (for persistence)
    session_id: str | None = None  # Claude session ID for resume capability
    pr_number: int | None = None  # GitHub PR number
    model: str | None = None  # Per-task model override
    provider: str | None = None  # Per-task provider override
    merge_status: str | None = None  # None, 'unmerged', or 'merged'
    failure_reason: str | None = None
    skip_learnings: bool = False
    diff_files_changed: int | None = None  # Files changed vs. main (v13)
    diff_lines_added: int | None = None    # Lines added vs. main (v13)
    diff_lines_removed: int | None = None  # Lines removed vs. main (v13)

    def is_explore(self) -> bool:
        """Check if this is an exploration task."""
        return self.task_type == "explore"

    def is_blocked(self) -> bool:
        """Check if this task is blocked by a dependency."""
        return self.depends_on is not None


@dataclass
class TaskStats:
    """Statistics from a task run."""
    duration_seconds: float | None = None
    num_turns_reported: int | None = None  # Turn count reported by the provider
    num_turns_computed: int | None = None  # Turn count computed internally
    cost_usd: float | None = None
    input_tokens: int | None = None   # Total input tokens (including cache tokens)
    output_tokens: int | None = None  # Total output tokens


# Schema version for migrations
SCHEMA_VERSION = 13

SCHEMA = """
CREATE TABLE IF NOT EXISTS schema_version (
    version INTEGER PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS tasks (
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
    num_turns INTEGER,  -- kept for backward compat; use num_turns_reported instead
    num_turns_reported INTEGER,
    num_turns_computed INTEGER,
    cost_usd REAL,
    created_at TEXT NOT NULL,
    started_at TEXT,
    completed_at TEXT,
    -- New fields for task import/chaining (v2)
    "group" TEXT,
    depends_on INTEGER REFERENCES tasks(id),
    spec TEXT,
    create_review INTEGER DEFAULT 0,
    -- New field for task chaining (v3)
    same_branch INTEGER DEFAULT 0,
    -- New fields (v4)
    task_type_hint TEXT,
    output_content TEXT,
    -- New field for task resume (v5)
    session_id TEXT,
    -- New field for PR tracking (v6)
    pr_number INTEGER,
    -- New fields for per-task model/provider overrides (v7)
    model TEXT,
    provider TEXT,
    -- num_turns_reported and num_turns_computed added inline above (v8)
    -- Raw token counts for cost recalculation (v9)
    input_tokens INTEGER,
    output_tokens INTEGER,
    -- Merge status tracking (v10)
    merge_status TEXT,
    -- Failure reason tracking (v11)
    failure_reason TEXT,
    -- Skip learnings injection (v12)
    skip_learnings INTEGER DEFAULT 0,
    -- Diff stats vs. main branch (v13)
    diff_files_changed INTEGER,
    diff_lines_added INTEGER,
    diff_lines_removed INTEGER
);

CREATE INDEX IF NOT EXISTS idx_tasks_status ON tasks(status);
CREATE INDEX IF NOT EXISTS idx_tasks_task_id ON tasks(task_id);
CREATE INDEX IF NOT EXISTS idx_tasks_created_at ON tasks(created_at);
CREATE INDEX IF NOT EXISTS idx_tasks_group ON tasks("group");
CREATE INDEX IF NOT EXISTS idx_tasks_depends_on ON tasks(depends_on);
CREATE INDEX IF NOT EXISTS idx_tasks_merge_status ON tasks(merge_status);
"""

# Migration from v1 to v2
MIGRATION_V1_TO_V2 = """
ALTER TABLE tasks ADD COLUMN "group" TEXT;
ALTER TABLE tasks ADD COLUMN depends_on INTEGER REFERENCES tasks(id);
ALTER TABLE tasks ADD COLUMN spec TEXT;
ALTER TABLE tasks ADD COLUMN create_review INTEGER DEFAULT 0;
CREATE INDEX IF NOT EXISTS idx_tasks_group ON tasks("group");
CREATE INDEX IF NOT EXISTS idx_tasks_depends_on ON tasks(depends_on);
"""

# Migration from v2 to v3
MIGRATION_V2_TO_V3 = """
ALTER TABLE tasks ADD COLUMN same_branch INTEGER DEFAULT 0;
"""

# Migration from v3 to v4
MIGRATION_V3_TO_V4 = """
ALTER TABLE tasks ADD COLUMN task_type_hint TEXT;
ALTER TABLE tasks ADD COLUMN output_content TEXT;
"""

# Migration from v4 to v5
MIGRATION_V4_TO_V5 = """
ALTER TABLE tasks ADD COLUMN session_id TEXT;
"""

# Migration from v5 to v6
MIGRATION_V5_TO_V6 = """
ALTER TABLE tasks ADD COLUMN pr_number INTEGER;
"""

# Migration from v6 to v7
MIGRATION_V6_TO_V7 = """
ALTER TABLE tasks ADD COLUMN model TEXT;
ALTER TABLE tasks ADD COLUMN provider TEXT;
"""

# Migration from v7 to v8
MIGRATION_V7_TO_V8 = """
ALTER TABLE tasks ADD COLUMN num_turns_reported INTEGER;
ALTER TABLE tasks ADD COLUMN num_turns_computed INTEGER;
UPDATE tasks SET num_turns_reported = num_turns WHERE num_turns IS NOT NULL;
"""

# Migration from v8 to v9
MIGRATION_V8_TO_V9 = """
ALTER TABLE tasks ADD COLUMN input_tokens INTEGER;
ALTER TABLE tasks ADD COLUMN output_tokens INTEGER;
"""

# Migration from v9 to v10
MIGRATION_V9_TO_V10 = """
ALTER TABLE tasks ADD COLUMN merge_status TEXT;
CREATE INDEX IF NOT EXISTS idx_tasks_merge_status ON tasks(merge_status);
"""

# Migration from v10 to v11
MIGRATION_V10_TO_V11 = """
ALTER TABLE tasks ADD COLUMN failure_reason TEXT;
UPDATE tasks SET failure_reason = 'UNKNOWN' WHERE status = 'failed';
"""

# Migration from v11 to v12
MIGRATION_V11_TO_V12 = """
ALTER TABLE tasks ADD COLUMN skip_learnings INTEGER DEFAULT 0;
"""

# Migration from v12 to v13
MIGRATION_V12_TO_V13 = """
ALTER TABLE tasks ADD COLUMN diff_files_changed INTEGER;
ALTER TABLE tasks ADD COLUMN diff_lines_added INTEGER;
ALTER TABLE tasks ADD COLUMN diff_lines_removed INTEGER;
"""


def extract_failure_reason(log_file_path: Path) -> str:
    """Scan a log file for failure reason markers.

    Looks for the pattern [GZA_FAILURE:REASON] and returns the last match.
    Validates against the known set; returns 'UNKNOWN' if no valid match found.

    Args:
        log_file_path: Path to the log file to scan.

    Returns:
        A failure reason string from KNOWN_FAILURE_REASONS.
    """
    if not log_file_path.exists():
        return "UNKNOWN"

    try:
        content = log_file_path.read_text(errors="replace")
    except OSError:
        return "UNKNOWN"

    last_reason = None
    for match in _FAILURE_MARKER_RE.finditer(content):
        reason = match.group(1)
        if reason in KNOWN_FAILURE_REASONS:
            last_reason = reason

    return last_reason if last_reason is not None else "UNKNOWN"

class SqliteTaskStore:
    """SQLite-based task storage."""

    def __init__(self, db_path: Path):
        self.db_path = db_path
        self._ensure_db()

    def _ensure_db(self) -> None:
        """Ensure database exists and schema is current."""
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as conn:
            # Check if schema_version table exists
            cur = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='schema_version'"
            )
            if cur.fetchone() is None:
                # Fresh database - create full schema
                conn.executescript(SCHEMA)
                conn.execute("INSERT INTO schema_version (version) VALUES (?)", (SCHEMA_VERSION,))
            else:
                # Check current version and migrate if needed
                cur = conn.execute("SELECT version FROM schema_version LIMIT 1")
                row = cur.fetchone()
                current_version = row["version"] if row else 0

                if current_version < 2:
                    # Run migration v1 -> v2
                    for stmt in MIGRATION_V1_TO_V2.strip().split(";"):
                        stmt = stmt.strip()
                        if stmt:
                            try:
                                conn.execute(stmt)
                            except sqlite3.OperationalError:
                                # Column/index might already exist
                                pass
                    current_version = 2
                    conn.execute("UPDATE schema_version SET version = ?", (2,))

                if current_version < 3:
                    # Run migration v2 -> v3
                    for stmt in MIGRATION_V2_TO_V3.strip().split(";"):
                        stmt = stmt.strip()
                        if stmt:
                            try:
                                conn.execute(stmt)
                            except sqlite3.OperationalError:
                                # Column might already exist
                                pass
                    current_version = 3
                    conn.execute("UPDATE schema_version SET version = ?", (3,))

                if current_version < 4:
                    # Run migration v3 -> v4
                    for stmt in MIGRATION_V3_TO_V4.strip().split(";"):
                        stmt = stmt.strip()
                        if stmt:
                            try:
                                conn.execute(stmt)
                            except sqlite3.OperationalError:
                                # Column might already exist
                                pass
                    current_version = 4
                    conn.execute("UPDATE schema_version SET version = ?", (4,))

                if current_version < 5:
                    # Run migration v4 -> v5
                    for stmt in MIGRATION_V4_TO_V5.strip().split(";"):
                        stmt = stmt.strip()
                        if stmt:
                            try:
                                conn.execute(stmt)
                            except sqlite3.OperationalError:
                                # Column might already exist
                                pass
                    current_version = 5
                    conn.execute("UPDATE schema_version SET version = ?", (5,))

                if current_version < 6:
                    # Run migration v5 -> v6
                    for stmt in MIGRATION_V5_TO_V6.strip().split(";"):
                        stmt = stmt.strip()
                        if stmt:
                            try:
                                conn.execute(stmt)
                            except sqlite3.OperationalError:
                                # Column might already exist
                                pass
                    current_version = 6
                    conn.execute("UPDATE schema_version SET version = ?", (6,))

                if current_version < 7:
                    # Run migration v6 -> v7
                    for stmt in MIGRATION_V6_TO_V7.strip().split(";"):
                        stmt = stmt.strip()
                        if stmt:
                            try:
                                conn.execute(stmt)
                            except sqlite3.OperationalError:
                                # Column might already exist
                                pass
                    current_version = 7
                    conn.execute("UPDATE schema_version SET version = ?", (7,))

                if current_version < 8:
                    # Run migration v7 -> v8
                    for stmt in MIGRATION_V7_TO_V8.strip().split(";"):
                        stmt = stmt.strip()
                        if stmt:
                            try:
                                conn.execute(stmt)
                            except sqlite3.OperationalError:
                                # Column might already exist
                                pass
                    current_version = 8
                    conn.execute("UPDATE schema_version SET version = ?", (8,))

                if current_version < 9:
                    # Run migration v8 -> v9
                    for stmt in MIGRATION_V8_TO_V9.strip().split(";"):
                        stmt = stmt.strip()
                        if stmt:
                            try:
                                conn.execute(stmt)
                            except sqlite3.OperationalError:
                                # Column might already exist
                                pass
                    conn.execute("UPDATE schema_version SET version = ?", (9,))

                if current_version < 10:
                    # Run migration v9 -> v10
                    for stmt in MIGRATION_V9_TO_V10.strip().split(";"):
                        stmt = stmt.strip()
                        if stmt:
                            try:
                                conn.execute(stmt)
                            except sqlite3.OperationalError:
                                # Column/index might already exist
                                pass
                    conn.execute("UPDATE schema_version SET version = ?", (10,))
                    current_version = 10

                if current_version < 11:
                    # Run migration v10 -> v11
                    for stmt in MIGRATION_V10_TO_V11.strip().split(";"):
                        stmt = stmt.strip()
                        if stmt:
                            try:
                                conn.execute(stmt)
                            except sqlite3.OperationalError:
                                # Column might already exist
                                pass
                    conn.execute("UPDATE schema_version SET version = ?", (11,))
                    current_version = 11

                if current_version < 12:
                    # Run migration v11 -> v12
                    for stmt in MIGRATION_V11_TO_V12.strip().split(";"):
                        stmt = stmt.strip()
                        if stmt:
                            try:
                                conn.execute(stmt)
                            except sqlite3.OperationalError:
                                # Column might already exist
                                pass
                    conn.execute("UPDATE schema_version SET version = ?", (12,))
                    current_version = 12

                if current_version < 13:
                    # Run migration v12 -> v13
                    for stmt in MIGRATION_V12_TO_V13.strip().split(";"):
                        stmt = stmt.strip()
                        if stmt:
                            try:
                                conn.execute(stmt)
                            except sqlite3.OperationalError:
                                # Column might already exist
                                pass
                    conn.execute("UPDATE schema_version SET version = ?", (13,))

                if row is None:
                    conn.execute("INSERT INTO schema_version (version) VALUES (?)", (SCHEMA_VERSION,))

    def _connect(self) -> sqlite3.Connection:
        """Create a database connection with auto-commit."""
        conn = sqlite3.connect(self.db_path, isolation_level=None)
        conn.row_factory = sqlite3.Row
        return conn

    def _row_to_task(self, row: sqlite3.Row) -> Task:
        """Convert a database row to a Task."""
        return Task(
            id=row["id"],
            prompt=row["prompt"],
            status=row["status"],
            task_type=row["task_type"],
            task_id=row["task_id"],
            branch=row["branch"],
            log_file=row["log_file"],
            report_file=row["report_file"],
            based_on=row["based_on"],
            has_commits=bool(row["has_commits"]) if row["has_commits"] is not None else None,
            duration_seconds=row["duration_seconds"],
            num_turns_reported=row["num_turns_reported"] if "num_turns_reported" in row.keys() else None,
            num_turns_computed=row["num_turns_computed"] if "num_turns_computed" in row.keys() else None,
            cost_usd=row["cost_usd"],
            input_tokens=row["input_tokens"] if "input_tokens" in row.keys() else None,
            output_tokens=row["output_tokens"] if "output_tokens" in row.keys() else None,
            created_at=datetime.fromisoformat(row["created_at"]) if row["created_at"] else None,
            started_at=datetime.fromisoformat(row["started_at"]) if row["started_at"] else None,
            completed_at=datetime.fromisoformat(row["completed_at"]) if row["completed_at"] else None,
            group=row["group"],
            depends_on=row["depends_on"],
            spec=row["spec"],
            create_review=bool(row["create_review"]) if row["create_review"] is not None else False,
            same_branch=bool(row["same_branch"]) if row["same_branch"] is not None else False,
            task_type_hint=row["task_type_hint"] if "task_type_hint" in row.keys() else None,
            output_content=row["output_content"] if "output_content" in row.keys() else None,
            session_id=row["session_id"] if "session_id" in row.keys() else None,
            pr_number=row["pr_number"] if "pr_number" in row.keys() else None,
            model=row["model"] if "model" in row.keys() else None,
            provider=row["provider"] if "provider" in row.keys() else None,
            merge_status=row["merge_status"] if "merge_status" in row.keys() else None,
            failure_reason=row["failure_reason"] if "failure_reason" in row.keys() else None,
            skip_learnings=bool(row["skip_learnings"]) if "skip_learnings" in row.keys() and row["skip_learnings"] is not None else False,
            diff_files_changed=row["diff_files_changed"] if "diff_files_changed" in row.keys() else None,
            diff_lines_added=row["diff_lines_added"] if "diff_lines_added" in row.keys() else None,
            diff_lines_removed=row["diff_lines_removed"] if "diff_lines_removed" in row.keys() else None,
        )

    # === Task CRUD ===

    def add(
        self,
        prompt: str,
        task_type: str = "task",
        based_on: int | None = None,
        group: str | None = None,
        depends_on: int | None = None,
        spec: str | None = None,
        create_review: bool = False,
        same_branch: bool = False,
        task_type_hint: str | None = None,
        model: str | None = None,
        provider: str | None = None,
        skip_learnings: bool = False,
    ) -> Task:
        """Add a new task."""
        now = datetime.now(timezone.utc).isoformat()
        with self._connect() as conn:
            cur = conn.execute(
                """
                INSERT INTO tasks (prompt, task_type, based_on, created_at, "group", depends_on, spec, create_review, same_branch, task_type_hint, model, provider, skip_learnings)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (prompt, task_type, based_on, now, group, depends_on, spec, 1 if create_review else 0, 1 if same_branch else 0, task_type_hint, model, provider, 1 if skip_learnings else 0),
            )
            task_id = cur.lastrowid
            assert task_id is not None
            result = self.get(task_id)
            assert result is not None
            return result

    def get(self, task_id: int) -> Task | None:
        """Get a task by ID."""
        with self._connect() as conn:
            cur = conn.execute("SELECT * FROM tasks WHERE id = ?", (task_id,))
            row = cur.fetchone()
            return self._row_to_task(row) if row else None

    def get_by_task_id(self, task_id: str) -> Task | None:
        """Get a task by task_id (slug)."""
        with self._connect() as conn:
            cur = conn.execute("SELECT * FROM tasks WHERE task_id = ?", (task_id,))
            row = cur.fetchone()
            return self._row_to_task(row) if row else None

    def update(self, task: Task) -> None:
        """Update a task."""
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE tasks SET
                    prompt = ?,
                    status = ?,
                    task_type = ?,
                    task_id = ?,
                    branch = ?,
                    log_file = ?,
                    report_file = ?,
                    based_on = ?,
                    has_commits = ?,
                    duration_seconds = ?,
                    num_turns_reported = ?,
                    num_turns_computed = ?,
                    cost_usd = ?,
                    input_tokens = ?,
                    output_tokens = ?,
                    started_at = ?,
                    completed_at = ?,
                    "group" = ?,
                    depends_on = ?,
                    spec = ?,
                    create_review = ?,
                    same_branch = ?,
                    output_content = ?,
                    session_id = ?,
                    pr_number = ?,
                    model = ?,
                    provider = ?,
                    merge_status = ?,
                    failure_reason = ?,
                    skip_learnings = ?,
                    diff_files_changed = ?,
                    diff_lines_added = ?,
                    diff_lines_removed = ?
                WHERE id = ?
                """,
                (
                    task.prompt,
                    task.status,
                    task.task_type,
                    task.task_id,
                    task.branch,
                    task.log_file,
                    task.report_file,
                    task.based_on,
                    1 if task.has_commits else (0 if task.has_commits is False else None),
                    task.duration_seconds,
                    task.num_turns_reported,
                    task.num_turns_computed,
                    task.cost_usd,
                    task.input_tokens,
                    task.output_tokens,
                    task.started_at.isoformat() if task.started_at else None,
                    task.completed_at.isoformat() if task.completed_at else None,
                    task.group,
                    task.depends_on,
                    task.spec,
                    1 if task.create_review else 0,
                    1 if task.same_branch else 0,
                    task.output_content,
                    task.session_id,
                    task.pr_number,
                    task.model,
                    task.provider,
                    task.merge_status,
                    task.failure_reason,
                    1 if task.skip_learnings else 0,
                    task.diff_files_changed,
                    task.diff_lines_added,
                    task.diff_lines_removed,
                    task.id,
                ),
            )

    def delete(self, task_id: int) -> bool:
        """Delete a task by ID. Returns True if deleted."""
        with self._connect() as conn:
            cur = conn.execute("DELETE FROM tasks WHERE id = ?", (task_id,))
            return cur.rowcount > 0

    # === Query methods ===

    def get_next_pending(self) -> Task | None:
        """Get the next pending task (oldest first), skipping blocked tasks.

        A task is blocked if it depends on another task that is not completed.
        """
        with self._connect() as conn:
            cur = conn.execute(
                """
                SELECT t.* FROM tasks t
                WHERE t.status = 'pending'
                AND (
                    t.depends_on IS NULL
                    OR EXISTS (
                        SELECT 1 FROM tasks dep
                        WHERE dep.id = t.depends_on
                        AND dep.status = 'completed'
                    )
                )
                ORDER BY t.created_at ASC
                LIMIT 1
                """
            )
            row = cur.fetchone()
            return self._row_to_task(row) if row else None

    def claim_next_pending_task(self) -> Task | None:
        """Atomically claim the next pending task by marking it in_progress.

        This is used by background workers to ensure only one worker
        picks up a given task, even in concurrent scenarios.

        Returns:
            The claimed task, or None if no tasks are available
        """
        with self._connect() as conn:
            # Use a transaction to atomically claim the task
            conn.execute("BEGIN IMMEDIATE")
            try:
                # Find next pending task
                cur = conn.execute(
                    """
                    SELECT t.* FROM tasks t
                    WHERE t.status = 'pending'
                    AND (
                        t.depends_on IS NULL
                        OR EXISTS (
                            SELECT 1 FROM tasks dep
                            WHERE dep.id = t.depends_on
                            AND dep.status = 'completed'
                        )
                    )
                    ORDER BY t.created_at ASC
                    LIMIT 1
                    """
                )
                row = cur.fetchone()

                if not row:
                    conn.rollback()
                    return None

                task = self._row_to_task(row)

                # Mark as in_progress with timestamp
                conn.execute(
                    "UPDATE tasks SET status = 'in_progress', started_at = ? WHERE id = ?",
                    (datetime.now(timezone.utc).isoformat(), task.id)
                )

                conn.commit()
                task.status = "in_progress"
                task.started_at = datetime.now(timezone.utc)
                return task

            except Exception:
                conn.rollback()
                raise

    def get_pending(self, limit: int | None = None) -> list[Task]:
        """Get all pending tasks."""
        with self._connect() as conn:
            query = "SELECT * FROM tasks WHERE status = 'pending' ORDER BY created_at ASC"
            if limit:
                query += f" LIMIT {limit}"
            cur = conn.execute(query)
            return [self._row_to_task(row) for row in cur.fetchall()]

    def get_history(self, limit: int | None = 10, status: str | None = None, task_type: str | None = None) -> list[Task]:
        """Get completed/failed tasks, most recent first.

        Args:
            limit: Maximum number of tasks to return (None for all)
            status: Filter by specific status (e.g., 'completed', 'failed', 'unmerged')
                   If None, returns all completed/failed/unmerged tasks
            task_type: Filter by specific task_type (e.g., 'task', 'explore', 'plan', 'implement', 'review', 'improve')
                      If None, returns all task types
        """
        with self._connect() as conn:
            # Build WHERE clause based on status and task_type filters
            where_clauses = []
            params = []

            if status:
                where_clauses.append("status = ?")
                params.append(status)
            else:
                where_clauses.append("status IN ('completed', 'failed', 'unmerged')")

            if task_type:
                where_clauses.append("task_type = ?")
                params.append(task_type)

            where_clause = "WHERE " + " AND ".join(where_clauses)

            # Add LIMIT clause if specified
            if limit is None:
                query = f"""
                    SELECT * FROM tasks
                    {where_clause}
                    ORDER BY completed_at DESC, id DESC
                """
                cur = conn.execute(query, params)
            else:
                query = f"""
                    SELECT * FROM tasks
                    {where_clause}
                    ORDER BY completed_at DESC, id DESC
                    LIMIT ?
                """
                params.append(str(limit))
                cur = conn.execute(query, params)

            return [self._row_to_task(row) for row in cur.fetchall()]

    def get_unmerged(self) -> list[Task]:
        """Get tasks with unmerged code (merge_status = 'unmerged')."""
        with self._connect() as conn:
            cur = conn.execute(
                """
                SELECT * FROM tasks
                WHERE merge_status = 'unmerged'
                ORDER BY completed_at DESC
                """
            )
            return [self._row_to_task(row) for row in cur.fetchall()]

    def set_merge_status(self, task_id: int, merge_status: str | None) -> None:
        """Set the merge_status for a task."""
        with self._connect() as conn:
            conn.execute(
                "UPDATE tasks SET merge_status = ? WHERE id = ?",
                (merge_status, task_id),
            )

    def get_all(self) -> list[Task]:
        """Get all tasks."""
        with self._connect() as conn:
            cur = conn.execute("SELECT * FROM tasks ORDER BY created_at DESC")
            return [self._row_to_task(row) for row in cur.fetchall()]

    def get_reviews_for_task(self, task_id: int) -> list[Task]:
        """Get all review tasks that depend on the given task, ordered by created_at DESC."""
        with self._connect() as conn:
            cur = conn.execute(
                """
                SELECT * FROM tasks
                WHERE task_type = 'review' AND depends_on = ?
                ORDER BY created_at DESC
                """,
                (task_id,),
            )
            return [self._row_to_task(row) for row in cur.fetchall()]

    def get_stats(self) -> dict:
        """Get aggregate statistics."""
        with self._connect() as conn:
            cur = conn.execute(
                """
                SELECT
                    COUNT(*) FILTER (WHERE status = 'completed') as completed,
                    COUNT(*) FILTER (WHERE status = 'failed') as failed,
                    COUNT(*) FILTER (WHERE status = 'pending') as pending,
                    SUM(cost_usd) as total_cost,
                    SUM(duration_seconds) as total_duration,
                    SUM(num_turns_reported) as total_turns,
                    SUM(input_tokens) as total_input_tokens,
                    SUM(output_tokens) as total_output_tokens
                FROM tasks
                """
            )
            row = cur.fetchone()
            return {
                "completed": row["completed"] or 0,
                "failed": row["failed"] or 0,
                "pending": row["pending"] or 0,
                "total_cost": row["total_cost"] or 0,
                "total_duration": row["total_duration"] or 0,
                "total_turns": row["total_turns"] or 0,
                "total_input_tokens": row["total_input_tokens"] or 0,
                "total_output_tokens": row["total_output_tokens"] or 0,
            }

    def search(self, query: str) -> list[Task]:
        """Search tasks by prompt content."""
        with self._connect() as conn:
            cur = conn.execute(
                """
                SELECT * FROM tasks
                WHERE prompt LIKE ?
                ORDER BY created_at DESC
                """,
                (f"%{query}%",),
            )
            return [self._row_to_task(row) for row in cur.fetchall()]

    def get_groups(self) -> dict[str, dict[str, int]]:
        """Get all groups with task counts by status.

        Returns:
            Dict mapping group name to dict of status counts.
            Example: {"tarantino-v2": {"pending": 1, "completed": 2}, ...}
        """
        with self._connect() as conn:
            cur = conn.execute(
                """
                SELECT "group", status, COUNT(*) as count
                FROM tasks
                WHERE "group" IS NOT NULL
                GROUP BY "group", status
                """
            )
            groups: dict[str, dict[str, int]] = {}
            for row in cur.fetchall():
                group_name = row["group"]
                status = row["status"]
                count = row["count"]
                if group_name not in groups:
                    groups[group_name] = {}
                groups[group_name][status] = count
            return groups

    def get_by_group(self, group: str) -> list[Task]:
        """Get all tasks in a group, ordered by creation time."""
        with self._connect() as conn:
            cur = conn.execute(
                """
                SELECT * FROM tasks
                WHERE "group" = ?
                ORDER BY created_at ASC
                """,
                (group,)
            )
            return [self._row_to_task(row) for row in cur.fetchall()]

    def is_task_blocked(self, task: Task) -> tuple[bool, int | None, str | None]:
        """Check if a task is blocked by an incomplete dependency.

        Returns:
            Tuple of (is_blocked, blocking_task_id, blocking_task_status)
        """
        if task.depends_on is None:
            return (False, None, None)

        dep = self.get(task.depends_on)
        if dep is None:
            return (False, None, None)

        if dep.status == "completed":
            return (False, None, None)

        return (True, dep.id, dep.status)

    def count_blocked_tasks(self) -> int:
        """Count pending tasks that are blocked by dependencies."""
        with self._connect() as conn:
            cur = conn.execute(
                """
                SELECT COUNT(*) as count FROM tasks t
                WHERE t.status = 'pending'
                AND t.depends_on IS NOT NULL
                AND NOT EXISTS (
                    SELECT 1 FROM tasks dep
                    WHERE dep.id = t.depends_on
                    AND dep.status = 'completed'
                )
                """
            )
            row = cur.fetchone()
            return row["count"] if row else 0

    # === Status transitions (TaskStore protocol) ===

    def mark_in_progress(self, task: Task) -> None:
        """Mark a task as in progress."""
        task.status = "in_progress"
        task.started_at = datetime.now(timezone.utc)
        self.update(task)

    def mark_completed(
        self,
        task: Task,
        branch: str | None = None,
        log_file: str | None = None,
        report_file: str | None = None,
        output_content: str | None = None,
        has_commits: bool = False,
        stats: TaskStats | None = None,
        diff_files_changed: int | None = None,
        diff_lines_added: int | None = None,
        diff_lines_removed: int | None = None,
    ) -> None:
        """Mark a task as completed."""
        task.status = "completed"
        task.completed_at = datetime.now(timezone.utc)
        task.has_commits = has_commits
        if has_commits:
            task.merge_status = "unmerged"
        if branch:
            task.branch = branch
        if log_file:
            task.log_file = log_file
        if report_file:
            task.report_file = report_file
        if output_content is not None:
            task.output_content = output_content
        if stats:
            task.duration_seconds = stats.duration_seconds
            task.num_turns_reported = stats.num_turns_reported
            task.num_turns_computed = stats.num_turns_computed
            task.cost_usd = stats.cost_usd
            task.input_tokens = stats.input_tokens
            task.output_tokens = stats.output_tokens
        task.diff_files_changed = diff_files_changed
        task.diff_lines_added = diff_lines_added
        task.diff_lines_removed = diff_lines_removed
        self.update(task)

    def update_diff_stats(
        self,
        task_id: int,
        files_changed: int | None,
        lines_added: int | None,
        lines_removed: int | None,
    ) -> None:
        """Update only the diff stats columns for a task."""
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE tasks SET
                    diff_files_changed = ?,
                    diff_lines_added = ?,
                    diff_lines_removed = ?
                WHERE id = ?
                """,
                (files_changed, lines_added, lines_removed, task_id),
            )

    def mark_failed(
        self,
        task: Task,
        log_file: str | None = None,
        has_commits: bool = False,
        stats: TaskStats | None = None,
        branch: str | None = None,
        failure_reason: str | None = None,
    ) -> None:
        """Mark a task as failed."""
        task.status = "failed"
        task.completed_at = datetime.now(timezone.utc)
        task.has_commits = has_commits
        if log_file:
            task.log_file = log_file
        if branch:
            task.branch = branch
        if stats:
            task.duration_seconds = stats.duration_seconds
            task.num_turns_reported = stats.num_turns_reported
            task.num_turns_computed = stats.num_turns_computed
            task.cost_usd = stats.cost_usd
            task.input_tokens = stats.input_tokens
            task.output_tokens = stats.output_tokens
        task.failure_reason = failure_reason if failure_reason is not None else "UNKNOWN"
        self.update(task)

    def mark_unmerged(
        self,
        task: Task,
        branch: str | None = None,
        log_file: str | None = None,
        has_commits: bool = False,
        stats: TaskStats | None = None,
    ) -> None:
        """Mark a task as unmerged."""
        task.status = "unmerged"
        task.completed_at = datetime.now(timezone.utc)
        task.has_commits = has_commits
        if branch:
            task.branch = branch
        if log_file:
            task.log_file = log_file
        if stats:
            task.duration_seconds = stats.duration_seconds
            task.num_turns_reported = stats.num_turns_reported
            task.num_turns_computed = stats.num_turns_computed
            task.cost_usd = stats.cost_usd
            task.input_tokens = stats.input_tokens
            task.output_tokens = stats.output_tokens
        self.update(task)


# === Merge status migration ===

def needs_merge_status_migration(store: "SqliteTaskStore") -> bool:
    """Check if any tasks need merge_status backfilled."""
    with store._connect() as conn:
        cur = conn.execute(
            "SELECT COUNT(*) FROM tasks WHERE merge_status IS NULL AND has_commits = 1"
        )
        return cur.fetchone()[0] > 0


def migrate_merge_status(store: "SqliteTaskStore", git: "object") -> None:
    """Infer merge_status for existing tasks based on current git state.

    This is used to backfill merge_status for tasks created before the
    merge_status column was added.

    Args:
        store: The task store
        git: A Git instance for the project
    """
    from .git import Git as GitClass
    assert isinstance(git, GitClass)

    with store._connect() as conn:
        cur = conn.execute(
            "SELECT id, has_commits, branch FROM tasks WHERE merge_status IS NULL"
        )
        rows = cur.fetchall()

    default_branch = git.default_branch()

    for row in rows:
        task_id = row["id"]
        has_commits = row["has_commits"]
        branch = row["branch"]

        if not has_commits:
            merge_status = None
        elif not branch:
            merge_status = None
        elif not git.branch_exists(branch):
            # Branch deleted - assume merged
            merge_status = "merged"
        else:
            try:
                # Check if branch is an ancestor of main (i.e., fully merged)
                result = git._run(
                    "merge-base", "--is-ancestor", branch, default_branch, check=False
                )
                if result.returncode == 0:
                    merge_status = "merged"
                elif git.is_merged(branch, default_branch):
                    # No diff (squash merged or equivalent content)
                    merge_status = "merged"
                else:
                    merge_status = "unmerged"
            except Exception:
                merge_status = "unmerged"

        store.set_merge_status(task_id, merge_status)


# === Editor support ===

TASK_TEMPLATE_HEADER = """# Enter your task prompt below.
# Lines starting with # are comments and will be ignored.
# Save and close the editor when done.
#
"""


def edit_prompt(
    initial_content: str = "",
    task_type: str = "task",
    based_on: int | None = None,
    spec: str | None = None,
    group: str | None = None,
    depends_on: int | None = None,
    create_review: bool = False,
    same_branch: bool = False,
    model: str | None = None,
    provider: str | None = None,
) -> str | None:
    """Open $EDITOR for the user to enter/edit a prompt.

    Returns the prompt text, or None if cancelled/empty.
    """
    editor = os.environ.get("EDITOR", "vim")

    # Build options section
    options = [f"# Type: {task_type}"]
    if based_on:
        options.append(f"# Based on: #{based_on}")
    if depends_on:
        options.append(f"# Depends on: #{depends_on}")
    if group:
        options.append(f"# Group: {group}")
    if spec:
        options.append(f"# Spec: {spec}")
    if create_review:
        options.append("# Create review: yes")
    if same_branch:
        options.append("# Same branch: yes")
    if model:
        options.append(f"# Model: {model}")
    if provider:
        options.append(f"# Provider: {provider}")

    template = TASK_TEMPLATE_HEADER + "\n".join(options) + "\n"

    # Provide default prompt for implement tasks with based_on
    # This makes the slug unique by including the task ID
    if not initial_content and task_type == "implement" and based_on:
        initial_content = f"Implement the plan from task #{based_on}"

    content = template + "\n" + initial_content

    with tempfile.NamedTemporaryFile(mode="w", suffix=".md", delete=False) as f:
        f.write(content)
        tmp_path = f.name

    try:
        result = subprocess.run([editor, tmp_path])
        if result.returncode != 0:
            return None

        with open(tmp_path) as f:
            lines = f.readlines()

        # Strip comments and empty lines
        prompt_lines = [line for line in lines if not line.startswith("#")]
        prompt = "".join(prompt_lines).strip()

        return prompt if prompt else None
    finally:
        os.unlink(tmp_path)


def add_task_interactive(
    store: SqliteTaskStore,
    task_type: str = "task",
    based_on: int | None = None,
    spec: str | None = None,
    group: str | None = None,
    depends_on: int | None = None,
    create_review: bool = False,
    same_branch: bool = False,
    task_type_hint: str | None = None,
    model: str | None = None,
    provider: str | None = None,
    skip_learnings: bool = False,
) -> Task | None:
    """Interactively add a task using $EDITOR.

    Returns the created task, or None if cancelled.
    """
    while True:
        prompt = edit_prompt(
            task_type=task_type,
            based_on=based_on,
            spec=spec,
            group=group,
            depends_on=depends_on,
            create_review=create_review,
            same_branch=same_branch,
            model=model,
            provider=provider,
        )

        if prompt is None:
            print("Task cancelled (empty prompt)")
            return None

        # Validate prompt
        errors = validate_prompt(prompt)

        if not errors:
            return store.add(
                prompt,
                task_type=task_type,
                based_on=based_on,
                group=group,
                depends_on=depends_on,
                spec=spec,
                create_review=create_review,
                same_branch=same_branch,
                task_type_hint=task_type_hint,
                model=model,
                provider=provider,
                skip_learnings=skip_learnings,
            )

        # Show errors and ask what to do
        print("Validation errors:")
        for error in errors:
            print(f"  - {error}")

        choice = input("\n(e)dit again, (q)uit? ").strip().lower()
        if choice == 'q':
            print("Task not created.")
            return None
        # Otherwise loop back to editor


def edit_task_interactive(store: SqliteTaskStore, task: Task) -> bool:
    """Interactively edit a task's prompt using $EDITOR.

    Returns True if edited successfully, False if cancelled.
    """
    while True:
        prompt = edit_prompt(
            initial_content=task.prompt,
            task_type=task.task_type,
            based_on=task.based_on,
            spec=task.spec,
            group=task.group,
            depends_on=task.depends_on,
            create_review=task.create_review,
            same_branch=task.same_branch,
            model=task.model,
            provider=task.provider,
        )

        if prompt is None:
            print("Edit cancelled")
            return False

        errors = validate_prompt(prompt)

        if not errors:
            task.prompt = prompt
            store.update(task)
            return True

        print("Validation errors:")
        for error in errors:
            print(f"  - {error}")

        choice = input("\n(e)dit again, (q)uit? ").strip().lower()
        if choice == 'q':
            print("Edit cancelled.")
            return False


def validate_prompt(prompt: str) -> list[str]:
    """Validate a task prompt.

    Returns list of error messages (empty if valid).
    """
    errors = []

    if not prompt:
        errors.append("Prompt cannot be empty")
    elif len(prompt) < 10:
        errors.append("Prompt is too short (minimum 10 characters)")
    elif len(prompt) > 10000:
        errors.append("Prompt is too long (maximum 10000 characters)")

    return errors
