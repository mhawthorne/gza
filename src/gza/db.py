"""SQLite-based task storage."""

import os
import sqlite3
import subprocess
import tempfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path


@dataclass
class Task:
    """A task in the database."""
    id: int | None  # None for unsaved tasks
    prompt: str
    status: str = "pending"  # pending, in_progress, completed, failed, unmerged
    task_type: str = "task"  # task, explore, plan, implement, review
    task_id: str | None = None  # YYYYMMDD-slug format
    branch: str | None = None
    log_file: str | None = None
    report_file: str | None = None
    based_on: int | None = None  # Reference to parent task id
    has_commits: bool | None = None
    duration_seconds: float | None = None
    num_turns: int | None = None
    cost_usd: float | None = None
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
    num_turns: int | None = None
    cost_usd: float | None = None


# Schema version for migrations
SCHEMA_VERSION = 5

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
    num_turns INTEGER,
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
    session_id TEXT
);

CREATE INDEX IF NOT EXISTS idx_tasks_status ON tasks(status);
CREATE INDEX IF NOT EXISTS idx_tasks_task_id ON tasks(task_id);
CREATE INDEX IF NOT EXISTS idx_tasks_created_at ON tasks(created_at);
CREATE INDEX IF NOT EXISTS idx_tasks_group ON tasks("group");
CREATE INDEX IF NOT EXISTS idx_tasks_depends_on ON tasks(depends_on);
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
                    conn.execute("UPDATE schema_version SET version = ?", (5,))

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
            num_turns=row["num_turns"],
            cost_usd=row["cost_usd"],
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
    ) -> Task:
        """Add a new task."""
        now = datetime.now(timezone.utc).isoformat()
        with self._connect() as conn:
            cur = conn.execute(
                """
                INSERT INTO tasks (prompt, task_type, based_on, created_at, "group", depends_on, spec, create_review, same_branch, task_type_hint)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (prompt, task_type, based_on, now, group, depends_on, spec, 1 if create_review else 0, 1 if same_branch else 0, task_type_hint),
            )
            task_id = cur.lastrowid
            return self.get(task_id)

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
                    num_turns = ?,
                    cost_usd = ?,
                    started_at = ?,
                    completed_at = ?,
                    "group" = ?,
                    depends_on = ?,
                    spec = ?,
                    create_review = ?,
                    same_branch = ?,
                    output_content = ?,
                    session_id = ?
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
                    task.num_turns,
                    task.cost_usd,
                    task.started_at.isoformat() if task.started_at else None,
                    task.completed_at.isoformat() if task.completed_at else None,
                    task.group,
                    task.depends_on,
                    task.spec,
                    1 if task.create_review else 0,
                    1 if task.same_branch else 0,
                    task.output_content,
                    task.session_id,
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

    def get_history(self, limit: int = 10) -> list[Task]:
        """Get completed/failed tasks, most recent first."""
        with self._connect() as conn:
            cur = conn.execute(
                """
                SELECT * FROM tasks
                WHERE status IN ('completed', 'failed', 'unmerged')
                ORDER BY completed_at DESC, id DESC
                LIMIT ?
                """,
                (limit,),
            )
            return [self._row_to_task(row) for row in cur.fetchall()]

    def get_unmerged(self) -> list[Task]:
        """Get tasks with unmerged status."""
        with self._connect() as conn:
            cur = conn.execute(
                """
                SELECT * FROM tasks
                WHERE status = 'unmerged'
                ORDER BY completed_at DESC
                """
            )
            return [self._row_to_task(row) for row in cur.fetchall()]

    def get_all(self) -> list[Task]:
        """Get all tasks."""
        with self._connect() as conn:
            cur = conn.execute("SELECT * FROM tasks ORDER BY created_at DESC")
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
                    SUM(num_turns) as total_turns
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
    ) -> None:
        """Mark a task as completed."""
        task.status = "completed"
        task.completed_at = datetime.now(timezone.utc)
        task.has_commits = has_commits
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
            task.num_turns = stats.num_turns
            task.cost_usd = stats.cost_usd
        self.update(task)

    def mark_failed(
        self,
        task: Task,
        log_file: str | None = None,
        has_commits: bool = False,
        stats: TaskStats | None = None,
    ) -> None:
        """Mark a task as failed."""
        task.status = "failed"
        task.completed_at = datetime.now(timezone.utc)
        task.has_commits = has_commits
        if log_file:
            task.log_file = log_file
        if stats:
            task.duration_seconds = stats.duration_seconds
            task.num_turns = stats.num_turns
            task.cost_usd = stats.cost_usd
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
            task.num_turns = stats.num_turns
            task.cost_usd = stats.cost_usd
        self.update(task)


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

    template = TASK_TEMPLATE_HEADER + "\n".join(options) + "\n"

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
