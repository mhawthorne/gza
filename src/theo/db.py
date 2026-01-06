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
    task_type: str = "task"  # task, explore
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

    def is_explore(self) -> bool:
        """Check if this is an exploration task."""
        return self.task_type == "explore"


@dataclass
class TaskStats:
    """Statistics from a task run."""
    duration_seconds: float | None = None
    num_turns: int | None = None
    cost_usd: float | None = None


# Schema version for migrations
SCHEMA_VERSION = 1

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
    completed_at TEXT
);

CREATE INDEX IF NOT EXISTS idx_tasks_status ON tasks(status);
CREATE INDEX IF NOT EXISTS idx_tasks_task_id ON tasks(task_id);
CREATE INDEX IF NOT EXISTS idx_tasks_created_at ON tasks(created_at);
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
            conn.executescript(SCHEMA)
            # Check/set schema version
            cur = conn.execute("SELECT version FROM schema_version LIMIT 1")
            row = cur.fetchone()
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
        )

    # === Task CRUD ===

    def add(self, prompt: str, task_type: str = "task", based_on: int | None = None) -> Task:
        """Add a new task."""
        now = datetime.now(timezone.utc).isoformat()
        with self._connect() as conn:
            cur = conn.execute(
                """
                INSERT INTO tasks (prompt, task_type, based_on, created_at)
                VALUES (?, ?, ?, ?)
                """,
                (prompt, task_type, based_on, now),
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
                    completed_at = ?
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
        """Get the next pending task (oldest first)."""
        with self._connect() as conn:
            cur = conn.execute(
                """
                SELECT * FROM tasks
                WHERE status = 'pending'
                ORDER BY created_at ASC
                LIMIT 1
                """
            )
            row = cur.fetchone()
            return self._row_to_task(row) if row else None

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

TASK_TEMPLATE = """# Enter your task prompt below.
# Lines starting with # are comments and will be ignored.
# Save and close the editor when done.
#
# Task type: {task_type}
{based_on_line}
"""


def edit_prompt(initial_content: str = "", task_type: str = "task", based_on: int | None = None) -> str | None:
    """Open $EDITOR for the user to enter/edit a prompt.

    Returns the prompt text, or None if cancelled/empty.
    """
    editor = os.environ.get("EDITOR", "vim")

    based_on_line = f"# Based on task: {based_on}" if based_on else ""
    template = TASK_TEMPLATE.format(task_type=task_type, based_on_line=based_on_line)

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


def add_task_interactive(store: SqliteTaskStore, task_type: str = "task", based_on: int | None = None) -> Task | None:
    """Interactively add a task using $EDITOR.

    Returns the created task, or None if cancelled.
    """
    while True:
        prompt = edit_prompt(task_type=task_type, based_on=based_on)

        if prompt is None:
            print("Task cancelled (empty prompt)")
            return None

        # Validate prompt
        errors = validate_prompt(prompt)

        if not errors:
            return store.add(prompt, task_type=task_type, based_on=based_on)

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
            based_on=task.based_on
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
