"""SQLite-based task storage."""

import json
import logging
import os
import re
import sqlite3
import subprocess
import tempfile
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal, TypedDict

from gza.resume_policy import RESUMABLE_FAILURE_REASONS, is_resumable_failure_reason

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from gza.config import Config


def _launch_editor(cmd: list[str]) -> subprocess.CompletedProcess[bytes]:
    """Seam for tests: invoke the user's editor. Tests patch this, not ``subprocess.run``."""
    return subprocess.run(cmd)

__all__ = [
    "KNOWN_FAILURE_REASONS",
    "KNOWN_EXECUTION_MODES",
    "InvalidTaskIdError",
    "ManualMigrationRequired",
    "SchemaIntegrityError",
    "Task",
    "TaskComment",
    "TaskStats",
    "SqliteTaskStore",
    "extract_failure_reason",
    "run_v25_migration",
    "run_v26_migration",
    "run_v27_migration",
    "preview_v25_migration",
    "preview_v26_migration",
    "check_migration_status",
    "import_legacy_local_db",
    "resolve_task_id",
    "task_id_numeric_key",
]

StoreOpenMode = Literal["readwrite", "query_only"]


# Known failure reason categories
KNOWN_FAILURE_REASONS = {
    "INFRASTRUCTURE_ERROR",
    "MAX_STEPS",
    "MAX_TURNS",
    "PR_REQUIRED",
    "PREREQUISITE_UNMERGED",
    "TEST_FAILURE",
    "TIMEOUT",
    "TERMINATED",
    "WORKER_DIED",
    "KILLED",
    "UNKNOWN",
}

KNOWN_EXECUTION_MODES = {
    "worker_background",
    "worker_foreground",
    "foreground_inline",
    "foreground_attach_resume",
    "manual",
    "skill_inline",
}

_FAILURE_MARKER_RE = re.compile(r"\[GZA_FAILURE:(\w+)\]")

# Legacy base36 alphabet used only by v25 migration helpers.
_B36_CHARS = "0123456789abcdefghijklmnopqrstuvwxyz"

# Legacy fixed width used only for v25 migration helper encoding.
_TASK_ID_SEQ_WIDTH = 6
_FULL_TASK_ID_RE = re.compile(r"^[a-z0-9]{1,12}-[0-9]+$")
_TAG_WS_RE = re.compile(r"\s+")


class InvalidTaskIdError(ValueError):
    """Raised when a user-supplied task ID is not a full prefixed ID."""


class SchemaIntegrityError(RuntimeError):
    """Raised when persisted DB artifacts are internally inconsistent."""


class _ClosingSqliteConnection(sqlite3.Connection):
    """sqlite3 connection that always closes when exiting a context manager."""

    def __exit__(self, exc_type, exc, tb) -> Literal[False]:
        try:
            return super().__exit__(exc_type, exc, tb)
        finally:
            self.close()


def _encode_v25_base36(n: int) -> str:
    """Encode integer IDs the same way v25 migration encoded them."""
    chars = "0123456789abcdefghijklmnopqrstuvwxyz"
    width = 6
    if n < 0:
        raise ValueError("n must be non-negative")
    if n == 0:
        return "0".zfill(width)
    result: list[str] = []
    while n > 0:
        result.append(chars[n % 36])
        n //= 36
    return "".join(reversed(result)).zfill(width)


def _decode_base36(s: str) -> int:
    """Decode a base-36 string to an integer."""
    return int(s, 36)


def task_id_numeric_key(task_id: str | None) -> int:
    """Return an integer sort key for a task ID that preserves creation order.

    Task IDs are ``{prefix}-{decimal_seq}`` (e.g. ``"gza-1234"``). String sort
    does not match numeric order for variable-width decimal IDs
    (``gza-10`` vs ``gza-2``), so callers should sort via this helper.

    Returns 0 for ``None``, empty, or IDs without a hyphen (e.g. legacy bare
    integers stored as strings), and 0 for suffixes that fail decimal parsing.
    """
    if not task_id or "-" not in task_id:
        return 0
    suffix = task_id.rsplit("-", 1)[-1]
    try:
        return int(suffix)
    except ValueError:
        return 0


def _normalize_tag(tag: str) -> str:
    """Normalize user-provided tag text into canonical storage format."""
    normalized = _TAG_WS_RE.sub(" ", tag.strip()).lower()
    if not normalized:
        raise ValueError("tag must not be empty")
    return normalized


def _normalize_tags(tags: Iterable[str] | None) -> tuple[str, ...]:
    """Normalize and deduplicate tags with deterministic ordering."""
    if tags is None:
        return ()
    normalized: set[str] = set()
    for raw in tags:
        normalized.add(_normalize_tag(str(raw)))
    return tuple(sorted(normalized))


def _backfill_task_tags_from_group(conn: sqlite3.Connection) -> None:
    """Backfill legacy tasks.group values into task_tags."""
    if not _table_exists(conn, "task_tags"):
        return
    rows = conn.execute('SELECT project_id, id, "group" FROM tasks WHERE "group" IS NOT NULL').fetchall()
    pairs: list[tuple[str, str, str]] = []
    for row in rows:
        group_value = row["group"]
        if group_value is None:
            continue
        try:
            normalized = _normalize_tag(str(group_value))
        except ValueError:
            continue
        project_id = str(row["project_id"]) if "project_id" in row.keys() and row["project_id"] else "default"
        pairs.append((project_id, str(row["id"]), normalized))
    if pairs:
        conn.executemany(
            "INSERT OR IGNORE INTO task_tags(project_id, task_id, tag) VALUES (?, ?, ?)",
            pairs,
        )


def _next_monotonic_iso_timestamp(now: datetime, floor_iso: str | None) -> str:
    """Return an ISO timestamp that is strictly later than ``floor_iso`` when needed."""
    if floor_iso:
        try:
            floor = _parse_db_timestamp(floor_iso)
        except ValueError:
            floor = None
        if floor is not None and now <= floor:
            now = floor + timedelta(microseconds=1)
    return now.isoformat()


def _parse_db_timestamp(value: str | None) -> datetime | None:
    """Parse persisted timestamps and normalize legacy naive values to UTC-aware."""
    if not value:
        return None
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


class ManualMigrationRequired(Exception):
    """Raised when the DB needs a manual schema migration (e.g. v25/v26).

    Callers should run ``gza migrate`` (or call the relevant manual migration)
    and then re-open the store.
    """

    def __init__(self, pending_versions: list[int]) -> None:
        self.pending_versions = pending_versions
        versions_str = ", ".join(f"v{v}" for v in pending_versions)
        super().__init__(
            f"Database requires manual migration(s): {versions_str}. "
            "Run 'gza migrate' to upgrade."
        )


def _compute_percentiles(values: list[float]) -> dict | None:
    """Compute min/max/avg/median/p90 for a list of numeric values.

    Returns None if the list is empty.

    Note: For samples with fewer than 10 values, ``p90`` equals ``max``.
    This is a deliberate conservative choice — the nearest-rank formula
    would give the same result for most small samples anyway, and returning
    ``max`` avoids the misleading impression of a precise 90th-percentile
    estimate from very few data points.
    """
    if not values:
        return None
    sorted_vals = sorted(values)
    n = len(sorted_vals)
    total = sum(sorted_vals)
    avg = total / n

    # Median: average of two middle elements for even n
    mid = n // 2
    if n % 2 == 1:
        median = sorted_vals[mid]
    else:
        median = (sorted_vals[mid - 1] + sorted_vals[mid]) / 2

    # p90: 90th percentile via nearest-rank method
    p90_idx = max(0, int(0.9 * n) - 1)
    p90 = sorted_vals[p90_idx] if n >= 10 else sorted_vals[-1]

    return {
        "min": sorted_vals[0],
        "max": sorted_vals[-1],
        "avg": round(avg, 2),
        "median": median,
        "p90": p90,
        "count": n,
    }


@dataclass
class Task:
    """A task in the database."""
    id: str | None  # None for unsaved tasks; project-prefixed decimal (e.g. "gza-1234")
    prompt: str
    status: str = "pending"  # pending, in_progress, completed, failed, unmerged, dropped
    task_type: str = "implement"  # explore, plan, implement, review, improve, rebase, internal
    slug: str | None = None  # YYYYMMDD-slug format (DB column: slug, was task_id)
    branch: str | None = None
    log_file: str | None = None
    report_file: str | None = None
    based_on: str | None = None  # Reference to parent task id (string)
    has_commits: bool | None = None
    duration_seconds: float | None = None
    num_steps_reported: int | None = None  # Step count reported by the provider
    num_steps_computed: int | None = None  # Step count computed internally
    num_turns_reported: int | None = None  # Turn count reported by the provider
    num_turns_computed: int | None = None  # Turn count computed internally
    attach_count: int | None = None  # Number of interactive attach sessions
    attach_duration_seconds: float | None = None  # Total interactive attach wall time
    cost_usd: float | None = None
    input_tokens: int | None = None   # Total input tokens (including cache tokens)
    output_tokens: int | None = None  # Total output tokens
    created_at: datetime | None = None
    started_at: datetime | None = None
    running_pid: int | None = None
    completed_at: datetime | None = None
    # New fields for task import/chaining
    group: str | None = None  # Group name for related tasks
    tags: tuple[str, ...] = ()  # Canonical multi-valued task labels
    depends_on: str | None = None  # Task ID this task depends on (string)
    spec: str | None = None  # Path to spec file for context
    create_review: bool = False  # Auto-create review task on completion
    create_pr: bool = False  # Auto-create/reuse PR on successful code-task completion
    same_branch: bool = False  # Continue on depends_on task's branch instead of creating new
    base_branch: str | None = None  # Optional branch ref used when creating a fresh retry branch
    task_type_hint: str | None = None  # Explicit branch type hint (e.g., "fix", "feature")
    output_content: str | None = None  # Actual content of report/plan/review (for persistence)
    session_id: str | None = None  # Claude session ID for resume capability
    pr_number: int | None = None  # GitHub PR number
    pr_state: str | None = None  # Cached GitHub PR state: open, closed, merged
    pr_last_synced_at: datetime | None = None  # When PR metadata was last refreshed
    model: str | None = None  # Per-task model override
    provider: str | None = None  # Per-task provider override
    provider_is_explicit: bool = False  # True when provider was explicitly set by user input
    urgent: bool = False  # Queue lane flag: urgent tasks are picked before normal pending tasks
    queue_position: int | None = None  # Optional explicit queue order within the selected queue bucket
    merge_status: str | None = None  # None, 'unmerged', or 'merged'
    merged_at: datetime | None = None  # When merge_status was set to 'merged'
    failure_reason: str | None = None
    skip_learnings: bool = False
    diff_files_changed: int | None = None  # Files changed vs. main (v13)
    diff_lines_added: int | None = None    # Lines added vs. main (v13)
    diff_lines_removed: int | None = None  # Lines removed vs. main (v13)
    review_cleared_at: datetime | None = None  # When review state was cleared by an improve task (v14)
    review_score: int | None = None  # Derived deterministic score for completed review tasks (v33)
    log_schema_version: int = 1  # 1=legacy logs, 2=message-step logs
    execution_mode: str | None = None  # worker_background, worker_foreground, foreground_inline, foreground_attach_resume, manual, skill_inline

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
    num_steps_reported: int | None = None  # Step count reported by the provider
    num_steps_computed: int | None = None  # Step count computed internally
    num_turns_reported: int | None = None  # Turn count reported by the provider
    num_turns_computed: int | None = None  # Turn count computed internally
    cost_usd: float | None = None
    input_tokens: int | None = None   # Total input tokens (including cache tokens)
    output_tokens: int | None = None  # Total output tokens
    tokens_estimated: bool = False
    cost_estimated: bool = False


@dataclass(frozen=True)
class TaskComment:
    """A user/operator comment attached to a task."""

    id: int
    task_id: str
    content: str
    source: str
    author: str | None
    created_at: datetime
    resolved_at: datetime | None

# Migration from v18 to v19
MIGRATION_V18_TO_V19 = """
CREATE INDEX IF NOT EXISTS idx_tasks_type_based_on ON tasks(task_type, based_on);
CREATE UNIQUE INDEX IF NOT EXISTS uq_task_cycle_iterations_cycle_iter ON task_cycle_iterations(cycle_id, iteration_index);
"""

# Migration from v19 to v20
MIGRATION_V19_TO_V20 = "UPDATE tasks SET task_type='implement' WHERE task_type='task';"

# Migration from v20 to v21
MIGRATION_V20_TO_V21 = """
ALTER TABLE tasks ADD COLUMN provider_is_explicit INTEGER DEFAULT 0;
"""

# Migration from v21 to v22
MIGRATION_V21_TO_V22 = "UPDATE tasks SET task_type='internal' WHERE task_type='learn';"

# Migration from v22 to v23
MIGRATION_V22_TO_V23 = "ALTER TABLE tasks ADD COLUMN running_pid INTEGER;"

# Migration from v23 to v24
MIGRATION_V23_TO_V24 = "ALTER TABLE tasks ADD COLUMN merged_at TEXT;"

# Migration from v27 to v28: add attach metrics columns
MIGRATION_V27_TO_V28 = """
ALTER TABLE tasks ADD COLUMN attach_count INTEGER;
ALTER TABLE tasks ADD COLUMN attach_duration_seconds REAL;
"""

# Migration from v28 to v29: add queue urgency flag
MIGRATION_V28_TO_V29 = """
ALTER TABLE tasks ADD COLUMN urgent INTEGER DEFAULT 0;
"""

# Migration from v29 to v30: record bump time so "queue bump" can move to front
MIGRATION_V29_TO_V30 = """
ALTER TABLE tasks ADD COLUMN urgent_bumped_at TEXT;
"""

# Migration from v30 to v31: persist execution provenance mode
MIGRATION_V30_TO_V31 = """
ALTER TABLE tasks ADD COLUMN execution_mode TEXT;
"""

# Migration from v31 to v32: task comments table
MIGRATION_V31_TO_V32 = """
CREATE TABLE IF NOT EXISTS task_comments (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id TEXT NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
    content TEXT NOT NULL,
    source TEXT NOT NULL,
    author TEXT,
    created_at TEXT NOT NULL,
    resolved_at TEXT
);
CREATE INDEX IF NOT EXISTS idx_task_comments_task_created ON task_comments(task_id, created_at ASC);
CREATE INDEX IF NOT EXISTS idx_task_comments_task_unresolved ON task_comments(task_id, resolved_at);
"""

# Migration from v32 to v33: derived review score persistence
MIGRATION_V32_TO_V33 = """
ALTER TABLE tasks ADD COLUMN review_score INTEGER;
"""

# Migration from v33 to v34: explicit queue positions for ordered pickup
MIGRATION_V33_TO_V34 = """
ALTER TABLE tasks ADD COLUMN queue_position INTEGER;
"""

# Migration from v34 to v35: first-class multi-valued tags
MIGRATION_V34_TO_V35 = """
CREATE TABLE IF NOT EXISTS task_tags (
    task_id TEXT NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
    tag TEXT NOT NULL,
    PRIMARY KEY(task_id, tag)
);
CREATE INDEX IF NOT EXISTS idx_task_tags_tag ON task_tags(tag);
CREATE INDEX IF NOT EXISTS idx_task_tags_task_id ON task_tags(task_id);
"""

# Migration from v35 to v36: shared DB project scoping primitives
MIGRATION_V35_TO_V36 = """
-- v36 requires a structural table rebuild to make project-scoped keys/fks/uniques
-- equivalent to fresh SCHEMA. See _run_v35_to_v36_migration().
"""

# Migration from v36 to v37: persisted PR-creation intent
MIGRATION_V36_TO_V37 = """
ALTER TABLE tasks ADD COLUMN create_pr INTEGER DEFAULT 0;
"""

# Migration from v37 to v38: cached PR state metadata
MIGRATION_V37_TO_V38 = """
ALTER TABLE tasks ADD COLUMN pr_state TEXT;
ALTER TABLE tasks ADD COLUMN pr_last_synced_at TEXT;
"""

# Schema version for migrations
SCHEMA_VERSION = 38

# Migration versions that require manual intervention (gza migrate).
# These are NOT run automatically in _ensure_db.
_MANUAL_MIGRATION_VERSIONS: frozenset[int] = frozenset({25, 26, 27})


def _is_ignorable_migration_operational_error(exc: sqlite3.OperationalError) -> bool:
    """Return True when an auto-migration OperationalError is a safe duplicate artifact case."""
    message = str(exc).lower()
    return "duplicate column name" in message or "already exists" in message


def _is_readonly_operational_error(exc: sqlite3.OperationalError) -> bool:
    """Return True when sqlite reports read-only write failures."""
    message = str(exc).lower()
    return "readonly" in message or "read-only" in message


def _table_has_column(conn: sqlite3.Connection, table: str, column: str) -> bool:
    """Check whether a table contains a specific column."""
    try:
        rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    except sqlite3.OperationalError:
        return False
    def _column_name(row: sqlite3.Row | tuple[Any, ...]) -> str:
        if isinstance(row, sqlite3.Row):
            return str(row["name"])
        return str(row[1])

    return any(_column_name(row) == column for row in rows)


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    """Return True when a table exists in sqlite_master."""
    cur = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name = ?",
        (table,),
    )
    return cur.fetchone() is not None


def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    """Return the set of column names for an existing table."""
    return {str(row["name"]) for row in conn.execute(f"PRAGMA table_info({table})")}


def _drop_indexes_for_table(conn: sqlite3.Connection, table: str) -> None:
    """Drop all explicit indexes currently bound to a table."""
    rows = list(conn.execute(f"PRAGMA index_list({table})"))
    for row in rows:
        index_name = str(row["name"])
        if index_name.startswith("sqlite_autoindex_"):
            continue
        conn.execute(f"DROP INDEX IF EXISTS {index_name}")


def _table_pk_columns(conn: sqlite3.Connection, table: str) -> tuple[str, ...]:
    """Return table primary-key columns in declaration order."""
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    pk_rows = sorted(
        (row for row in rows if int(row["pk"]) > 0),
        key=lambda row: int(row["pk"]),
    )
    return tuple(str(row["name"]) for row in pk_rows)


def _table_unique_sets(conn: sqlite3.Connection, table: str) -> set[tuple[str, ...]]:
    """Return all UNIQUE index column sets for a table."""
    uniques: set[tuple[str, ...]] = set()
    for row in conn.execute(f"PRAGMA index_list({table})"):
        if int(row["unique"]) != 1:
            continue
        idx_name = str(row["name"])
        idx_cols = tuple(str(col["name"]) for col in conn.execute(f"PRAGMA index_info({idx_name})"))
        if idx_cols:
            uniques.add(idx_cols)
    return uniques


def _has_fk_columns(conn: sqlite3.Connection, table: str, to_table: str, from_cols: tuple[str, ...]) -> bool:
    """Return True when a table has an FK to to_table that starts with from_cols."""
    grouped: dict[int, list[tuple[int, str, str, str]]] = {}
    for row in conn.execute(f"PRAGMA foreign_key_list({table})"):
        fk_id = int(row["id"])
        grouped.setdefault(fk_id, []).append(
            (
                int(row["seq"]),
                str(row["table"]),
                str(row["from"]),
                str(row["to"]),
            )
        )
    for entries in grouped.values():
        ordered = sorted(entries, key=lambda item: item[0])
        if not ordered:
            continue
        if ordered[0][1] != to_table:
            continue
        candidate = tuple(item[2] for item in ordered)
        if candidate[: len(from_cols)] == from_cols:
            return True
    return False


def _run_v35_to_v36_migration(conn: sqlite3.Connection, project_id: str, project_prefix: str) -> None:
    """Perform full structural v35→v36 migration to match fresh SCHEMA semantics."""
    conn.execute("PRAGMA foreign_keys=OFF")
    try:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS projects (
                id TEXT PRIMARY KEY,
                root_path TEXT NOT NULL DEFAULT '',
                config_path TEXT NOT NULL DEFAULT '',
                project_name TEXT NOT NULL DEFAULT '',
                project_prefix TEXT NOT NULL DEFAULT '',
                db_layout_version INTEGER NOT NULL DEFAULT 36,
                created_at TEXT NOT NULL DEFAULT '',
                last_seen_at TEXT NOT NULL DEFAULT ''
            )
            """
        )

        # Rebuild project_sequences from prefix-keyed legacy shape to project-keyed shape.
        legacy_active_prefix_next_seq: int | None = None
        if _table_exists(conn, "project_sequences"):
            sequence_cols = _table_columns(conn, "project_sequences")
            if "project_id" not in sequence_cols:
                conn.execute("ALTER TABLE project_sequences RENAME TO project_sequences_legacy")
                conn.execute(
                    """
                    CREATE TABLE project_sequences (
                        project_id TEXT PRIMARY KEY,
                        prefix TEXT NOT NULL,
                        next_seq INTEGER NOT NULL DEFAULT 1
                    )
                    """
                )
                row = conn.execute(
                    """
                    SELECT next_seq
                    FROM project_sequences_legacy
                    WHERE prefix = ?
                    ORDER BY next_seq DESC
                    LIMIT 1
                    """,
                    (project_prefix,),
                ).fetchone()
                if row is not None:
                    legacy_active_prefix_next_seq = int(row["next_seq"])
                    conn.execute(
                        """
                        INSERT INTO project_sequences(project_id, prefix, next_seq)
                        VALUES (?, ?, ?)
                        ON CONFLICT(project_id) DO UPDATE SET
                            prefix = excluded.prefix,
                            next_seq = MAX(project_sequences.next_seq, excluded.next_seq)
                        """,
                        (project_id, project_prefix, legacy_active_prefix_next_seq),
                    )
                conn.execute("DROP TABLE project_sequences_legacy")
        else:
            conn.execute(
                """
                CREATE TABLE project_sequences (
                    project_id TEXT PRIMARY KEY,
                    prefix TEXT NOT NULL,
                    next_seq INTEGER NOT NULL DEFAULT 1
                )
                """
            )

        tasks_src = "tasks_v35_legacy"
        conn.execute("ALTER TABLE tasks RENAME TO tasks_v35_legacy")
        task_tags_exists = _table_exists(conn, "task_tags")
        run_steps_exists = _table_exists(conn, "run_steps")
        run_substeps_exists = _table_exists(conn, "run_substeps")
        task_comments_exists = _table_exists(conn, "task_comments")
        if task_tags_exists:
            conn.execute("ALTER TABLE task_tags RENAME TO task_tags_v35_legacy")
            _drop_indexes_for_table(conn, "task_tags_v35_legacy")
        if run_steps_exists:
            conn.execute("ALTER TABLE run_steps RENAME TO run_steps_v35_legacy")
            _drop_indexes_for_table(conn, "run_steps_v35_legacy")
        if run_substeps_exists:
            conn.execute("ALTER TABLE run_substeps RENAME TO run_substeps_v35_legacy")
            _drop_indexes_for_table(conn, "run_substeps_v35_legacy")
        if task_comments_exists:
            conn.execute("ALTER TABLE task_comments RENAME TO task_comments_v35_legacy")
            _drop_indexes_for_table(conn, "task_comments_v35_legacy")

        conn.executescript(
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
            );
            CREATE INDEX IF NOT EXISTS idx_tasks_project_status ON tasks(project_id, status);
            CREATE INDEX IF NOT EXISTS idx_tasks_project_slug ON tasks(project_id, slug);
            CREATE INDEX IF NOT EXISTS idx_tasks_project_created ON tasks(project_id, created_at);
            CREATE INDEX IF NOT EXISTS idx_tasks_project_group ON tasks(project_id, "group");
            CREATE INDEX IF NOT EXISTS idx_tasks_project_depends_on ON tasks(project_id, depends_on);
            CREATE INDEX IF NOT EXISTS idx_tasks_project_merge_status ON tasks(project_id, merge_status);
            CREATE INDEX IF NOT EXISTS idx_tasks_project_type_based_on ON tasks(project_id, task_type, based_on);

            CREATE TABLE task_tags (
                project_id TEXT NOT NULL,
                task_id TEXT NOT NULL,
                tag TEXT NOT NULL,
                PRIMARY KEY(project_id, task_id, tag),
                FOREIGN KEY(project_id, task_id) REFERENCES tasks(project_id, id) ON DELETE CASCADE
            );
            CREATE INDEX IF NOT EXISTS idx_task_tags_project_tag ON task_tags(project_id, tag);
            CREATE INDEX IF NOT EXISTS idx_task_tags_project_task_id ON task_tags(project_id, task_id);

            CREATE TABLE run_steps (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                project_id TEXT NOT NULL,
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
                UNIQUE(project_id, run_id, step_index),
                UNIQUE(project_id, run_id, step_id),
                FOREIGN KEY(project_id, run_id) REFERENCES tasks(project_id, id) ON DELETE CASCADE
            );
            CREATE INDEX IF NOT EXISTS idx_run_steps_project_run_id ON run_steps(project_id, run_id);
            CREATE INDEX IF NOT EXISTS idx_run_steps_project_step_index ON run_steps(project_id, run_id, step_index);

            CREATE TABLE run_substeps (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                project_id TEXT NOT NULL,
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
                UNIQUE(project_id, step_id, substep_index),
                UNIQUE(project_id, step_id, substep_id),
                FOREIGN KEY(project_id, run_id) REFERENCES tasks(project_id, id) ON DELETE CASCADE
            );
            CREATE INDEX IF NOT EXISTS idx_run_substeps_project_run_id ON run_substeps(project_id, run_id);
            CREATE INDEX IF NOT EXISTS idx_run_substeps_project_step_id ON run_substeps(project_id, step_id);

            CREATE TABLE task_comments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                project_id TEXT NOT NULL,
                task_id TEXT NOT NULL,
                content TEXT NOT NULL,
                source TEXT NOT NULL,
                author TEXT,
                created_at TEXT NOT NULL,
                resolved_at TEXT,
                FOREIGN KEY(project_id, task_id) REFERENCES tasks(project_id, id) ON DELETE CASCADE
            );
            CREATE INDEX IF NOT EXISTS idx_task_comments_project_task_created ON task_comments(project_id, task_id, created_at ASC);
            CREATE INDEX IF NOT EXISTS idx_task_comments_project_task_unresolved ON task_comments(project_id, task_id, resolved_at);
            """
        )

        tasks_src_cols = _table_columns(conn, tasks_src)
        tasks_project_expr = "COALESCE(NULLIF(project_id, ''), ?)" if "project_id" in tasks_src_cols else "?"
        task_columns = (
            "id", "prompt", "status", "task_type", "slug", "branch", "log_file", "report_file", "based_on", "has_commits",
            "duration_seconds", "num_steps_reported", "num_steps_computed", "num_turns", "num_turns_reported", "num_turns_computed",
            "attach_count", "attach_duration_seconds", "cost_usd", "created_at", "started_at", "running_pid", "completed_at",
            "group", "depends_on", "spec", "create_review", "create_pr", "same_branch", "task_type_hint", "output_content", "session_id", "pr_number",
            "pr_state", "pr_last_synced_at", "model", "provider", "provider_is_explicit", "urgent", "urgent_bumped_at", "queue_position", "input_tokens", "output_tokens",
            "merge_status", "merged_at", "failure_reason", "skip_learnings", "diff_files_changed", "diff_lines_added", "diff_lines_removed",
            "review_cleared_at", "review_score", "log_schema_version", "execution_mode", "base_branch",
        )
        task_defaults: dict[str, str] = {
            "prompt": "''",
            "status": "'pending'",
            "task_type": "'implement'",
            "create_review": "0",
            "create_pr": "0",
            "same_branch": "0",
            "provider_is_explicit": "0",
            "urgent": "0",
            "skip_learnings": "0",
            "log_schema_version": "1",
            "created_at": "strftime('%Y-%m-%dT%H:%M:%f+00:00', 'now')",
        }
        task_select_exprs: list[str] = []
        for column in task_columns:
            if column == "slug":
                if "slug" in tasks_src_cols:
                    task_select_exprs.append("slug")
                elif "task_id" in tasks_src_cols:
                    task_select_exprs.append("task_id")
                else:
                    task_select_exprs.append("NULL")
                continue
            source_name = f'"{column}"' if column == "group" else column
            if column in tasks_src_cols:
                task_select_exprs.append(source_name)
            else:
                task_select_exprs.append(task_defaults.get(column, "NULL"))

        conn.execute(
            f"""
            INSERT INTO tasks (
                project_id, id, prompt, status, task_type, slug, branch, log_file, report_file, based_on, has_commits,
                duration_seconds, num_steps_reported, num_steps_computed, num_turns, num_turns_reported, num_turns_computed,
                attach_count, attach_duration_seconds, cost_usd, created_at, started_at, running_pid, completed_at,
                "group", depends_on, spec, create_review, create_pr, same_branch, task_type_hint, output_content, session_id, pr_number,
                pr_state, pr_last_synced_at, model, provider, provider_is_explicit, urgent, urgent_bumped_at, queue_position, input_tokens, output_tokens,
                merge_status, merged_at, failure_reason, skip_learnings, diff_files_changed, diff_lines_added, diff_lines_removed,
                review_cleared_at, review_score, log_schema_version, execution_mode, base_branch
            )
            SELECT
                {tasks_project_expr}, {", ".join(task_select_exprs)}
            FROM {tasks_src}
            """,
            (project_id,),
        )

        max_task_suffix_row = conn.execute(
            """
            SELECT COALESCE(MAX(CAST(substr(id, length(?) + 2) AS INTEGER)), 0) AS max_suffix
            FROM tasks
            WHERE project_id = ?
              AND id LIKE (? || '-%')
              AND substr(id, length(?) + 2) GLOB '[0-9]*'
            """,
            (project_prefix, project_id, project_prefix, project_prefix),
        ).fetchone()
        max_task_suffix = int(max_task_suffix_row["max_suffix"]) if max_task_suffix_row is not None else 0
        sequence_seed = max_task_suffix
        if legacy_active_prefix_next_seq is not None:
            sequence_seed = max(sequence_seed, legacy_active_prefix_next_seq)
        if sequence_seed > 0:
            conn.execute(
                """
                INSERT INTO project_sequences(project_id, prefix, next_seq)
                VALUES (?, ?, ?)
                ON CONFLICT(project_id) DO UPDATE SET
                    prefix = excluded.prefix,
                    next_seq = MAX(project_sequences.next_seq, excluded.next_seq)
                """,
                (project_id, project_prefix, sequence_seed),
            )

        if task_tags_exists:
            task_tags_cols = _table_columns(conn, "task_tags_v35_legacy")
            task_tags_project_expr = "COALESCE(NULLIF(project_id, ''), ?)" if "project_id" in task_tags_cols else "?"
            conn.execute(
                f"""
                INSERT OR IGNORE INTO task_tags(project_id, task_id, tag)
                SELECT {task_tags_project_expr}, task_id, tag
                FROM task_tags_v35_legacy
                """,
                (project_id,),
            )

        if run_steps_exists:
            run_steps_cols = _table_columns(conn, "run_steps_v35_legacy")
            run_steps_project_expr = "COALESCE(NULLIF(project_id, ''), ?)" if "project_id" in run_steps_cols else "?"
            conn.execute(
                f"""
                INSERT INTO run_steps (
                    id, project_id, run_id, step_index, step_id, provider, message_role, message_text,
                    started_at, completed_at, outcome, summary, legacy_turn_id, legacy_event_id
                )
                SELECT
                    id, {run_steps_project_expr}, run_id, step_index, step_id, provider, message_role, message_text,
                    started_at, completed_at, outcome, summary, legacy_turn_id, legacy_event_id
                FROM run_steps_v35_legacy
                """,
                (project_id,),
            )

        if run_substeps_exists:
            run_substeps_cols = _table_columns(conn, "run_substeps_v35_legacy")
            run_substeps_project_expr = (
                "COALESCE(NULLIF(project_id, ''), ?)" if "project_id" in run_substeps_cols else "?"
            )
            conn.execute(
                f"""
                INSERT INTO run_substeps (
                    id, project_id, run_id, step_id, substep_index, substep_id, type, source, call_id,
                    payload_json, timestamp, legacy_turn_id, legacy_event_id
                )
                SELECT
                    id, {run_substeps_project_expr}, run_id, step_id, substep_index, substep_id, type, source, call_id,
                    payload_json, timestamp, legacy_turn_id, legacy_event_id
                FROM run_substeps_v35_legacy
                """,
                (project_id,),
            )

        if task_comments_exists:
            task_comments_cols = _table_columns(conn, "task_comments_v35_legacy")
            task_comments_project_expr = (
                "COALESCE(NULLIF(project_id, ''), ?)" if "project_id" in task_comments_cols else "?"
            )
            conn.execute(
                f"""
                INSERT INTO task_comments (id, project_id, task_id, content, source, author, created_at, resolved_at)
                SELECT id, {task_comments_project_expr}, task_id, content, source, author, created_at, resolved_at
                FROM task_comments_v35_legacy
                """,
                (project_id,),
            )

        conn.execute("DROP TABLE tasks_v35_legacy")
        if task_tags_exists:
            conn.execute("DROP TABLE task_tags_v35_legacy")
        if run_steps_exists:
            conn.execute("DROP TABLE run_steps_v35_legacy")
        if run_substeps_exists:
            conn.execute("DROP TABLE run_substeps_v35_legacy")
        if task_comments_exists:
            conn.execute("DROP TABLE task_comments_v35_legacy")
        conn.executescript(
            """
            CREATE INDEX IF NOT EXISTS idx_tasks_project_status ON tasks(project_id, status);
            CREATE INDEX IF NOT EXISTS idx_tasks_project_slug ON tasks(project_id, slug);
            CREATE INDEX IF NOT EXISTS idx_tasks_project_created ON tasks(project_id, created_at);
            CREATE INDEX IF NOT EXISTS idx_tasks_project_group ON tasks(project_id, "group");
            CREATE INDEX IF NOT EXISTS idx_tasks_project_depends_on ON tasks(project_id, depends_on);
            CREATE INDEX IF NOT EXISTS idx_tasks_project_merge_status ON tasks(project_id, merge_status);
            CREATE INDEX IF NOT EXISTS idx_tasks_project_type_based_on ON tasks(project_id, task_type, based_on);
            CREATE INDEX IF NOT EXISTS idx_task_tags_project_tag ON task_tags(project_id, tag);
            CREATE INDEX IF NOT EXISTS idx_task_tags_project_task_id ON task_tags(project_id, task_id);
            CREATE INDEX IF NOT EXISTS idx_run_steps_project_run_id ON run_steps(project_id, run_id);
            CREATE INDEX IF NOT EXISTS idx_run_steps_project_step_index ON run_steps(project_id, run_id, step_index);
            CREATE INDEX IF NOT EXISTS idx_run_substeps_project_run_id ON run_substeps(project_id, run_id);
            CREATE INDEX IF NOT EXISTS idx_run_substeps_project_step_id ON run_substeps(project_id, step_id);
            CREATE INDEX IF NOT EXISTS idx_task_comments_project_task_created ON task_comments(project_id, task_id, created_at ASC);
            CREATE INDEX IF NOT EXISTS idx_task_comments_project_task_unresolved ON task_comments(project_id, task_id, resolved_at);
            """
        )
    finally:
        conn.execute("PRAGMA foreign_keys=ON")


def _validate_v36_structural_schema(conn: sqlite3.Connection) -> None:
    """Validate v36 structural key/fk/unique semantics."""
    required_pk = {
        "tasks": ("project_id", "id"),
        "task_tags": ("project_id", "task_id", "tag"),
    }
    for table, pk_columns in required_pk.items():
        if not _table_exists(conn, table):
            raise RuntimeError(f"Auto-migration to v36 incomplete: missing required table {table}")
        if _table_pk_columns(conn, table) != pk_columns:
            raise RuntimeError(
                f"Auto-migration to v36 incomplete: expected PRIMARY KEY{pk_columns} on {table}"
            )

    run_steps_uniques = _table_unique_sets(conn, "run_steps")
    if ("project_id", "run_id", "step_index") not in run_steps_uniques:
        raise RuntimeError(
            "Auto-migration to v36 incomplete: run_steps must enforce UNIQUE(project_id, run_id, step_index)"
        )
    if ("project_id", "run_id", "step_id") not in run_steps_uniques:
        raise RuntimeError(
            "Auto-migration to v36 incomplete: run_steps must enforce UNIQUE(project_id, run_id, step_id)"
        )

    run_substeps_uniques = _table_unique_sets(conn, "run_substeps")
    if ("project_id", "step_id", "substep_index") not in run_substeps_uniques:
        raise RuntimeError(
            "Auto-migration to v36 incomplete: run_substeps must enforce UNIQUE(project_id, step_id, substep_index)"
        )
    if ("project_id", "step_id", "substep_id") not in run_substeps_uniques:
        raise RuntimeError(
            "Auto-migration to v36 incomplete: run_substeps must enforce UNIQUE(project_id, step_id, substep_id)"
        )

    required_fks = (
        ("task_tags", "tasks", ("project_id", "task_id")),
        ("run_steps", "tasks", ("project_id", "run_id")),
        ("run_substeps", "tasks", ("project_id", "run_id")),
        ("task_comments", "tasks", ("project_id", "task_id")),
    )
    for table, to_table, from_cols in required_fks:
        if not _has_fk_columns(conn, table, to_table, from_cols):
            raise RuntimeError(
                "Auto-migration to v36 incomplete: "
                f"{table} must include FOREIGN KEY{from_cols} REFERENCES {to_table}"
            )


_TASK_COMMENTS_REQUIRED_COLUMNS: tuple[str, ...] = (
    "id",
    "task_id",
    "content",
    "source",
    "author",
    "created_at",
    "resolved_at",
)

_QUERY_ONLY_REQUIRED_RUN_STEP_COUNT_COLUMNS: tuple[str, ...] = (
    "project_id",
    "run_id",
)

_QUERY_ONLY_REQUIRED_COMMENT_COLUMNS: tuple[str, ...] = (
    "project_id",
    "id",
    "task_id",
    "content",
    "created_at",
    "resolved_at",
)
_QUERY_ONLY_REQUIRED_TASK_COLUMNS: tuple[str, ...] = (
    "project_id",
    "id",
    "prompt",
    "status",
    "task_type",
    "slug",
    "branch",
    "log_file",
    "report_file",
    "based_on",
    "has_commits",
    "duration_seconds",
    "num_steps_reported",
    "num_steps_computed",
    "num_turns_reported",
    "num_turns_computed",
    "attach_count",
    "attach_duration_seconds",
    "cost_usd",
    "created_at",
    "started_at",
    "running_pid",
    "completed_at",
    "group",
    "depends_on",
    "spec",
    "create_review",
    "create_pr",
    "same_branch",
    "task_type_hint",
    "output_content",
    "session_id",
    "pr_number",
    "pr_state",
    "pr_last_synced_at",
    "model",
    "provider",
    "provider_is_explicit",
    "urgent",
    "input_tokens",
    "output_tokens",
    "merge_status",
    "merged_at",
    "failure_reason",
    "skip_learnings",
    "diff_files_changed",
    "diff_lines_added",
    "diff_lines_removed",
    "review_cleared_at",
    "review_score",
    "log_schema_version",
    "base_branch",
)


def _missing_required_columns(conn: sqlite3.Connection, table: str, required_columns: tuple[str, ...]) -> list[str]:
    """Return required columns missing from a table."""
    return [column for column in required_columns if not _table_has_column(conn, table, column)]


def _rebuild_task_comments_table(conn: sqlite3.Connection) -> None:
    """Rebuild task_comments with the required schema, preserving existing rows."""
    existing_columns = {row["name"] for row in conn.execute("PRAGMA table_info(task_comments)")}
    conn.execute("ALTER TABLE task_comments RENAME TO task_comments_damaged")
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
    select_exprs = {
        "id": "id" if "id" in existing_columns else "NULL",
        "task_id": "task_id" if "task_id" in existing_columns else "''",
        "content": "content" if "content" in existing_columns else "''",
        "source": "source" if "source" in existing_columns else "'direct'",
        "author": "author" if "author" in existing_columns else "NULL",
        "created_at": (
            "created_at" if "created_at" in existing_columns else "strftime('%Y-%m-%dT%H:%M:%f+00:00', 'now')"
        ),
        "resolved_at": "resolved_at" if "resolved_at" in existing_columns else "NULL",
    }
    conn.execute(
        """
        INSERT INTO task_comments (id, task_id, content, source, author, created_at, resolved_at)
        SELECT
            {id},
            {task_id},
            {content},
            {source},
            {author},
            {created_at},
            {resolved_at}
        FROM task_comments_damaged
        """.format(**select_exprs)
    )
    conn.execute("DROP TABLE task_comments_damaged")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_task_comments_task_created ON task_comments(task_id, created_at ASC)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_task_comments_task_unresolved ON task_comments(task_id, resolved_at)")


def _validate_auto_migration_target(conn: sqlite3.Connection, target_version: int) -> None:
    """Validate required schema artifacts for selected automatic migration targets."""
    if target_version == 32:
        if not _table_exists(conn, "task_comments"):
            raise RuntimeError("Auto-migration to v32 incomplete: missing required table task_comments")
        missing_comment_columns = _missing_required_columns(conn, "task_comments", _TASK_COMMENTS_REQUIRED_COLUMNS)
        if missing_comment_columns:
            raise RuntimeError(
                "Auto-migration to v32 incomplete: missing required column "
                f"task_comments.{missing_comment_columns[0]}"
            )
    if target_version == 36:
        _validate_v36_structural_schema(conn)
    if target_version == 38:
        for column in ("pr_state", "pr_last_synced_at"):
            if not _table_has_column(conn, "tasks", column):
                raise RuntimeError(
                    f"Auto-migration to v38 incomplete: missing required column tasks.{column}"
                )

    required_columns_by_version: dict[int, tuple[str, str]] = {
        30: ("tasks", "urgent_bumped_at"),
        31: ("tasks", "execution_mode"),
        33: ("tasks", "review_score"),
        34: ("tasks", "queue_position"),
        37: ("tasks", "create_pr"),
    }
    requirement = required_columns_by_version.get(target_version)
    if requirement is None:
        return
    table, column = requirement
    if not _table_has_column(conn, table, column):
        raise RuntimeError(
            f"Auto-migration to v{target_version} incomplete: missing required column {table}.{column}"
        )


def _ensure_required_auto_migration_artifacts(
    conn: sqlite3.Connection,
    *,
    target_version: int = SCHEMA_VERSION,
) -> None:
    """Repair required current-schema artifacts removed by external damage.

    For writable databases, missing artifacts are recreated in place.
    For read-only/damaged databases where repair cannot be applied, raise
    SchemaIntegrityError with deterministic remediation guidance.
    """
    if target_version >= 32 and not _table_exists(conn, "task_comments"):
        try:
            conn.executescript(MIGRATION_V31_TO_V32)
        except sqlite3.OperationalError as exc:
            raise SchemaIntegrityError(
                "Schema integrity check failed while repairing v32: "
                "missing task_comments table; use a writable database."
            ) from exc

    missing_comment_columns = (
        _missing_required_columns(conn, "task_comments", _TASK_COMMENTS_REQUIRED_COLUMNS)
        if target_version >= 32
        else []
    )
    if target_version >= 32 and missing_comment_columns:
        missing_column = missing_comment_columns[0]
        try:
            _rebuild_task_comments_table(conn)
        except sqlite3.OperationalError as exc:
            raise SchemaIntegrityError(
                "Schema integrity check failed while repairing required column "
                f"task_comments.{missing_column}: use a writable database."
            ) from exc
        remaining_missing = _missing_required_columns(conn, "task_comments", _TASK_COMMENTS_REQUIRED_COLUMNS)
        if remaining_missing:
            raise SchemaIntegrityError(
                "Schema integrity check failed while repairing required column "
                f"task_comments.{remaining_missing[0]}: use a writable database."
            )

    required_columns: tuple[tuple[int, str, str, str], ...] = (
        (30, "tasks", "urgent_bumped_at", "ALTER TABLE tasks ADD COLUMN urgent_bumped_at TEXT"),
        (31, "tasks", "execution_mode", "ALTER TABLE tasks ADD COLUMN execution_mode TEXT"),
        (31, "tasks", "base_branch", "ALTER TABLE tasks ADD COLUMN base_branch TEXT"),
        (33, "tasks", "review_score", "ALTER TABLE tasks ADD COLUMN review_score INTEGER"),
        (34, "tasks", "queue_position", "ALTER TABLE tasks ADD COLUMN queue_position INTEGER"),
        (36, "tasks", "project_id", "ALTER TABLE tasks ADD COLUMN project_id TEXT"),
        (36, "run_steps", "project_id", "ALTER TABLE run_steps ADD COLUMN project_id TEXT"),
        (36, "run_substeps", "project_id", "ALTER TABLE run_substeps ADD COLUMN project_id TEXT"),
        (36, "task_comments", "project_id", "ALTER TABLE task_comments ADD COLUMN project_id TEXT"),
        (36, "task_tags", "project_id", "ALTER TABLE task_tags ADD COLUMN project_id TEXT"),
        (37, "tasks", "create_pr", "ALTER TABLE tasks ADD COLUMN create_pr INTEGER DEFAULT 0"),
        (38, "tasks", "pr_state", "ALTER TABLE tasks ADD COLUMN pr_state TEXT"),
        (38, "tasks", "pr_last_synced_at", "ALTER TABLE tasks ADD COLUMN pr_last_synced_at TEXT"),
    )
    for min_version, table, column, alter_sql in required_columns:
        if target_version < min_version:
            continue
        if min_version >= 36 and not _table_exists(conn, table):
            # Older synthetic schemas used in tests may not have all optional
            # child tables; do not fail v36 repair for those shapes.
            continue
        if _table_has_column(conn, table, column):
            continue
        try:
            conn.execute(alter_sql)
        except sqlite3.OperationalError as exc:
            raise SchemaIntegrityError(
                f"Schema integrity check failed while repairing required column "
                f"{table}.{column}: use a writable database."
            ) from exc

    if target_version >= 35 and not _table_exists(conn, "task_tags"):
        try:
            conn.executescript(MIGRATION_V34_TO_V35)
        except sqlite3.OperationalError as exc:
            raise SchemaIntegrityError(
                "Schema integrity check failed while repairing required table task_tags: "
                "use a writable database."
            ) from exc
    if target_version >= 36 and not _table_exists(conn, "projects"):
        try:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS projects (
                    id TEXT PRIMARY KEY,
                    root_path TEXT NOT NULL DEFAULT '',
                    config_path TEXT NOT NULL DEFAULT '',
                    project_name TEXT NOT NULL DEFAULT '',
                    project_prefix TEXT NOT NULL DEFAULT '',
                    db_layout_version INTEGER NOT NULL DEFAULT 36,
                    created_at TEXT NOT NULL DEFAULT '',
                    last_seen_at TEXT NOT NULL DEFAULT ''
                )
                """
            )
        except sqlite3.OperationalError as exc:
            raise SchemaIntegrityError(
                "Schema integrity check failed while repairing required table projects: "
                "use a writable database."
            ) from exc

SCHEMA = """
CREATE TABLE IF NOT EXISTS schema_version (
    version INTEGER PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS projects (
    id TEXT PRIMARY KEY,
    root_path TEXT NOT NULL,
    config_path TEXT NOT NULL,
    project_name TEXT NOT NULL,
    project_prefix TEXT NOT NULL,
    db_layout_version INTEGER NOT NULL,
    created_at TEXT NOT NULL,
    last_seen_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS project_sequences (
    project_id TEXT PRIMARY KEY,
    prefix TEXT NOT NULL,
    next_seq INTEGER NOT NULL DEFAULT 1
);

CREATE TABLE IF NOT EXISTS tasks (
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
    num_turns INTEGER,  -- kept for backward compat; use num_turns_reported instead
    num_turns_reported INTEGER,
    num_turns_computed INTEGER,
    attach_count INTEGER,
    attach_duration_seconds REAL,
    cost_usd REAL,
    created_at TEXT NOT NULL,
    started_at TEXT,
    running_pid INTEGER,
    completed_at TEXT,
    -- Task import/chaining (v2+)
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
);

CREATE INDEX IF NOT EXISTS idx_tasks_project_status ON tasks(project_id, status);
CREATE INDEX IF NOT EXISTS idx_tasks_project_slug ON tasks(project_id, slug);
CREATE INDEX IF NOT EXISTS idx_tasks_project_created ON tasks(project_id, created_at);
CREATE INDEX IF NOT EXISTS idx_tasks_project_group ON tasks(project_id, "group");
CREATE INDEX IF NOT EXISTS idx_tasks_project_depends_on ON tasks(project_id, depends_on);
CREATE INDEX IF NOT EXISTS idx_tasks_project_merge_status ON tasks(project_id, merge_status);

CREATE TABLE IF NOT EXISTS task_tags (
    project_id TEXT NOT NULL,
    task_id TEXT NOT NULL,
    tag TEXT NOT NULL,
    PRIMARY KEY(project_id, task_id, tag),
    FOREIGN KEY(project_id, task_id) REFERENCES tasks(project_id, id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_task_tags_project_tag ON task_tags(project_id, tag);
CREATE INDEX IF NOT EXISTS idx_task_tags_project_task_id ON task_tags(project_id, task_id);

CREATE TABLE IF NOT EXISTS run_steps (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    project_id TEXT NOT NULL,
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
    UNIQUE(project_id, run_id, step_index),
    UNIQUE(project_id, run_id, step_id),
    FOREIGN KEY(project_id, run_id) REFERENCES tasks(project_id, id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS run_substeps (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    project_id TEXT NOT NULL,
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
    UNIQUE(project_id, step_id, substep_index),
    UNIQUE(project_id, step_id, substep_id),
    FOREIGN KEY(project_id, run_id) REFERENCES tasks(project_id, id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_run_steps_project_run_id ON run_steps(project_id, run_id);
CREATE INDEX IF NOT EXISTS idx_run_steps_project_step_index ON run_steps(project_id, run_id, step_index);
CREATE INDEX IF NOT EXISTS idx_run_substeps_project_run_id ON run_substeps(project_id, run_id);
CREATE INDEX IF NOT EXISTS idx_run_substeps_project_step_id ON run_substeps(project_id, step_id);

CREATE INDEX IF NOT EXISTS idx_tasks_project_type_based_on ON tasks(project_id, task_type, based_on);
CREATE TABLE IF NOT EXISTS task_comments (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    project_id TEXT NOT NULL,
    task_id TEXT NOT NULL,
    content TEXT NOT NULL,
    source TEXT NOT NULL,
    author TEXT,
    created_at TEXT NOT NULL,
    resolved_at TEXT,
    FOREIGN KEY(project_id, task_id) REFERENCES tasks(project_id, id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_task_comments_project_task_created ON task_comments(project_id, task_id, created_at ASC);
CREATE INDEX IF NOT EXISTS idx_task_comments_project_task_unresolved ON task_comments(project_id, task_id, resolved_at);
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

# Migration from v13 to v14
MIGRATION_V13_TO_V14 = """
ALTER TABLE tasks ADD COLUMN review_cleared_at TEXT;
"""

# Migration from v14 to v15
MIGRATION_V14_TO_V15 = """
ALTER TABLE tasks ADD COLUMN num_steps_reported INTEGER;
ALTER TABLE tasks ADD COLUMN num_steps_computed INTEGER;
UPDATE tasks
SET num_steps_reported = num_turns_reported
WHERE num_steps_reported IS NULL AND num_turns_reported IS NOT NULL;
UPDATE tasks
SET num_steps_computed = num_turns_computed
WHERE num_steps_computed IS NULL AND num_turns_computed IS NOT NULL;
"""

# Migration from v15 to v16
MIGRATION_V15_TO_V16 = """
CREATE TABLE IF NOT EXISTS run_steps (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id INTEGER NOT NULL REFERENCES tasks(id),
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
    UNIQUE(run_id, step_id)
);
CREATE TABLE IF NOT EXISTS run_substeps (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id INTEGER NOT NULL REFERENCES tasks(id),
    step_id INTEGER NOT NULL REFERENCES run_steps(id),
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
    UNIQUE(step_id, substep_id)
);
CREATE INDEX IF NOT EXISTS idx_run_steps_run_id ON run_steps(run_id);
CREATE INDEX IF NOT EXISTS idx_run_steps_step_index ON run_steps(run_id, step_index);
CREATE INDEX IF NOT EXISTS idx_run_substeps_run_id ON run_substeps(run_id);
CREATE INDEX IF NOT EXISTS idx_run_substeps_step_id ON run_substeps(step_id);
"""

# Migration from v16 to v17
MIGRATION_V16_TO_V17 = """
ALTER TABLE tasks ADD COLUMN log_schema_version INTEGER DEFAULT 1;
UPDATE tasks SET log_schema_version = 1 WHERE log_schema_version IS NULL;
"""

# Migration from v17 to v18
MIGRATION_V17_TO_V18 = """
ALTER TABLE tasks ADD COLUMN cycle_id INTEGER;
ALTER TABLE tasks ADD COLUMN cycle_iteration_index INTEGER;
ALTER TABLE tasks ADD COLUMN cycle_role TEXT;
CREATE TABLE IF NOT EXISTS task_cycles (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    implementation_task_id INTEGER NOT NULL REFERENCES tasks(id),
    status TEXT NOT NULL DEFAULT 'active',
    max_iterations INTEGER NOT NULL DEFAULT 3,
    started_at TEXT NOT NULL,
    ended_at TEXT,
    stop_reason TEXT
);
CREATE TABLE IF NOT EXISTS task_cycle_iterations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    cycle_id INTEGER NOT NULL REFERENCES task_cycles(id),
    iteration_index INTEGER NOT NULL,
    review_task_id INTEGER REFERENCES tasks(id),
    review_verdict TEXT,
    improve_task_id INTEGER REFERENCES tasks(id),
    state TEXT NOT NULL DEFAULT 'review_created',
    started_at TEXT NOT NULL,
    ended_at TEXT
);
CREATE INDEX IF NOT EXISTS idx_task_cycles_impl_id ON task_cycles(implementation_task_id);
CREATE INDEX IF NOT EXISTS idx_task_cycles_status ON task_cycles(status);
CREATE INDEX IF NOT EXISTS idx_task_cycle_iterations_cycle_idx ON task_cycle_iterations(cycle_id, iteration_index);
CREATE INDEX IF NOT EXISTS idx_tasks_cycle_id ON tasks(cycle_id);
"""


@dataclass(frozen=True)
class StepRef:
    """Opaque step reference used when writing substeps/finalization."""

    id: int
    run_id: str  # References tasks.id (project-prefixed decimal)
    step_index: int
    step_id: str


@dataclass(frozen=True)
class RunStep:
    """Persisted top-level message step for a run."""

    id: int
    run_id: str  # References tasks.id (project-prefixed decimal)
    step_index: int
    step_id: str
    provider: str
    message_role: str
    message_text: str | None
    started_at: datetime
    completed_at: datetime | None
    outcome: str | None
    summary: str | None
    legacy_turn_id: str | None
    legacy_event_id: str | None


@dataclass(frozen=True)
class RunSubstep:
    """Persisted substep/tool event under a top-level message step."""

    id: int
    run_id: str  # References tasks.id (project-prefixed decimal)
    step_id: int
    substep_index: int
    substep_id: str
    type: str
    source: str
    call_id: str | None
    payload: Any
    timestamp: datetime
    legacy_turn_id: str | None
    legacy_event_id: str | None


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

_MIGRATIONS: list[tuple[int, str | None]] = [
    (2, MIGRATION_V1_TO_V2),
    (3, MIGRATION_V2_TO_V3),
    (4, MIGRATION_V3_TO_V4),
    (5, MIGRATION_V4_TO_V5),
    (6, MIGRATION_V5_TO_V6),
    (7, MIGRATION_V6_TO_V7),
    (8, MIGRATION_V7_TO_V8),
    (9, MIGRATION_V8_TO_V9),
    (10, MIGRATION_V9_TO_V10),
    (11, MIGRATION_V10_TO_V11),
    (12, MIGRATION_V11_TO_V12),
    (13, MIGRATION_V12_TO_V13),
    (14, MIGRATION_V13_TO_V14),
    (15, MIGRATION_V14_TO_V15),
    (16, MIGRATION_V15_TO_V16),
    (17, MIGRATION_V16_TO_V17),
    (18, MIGRATION_V17_TO_V18),
    (19, MIGRATION_V18_TO_V19),
    (20, MIGRATION_V19_TO_V20),
    (21, MIGRATION_V20_TO_V21),
    (22, MIGRATION_V21_TO_V22),
    (23, MIGRATION_V22_TO_V23),
    (24, MIGRATION_V23_TO_V24),
    (25, None),  # Manual migration: INTEGER PK → TEXT IDs
    (26, None),  # Manual migration: base36-text IDs → decimal-text IDs
    (27, None),  # Manual migration: remove TaskCycle bookkeeping tables/columns
    (28, MIGRATION_V27_TO_V28),
    (29, MIGRATION_V28_TO_V29),
    (30, MIGRATION_V29_TO_V30),
    (31, MIGRATION_V30_TO_V31),
    (32, MIGRATION_V31_TO_V32),
    (33, MIGRATION_V32_TO_V33),
    (34, MIGRATION_V33_TO_V34),
    (35, MIGRATION_V34_TO_V35),
    (36, MIGRATION_V35_TO_V36),
    (37, MIGRATION_V36_TO_V37),
    (38, MIGRATION_V37_TO_V38),
]

_SHARED_DB_IMPORT_MARKER = "shared-db-import.json"


def _legacy_local_db_path(project_dir: Path) -> Path:
    return project_dir / ".gza" / "gza.db"


def _shared_import_marker_path(project_dir: Path) -> Path:
    return project_dir / ".gza" / _SHARED_DB_IMPORT_MARKER


def _db_fingerprint(path: Path) -> str:
    hasher = sha256()
    with open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def _db_metadata(path: Path) -> tuple[int, int, int]:
    """Return stat metadata persisted in shared-import markers."""
    stat = path.stat()
    return (int(stat.st_size), int(stat.st_mtime_ns), int(stat.st_ctime_ns))


def _marker_matches_shared_db(project_dir: Path, local_db_path: Path, active_db_path: Path) -> bool:
    marker_path = _shared_import_marker_path(project_dir)
    if not marker_path.exists():
        return False
    try:
        marker = json.loads(marker_path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return False
    if not isinstance(marker, dict):
        return False
    marker_shared = marker.get("shared_db_path")
    marker_fingerprint = marker.get("local_db_fingerprint")
    if not isinstance(marker_shared, str) or not isinstance(marker_fingerprint, str):
        return False
    active_db_str = str(active_db_path.expanduser().resolve())
    if marker_shared != active_db_str:
        return False

    # Metadata acts as a cheap prefilter; content hash still decides correctness.
    marker_size = marker.get("local_db_size")
    marker_mtime_ns = marker.get("local_db_mtime_ns")
    marker_ctime_ns = marker.get("local_db_ctime_ns")
    if (
        isinstance(marker_size, int)
        and isinstance(marker_mtime_ns, int)
        and isinstance(marker_ctime_ns, int)
    ):
        try:
            local_size, local_mtime_ns, local_ctime_ns = _db_metadata(local_db_path)
        except OSError:
            return False
        if (local_size, local_mtime_ns, local_ctime_ns) != (
            marker_size,
            marker_mtime_ns,
            marker_ctime_ns,
        ):
            # Metadata drift alone is not authoritative; content hash decides.
            pass

    try:
        local_fingerprint = _db_fingerprint(local_db_path)
    except OSError:
        return False
    return marker_fingerprint == local_fingerprint


def _write_shared_import_marker(project_dir: Path, local_db_path: Path, active_db_path: Path) -> None:
    marker_path = _shared_import_marker_path(project_dir)
    marker_path.parent.mkdir(parents=True, exist_ok=True)
    size, mtime_ns, ctime_ns = _db_metadata(local_db_path)
    payload = {
        "shared_db_path": str(active_db_path.expanduser().resolve()),
        "local_db_fingerprint": _db_fingerprint(local_db_path),
        "local_db_size": size,
        "local_db_mtime_ns": mtime_ns,
        "local_db_ctime_ns": ctime_ns,
        "imported_at": datetime.now(UTC).isoformat(),
        "version": 2,
    }
    marker_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _project_identity_from_config(config: "Config") -> tuple[str, str]:
    project_id = getattr(config, "project_id", None)
    if not isinstance(project_id, str) or not project_id:
        project_id = "default"
    project_prefix = getattr(config, "project_prefix", "gza")
    if not isinstance(project_prefix, str) or not project_prefix:
        project_prefix = "gza"
    return project_id, project_prefix


class SqliteTaskStore:
    """SQLite-based task storage."""

    def __init__(
        self,
        db_path: Path,
        prefix: str = "gza",
        project_id: str | None = None,
        *,
        project_root: Path | None = None,
        config_path: Path | None = None,
        project_name: str | None = None,
        open_mode: StoreOpenMode = "readwrite",
    ):
        self.db_path = db_path
        self._prefix = prefix
        self._project_id = project_id or "default"
        self._project_root = project_root
        self._config_path = config_path
        self._project_name = project_name
        self._open_mode = open_mode
        self._startup_warnings: list[str] = []
        self._query_only_empty_db = False
        self._query_only_table_exists: dict[str, bool] = {}
        self._query_only_columns: dict[str, set[str]] = {}
        if self._open_mode == "query_only":
            self._ensure_db_query_only()
        else:
            self._ensure_db()
            self._ensure_project_row()

    @classmethod
    def default(cls, project_dir: Path | None = None) -> "SqliteTaskStore":
        """Create a store using the db_path derived from config.

        Args:
            project_dir: Project root. Defaults to cwd.
        """
        from .config import Config

        config = Config.load(project_dir or Path.cwd(), discover=True)
        return cls.from_config(config)

    @classmethod
    def from_config(
        cls,
        config: "Config",
        *,
        allow_legacy_local_db: bool = False,
        open_mode: StoreOpenMode = "readwrite",
    ) -> "SqliteTaskStore":
        """Create a store from a loaded Config instance."""
        project_id, project_prefix = _project_identity_from_config(config)
        project_dir = getattr(config, "project_dir", None)
        if isinstance(project_dir, Path) and not allow_legacy_local_db:
            local_db = _legacy_local_db_path(project_dir)
            active_db = config.db_path
            if local_db.exists() and local_db.resolve() != active_db.resolve():
                if not _marker_matches_shared_db(project_dir, local_db, active_db):
                    from .config import ConfigError

                    raise ConfigError(
                        "Legacy local DB detected at "
                        f"{local_db} while active db_path is {active_db}. "
                        "Run 'uv run gza migrate --import-local-db --yes' to import tasks "
                        "and create an import marker."
                    )
        config_path = getattr(config, "config_path", None)
        resolved_config_path = (
            config_path(project_dir)
            if callable(config_path) and isinstance(project_dir, Path)
            else None
        )
        project_name = getattr(config, "project_name", None)
        if not isinstance(project_name, str):
            project_name = None
        return cls(
            config.db_path,
            prefix=project_prefix,
            project_id=project_id,
            project_root=project_dir if isinstance(project_dir, Path) else None,
            config_path=resolved_config_path,
            project_name=project_name,
            open_mode=open_mode,
        )

    def startup_warnings(self) -> tuple[str, ...]:
        """Return deterministic startup warnings collected during store open."""
        return tuple(self._startup_warnings)

    def _ensure_db(self) -> None:
        """Ensure database exists and schema is current.

        Raises:
            ManualMigrationRequired: When the DB needs a manual migration (e.g. v25/v26).
                The caller should run ``gza migrate`` then re-open the store.
        """
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as conn:
            # Check if schema_version table exists
            cur = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='schema_version'"
            )
            if cur.fetchone() is None:
                # Fresh database - create full current schema directly
                conn.executescript(SCHEMA)
                conn.execute("INSERT INTO schema_version (version) VALUES (?)", (SCHEMA_VERSION,))
            else:
                # Check current version and migrate if needed
                cur = conn.execute("SELECT version FROM schema_version LIMIT 1")
                row = cur.fetchone()
                current_version = row["version"] if row else 0
                _ensure_required_auto_migration_artifacts(conn, target_version=current_version)

                pending_manual: list[int] = []
                for target_version, migration_sql in _MIGRATIONS:
                    if current_version < target_version:
                        if target_version in _MANUAL_MIGRATION_VERSIONS:
                            pending_manual.append(target_version)
                            # Don't advance current_version; stop processing further
                            break
                        if target_version == 36:
                            try:
                                _run_v35_to_v36_migration(conn, self._project_id, self._prefix)
                            except sqlite3.OperationalError as exc:
                                if _is_readonly_operational_error(exc):
                                    raise SchemaIntegrityError(
                                        "Cannot auto-migrate schema v35->v36 on a read-only database. "
                                        "Use a writable database to complete migration, then retry."
                                    ) from exc
                                raise
                        elif migration_sql is not None:
                            for stmt in migration_sql.strip().split(";"):
                                stmt = stmt.strip()
                                if stmt:
                                    try:
                                        conn.execute(stmt)
                                    except sqlite3.OperationalError as exc:
                                        if _is_ignorable_migration_operational_error(exc):
                                            # Duplicate artifact from partially-applied/idempotent migration.
                                            continue
                                        raise
                        _validate_auto_migration_target(conn, target_version)
                        conn.execute("UPDATE schema_version SET version = ?", (target_version,))
                        current_version = target_version

                if pending_manual:
                    raise ManualMigrationRequired(pending_manual)

                if row is None:
                    conn.execute("INSERT INTO schema_version (version) VALUES (?)", (SCHEMA_VERSION,))

            _backfill_task_tags_from_group(conn)
            def _safe_project_backfill(sql: str) -> None:
                try:
                    conn.execute(sql, (self._project_id,))
                except sqlite3.OperationalError as exc:
                    if "readonly" not in str(exc).lower():
                        raise

            if _table_has_column(conn, "tasks", "project_id"):
                _safe_project_backfill(
                    "UPDATE tasks SET project_id = ? WHERE project_id IS NULL OR project_id = ''"
                )
            if _table_has_column(conn, "run_steps", "project_id"):
                _safe_project_backfill(
                    "UPDATE run_steps SET project_id = ? WHERE project_id IS NULL OR project_id = ''"
                )
            if _table_has_column(conn, "run_substeps", "project_id"):
                _safe_project_backfill(
                    "UPDATE run_substeps SET project_id = ? WHERE project_id IS NULL OR project_id = ''"
                )
            if _table_has_column(conn, "task_comments", "project_id"):
                _safe_project_backfill(
                    "UPDATE task_comments SET project_id = ? WHERE project_id IS NULL OR project_id = ''"
                )
            if _table_has_column(conn, "task_tags", "project_id"):
                _safe_project_backfill(
                    "UPDATE task_tags SET project_id = ? WHERE project_id IS NULL OR project_id = ''"
                )

            # Repair required artifacts for current schemas when external damage
            # or partial migrations removed them.
            _ensure_required_auto_migration_artifacts(conn, target_version=SCHEMA_VERSION)

    def _ensure_db_query_only(self) -> None:
        """Open a store for best-effort reads without any startup writes."""
        if not self.db_path.exists():
            self._query_only_empty_db = True
            return

        with self._connect() as conn:
            cur = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='schema_version'"
            )
            if cur.fetchone() is None:
                raise SchemaIntegrityError(
                    "Database is missing schema_version; query-only mode cannot initialize schema. "
                    "Use a writable database to repair or initialize it, then retry."
                )

            row = conn.execute("SELECT version FROM schema_version LIMIT 1").fetchone()
            current_version = int(row["version"]) if row and row["version"] is not None else 0

            pending_manual: list[int] = []
            for target_version, _migration_sql in _MIGRATIONS:
                if current_version >= target_version:
                    continue
                if target_version in _MANUAL_MIGRATION_VERSIONS:
                    pending_manual.append(target_version)
                    break
                raise SchemaIntegrityError(
                    f"Database is at schema v{current_version}; query-only mode does not run "
                    f"automatic migrations to v{SCHEMA_VERSION}. Use a writable database to "
                    "upgrade, then retry."
                )

            if pending_manual:
                raise ManualMigrationRequired(pending_manual)

            self._prime_query_only_schema_state(conn)
            self._validate_query_only_core_schema()
            self._collect_query_only_warnings()

    def _prime_query_only_schema_state(self, conn: sqlite3.Connection) -> None:
        """Cache table and column availability for degraded query-only reads."""
        tables = (
            "tasks",
            "task_tags",
            "task_comments",
            "run_steps",
            "run_substeps",
            "projects",
        )
        self._query_only_table_exists = {table: _table_exists(conn, table) for table in tables}
        self._query_only_columns = {
            table: (_table_columns(conn, table) if self._query_only_table_exists[table] else set())
            for table in tables
        }

    def _collect_query_only_warnings(self) -> None:
        """Record warnings for degraded query-only reads on damaged current schemas."""
        if not self._query_only_has_column("tasks", "execution_mode"):
            self._startup_warnings.append(
                "Query-only DB open detected missing required column tasks.execution_mode; "
                "continuing with degraded read behavior."
            )
        if not self._query_only_has_column("tasks", "urgent_bumped_at"):
            self._startup_warnings.append(
                "Query-only DB open detected missing required column tasks.urgent_bumped_at; "
                "queue ordering will fall back to created_at."
            )
        if not self._query_only_has_column("tasks", "queue_position"):
            self._startup_warnings.append(
                "Query-only DB open detected missing required column tasks.queue_position; "
                "explicit queue ordering will be unavailable."
            )
        if not self._query_only_supports_tags():
            self._startup_warnings.append(
                "Query-only DB open detected incomplete task_tags schema; "
                "tag metadata and tag-filtered queries will be unavailable."
            )
        if not self._query_only_supports_comments():
            self._startup_warnings.append(
                "Query-only DB open detected incomplete task_comments schema; "
                "task comments will be unavailable."
            )
        elif not self._query_only_has_column("task_comments", "source"):
            self._startup_warnings.append(
                "Query-only DB open detected missing required column task_comments.source; "
                "task comments will use default source values."
            )
        run_steps_issue = self._query_only_run_steps_warning()
        if run_steps_issue is not None:
            self._startup_warnings.append(run_steps_issue)

    def _validate_query_only_core_schema(self) -> None:
        """Fail closed when the core task schema is too damaged for safe reads."""
        if self._query_only_empty_db:
            return
        if not self._query_only_table_exists.get("tasks", False):
            raise SchemaIntegrityError(
                "Query-only DB open detected missing required table tasks; "
                "cannot safely read task data from this snapshot."
            )
        missing_columns = [
            column
            for column in _QUERY_ONLY_REQUIRED_TASK_COLUMNS
            if not self._query_only_has_column("tasks", column)
        ]
        if missing_columns:
            raise SchemaIntegrityError(
                "Query-only DB open detected missing required column "
                f"tasks.{missing_columns[0]}; cannot safely read task data from this snapshot."
            )

    def _query_only_has_column(self, table: str, column: str) -> bool:
        """Return True when a cached query-only schema contains the given column."""
        if self._open_mode != "query_only":
            return True
        return column in self._query_only_columns.get(table, set())

    def _query_only_supports_tags(self) -> bool:
        """Return True when query-only reads can safely use task tag artifacts."""
        if self._open_mode != "query_only":
            return True
        return self._query_only_table_exists.get("task_tags", False) and all(
            self._query_only_has_column("task_tags", column)
            for column in ("project_id", "task_id", "tag")
        )

    def _query_only_supports_comments(self) -> bool:
        """Return True when query-only reads can safely use task comment artifacts."""
        if self._open_mode != "query_only":
            return True
        return self._query_only_table_exists.get("task_comments", False) and all(
            self._query_only_has_column("task_comments", column)
            for column in _QUERY_ONLY_REQUIRED_COMMENT_COLUMNS
        )

    def _query_only_supports_run_steps(self) -> bool:
        """Return True when query-only reads can safely use run_steps."""
        if self._open_mode != "query_only":
            return True
        return self._query_only_table_exists.get("run_steps", False) and all(
            self._query_only_has_column("run_steps", column)
            for column in _QUERY_ONLY_REQUIRED_RUN_STEP_COUNT_COLUMNS
        )

    def _query_only_run_steps_warning(self) -> str | None:
        """Describe degraded query-only run_steps support, if any."""
        if self._open_mode != "query_only":
            return None
        if not self._query_only_table_exists.get("run_steps", False):
            return (
                "Query-only DB open detected missing required table run_steps; "
                "step counts will be unavailable."
            )
        missing_columns = [
            column
            for column in _QUERY_ONLY_REQUIRED_RUN_STEP_COUNT_COLUMNS
            if not self._query_only_has_column("run_steps", column)
        ]
        if missing_columns:
            return (
                "Query-only DB open detected missing required column "
                f"run_steps.{missing_columns[0]}; step counts will be unavailable."
            )
        return None

    def _task_pickup_order_sql(self, alias: str = "t") -> str:
        """Return the canonical pending-task ORDER BY clause for the current schema."""
        queue_position = f"{alias}.queue_position"
        urgent = f"{alias}.urgent"
        urgent_bumped_at = f"{alias}.urgent_bumped_at"
        created_at = f"{alias}.created_at"
        order_parts: list[str] = []
        if self._query_only_has_column("tasks", "queue_position"):
            order_parts.append(f"CASE WHEN {queue_position} IS NULL THEN 1 ELSE 0 END")
            order_parts.append(f"COALESCE({queue_position}, 0) ASC")
        if self._query_only_has_column("tasks", "urgent"):
            order_parts.append(f"{urgent} DESC")
        if self._query_only_has_column("tasks", "urgent_bumped_at"):
            order_parts.append(f"COALESCE({urgent_bumped_at}, '') DESC")
        order_parts.append(f"{created_at} ASC")
        return ", ".join(order_parts)

    def _ensure_project_row(self) -> None:
        """Ensure the current project is registered in the shared DB."""
        now = datetime.now(UTC).isoformat()
        with self._connect() as conn:
            try:
                conn.execute(
                    """
                    INSERT INTO projects (
                        id, root_path, config_path, project_name, project_prefix, db_layout_version, created_at, last_seen_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(id) DO UPDATE SET
                        root_path = excluded.root_path,
                        config_path = excluded.config_path,
                        project_name = excluded.project_name,
                        project_prefix = excluded.project_prefix,
                        db_layout_version = excluded.db_layout_version,
                        last_seen_at = excluded.last_seen_at
                    """,
                    (
                        self._project_id,
                        str(self._project_root.resolve()) if self._project_root else "",
                        str(self._config_path.resolve()) if self._config_path else "",
                        self._project_name or self._prefix,
                        self._prefix,
                        SCHEMA_VERSION,
                        now,
                        now,
                    ),
                )
            except sqlite3.OperationalError as exc:
                if "readonly" not in str(exc).lower():
                    raise

    def _connect(self) -> sqlite3.Connection:
        """Create a database connection with auto-commit."""
        if self._query_only_empty_db:
            conn = sqlite3.connect(
                ":memory:",
                isolation_level=None,
                timeout=15,
                factory=_ClosingSqliteConnection,
            )
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA busy_timeout=15000")
            conn.executescript(SCHEMA)
            conn.execute("INSERT INTO schema_version (version) VALUES (?)", (SCHEMA_VERSION,))
            return conn

        conn = sqlite3.connect(
            self.db_path,
            isolation_level=None,
            timeout=15,
            factory=_ClosingSqliteConnection,
        )
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout=15000")
        if self._open_mode == "query_only":
            try:
                conn.execute("PRAGMA query_only=ON")
            except sqlite3.OperationalError:
                pass
            return conn
        for pragma in ("PRAGMA journal_mode=WAL", "PRAGMA synchronous=NORMAL"):
            try:
                conn.execute(pragma)
            except sqlite3.OperationalError as exc:
                if "readonly" not in str(exc).lower() and "read-only" not in str(exc).lower():
                    raise
        return conn

    def _row_to_task(self, row: sqlite3.Row, *, tags: tuple[str, ...] = ()) -> Task:
        """Convert a database row to a Task."""
        keys = row.keys()
        # Support both old column name ('task_id') and new ('slug') for migration compat
        if "slug" in keys:
            slug_val = row["slug"]
        elif "task_id" in keys:
            slug_val = row["task_id"]
        else:
            slug_val = None
        return Task(
            id=row["id"],
            prompt=row["prompt"].decode("utf-8", errors="replace") if isinstance(row["prompt"], bytes) else row["prompt"],
            status=row["status"],
            task_type=row["task_type"],
            slug=slug_val,
            branch=row["branch"],
            log_file=row["log_file"],
            report_file=row["report_file"],
            based_on=row["based_on"],
            has_commits=bool(row["has_commits"]) if row["has_commits"] is not None else None,
            duration_seconds=row["duration_seconds"],
            num_steps_reported=row["num_steps_reported"] if "num_steps_reported" in keys else None,
            num_steps_computed=row["num_steps_computed"] if "num_steps_computed" in keys else None,
            num_turns_reported=row["num_turns_reported"] if "num_turns_reported" in keys else None,
            num_turns_computed=row["num_turns_computed"] if "num_turns_computed" in keys else None,
            attach_count=row["attach_count"] if "attach_count" in keys else None,
            attach_duration_seconds=row["attach_duration_seconds"] if "attach_duration_seconds" in keys else None,
            cost_usd=row["cost_usd"],
            input_tokens=row["input_tokens"] if "input_tokens" in keys else None,
            output_tokens=row["output_tokens"] if "output_tokens" in keys else None,
            created_at=_parse_db_timestamp(row["created_at"]),
            started_at=_parse_db_timestamp(row["started_at"]),
            running_pid=row["running_pid"] if "running_pid" in keys else None,
            completed_at=_parse_db_timestamp(row["completed_at"]),
            group=row["group"],
            tags=tags,
            depends_on=row["depends_on"],
            spec=row["spec"],
            create_review=bool(row["create_review"]) if row["create_review"] is not None else False,
            create_pr=bool(row["create_pr"]) if "create_pr" in keys and row["create_pr"] is not None else False,
            same_branch=bool(row["same_branch"]) if row["same_branch"] is not None else False,
            base_branch=row["base_branch"] if "base_branch" in keys else None,
            task_type_hint=row["task_type_hint"] if "task_type_hint" in keys else None,
            output_content=row["output_content"] if "output_content" in keys else None,
            session_id=row["session_id"] if "session_id" in keys else None,
            pr_number=row["pr_number"] if "pr_number" in keys else None,
            pr_state=row["pr_state"] if "pr_state" in keys else None,
            pr_last_synced_at=_parse_db_timestamp(row["pr_last_synced_at"]) if "pr_last_synced_at" in keys else None,
            model=row["model"] if "model" in keys else None,
            provider=row["provider"] if "provider" in keys else None,
            provider_is_explicit=bool(row["provider_is_explicit"]) if "provider_is_explicit" in keys and row["provider_is_explicit"] is not None else False,
            urgent=bool(row["urgent"]) if "urgent" in keys and row["urgent"] is not None else False,
            queue_position=row["queue_position"] if "queue_position" in keys else None,
            merge_status=row["merge_status"] if "merge_status" in keys else None,
            merged_at=_parse_db_timestamp(row["merged_at"]) if "merged_at" in keys else None,
            failure_reason=row["failure_reason"] if "failure_reason" in keys else None,
            skip_learnings=bool(row["skip_learnings"]) if "skip_learnings" in keys and row["skip_learnings"] is not None else False,
            diff_files_changed=row["diff_files_changed"] if "diff_files_changed" in keys else None,
            diff_lines_added=row["diff_lines_added"] if "diff_lines_added" in keys else None,
            diff_lines_removed=row["diff_lines_removed"] if "diff_lines_removed" in keys else None,
            review_cleared_at=_parse_db_timestamp(row["review_cleared_at"]) if "review_cleared_at" in keys else None,
            review_score=row["review_score"] if "review_score" in keys else None,
            log_schema_version=(
                row["log_schema_version"]
                if "log_schema_version" in keys and row["log_schema_version"] is not None
                else 1
            ),
            execution_mode=row["execution_mode"] if "execution_mode" in keys else None,
        )

    def _fetch_tags_for_task_ids(self, conn: sqlite3.Connection, task_ids: Iterable[str]) -> dict[str, tuple[str, ...]]:
        """Fetch tags for many tasks in one query."""
        if not self._query_only_supports_tags():
            return {}
        ids = sorted({task_id for task_id in task_ids if task_id})
        if not ids:
            return {}
        placeholders = ",".join("?" for _ in ids)
        cur = conn.execute(
            f"""
            SELECT task_id, tag
            FROM task_tags
            WHERE project_id = ?
              AND task_id IN ({placeholders})
            ORDER BY task_id ASC, tag ASC
            """,
            (self._project_id, *ids),
        )
        by_task: dict[str, list[str]] = {}
        for row in cur.fetchall():
            by_task.setdefault(str(row["task_id"]), []).append(str(row["tag"]))
        return {task_id: tuple(tags) for task_id, tags in by_task.items()}

    def _rows_to_tasks(self, conn: sqlite3.Connection, rows: list[sqlite3.Row]) -> list[Task]:
        """Hydrate rows into tasks and attach tags in batch."""
        if not rows:
            return []
        tag_map = self._fetch_tags_for_task_ids(conn, (str(row["id"]) for row in rows))
        tasks: list[Task] = []
        for row in rows:
            task_id = str(row["id"])
            tags = tag_map.get(task_id, ())
            task = self._row_to_task(row, tags=tags)
            task.group = tags[0] if len(tags) == 1 else task.group
            tasks.append(task)
        return tasks

    def _replace_task_tags_conn(self, conn: sqlite3.Connection, task_id: str, tags: tuple[str, ...]) -> None:
        """Replace all tags for a task inside an open transaction."""
        conn.execute(
            "DELETE FROM task_tags WHERE project_id = ? AND task_id = ?",
            (self._project_id, task_id),
        )
        if tags:
            conn.executemany(
                "INSERT INTO task_tags(project_id, task_id, tag) VALUES (?, ?, ?)",
                [(self._project_id, task_id, tag) for tag in tags],
            )

    def _row_to_run_step(self, row: sqlite3.Row) -> RunStep:
        """Convert a database row to a RunStep."""
        started_at = _parse_db_timestamp(row["started_at"])
        assert started_at is not None
        return RunStep(
            id=row["id"],
            run_id=row["run_id"],
            step_index=row["step_index"],
            step_id=row["step_id"],
            provider=row["provider"],
            message_role=row["message_role"],
            message_text=row["message_text"],
            started_at=started_at,
            completed_at=_parse_db_timestamp(row["completed_at"]),
            outcome=row["outcome"],
            summary=row["summary"],
            legacy_turn_id=row["legacy_turn_id"],
            legacy_event_id=row["legacy_event_id"],
        )

    def _row_to_run_substep(self, row: sqlite3.Row) -> RunSubstep:
        """Convert a database row to a RunSubstep."""
        payload: Any
        try:
            payload = json.loads(row["payload_json"])
        except (TypeError, json.JSONDecodeError):
            payload = row["payload_json"]
        timestamp = _parse_db_timestamp(row["timestamp"])
        assert timestamp is not None

        return RunSubstep(
            id=row["id"],
            run_id=row["run_id"],
            step_id=row["step_id"],
            substep_index=row["substep_index"],
            substep_id=row["substep_id"],
            type=row["type"],
            source=row["source"],
            call_id=row["call_id"],
            payload=payload,
            timestamp=timestamp,
            legacy_turn_id=row["legacy_turn_id"],
            legacy_event_id=row["legacy_event_id"],
        )

    def _row_to_task_comment(self, row: sqlite3.Row) -> TaskComment:
        """Convert a database row to a TaskComment."""
        keys = row.keys()
        created_at = _parse_db_timestamp(row["created_at"])
        assert created_at is not None
        return TaskComment(
            id=int(row["id"]),
            task_id=row["task_id"],
            content=row["content"],
            source=row["source"] if "source" in keys else "direct",
            author=row["author"] if "author" in keys else None,
            created_at=created_at,
            resolved_at=_parse_db_timestamp(row["resolved_at"]),
        )

    # === Task CRUD ===

    def _next_id(self, conn: sqlite3.Connection) -> str:
        """Allocate the next project-prefixed decimal task ID (within an open connection).

        Uses a single RETURNING statement so the increment and read are atomic —
        no concurrent writer can observe the same sequence value.
        """
        # INSERT path (fresh DB, no existing row): inserts next_seq=1 and
        # RETURNING returns 1 directly — no increment — so the first task is
        # {prefix}-1, not {prefix}-0.
        # ON CONFLICT path (row already exists): increments and returns the new
        # value. After v25/v26 migrations the row is seeded to preserve continuity, so
        # the first post-migration task gets max_old_int_id + 1.
        cur = conn.execute(
            "INSERT INTO project_sequences (project_id, prefix, next_seq) VALUES (?, ?, 1) "
            "ON CONFLICT(project_id) DO UPDATE SET next_seq = next_seq + 1, prefix = excluded.prefix "
            "RETURNING next_seq",
            (self._project_id, self._prefix),
        )
        seq = int(cur.fetchone()["next_seq"])
        return f"{self._prefix}-{seq}"

    def add(
        self,
        prompt: str,
        task_type: str = "implement",
        based_on: str | None = None,
        group: str | None = None,
        tags: Iterable[str] | None = None,
        depends_on: str | None = None,
        spec: str | None = None,
        create_review: bool = False,
        create_pr: bool = False,
        same_branch: bool = False,
        base_branch: str | None = None,
        task_type_hint: str | None = None,
        model: str | None = None,
        provider: str | None = None,
        provider_is_explicit: bool | None = None,
        urgent: bool = False,
        skip_learnings: bool = False,
    ) -> Task:
        """Add a new task. Returns the created Task with its generated string ID."""
        now = datetime.now(UTC).isoformat()
        normalized_tags = _normalize_tags(tags)
        if group is not None:
            normalized_tags = _normalize_tags((*normalized_tags, group))
        persisted_group = normalized_tags[0] if len(normalized_tags) == 1 else None
        if provider_is_explicit is None:
            provider_is_explicit = provider is not None
        with self._connect() as conn:
            new_id = self._next_id(conn)
            conn.execute(
                """
                INSERT INTO tasks (project_id, id, prompt, task_type, based_on, created_at, "group", depends_on, spec, create_review, create_pr, same_branch, base_branch, task_type_hint, model, provider, provider_is_explicit, urgent, skip_learnings)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    self._project_id,
                    new_id,
                    prompt,
                    task_type,
                    based_on,
                    now,
                    persisted_group,
                    depends_on,
                    spec,
                    1 if create_review else 0,
                    1 if create_pr else 0,
                    1 if same_branch else 0,
                    base_branch,
                    task_type_hint,
                    model,
                    provider,
                    1 if provider_is_explicit else 0,
                    1 if urgent else 0,
                    1 if skip_learnings else 0,
                ),
            )
            self._replace_task_tags_conn(conn, new_id, normalized_tags)
            row = conn.execute(
                "SELECT * FROM tasks WHERE project_id = ? AND id = ?",
                (self._project_id, new_id),
            ).fetchone()
            assert row is not None
            return self._rows_to_tasks(conn, [row])[0]

    def get(self, task_id: str) -> Task | None:
        """Get a task by its string ID (e.g. 'gza-1234')."""
        with self._connect() as conn:
            cur = conn.execute(
                "SELECT * FROM tasks WHERE project_id = ? AND id = ?",
                (self._project_id, task_id),
            )
            row = cur.fetchone()
            if row is None:
                return None
            return self._rows_to_tasks(conn, [row])[0]

    def get_by_seq(self, seq_number: int, prefix: str | None = None) -> Task | None:
        """Get task by ordinal sequence number within a project prefix.

        This is equivalent to ``get(f"{prefix}-{seq_number}")`` and exists for
        call sites that need explicit ordinal lookup without string formatting.
        """
        if seq_number < 1:
            return None
        task_prefix = prefix or self._prefix
        return self.get(f"{task_prefix}-{seq_number}")

    def next_task_after(self, task_id: str) -> Task | None:
        """Return the next existing task by allocated sequence order.

        Sequence order is defined by ``project_sequences`` allocation and the
        decimal suffix in task IDs. Gaps are allowed (e.g. deletions), so this
        returns the smallest existing sequence strictly greater than ``task_id``.
        """
        try:
            canonical_id = resolve_task_id(task_id, self._prefix)
        except InvalidTaskIdError:
            return None
        prefix, suffix = canonical_id.rsplit("-", 1)
        seq = int(suffix)

        with self._connect() as conn:
            cur = conn.execute(
                """
                SELECT * FROM tasks
                WHERE project_id = ?
                  AND id LIKE (? || '-%')
                  AND substr(id, length(?) + 2) GLOB '[0-9]*'
                  AND CAST(substr(id, length(?) + 2) AS INTEGER) > ?
                ORDER BY CAST(substr(id, length(?) + 2) AS INTEGER) ASC
                LIMIT 1
                """,
                (self._project_id, prefix, prefix, prefix, seq, prefix),
            )
            row = cur.fetchone()
            if row is None:
                return None
            return self._rows_to_tasks(conn, [row])[0]

    def get_by_slug(self, slug: str) -> Task | None:
        """Get a task by slug (YYYYMMDD-... format, stored in the 'slug' DB column)."""
        with self._connect() as conn:
            cur = conn.execute(
                "SELECT * FROM tasks WHERE project_id = ? AND slug = ?",
                (self._project_id, slug),
            )
            row = cur.fetchone()
            if row is None:
                return None
            return self._rows_to_tasks(conn, [row])[0]

    def update(self, task: Task) -> None:
        """Update a task."""
        normalized_tags = _normalize_tags(task.tags or (() if task.group is None else (task.group,)))
        persisted_group = normalized_tags[0] if len(normalized_tags) == 1 else None
        task.tags = normalized_tags
        task.group = persisted_group
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE tasks SET
                    prompt = ?,
                    status = ?,
                    task_type = ?,
                    slug = ?,
                    branch = ?,
                    log_file = ?,
                    report_file = ?,
                    based_on = ?,
                    has_commits = ?,
                    duration_seconds = ?,
                    num_steps_reported = ?,
                    num_steps_computed = ?,
                    num_turns_reported = ?,
                    num_turns_computed = ?,
                    attach_count = ?,
                    attach_duration_seconds = ?,
                    cost_usd = ?,
                    input_tokens = ?,
                    output_tokens = ?,
                    started_at = ?,
                    running_pid = ?,
                    completed_at = ?,
                    "group" = ?,
                    depends_on = ?,
                    spec = ?,
                    create_review = ?,
                    create_pr = ?,
                    same_branch = ?,
                    base_branch = ?,
                    task_type_hint = ?,
                    output_content = ?,
                    session_id = ?,
                    pr_number = ?,
                    pr_state = ?,
                    pr_last_synced_at = ?,
                    model = ?,
                    provider = ?,
                    provider_is_explicit = ?,
                    urgent = ?,
                    queue_position = ?,
                    merge_status = ?,
                    merged_at = ?,
                    failure_reason = ?,
                    skip_learnings = ?,
                    diff_files_changed = ?,
                    diff_lines_added = ?,
                    diff_lines_removed = ?,
                    review_cleared_at = ?,
                    review_score = ?,
                    log_schema_version = ?,
                    execution_mode = ?
                WHERE project_id = ? AND id = ?
                """,
                (
                    task.prompt,
                    task.status,
                    task.task_type,
                    task.slug,
                    task.branch,
                    task.log_file,
                    task.report_file,
                    task.based_on,
                    1 if task.has_commits else (0 if task.has_commits is False else None),
                    task.duration_seconds,
                    task.num_steps_reported,
                    task.num_steps_computed,
                    task.num_turns_reported,
                    task.num_turns_computed,
                    task.attach_count,
                    task.attach_duration_seconds,
                    task.cost_usd,
                    task.input_tokens,
                    task.output_tokens,
                    task.started_at.isoformat() if task.started_at else None,
                    task.running_pid,
                    task.completed_at.isoformat() if task.completed_at else None,
                    persisted_group,
                    task.depends_on,
                    task.spec,
                    1 if task.create_review else 0,
                    1 if task.create_pr else 0,
                    1 if task.same_branch else 0,
                    task.base_branch,
                    task.task_type_hint,
                    task.output_content,
                    task.session_id,
                    task.pr_number,
                    task.pr_state,
                    task.pr_last_synced_at.isoformat() if task.pr_last_synced_at else None,
                    task.model,
                    task.provider,
                    1 if task.provider_is_explicit else 0,
                    1 if task.urgent else 0,
                    task.queue_position,
                    task.merge_status,
                    task.merged_at.isoformat() if task.merged_at else None,
                    task.failure_reason,
                    1 if task.skip_learnings else 0,
                    task.diff_files_changed,
                    task.diff_lines_added,
                    task.diff_lines_removed,
                    task.review_cleared_at.isoformat() if task.review_cleared_at else None,
                    task.review_score,
                    task.log_schema_version,
                    task.execution_mode,
                    self._project_id,
                    task.id,
                ),
            )
            if task.id is not None:
                self._replace_task_tags_conn(conn, task.id, normalized_tags)

    def delete(self, task_id: str) -> bool:
        """Delete a task by ID. Returns True if deleted."""
        with self._connect() as conn:
            cur = conn.execute(
                "DELETE FROM tasks WHERE project_id = ? AND id = ?",
                (self._project_id, task_id),
            )
            return cur.rowcount > 0

    # === Query methods ===

    def get_next_pending(
        self,
        group: str | None = None,
        *,
        tags: Iterable[str] | None = None,
        any_tag: bool = False,
    ) -> Task | None:
        """Get the next pending task (oldest first), skipping blocked tasks.

        A task is unblocked when its dependency is completed OR when its
        dependency is failed but a completed retry exists anywhere in the
        based_on chain.
        """
        pending = self.get_pending_pickup(limit=1, group=group, tags=tags, any_tag=any_tag)
        return pending[0] if pending else None

    def get_pending_pickup(
        self,
        limit: int | None = None,
        group: str | None = None,
        *,
        tags: Iterable[str] | None = None,
        any_tag: bool = False,
    ) -> list[Task]:
        """Get runnable pending tasks in pickup order.

        Pickup semantics match default worker selection: excludes internal and
        dependency-blocked tasks. Ordering is urgent-first, with recently bumped
        urgent tasks at the front, then FIFO by creation time. Explicit
        queue_position values sort ahead of the lane-based fallback ordering.
        """
        normalized_tags = _normalize_tags(tags)
        if not normalized_tags and group is not None:
            normalized_tags = _normalize_tags((group,))
        with self._connect() as conn:
            query = """
                WITH RECURSIVE successful_ancestors(id) AS (
                    SELECT id FROM tasks WHERE project_id = ? AND status = 'completed'
                    UNION ALL
                    SELECT t2.based_on FROM tasks t2
                    JOIN successful_ancestors sa ON t2.id = sa.id
                    WHERE t2.project_id = ? AND t2.based_on IS NOT NULL
                )
                SELECT t.* FROM tasks t
                WHERE t.project_id = ?
                AND t.status = 'pending'
                AND t.task_type != 'internal'
                AND (
                    t.depends_on IS NULL
                    OR t.depends_on IN (SELECT id FROM successful_ancestors)
                )
                ORDER BY
                    {order_by}
                """
            params: list[str | int | None] = [self._project_id, self._project_id, self._project_id]
            query = query.format(order_by=self._task_pickup_order_sql("t"))
            if normalized_tags:
                if not self._query_only_supports_tags():
                    return []
                placeholders = ",".join("?" for _ in normalized_tags)
                if any_tag:
                    query = query.replace(
                        "AND (",
                        f"AND EXISTS (SELECT 1 FROM task_tags tt WHERE tt.project_id = t.project_id AND tt.task_id = t.id AND tt.tag IN ({placeholders}))\nAND (",
                        1,
                    )
                    params.extend(normalized_tags)
                else:
                    query = query.replace(
                        "AND (",
                        f"AND (SELECT COUNT(DISTINCT tt.tag) FROM task_tags tt WHERE tt.project_id = t.project_id AND tt.task_id = t.id AND tt.tag IN ({placeholders})) = ?\nAND (",
                        1,
                    )
                    params.extend(normalized_tags)
                    params.append(len(normalized_tags))
            if limit is not None:
                query += " LIMIT ?"
                params.append(limit)
            cur = conn.execute(query, tuple(params))
            return self._rows_to_tasks(conn, cur.fetchall())

    def try_mark_in_progress(self, task_id: str, pid: int) -> Task | None:
        """Compare-and-swap pending -> in_progress for a specific task.

        Returns the updated Task on success, or None if the task was already
        claimed (CAS loss) or the database was busy.
        """
        started_at = datetime.now(UTC)
        try:
            with self._connect() as conn:
                cur = conn.execute(
                    """
                    UPDATE tasks
                    SET status = 'in_progress',
                        started_at = ?,
                        completed_at = NULL,
                        failure_reason = NULL,
                        running_pid = ?
                    WHERE project_id = ? AND id = ? AND status = 'pending'
                    """,
                    (started_at.isoformat(), pid, self._project_id, task_id),
                )
                if cur.rowcount == 0:
                    return None
                task = self.get(task_id)
                if task is None:
                    return None
                return task
        except sqlite3.OperationalError:
            # Database busy after timeout — treat as CAS loss.
            return None

    def get_pending(
        self,
        limit: int | None = None,
        group: str | None = None,
        *,
        tags: Iterable[str] | None = None,
        any_tag: bool = False,
    ) -> list[Task]:
        """Get all pending tasks."""
        normalized_tags = _normalize_tags(tags)
        if not normalized_tags and group is not None:
            normalized_tags = _normalize_tags((group,))
        with self._connect() as conn:
            query = (
                "SELECT t.* FROM tasks t WHERE t.project_id = ? AND t.status = 'pending' "
                "ORDER BY "
                f"{self._task_pickup_order_sql('t')}"
            )
            params: list[str | int | None] = [self._project_id]
            if normalized_tags:
                if not self._query_only_supports_tags():
                    return []
                placeholders = ",".join("?" for _ in normalized_tags)
                if any_tag:
                    query = query.replace(
                        "ORDER BY",
                        f"AND EXISTS (SELECT 1 FROM task_tags tt WHERE tt.project_id = t.project_id AND tt.task_id = t.id AND tt.tag IN ({placeholders})) ORDER BY",
                        1,
                    )
                    params.extend(normalized_tags)
                else:
                    query = query.replace(
                        "ORDER BY",
                        f"AND (SELECT COUNT(DISTINCT tt.tag) FROM task_tags tt WHERE tt.project_id = t.project_id AND tt.task_id = t.id AND tt.tag IN ({placeholders})) = ? ORDER BY",
                        1,
                    )
                    params.extend(normalized_tags)
                    params.append(len(normalized_tags))
            if limit is not None:
                query += " LIMIT ?"
                params.append(limit)
            cur = conn.execute(query, tuple(params))
            return self._rows_to_tasks(conn, cur.fetchall())

    def set_urgent(self, task_id: str, urgent: bool) -> bool:
        """Set or clear a task's urgent queue flag.

        Setting urgent=True records a bump timestamp so the task moves to the
        front of the urgent pickup lane.
        """
        bumped_at = datetime.now(UTC).isoformat() if urgent else None
        with self._connect() as conn:
            cur = conn.execute(
                "UPDATE tasks SET urgent = ?, urgent_bumped_at = ? WHERE project_id = ? AND id = ?",
                (1 if urgent else 0, bumped_at, self._project_id, task_id),
            )
            return cur.rowcount > 0

    def set_queue_position(
        self,
        task_id: str,
        position: int,
        *,
        tags: Iterable[str] | None = None,
        any_tag: bool = False,
    ) -> bool:
        """Assign an explicit queue position within the task's queue bucket.

        When tag filters are provided and the target task matches them, bucket
        membership uses those filters so tag-scoped queue workflows share one
        ordering across tasks with extra unrelated tags.

        Ordered tasks stay contiguous starting from 1 within their bucket.
        Unordered tasks keep queue_position=NULL and sort after ordered tasks.
        """
        if position < 1:
            raise ValueError("position must be >= 1")

        with self._connect() as conn:
            row = conn.execute(
                "SELECT id, queue_position FROM tasks WHERE project_id = ? AND id = ?",
                (self._project_id, task_id),
            ).fetchone()
            if row is None:
                return False
            current_position = row["queue_position"]
            bucket_predicate, bucket_params = self._queue_bucket_predicate_and_params_conn(
                conn,
                task_id,
                tags=tags,
                any_tag=any_tag,
            )
            desired_position = position

            if current_position is None:
                max_position_row = conn.execute(
                    f"""
                    SELECT MAX(queue_position) AS max_position
                    FROM tasks
                    WHERE {bucket_predicate}
                    """,
                    bucket_params,
                ).fetchone()
                max_position = int(max_position_row["max_position"] or 0)
                desired_position = min(position, max_position + 1)
                conn.execute(
                    f"""
                    UPDATE tasks
                    SET queue_position = queue_position + 1
                    WHERE {bucket_predicate}
                      AND queue_position IS NOT NULL
                      AND queue_position >= ?
                    """,
                    (*bucket_params, desired_position),
                )
            else:
                max_position_row = conn.execute(
                    f"""
                    SELECT MAX(queue_position) AS max_position
                    FROM tasks
                    WHERE {bucket_predicate}
                    """,
                    bucket_params,
                ).fetchone()
                max_position = int(max_position_row["max_position"] or current_position)
                desired_position = min(position, max_position)
                if desired_position < current_position:
                    conn.execute(
                        f"""
                        UPDATE tasks
                        SET queue_position = queue_position + 1
                        WHERE {bucket_predicate}
                          AND queue_position IS NOT NULL
                          AND queue_position >= ?
                          AND queue_position < ?
                          AND id != ?
                        """,
                        (*bucket_params, desired_position, current_position, task_id),
                    )
                elif desired_position > current_position:
                    conn.execute(
                        f"""
                        UPDATE tasks
                        SET queue_position = queue_position - 1
                        WHERE {bucket_predicate}
                          AND queue_position IS NOT NULL
                          AND queue_position > ?
                          AND queue_position <= ?
                          AND id != ?
                        """,
                        (*bucket_params, current_position, desired_position, task_id),
                    )

            cur = conn.execute(
                "UPDATE tasks SET queue_position = ? WHERE project_id = ? AND id = ?",
                (desired_position, self._project_id, task_id),
            )
            return cur.rowcount > 0

    def clear_queue_position(
        self,
        task_id: str,
        *,
        tags: Iterable[str] | None = None,
        any_tag: bool = False,
    ) -> bool:
        """Clear explicit queue ordering and close the bucket gap if needed."""
        with self._connect() as conn:
            row = conn.execute(
                "SELECT id, queue_position FROM tasks WHERE project_id = ? AND id = ?",
                (self._project_id, task_id),
            ).fetchone()
            if row is None:
                return False
            current_position = row["queue_position"]
            if current_position is None:
                return True
            bucket_predicate, bucket_params = self._queue_bucket_predicate_and_params_conn(
                conn,
                task_id,
                tags=tags,
                any_tag=any_tag,
            )
            conn.execute(
                "UPDATE tasks SET queue_position = NULL WHERE project_id = ? AND id = ?",
                (self._project_id, task_id),
            )
            conn.execute(
                f"""
                UPDATE tasks
                SET queue_position = queue_position - 1
                WHERE {bucket_predicate}
                  AND queue_position IS NOT NULL
                  AND queue_position > ?
                """,
                (*bucket_params, current_position),
            )
            return True

    def _queue_bucket_predicate_and_params_conn(
        self,
        conn: sqlite3.Connection,
        task_id: str,
        *,
        tags: Iterable[str] | None = None,
        any_tag: bool = False,
    ) -> tuple[str, tuple[str | int, ...]]:
        """Return SQL predicate/params for the task's canonical queue bucket.

        Default bucket identity is exact normalized tag-set equality.
        For tag-scoped queue operations, when requested tags are provided and
        the target task matches the scope, bucket identity is the same
        tag-filter match (all tags or any tag) so extra task tags do not split
        the ordering.
        """
        task_tags = self._fetch_tags_for_task_ids(conn, (task_id,)).get(task_id, ())
        normalized_scope_tags = _normalize_tags(tags)

        if normalized_scope_tags:
            task_tag_set = set(task_tags)
            scope_matches_target = (
                any(tag in task_tag_set for tag in normalized_scope_tags)
                if any_tag
                else all(tag in task_tag_set for tag in normalized_scope_tags)
            )
            if scope_matches_target:
                placeholders = ",".join("?" for _ in normalized_scope_tags)
                if any_tag:
                    return (
                        f"EXISTS (SELECT 1 FROM task_tags qbt WHERE qbt.task_id = tasks.id AND qbt.tag IN ({placeholders}))",
                        normalized_scope_tags,
                    )
                return (
                    f"(SELECT COUNT(DISTINCT qbt.tag) FROM task_tags qbt WHERE qbt.task_id = tasks.id AND qbt.tag IN ({placeholders})) = ?",
                    (*normalized_scope_tags, len(normalized_scope_tags)),
                )

        if not task_tags:
            return (
                "NOT EXISTS (SELECT 1 FROM task_tags qbt WHERE qbt.project_id = tasks.project_id AND qbt.task_id = tasks.id)",
                (),
            )

        placeholders = ",".join("?" for _ in task_tags)
        predicate = (
            "(SELECT COUNT(*) FROM task_tags qbt WHERE qbt.project_id = tasks.project_id AND qbt.task_id = tasks.id) = ? "
            f"AND (SELECT COUNT(*) FROM task_tags qbt WHERE qbt.project_id = tasks.project_id AND qbt.task_id = tasks.id AND qbt.tag IN ({placeholders})) = ?"
        )
        params: tuple[str | int, ...] = (len(task_tags), *task_tags, len(task_tags))
        return predicate, params

    def get_in_progress(self) -> list[Task]:
        """Get all in-progress tasks, oldest first."""
        with self._connect() as conn:
            cur = conn.execute(
                "SELECT * FROM tasks WHERE project_id = ? AND status = 'in_progress' ORDER BY started_at ASC, created_at ASC",
                (self._project_id,),
            )
            return self._rows_to_tasks(conn, cur.fetchall())

    def get_history(
        self,
        limit: int | None = 10,
        status: str | None = None,
        task_type: str | None = None,
        since: "datetime | None" = None,
        until: "datetime | None" = None,
    ) -> list[Task]:
        """Get completed/failed tasks, most recent first.

        Args:
            limit: Maximum number of tasks to return (None for all)
            status: Filter by specific status (e.g., 'completed', 'failed', 'unmerged')
                   If None, returns all completed/failed/unmerged tasks
            task_type: Filter by specific task_type (e.g., 'explore', 'plan', 'implement', 'review', 'improve', 'internal')
                      If None, returns all non-internal task types
                      (use task_type='internal' to include internal tasks)
            since: If specified, only return tasks where completed_at >= since
                   (falls back to created_at when completed_at is NULL)
            until: If specified, only return tasks where completed_at <= until
                   (falls back to created_at when completed_at is NULL)
        """
        with self._connect() as conn:
            # Build WHERE clause based on status and task_type filters
            where_clauses = ["project_id = ?"]
            params = [self._project_id]

            if status == "unmerged":
                # Unmerged tasks: either merge_status='unmerged' (current) or
                # legacy status='unmerged' (old data)
                where_clauses.append(
                    "(merge_status = 'unmerged' OR status = 'unmerged')"
                )
            elif status:
                where_clauses.append("status = ?")
                params.append(status)
            else:
                where_clauses.append("status IN ('completed', 'failed', 'unmerged', 'dropped')")

            if task_type:
                where_clauses.append("task_type = ?")
                params.append(task_type)
            else:
                where_clauses.append("task_type != 'internal'")

            if since is not None:
                since_str = since.isoformat()
                where_clauses.append(
                    "(completed_at >= ? OR (completed_at IS NULL AND created_at >= ?))"
                )
                params.extend([since_str, since_str])

            if until is not None:
                until_str = until.isoformat()
                where_clauses.append(
                    "(completed_at <= ? OR (completed_at IS NULL AND created_at <= ?))"
                )
                params.extend([until_str, until_str])

            where_clause = "WHERE " + " AND ".join(where_clauses)

            # Add LIMIT clause if specified
            if limit is None:
                query = f"""
                    SELECT * FROM tasks
                    {where_clause}
                    ORDER BY completed_at DESC, created_at DESC
                """
                cur = conn.execute(query, params)
            else:
                query = f"""
                    SELECT * FROM tasks
                    {where_clause}
                    ORDER BY completed_at DESC, created_at DESC
                    LIMIT ?
                """
                params.append(str(limit))
                cur = conn.execute(query, params)

            return self._rows_to_tasks(conn, cur.fetchall())

    def get_based_on_children(self, task_id: str) -> list[Task]:
        """Return tasks where based_on = task_id (direct lineage descendants)."""
        with self._connect() as conn:
            cur = conn.execute(
                "SELECT * FROM tasks WHERE project_id = ? AND based_on = ? ORDER BY created_at ASC",
                (self._project_id, task_id),
            )
            return self._rows_to_tasks(conn, cur.fetchall())

    def get_based_on_children_by_type(self, task_id: str, task_type: str) -> list[Task]:
        """Return tasks where based_on = task_id and task_type matches."""
        with self._connect() as conn:
            cur = conn.execute(
                """
                SELECT * FROM tasks
                WHERE project_id = ? AND based_on = ? AND task_type = ?
                ORDER BY created_at ASC
                """,
                (self._project_id, task_id, task_type),
            )
            return self._rows_to_tasks(conn, cur.fetchall())

    def get_lineage_children(self, task_id: str) -> list[Task]:
        """Return direct lineage children linked by based_on or depends_on.

        This is the canonical query used by lineage tree construction.
        """
        with self._connect() as conn:
            cur = conn.execute(
                """
                SELECT * FROM tasks
                WHERE project_id = ? AND (based_on = ? OR depends_on = ?)
                ORDER BY created_at ASC
                """,
                (self._project_id, task_id, task_id),
            )
            return self._rows_to_tasks(conn, cur.fetchall())

    def get_resumable_failed_tasks(self) -> list[Task]:
        """Return failed tasks that can be auto-resumed.

        A task is resumable if:
        - status = 'failed'
        - failure_reason is in the shared resumable failure policy
        - session_id IS NOT NULL
        """
        with self._connect() as conn:
            resumable_reasons = tuple(sorted(RESUMABLE_FAILURE_REASONS))
            placeholders = ",".join("?" for _ in resumable_reasons)
            cur = conn.execute(
                f"""
                SELECT * FROM tasks
                WHERE project_id = ? AND status = 'failed'
                AND failure_reason IN ({placeholders})
                AND session_id IS NOT NULL
                ORDER BY completed_at DESC, created_at DESC
                """,
                (self._project_id, *resumable_reasons),
            )
            return self._rows_to_tasks(conn, cur.fetchall())

    def count_resume_chain_depth(self, task_id: str) -> int:
        """Count consecutive failed ancestors with resumable failure reasons.

        Walks the based_on chain upward from task_id's parent, counting how many
        consecutive failed ancestors have a failure_reason in the shared resumable policy.
        This tells us how many times we've already tried resuming (not counting task_id itself).

        Examples:
          - Task gza-1 failed MAX_STEPS, based_on=None → depth=0 (no prior attempts)
          - Task gza-2 failed MAX_STEPS, based_on=gza-1 (also MAX_STEPS) → depth=1 (one prior)
          - Task gza-3 failed MAX_STEPS, based_on=gza-2 → depth=2 (two prior attempts)
        """
        seen_ids: set[str] = set()
        seen_ids.add(task_id)
        # Start from the parent (based_on of task_id)
        with self._connect() as conn:
            cur = conn.execute(
                "SELECT based_on FROM tasks WHERE project_id = ? AND id = ?",
                (self._project_id, task_id),
            )
            row = cur.fetchone()
            if row is None:
                return 0
            current_id: str | None = row["based_on"]

            depth = 0
            while current_id is not None:
                if current_id in seen_ids:
                    break  # Cycle detected
                seen_ids.add(current_id)
                cur = conn.execute(
                    "SELECT based_on, status, failure_reason FROM tasks WHERE project_id = ? AND id = ?",
                    (self._project_id, current_id),
                )
                row = cur.fetchone()
                if row is None:
                    break
                based_on, status, failure_reason = row["based_on"], row["status"], row["failure_reason"]
                if status == "failed" and is_resumable_failure_reason(failure_reason):
                    depth += 1
                    current_id = based_on
                else:
                    break
        return depth

    def get_recent_completed(self, limit: int = 15) -> list[Task]:
        """Get recent completed tasks, most recent first. Excludes internal tasks."""
        with self._connect() as conn:
            cur = conn.execute(
                """
                SELECT * FROM tasks
                WHERE project_id = ?
                  AND status = 'completed'
                  AND task_type != 'internal'
                ORDER BY completed_at DESC, created_at DESC
                LIMIT ?
                """,
                (self._project_id, limit),
            )
            return self._rows_to_tasks(conn, cur.fetchall())

    def get_unmerged(self) -> list[Task]:
        """Get tasks with unmerged code (merge_status = 'unmerged').

        Excludes improve and rebase tasks that have a parent (based_on)
        since they use same_branch=True and operate on the parent task's
        branch. Standalone improve tasks with their own branch are included.
        """
        with self._connect() as conn:
            cur = conn.execute(
                """
                SELECT * FROM tasks
                WHERE project_id = ?
                AND merge_status = 'unmerged'
                AND (
                    task_type NOT IN ('improve', 'rebase', 'fix')
                    OR based_on IS NULL
                )
                ORDER BY completed_at DESC
                """,
                (self._project_id,),
            )
            return self._rows_to_tasks(conn, cur.fetchall())

    def get_tasks_for_branch(self, branch: str) -> list[Task]:
        """Return all task rows attached to a branch, oldest first."""
        with self._connect() as conn:
            cur = conn.execute(
                """
                SELECT * FROM tasks
                WHERE project_id = ?
                  AND branch = ?
                ORDER BY created_at ASC, id ASC
                """,
                (self._project_id, branch),
            )
            return self._rows_to_tasks(conn, cur.fetchall())

    def get_sync_candidates(self, recent_days: int = 30) -> list[Task]:
        """Return a bounded set of branch-bearing task rows for `gza sync`."""
        cutoff = (datetime.now(UTC) - timedelta(days=recent_days)).isoformat()
        with self._connect() as conn:
            cur = conn.execute(
                """
                SELECT * FROM tasks
                WHERE project_id = ?
                  AND branch IS NOT NULL
                  AND has_commits = 1
                  AND (
                        merge_status = 'unmerged'
                        OR (pr_number IS NOT NULL AND (pr_state IS NULL OR pr_state = 'open'))
                        OR (
                            COALESCE(merged_at, completed_at, created_at) >= ?
                            AND (pr_number IS NOT NULL OR create_pr = 1)
                        )
                  )
                ORDER BY COALESCE(merged_at, completed_at, created_at) DESC, created_at DESC
                """,
                (self._project_id, cutoff),
            )
            return self._rows_to_tasks(conn, cur.fetchall())

    def set_merge_status(self, task_id: str, merge_status: str | None) -> None:
        """Set the merge_status for a task. Records merged_at when setting to 'merged'."""
        merged_at = datetime.now(UTC).isoformat() if merge_status == "merged" else None
        with self._connect() as conn:
            conn.execute(
                "UPDATE tasks SET merge_status = ?, merged_at = ? WHERE project_id = ? AND id = ?",
                (merge_status, merged_at, self._project_id, task_id),
            )

    def clear_review_state(self, task_id: str) -> None:
        """Clear the review state on an implementation task.

        Called when an improve task completes, to indicate that the previous
        review's feedback has been addressed. Sets review_cleared_at to now.

        Note: This is called whenever an improve task completes with commits.
        It cannot verify whether the improve task actually addressed the review
        feedback in a meaningful way — it only records that an improve task ran.

        If task_id does not exist, this is a no-op (no error is raised).
        """
        now = datetime.now(UTC).isoformat()
        with self._connect() as conn:
            conn.execute(
                "UPDATE tasks SET review_cleared_at = ? WHERE project_id = ? AND id = ?",
                (now, self._project_id, task_id),
            )

    def invalidate_review_state(self, task_id: str) -> None:
        """Invalidate review state on a task so it requires a new review.

        Called when a rebase task completes, since conflict resolution may have
        introduced changes not covered by prior reviews. Clears review_cleared_at
        so advance will create a new review before merging.

        If task_id does not exist, this is a no-op (no error is raised).
        """
        with self._connect() as conn:
            conn.execute(
                "UPDATE tasks SET review_cleared_at = NULL WHERE project_id = ? AND id = ?",
                (self._project_id, task_id),
            )

    def set_log_schema_version(self, task_id: str, version: int) -> None:
        """Set the persisted log schema marker for a task/run."""
        with self._connect() as conn:
            conn.execute(
                "UPDATE tasks SET log_schema_version = ? WHERE project_id = ? AND id = ?",
                (version, self._project_id, task_id),
            )

    def set_execution_mode(self, task_id: str, mode: str | None) -> None:
        """Set persisted execution provenance mode for a task/run."""
        if mode is not None and mode not in KNOWN_EXECUTION_MODES:
            raise ValueError(f"Unknown execution mode: {mode}")
        with self._connect() as conn:
            conn.execute(
                "UPDATE tasks SET execution_mode = ? WHERE project_id = ? AND id = ?",
                (mode, self._project_id, task_id),
            )

    # === Run step/substep persistence ===

    def emit_step(
        self,
        run_id: str,
        message: str | None,
        *,
        provider: str,
        message_role: str = "assistant",
        started_at: datetime | None = None,
        legacy_turn_id: str | None = None,
        legacy_event_id: str | None = None,
    ) -> StepRef:
        """Create and persist a top-level message step for a run."""
        timestamp = (started_at or datetime.now(UTC)).isoformat()
        with self._connect() as conn:
            cur = conn.execute(
                "SELECT COALESCE(MAX(step_index), 0) + 1 AS next_step FROM run_steps WHERE project_id = ? AND run_id = ?",
                (self._project_id, run_id),
            )
            next_step = int(cur.fetchone()["next_step"])
            step_label = f"S{next_step}"
            cur = conn.execute(
                """
                INSERT INTO run_steps (
                    project_id, run_id, step_index, step_id, provider, message_role,
                    message_text, started_at, legacy_turn_id, legacy_event_id
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    self._project_id,
                    run_id,
                    next_step,
                    step_label,
                    provider,
                    message_role,
                    message,
                    timestamp,
                    legacy_turn_id,
                    legacy_event_id,
                ),
            )
            step_row_id = cur.lastrowid
            assert step_row_id is not None
            return StepRef(id=step_row_id, run_id=run_id, step_index=next_step, step_id=step_label)

    def emit_substep(
        self,
        step_ref: StepRef,
        substep_type: str,
        payload: Any,
        *,
        source: str,
        call_id: str | None = None,
        timestamp: datetime | None = None,
        legacy_turn_id: str | None = None,
        legacy_event_id: str | None = None,
    ) -> RunSubstep:
        """Append a substep/tool event under an existing step."""
        ts = (timestamp or datetime.now(UTC)).isoformat()
        payload_json = json.dumps(payload)
        with self._connect() as conn:
            cur = conn.execute(
                "SELECT run_id, step_index, step_id FROM run_steps WHERE project_id = ? AND id = ?",
                (self._project_id, step_ref.id),
            )
            row = cur.fetchone()
            if row is None:
                raise ValueError(f"Unknown step reference: {step_ref.id}")
            if row["run_id"] != step_ref.run_id:
                raise ValueError(
                    f"Step reference run mismatch: step run_id={row['run_id']}, ref run_id={step_ref.run_id}"
                )
            if row["step_index"] != step_ref.step_index:
                raise ValueError(
                    f"Step reference index mismatch: step step_index={row['step_index']}, ref step_index={step_ref.step_index}"
                )
            if row["step_id"] != step_ref.step_id:
                raise ValueError(
                    f"Step reference label mismatch: step step_id={row['step_id']}, ref step_id={step_ref.step_id}"
                )

            cur = conn.execute(
                "SELECT COALESCE(MAX(substep_index), 0) + 1 AS next_substep FROM run_substeps WHERE project_id = ? AND step_id = ?",
                (self._project_id, step_ref.id),
            )
            next_substep = int(cur.fetchone()["next_substep"])
            substep_label = f"{step_ref.step_id}.{next_substep}"
            cur = conn.execute(
                """
                INSERT INTO run_substeps (
                    project_id, run_id, step_id, substep_index, substep_id, type, source, call_id,
                    payload_json, timestamp, legacy_turn_id, legacy_event_id
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    self._project_id,
                    step_ref.run_id,
                    step_ref.id,
                    next_substep,
                    substep_label,
                    substep_type,
                    source,
                    call_id,
                    payload_json,
                    ts,
                    legacy_turn_id,
                    legacy_event_id,
                ),
            )
            substep_row_id = cur.lastrowid
            assert substep_row_id is not None
            row = conn.execute(
                "SELECT * FROM run_substeps WHERE project_id = ? AND id = ?",
                (self._project_id, substep_row_id),
            ).fetchone()
            assert row is not None
            return self._row_to_run_substep(row)

    def finalize_step(
        self,
        step_ref: StepRef,
        outcome: str,
        summary: str | None = None,
        *,
        completed_at: datetime | None = None,
    ) -> None:
        """Mark a step complete and persist final outcome metadata."""
        ts = (completed_at or datetime.now(UTC)).isoformat()
        with self._connect() as conn:
            cur = conn.execute(
                "SELECT run_id, step_index, step_id FROM run_steps WHERE project_id = ? AND id = ?",
                (self._project_id, step_ref.id),
            )
            row = cur.fetchone()
            if row is None:
                raise ValueError(f"Unknown step reference: {step_ref.id}")
            if row["run_id"] != step_ref.run_id:
                raise ValueError(
                    f"Step reference run mismatch: step run_id={row['run_id']}, ref run_id={step_ref.run_id}"
                )
            if row["step_index"] != step_ref.step_index:
                raise ValueError(
                    f"Step reference index mismatch: step step_index={row['step_index']}, ref step_index={step_ref.step_index}"
                )
            if row["step_id"] != step_ref.step_id:
                raise ValueError(
                    f"Step reference label mismatch: step step_id={row['step_id']}, ref step_id={step_ref.step_id}"
                )

            cur = conn.execute(
                """
                UPDATE run_steps
                SET completed_at = ?, outcome = ?, summary = ?
                WHERE project_id = ? AND id = ?
                """,
                (ts, outcome, summary, self._project_id, step_ref.id),
            )
            if cur.rowcount == 0:
                raise ValueError(f"Unknown step reference: {step_ref.id}")

    def count_steps(self, run_id: str) -> int:
        """Count the number of run_steps rows for a given run_id."""
        if not self._query_only_supports_run_steps():
            return 0
        with self._connect() as conn:
            cur = conn.execute(
                "SELECT COUNT(*) AS cnt FROM run_steps WHERE project_id = ? AND run_id = ?",
                (self._project_id, run_id),
            )
            row = cur.fetchone()
            return int(row["cnt"]) if row else 0

    def get_run_steps(self, run_id: str) -> list[RunStep]:
        """Get all stored run steps for a run, ordered by step index."""
        with self._connect() as conn:
            cur = conn.execute(
                "SELECT * FROM run_steps WHERE project_id = ? AND run_id = ? ORDER BY step_index ASC",
                (self._project_id, run_id),
            )
            return [self._row_to_run_step(row) for row in cur.fetchall()]

    def get_run_substeps(self, step_ref: StepRef) -> list[RunSubstep]:
        """Get all stored substeps for a step, ordered by substep index."""
        with self._connect() as conn:
            cur = conn.execute(
                "SELECT run_id, step_index, step_id FROM run_steps WHERE project_id = ? AND id = ?",
                (self._project_id, step_ref.id),
            )
            row = cur.fetchone()
            if row is None:
                raise ValueError(f"Unknown step reference: {step_ref.id}")
            if row["run_id"] != step_ref.run_id:
                raise ValueError(
                    f"Step reference run mismatch: step run_id={row['run_id']}, ref run_id={step_ref.run_id}"
                )
            if row["step_index"] != step_ref.step_index:
                raise ValueError(
                    f"Step reference index mismatch: step step_index={row['step_index']}, ref step_index={step_ref.step_index}"
                )
            if row["step_id"] != step_ref.step_id:
                raise ValueError(
                    f"Step reference label mismatch: step step_id={row['step_id']}, ref step_id={step_ref.step_id}"
                )

            cur = conn.execute(
                """
                SELECT * FROM run_substeps
                WHERE project_id = ? AND run_id = ? AND step_id = ?
                ORDER BY substep_index ASC
                """,
                (self._project_id, step_ref.run_id, step_ref.id),
            )
            return [self._row_to_run_substep(row) for row in cur.fetchall()]

    def get_all(self) -> list[Task]:
        """Get all tasks."""
        with self._connect() as conn:
            cur = conn.execute(
                "SELECT * FROM tasks WHERE project_id = ? ORDER BY created_at DESC",
                (self._project_id,),
            )
            return self._rows_to_tasks(conn, cur.fetchall())

    def get_impl_based_on_ids(self) -> set[str]:
        """Return the set of plan IDs that already have an implement task.

        Checks both based_on and depends_on, since implement tasks may
        reference their plan via either column.
        """
        with self._connect() as conn:
            cur = conn.execute(
                "SELECT DISTINCT based_on FROM tasks"
                " WHERE project_id = ? AND task_type = 'implement' AND based_on IS NOT NULL"
                " UNION"
                " SELECT DISTINCT depends_on FROM tasks"
                " WHERE project_id = ? AND task_type = 'implement' AND depends_on IS NOT NULL",
                (self._project_id, self._project_id),
            )
            return {row[0] for row in cur.fetchall()}

    def get_reviews_for_task(self, task_id: str) -> list[Task]:
        """Get all review tasks that depend on the given task, ordered by completed_at DESC.

        Reviews are ordered by completed_at so that the most recently completed
        review is first (reviews[0]). Incomplete reviews (completed_at IS NULL) sort
        last. This ensures the staleness check in cmd_unmerged compares against the
        review that completed most recently, not merely the one created most recently.
        """
        with self._connect() as conn:
            cur = conn.execute(
                """
                SELECT * FROM tasks
                WHERE project_id = ? AND task_type = 'review' AND depends_on = ?
                ORDER BY completed_at DESC NULLS LAST
                """,
                (self._project_id, task_id),
            )
            return self._rows_to_tasks(conn, cur.fetchall())

    def get_unlinked_reviews_for_slug(self, slug: str) -> list[Task]:
        """Get completed review tasks not linked via depends_on, matched by slug.

        This is a fallback for review tasks created manually (e.g., prompt starts
        with "review <slug>") without an explicit depends_on relationship.
        """
        with self._connect() as conn:
            cur = conn.execute(
                """
                SELECT * FROM tasks
                WHERE project_id = ?
                  AND task_type = 'review'
                  AND status = 'completed'
                  AND depends_on IS NULL
                  AND (
                    slug LIKE ?
                    OR prompt LIKE ?
                  )
                ORDER BY completed_at DESC NULLS LAST
                """,
                (self._project_id, f"%review-{slug}%", f"review {slug}%"),
            )
            return self._rows_to_tasks(conn, cur.fetchall())

    def get_improve_tasks_for(self, impl_task_id: str, review_task_id: str) -> list[Task]:
        """Get improve tasks that match the given implementation and review task IDs."""
        with self._connect() as conn:
            cur = conn.execute(
                """
                WITH RECURSIVE improve_chain(id) AS (
                    SELECT id
                    FROM tasks
                    WHERE project_id = ?
                      AND task_type = 'improve'
                      AND based_on = ?
                      AND depends_on = ?
                    UNION ALL
                    SELECT child.id
                    FROM tasks child
                    JOIN improve_chain parent ON child.based_on = parent.id
                    WHERE child.project_id = ?
                      AND child.task_type = 'improve'
                      AND child.depends_on = ?
                )
                SELECT t.*
                FROM tasks t
                JOIN improve_chain c ON c.id = t.id
                WHERE t.project_id = ?
                ORDER BY created_at DESC
                """,
                (
                    self._project_id,
                    impl_task_id,
                    review_task_id,
                    self._project_id,
                    review_task_id,
                    self._project_id,
                ),
            )
            return self._rows_to_tasks(conn, cur.fetchall())

    def get_improve_tasks_by_root(self, root_task_id: str) -> list[Task]:
        """Get all improve tasks transitively rooted at the given implementation.

        This remains for review/improve workflow logic; lineage display should
        use get_lineage_children() via gza.query.build_lineage_tree().
        """
        with self._connect() as conn:
            cur = conn.execute(
                """
                WITH RECURSIVE improve_chain(id) AS (
                    SELECT id
                    FROM tasks
                    WHERE project_id = ? AND task_type = 'improve' AND based_on = ?
                    UNION ALL
                    SELECT child.id
                    FROM tasks child
                    JOIN improve_chain parent ON child.based_on = parent.id
                    WHERE child.project_id = ? AND child.task_type = 'improve'
                )
                SELECT t.*
                FROM tasks t
                JOIN improve_chain c ON c.id = t.id
                WHERE t.project_id = ?
                ORDER BY created_at DESC
                """,
                (self._project_id, root_task_id, self._project_id, self._project_id),
            )
            return self._rows_to_tasks(conn, cur.fetchall())

    def get_fix_tasks_by_root(self, root_task_id: str) -> list[Task]:
        """Get fix tasks transitively rooted at the given implementation."""
        with self._connect() as conn:
            cur = conn.execute(
                """
                WITH RECURSIVE code_chain(id, task_type) AS (
                    SELECT id, task_type
                    FROM tasks
                    WHERE project_id = ?
                      AND based_on = ?
                      AND task_type IN ('improve', 'fix')
                    UNION ALL
                    SELECT child.id, child.task_type
                    FROM tasks child
                    JOIN code_chain parent ON child.based_on = parent.id
                    WHERE child.project_id = ?
                      AND child.task_type IN ('improve', 'fix')
                )
                SELECT t.*
                FROM tasks t
                JOIN code_chain c ON c.id = t.id
                WHERE t.project_id = ?
                  AND t.task_type = 'fix'
                ORDER BY t.created_at DESC
                """,
                (self._project_id, root_task_id, self._project_id, self._project_id),
            )
            return self._rows_to_tasks(conn, cur.fetchall())

    def add_comment(
        self,
        task_id: str,
        content: str,
        *,
        source: str = "direct",
        author: str | None = None,
    ) -> TaskComment:
        """Persist a task comment and return it."""
        if source not in {"direct", "github"}:
            raise ValueError(f"Unknown comment source: {source}")
        normalized = content.strip()
        if not normalized:
            raise ValueError("Comment content cannot be empty")
        with self._connect() as conn:
            existing = conn.execute(
                "SELECT 1 FROM tasks WHERE project_id = ? AND id = ?",
                (self._project_id, task_id),
            ).fetchone()
            if existing is None:
                raise KeyError(f"Task {task_id} not found")
            previous_row = conn.execute(
                """
                SELECT created_at
                FROM task_comments
                WHERE project_id = ? AND task_id = ?
                ORDER BY created_at DESC, id DESC
                LIMIT 1
                """,
                (self._project_id, task_id),
            ).fetchone()
            previous_created_at = (
                str(previous_row["created_at"])
                if previous_row is not None and previous_row["created_at"] is not None
                else None
            )
            created_at = _next_monotonic_iso_timestamp(datetime.now(UTC), previous_created_at)
            cur = conn.execute(
                """
                INSERT INTO task_comments (project_id, task_id, content, source, author, created_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (self._project_id, task_id, normalized, source, author, created_at),
            )
            row = conn.execute(
                "SELECT * FROM task_comments WHERE project_id = ? AND id = ?",
                (self._project_id, cur.lastrowid),
            ).fetchone()
            assert row is not None
            return self._row_to_task_comment(row)

    def get_comments(
        self,
        task_id: str,
        *,
        unresolved_only: bool = False,
        created_on_or_before: datetime | None = None,
    ) -> list[TaskComment]:
        """Return comments for a task in creation order."""
        if not self._query_only_supports_comments():
            return []
        query = "SELECT * FROM task_comments WHERE project_id = ? AND task_id = ?"
        params: list[Any] = [self._project_id, task_id]
        if unresolved_only:
            query += " AND resolved_at IS NULL"
        if created_on_or_before is not None:
            cutoff = created_on_or_before
            if cutoff.tzinfo is not None:
                cutoff = cutoff.astimezone(UTC)
            query += " AND created_at <= ?"
            params.append(cutoff.isoformat())
        query += " ORDER BY created_at ASC, id ASC"
        with self._connect() as conn:
            cur = conn.execute(query, tuple(params))
            return [self._row_to_task_comment(row) for row in cur.fetchall()]

    def resolve_comments(
        self,
        task_id: str,
        *,
        created_on_or_before: datetime | None = None,
    ) -> None:
        """Mark unresolved comments as resolved for a task."""
        resolved_at = datetime.now(UTC).isoformat()
        query = (
            "UPDATE task_comments "
            "SET resolved_at = ? "
            "WHERE project_id = ? AND task_id = ? AND resolved_at IS NULL"
        )
        params: list[Any] = [resolved_at, self._project_id, task_id]
        if created_on_or_before is not None:
            cutoff = created_on_or_before
            if cutoff.tzinfo is not None:
                cutoff = cutoff.astimezone(UTC)
            query += " AND created_at <= ?"
            params.append(cutoff.isoformat())
        with self._connect() as conn:
            conn.execute(query, tuple(params))

    def get_impl_tasks_by_depends_on_or_based_on(self, task_id: str) -> list[Task]:
        """Get implement tasks that depend on or are based on a given task.

        Kept for implementation-focused callers that only need implement tasks.
        Lineage display should use get_lineage_children().
        """
        with self._connect() as conn:
            cur = conn.execute(
                """
                SELECT * FROM tasks
                WHERE project_id = ? AND task_type = 'implement' AND (based_on = ? OR depends_on = ?)
                ORDER BY created_at ASC
                """,
                (self._project_id, task_id, task_id),
            )
            return self._rows_to_tasks(conn, cur.fetchall())

    def get_stats(self) -> dict:
        """Get aggregate statistics."""
        with self._connect() as conn:
            cur = conn.execute(
                """
                SELECT
                    COUNT(*) FILTER (WHERE status = 'completed') as completed,
                    COUNT(*) FILTER (WHERE status = 'failed') as failed,
                    COUNT(*) FILTER (WHERE status = 'pending') as pending,
                    COUNT(*) FILTER (WHERE status = 'dropped') as dropped,
                    SUM(cost_usd) as total_cost,
                    SUM(duration_seconds) as total_duration,
                    SUM(COALESCE(num_steps_reported, num_steps_computed, num_turns_reported)) as total_steps,
                    SUM(num_turns_reported) as total_turns,
                    SUM(input_tokens) as total_input_tokens,
                    SUM(output_tokens) as total_output_tokens
                FROM tasks
                WHERE project_id = ?
                """,
                (self._project_id,),
            )
            row = cur.fetchone()
            return {
                "completed": row["completed"] or 0,
                "failed": row["failed"] or 0,
                "pending": row["pending"] or 0,
                "dropped": row["dropped"] or 0,
                "total_cost": row["total_cost"] or 0,
                "total_duration": row["total_duration"] or 0,
                "total_steps": row["total_steps"] or 0,
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
                WHERE project_id = ? AND prompt LIKE ?
                ORDER BY created_at DESC
                """,
                (self._project_id, f"%{query}%"),
            )
            return self._rows_to_tasks(conn, cur.fetchall())

    def get_task_tags(self, task_id: str) -> tuple[str, ...]:
        """Return canonical tags for a task."""
        if not self._query_only_supports_tags():
            return ()
        with self._connect() as conn:
            cur = conn.execute(
                "SELECT tag FROM task_tags WHERE project_id = ? AND task_id = ? ORDER BY tag ASC",
                (self._project_id, task_id),
            )
            return tuple(str(row["tag"]) for row in cur.fetchall())

    def replace_task_tags(self, task_id: str, tags: Iterable[str]) -> tuple[str, ...]:
        """Replace all tags on a task."""
        normalized = _normalize_tags(tags)
        with self._connect() as conn:
            self._replace_task_tags_conn(conn, task_id, normalized)
            conn.execute(
                'UPDATE tasks SET "group" = ? WHERE project_id = ? AND id = ?',
                (normalized[0] if len(normalized) == 1 else None, self._project_id, task_id),
            )
        return normalized

    def add_task_tags(self, task_id: str, tags: Iterable[str]) -> tuple[str, ...]:
        """Add one or more tags to a task."""
        current = set(self.get_task_tags(task_id))
        current.update(_normalize_tags(tags))
        return self.replace_task_tags(task_id, current)

    def remove_task_tags(self, task_id: str, tags: Iterable[str]) -> tuple[str, ...]:
        """Remove one or more tags from a task."""
        removed = set(_normalize_tags(tags))
        current = [tag for tag in self.get_task_tags(task_id) if tag not in removed]
        return self.replace_task_tags(task_id, current)

    def get_tag_counts(self) -> dict[str, int]:
        """Return counts for every known tag."""
        if not self._query_only_supports_tags():
            return {}
        with self._connect() as conn:
            cur = conn.execute(
                """
                SELECT tag, COUNT(*) AS count
                FROM task_tags
                WHERE project_id = ?
                GROUP BY tag
                ORDER BY tag ASC
                """,
                (self._project_id,),
            )
            return {str(row["tag"]): int(row["count"]) for row in cur.fetchall()}

    def list_tags(self) -> tuple[str, ...]:
        """Return all known tags sorted alphabetically."""
        return tuple(self.get_tag_counts().keys())

    def get_groups(self) -> dict[str, dict[str, int]]:
        """Compatibility alias: return tag counts by status.

        Returns:
            Dict mapping tag to dict of status counts.
        """
        if not self._query_only_supports_tags():
            return {}
        with self._connect() as conn:
            cur = conn.execute(
                """
                SELECT tt.tag AS tag, t.status AS status, COUNT(*) AS count
                FROM task_tags tt
                JOIN tasks t ON t.project_id = tt.project_id AND t.id = tt.task_id
                WHERE tt.project_id = ?
                GROUP BY tt.tag, t.status
                """,
                (self._project_id,),
            )
            groups: dict[str, dict[str, int]] = {}
            for row in cur.fetchall():
                group_name = row["tag"]
                status = row["status"]
                count = row["count"]
                if group_name not in groups:
                    groups[group_name] = {}
                groups[group_name][status] = count
            return groups

    def get_by_group(self, group: str) -> list[Task]:
        """Compatibility alias: get all tasks carrying a tag."""
        if not self._query_only_supports_tags():
            return []
        normalized = _normalize_tag(group)
        with self._connect() as conn:
            cur = conn.execute(
                """
                SELECT t.*
                FROM tasks t
                JOIN task_tags tt ON tt.project_id = t.project_id AND tt.task_id = t.id
                WHERE tt.project_id = ? AND tt.tag = ?
                ORDER BY t.created_at ASC
                """,
                (self._project_id, normalized),
            )
            return self._rows_to_tasks(conn, cur.fetchall())

    def rename_group(self, old_group: str, new_group: str) -> int:
        """Compatibility alias: rename a tag across all tasks.

        Returns the number of updated tasks.

        Raises:
            ValueError: If either name is empty, the source group does not exist,
                or the destination group already exists.
        """
        old_name = _normalize_tag(old_group)
        new_name = _normalize_tag(new_group)

        with self._connect() as conn:
            existing_old = conn.execute(
                "SELECT COUNT(*) AS count FROM task_tags WHERE project_id = ? AND tag = ?",
                (self._project_id, old_name),
            ).fetchone()
            assert existing_old is not None
            old_count = int(existing_old["count"])
            if old_count == 0:
                raise ValueError(f"group '{old_name}' not found")

            if old_name == new_name:
                return old_count

            existing_new = conn.execute(
                "SELECT 1 FROM task_tags WHERE project_id = ? AND tag = ? LIMIT 1",
                (self._project_id, new_name),
            ).fetchone()
            if existing_new is not None:
                raise ValueError(f"group '{new_name}' already exists")

            cur = conn.execute(
                "UPDATE task_tags SET tag = ? WHERE project_id = ? AND tag = ?",
                (new_name, self._project_id, old_name),
            )
            # Keep compatibility column aligned for single-tag tasks.
            conn.execute('UPDATE tasks SET "group" = NULL WHERE project_id = ?', (self._project_id,))
            conn.execute(
                """
                UPDATE tasks
                SET "group" = (
                    SELECT tt.tag
                    FROM task_tags tt
                    WHERE tt.project_id = tasks.project_id AND tt.task_id = tasks.id
                    GROUP BY tt.task_id
                    HAVING COUNT(*) = 1
                )
                WHERE project_id = ?
                """,
                (self._project_id,),
            )
            return cur.rowcount

    def _find_successful_retry_task(self, task_id: str) -> Task | None:
        """Return the first completed task in the retry chain rooted at task_id.

        Follows based_on links forward (task_id → retries → retries of retries)
        and returns the first status='completed' task encountered.
        No data is mutated; this is a read-only query-time check.

        NOTE: 'dropped' nodes in the retry chain are intentionally not treated as
        successful retries. A dropped task means the work was deliberately abandoned,
        not completed. Only status='completed' unblocks a dependency chain.
        """
        visited: set[str] = set()
        queue: list[str] = [task_id]
        while queue:
            current_id = queue.pop(0)
            if current_id in visited:
                continue
            visited.add(current_id)
            with self._connect() as conn:
                cur = conn.execute(
                    "SELECT id, status FROM tasks WHERE project_id = ? AND based_on = ? ORDER BY created_at ASC",
                    (self._project_id, current_id),
                )
                for row in cur.fetchall():
                    if row["status"] == "completed":
                        completed = self.get(row["id"])
                        if completed is not None:
                            return completed
                    queue.append(row["id"])
        return None

    def resolve_dependency_completion(self, task: Task) -> Task | None:
        """Resolve the completed task that satisfies task.depends_on.

        Mirrors dependency-unblock semantics used by ``is_task_blocked()``:
        - direct dependency completed => resolved to direct dependency
        - direct dependency failed/dropped but has completed retry descendant => resolved
          to the completed retry task
        - otherwise unresolved
        """
        if task.depends_on is None:
            return None

        dep = self.get(task.depends_on)
        if dep is None:
            return None

        if dep.status == "completed":
            return dep

        if dep.status in {"failed", "dropped"} and dep.id is not None:
            return self._find_successful_retry_task(dep.id)

        return None

    def is_task_blocked(self, task: Task) -> tuple[bool, str | None, str | None]:
        """Check if a task is blocked by an incomplete dependency.

        When the direct dependency has failed or dropped, follows the retry
        chain (via based_on) to see if a subsequent retry succeeded. If so, the
        task is treated as unblocked.

        Returns:
            Tuple of (is_blocked, blocking_task_id, blocking_task_status)
        """
        if task.depends_on is None:
            return (False, None, None)

        dep = self.get(task.depends_on)
        if dep is None:
            return (False, None, None)

        if self.resolve_dependency_completion(task) is not None:
            return (False, None, None)

        return (True, dep.id, dep.status)

    def count_blocked_tasks(self) -> int:
        """Count pending tasks that are blocked by dependencies.

        A task is unblocked (and therefore not counted) if its dependency is
        completed OR if the dependency is failed/dropped but a completed retry
        exists anywhere in the based_on chain.

        NOTE: This SQL intentionally mirrors the Python logic in is_task_blocked():
        both treat only 'completed' (or failed/dropped with a successful retry)
        as unblocking. If these semantics change, both sites must be updated
        together.
        """
        with self._connect() as conn:
            cur = conn.execute(
                """
                WITH RECURSIVE successful_ancestors(id) AS (
                    SELECT id FROM tasks WHERE project_id = ? AND status = 'completed'
                    UNION ALL
                    SELECT t2.based_on FROM tasks t2
                    JOIN successful_ancestors sa ON t2.id = sa.id
                    WHERE t2.project_id = ? AND t2.based_on IS NOT NULL
                )
                SELECT COUNT(*) as count FROM tasks t
                WHERE t.project_id = ?
                AND t.status = 'pending'
                AND t.depends_on IS NOT NULL
                AND t.depends_on NOT IN (SELECT id FROM successful_ancestors)
                """,
                (self._project_id, self._project_id, self._project_id),
            )
            row = cur.fetchone()
            return row["count"] if row else 0

    # === Status transitions (TaskStore protocol) ===

    def mark_in_progress(self, task: Task) -> None:
        """Mark a task as in progress."""
        task.status = "in_progress"
        task.started_at = datetime.now(UTC)
        task.running_pid = os.getpid()
        self.update(task)

    def record_attach_session(self, task: Task, duration_seconds: float) -> None:
        """Accumulate interactive attach metrics on a task."""
        prior_count = task.attach_count or 0
        prior_duration = task.attach_duration_seconds or 0.0
        task.attach_count = prior_count + 1
        task.attach_duration_seconds = prior_duration + max(0.0, duration_seconds)
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
        task.completed_at = datetime.now(UTC)
        task.running_pid = None
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
            task.num_steps_reported = stats.num_steps_reported
            task.num_steps_computed = stats.num_steps_computed
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
        task_id: str,
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
                WHERE project_id = ? AND id = ?
                """,
                (files_changed, lines_added, lines_removed, self._project_id, task_id),
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
        task.completed_at = datetime.now(UTC)
        task.running_pid = None
        task.has_commits = has_commits
        if log_file:
            task.log_file = log_file
        if branch:
            task.branch = branch
        if stats:
            task.duration_seconds = stats.duration_seconds
            task.num_steps_reported = stats.num_steps_reported
            task.num_steps_computed = stats.num_steps_computed
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
        task.completed_at = datetime.now(UTC)
        task.has_commits = has_commits
        if branch:
            task.branch = branch
        if log_file:
            task.log_file = log_file
        if stats:
            task.duration_seconds = stats.duration_seconds
            task.num_steps_reported = stats.num_steps_reported
            task.num_steps_computed = stats.num_steps_computed
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
            "SELECT COUNT(*) FROM tasks WHERE project_id = ? AND merge_status IS NULL AND has_commits = 1",
            (store._project_id,),
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
            "SELECT id, has_commits, branch FROM tasks WHERE project_id = ? AND merge_status IS NULL",
            (store._project_id,),
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
                logger.warning(
                    "Could not determine merge status for task_id=%s branch=%s against %s; defaulting to 'unmerged'",
                    task_id,
                    branch,
                    default_branch,
                    exc_info=True,
                )
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
    based_on: str | None = None,
    based_on_slug: str | None = None,
    spec: str | None = None,
    group: str | None = None,
    tags: Iterable[str] | None = None,
    depends_on: str | None = None,
    create_review: bool = False,
    create_pr: bool = False,
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
        options.append(f"# Based on: {based_on}")
    if depends_on:
        options.append(f"# Depends on: {depends_on}")
    if group:
        options.append(f"# Group: {group}")
    normalized_tags = _normalize_tags(tags)
    if normalized_tags:
        options.append(f"# Tags: {', '.join(normalized_tags)}")
    if spec:
        options.append(f"# Spec: {spec}")
    if create_review:
        options.append("# Create review: yes")
    if create_pr:
        options.append("# Create PR: yes")
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
        if based_on_slug:
            initial_content = f"Implement plan from task {based_on}: {based_on_slug}"
        else:
            initial_content = f"Implement plan from task {based_on}"

    content = template + "\n" + initial_content

    with tempfile.NamedTemporaryFile(mode="w", suffix=".md", delete=False) as f:
        f.write(content)
        tmp_path = f.name

    try:
        result = _launch_editor([editor, tmp_path])
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
    based_on: str | None = None,
    spec: str | None = None,
    group: str | None = None,
    tags: Iterable[str] | None = None,
    depends_on: str | None = None,
    create_review: bool = False,
    create_pr: bool = False,
    same_branch: bool = False,
    task_type_hint: str | None = None,
    model: str | None = None,
    provider: str | None = None,
    skip_learnings: bool = False,
) -> Task | None:
    """Interactively add a task using $EDITOR.

    Returns the created task, or None if cancelled.
    """
    # Look up slug from the based_on task's task_id (format: YYYYMMDD-slug)
    based_on_slug = None
    if based_on:
        based_on_task = store.get(based_on)
        if based_on_task and based_on_task.slug:
            based_on_slug = based_on_task.slug[9:]  # Skip YYYYMMDD-

    while True:
        prompt = edit_prompt(
            task_type=task_type,
            based_on=based_on,
            based_on_slug=based_on_slug,
            spec=spec,
            group=group,
            tags=tags,
            depends_on=depends_on,
            create_review=create_review,
            create_pr=create_pr,
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
                tags=tags,
                depends_on=depends_on,
                spec=spec,
                create_review=create_review,
                create_pr=create_pr,
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
            tags=task.tags,
            depends_on=task.depends_on,
            create_review=task.create_review,
            create_pr=task.create_pr,
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


def import_legacy_local_db(config: "Config", *, dry_run: bool = False) -> dict[str, Any]:
    """Import legacy project-local .gza/gza.db rows into the active shared DB.

    Returns a summary dictionary with status and imported row counts.
    """
    local_db = _legacy_local_db_path(config.project_dir)
    shared_db = config.db_path
    if not local_db.exists():
        return {
            "status": "no_local_db",
            "local_db_path": str(local_db),
            "shared_db_path": str(shared_db),
            "tasks_imported": 0,
        }
    if local_db.resolve() == shared_db.resolve():
        raise ValueError(
            "Active db_path already points to the legacy local DB; "
            "set db_path to shared storage before importing."
        )
    if _marker_matches_shared_db(config.project_dir, local_db, shared_db):
        return {
            "status": "already_imported",
            "local_db_path": str(local_db),
            "shared_db_path": str(shared_db),
            "tasks_imported": 0,
        }

    task_import_columns = (
        "id", "prompt", "status", "task_type", "slug", "branch", "log_file", "report_file", "based_on",
        "has_commits", "duration_seconds", "num_steps_reported", "num_steps_computed", "num_turns",
        "num_turns_reported", "num_turns_computed", "attach_count", "attach_duration_seconds", "cost_usd",
        "created_at", "started_at", "running_pid", "completed_at", "group", "depends_on", "spec", "create_review",
        "create_pr",
        "same_branch", "task_type_hint", "output_content", "session_id", "pr_number", "pr_state",
        "pr_last_synced_at", "model", "provider",
        "provider_is_explicit", "urgent", "urgent_bumped_at", "queue_position", "input_tokens", "output_tokens",
        "merge_status", "merged_at", "failure_reason", "skip_learnings", "diff_files_changed", "diff_lines_added",
        "diff_lines_removed", "review_cleared_at", "review_score", "log_schema_version", "execution_mode", "base_branch",
    )
    task_import_columns_sql = ", ".join(f'"{c}"' if c == "group" else c for c in task_import_columns)
    legacy_task_fallbacks = {
        "create_pr": "0",
        "pr_state": "NULL",
        "pr_last_synced_at": "NULL",
    }
    project_id, project_prefix = _project_identity_from_config(config)

    run_step_payload_columns = (
        "provider",
        "message_role",
        "message_text",
        "started_at",
        "completed_at",
        "outcome",
        "summary",
        "legacy_turn_id",
        "legacy_event_id",
    )
    run_substep_payload_columns = (
        "run_id",
        "type",
        "source",
        "call_id",
        "payload_json",
        "timestamp",
        "legacy_turn_id",
        "legacy_event_id",
    )

    def _find_task_conflicts(conn: sqlite3.Connection, local_rows: list[sqlite3.Row]) -> list[str]:
        local_ids = [str(row["id"]) for row in local_rows]
        if not local_ids:
            return []
        placeholders = ",".join(["?"] * len(local_ids))
        existing_rows = conn.execute(
            f"""
            SELECT {task_import_columns_sql}
            FROM tasks
            WHERE project_id = ? AND id IN ({placeholders})
            """,
            (project_id, *local_ids),
        ).fetchall()
        existing_by_id = {str(row["id"]): row for row in existing_rows}
        conflicts: list[str] = []
        for row in local_rows:
            existing = existing_by_id.get(str(row["id"]))
            if existing is None:
                continue
            if any(existing[column] != row[column] for column in task_import_columns):
                conflicts.append(str(row["id"]))
        return conflicts

    def _find_run_step_conflicts(
        shared_conn: sqlite3.Connection,
        legacy_conn: sqlite3.Connection,
    ) -> list[str]:
        if not _table_exists(legacy_conn, "run_steps"):
            return []
        local_rows = legacy_conn.execute(
            """
            SELECT run_id, step_index, step_id, provider, message_role, message_text,
                   started_at, completed_at, outcome, summary, legacy_turn_id, legacy_event_id
            FROM run_steps
            ORDER BY id
            """
        ).fetchall()
        conflicts: list[str] = []
        for local_row in local_rows:
            matches = shared_conn.execute(
                """
                SELECT id, run_id, step_index, step_id, provider, message_role, message_text,
                       started_at, completed_at, outcome, summary, legacy_turn_id, legacy_event_id
                FROM run_steps
                WHERE project_id = ? AND run_id = ? AND (step_index = ? OR step_id = ?)
                """,
                (
                    project_id,
                    local_row["run_id"],
                    local_row["step_index"],
                    local_row["step_id"],
                ),
            ).fetchall()
            if not matches:
                continue
            exact_matches = [
                row
                for row in matches
                if row["step_index"] == local_row["step_index"] and row["step_id"] == local_row["step_id"]
            ]
            identity = f"{local_row['run_id']}:{local_row['step_index']}:{local_row['step_id']}"
            if len(matches) > 1 or len(exact_matches) != 1:
                conflicts.append(identity)
                continue
            shared_row = exact_matches[0]
            if any(shared_row[column] != local_row[column] for column in run_step_payload_columns):
                conflicts.append(identity)
        return conflicts

    def _find_run_substep_conflicts(
        shared_conn: sqlite3.Connection,
        legacy_conn: sqlite3.Connection,
    ) -> list[str]:
        if not (_table_exists(legacy_conn, "run_steps") and _table_exists(legacy_conn, "run_substeps")):
            return []
        local_rows = legacy_conn.execute(
            """
            SELECT sub.run_id, sub.substep_index, sub.substep_id, sub.type, sub.source, sub.call_id,
                   sub.payload_json, sub.timestamp, sub.legacy_turn_id, sub.legacy_event_id,
                   step.run_id AS parent_run_id, step.step_index AS parent_step_index, step.step_id AS parent_step_id
            FROM run_substeps sub
            JOIN run_steps step ON sub.step_id = step.id
            ORDER BY sub.id
            """
        ).fetchall()
        conflicts: list[str] = []
        for local_row in local_rows:
            shared_step = shared_conn.execute(
                """
                SELECT id
                FROM run_steps
                WHERE project_id = ? AND run_id = ? AND step_index = ? AND step_id = ?
                """,
                (
                    project_id,
                    local_row["parent_run_id"],
                    local_row["parent_step_index"],
                    local_row["parent_step_id"],
                ),
            ).fetchone()
            if shared_step is None:
                continue
            shared_step_id = int(shared_step["id"])
            matches = shared_conn.execute(
                """
                SELECT run_id, substep_index, substep_id, type, source, call_id, payload_json,
                       timestamp, legacy_turn_id, legacy_event_id
                FROM run_substeps
                WHERE project_id = ? AND step_id = ? AND (substep_index = ? OR substep_id = ?)
                """,
                (
                    project_id,
                    shared_step_id,
                    local_row["substep_index"],
                    local_row["substep_id"],
                ),
            ).fetchall()
            if not matches:
                continue
            exact_matches = [
                row
                for row in matches
                if row["substep_index"] == local_row["substep_index"]
                and row["substep_id"] == local_row["substep_id"]
            ]
            identity = (
                f"{local_row['parent_run_id']}:{local_row['parent_step_index']}:"
                f"{local_row['substep_index']}:{local_row['substep_id']}"
            )
            if len(matches) > 1 or len(exact_matches) != 1:
                conflicts.append(identity)
                continue
            shared_row = exact_matches[0]
            if any(shared_row[column] != local_row[column] for column in run_substep_payload_columns):
                conflicts.append(identity)
        return conflicts

    def _legacy_task_projection_sql(*, source_prefix: str = "") -> str:
        legacy_task_columns = _table_columns(local_conn, "tasks")
        prefix = f"{source_prefix}." if source_prefix else ""
        select_exprs: list[str] = []
        for column in task_import_columns:
            quoted_column = f'"{column}"' if column == "group" else column
            quoted_alias = f'"{column}"' if column == "group" else column
            if column in legacy_task_columns:
                select_exprs.append(f"{prefix}{quoted_column} AS {quoted_alias}")
                continue
            fallback = legacy_task_fallbacks.get(column)
            if fallback is not None:
                select_exprs.append(f"{fallback} AS {quoted_alias}")
                continue
            raise ValueError(
                "Legacy local DB is missing required tasks column "
                f"{column}; run 'uv run gza migrate' on the legacy project first."
            )
        return ", ".join(select_exprs)

    local_conn = sqlite3.connect(f"file:{local_db}?mode=ro", uri=True)
    local_conn.row_factory = sqlite3.Row
    try:
        task_source_select_sql = _legacy_task_projection_sql()
        task_source_select_sql_qualified = _legacy_task_projection_sql(source_prefix="legacy_local.tasks")
        local_task_rows = local_conn.execute(
            f"SELECT {task_source_select_sql} FROM tasks ORDER BY id"
        ).fetchall()

        if dry_run:
            existing_count = 0
            if shared_db.exists():
                try:
                    shared_ro = sqlite3.connect(f"file:{shared_db}?mode=ro", uri=True)
                    shared_ro.row_factory = sqlite3.Row
                    try:
                        task_conflicts = _find_task_conflicts(shared_ro, local_task_rows)
                        if task_conflicts:
                            sample = ", ".join(task_conflicts[:5])
                            raise ValueError(
                                "Conflicting task IDs already exist in shared DB for this project_id: "
                                f"{sample}. Resolve conflicts before importing."
                            )
                        run_step_conflicts = _find_run_step_conflicts(shared_ro, local_conn)
                        if run_step_conflicts:
                            sample = ", ".join(run_step_conflicts[:5])
                            raise ValueError(
                                "Conflicting run_steps rows already exist in shared DB for this project_id: "
                                f"{sample}. Resolve conflicts before importing."
                            )
                        run_substep_conflicts = _find_run_substep_conflicts(shared_ro, local_conn)
                        if run_substep_conflicts:
                            sample = ", ".join(run_substep_conflicts[:5])
                            raise ValueError(
                                "Conflicting run_substeps rows already exist in shared DB for this project_id: "
                                f"{sample}. Resolve conflicts before importing."
                            )
                        existing_count = int(
                            shared_ro.execute(
                                "SELECT COUNT(*) FROM tasks WHERE project_id = ?",
                                (project_id,),
                            ).fetchone()[0]
                        )
                    finally:
                        shared_ro.close()
                except sqlite3.DatabaseError as exc:
                    raise ValueError(
                        f"Shared DB at {shared_db} is not initialized or readable: {exc}. "
                        "Run 'uv run gza migrate' first."
                    ) from exc
            return {
                "status": "dry_run",
                "local_db_path": str(local_db),
                "shared_db_path": str(shared_db),
                "project_id": project_id,
                "local_task_count": len(local_task_rows),
                "shared_existing_task_count": existing_count,
            }

        store = SqliteTaskStore.from_config(config, allow_legacy_local_db=True)
        with store._connect() as shared_conn:
            shared_conn.execute("ATTACH DATABASE ? AS legacy_local", (str(local_db),))
            task_conflicts = _find_task_conflicts(shared_conn, local_task_rows)
            if task_conflicts:
                sample = ", ".join(task_conflicts[:5])
                raise ValueError(
                    "Conflicting task IDs already exist in shared DB for this project_id: "
                    f"{sample}. Resolve conflicts before importing."
                )
            run_step_conflicts = _find_run_step_conflicts(shared_conn, local_conn)
            if run_step_conflicts:
                sample = ", ".join(run_step_conflicts[:5])
                raise ValueError(
                    "Conflicting run_steps rows already exist in shared DB for this project_id: "
                    f"{sample}. Resolve conflicts before importing."
                )
            run_substep_conflicts = _find_run_substep_conflicts(shared_conn, local_conn)
            if run_substep_conflicts:
                sample = ", ".join(run_substep_conflicts[:5])
                raise ValueError(
                    "Conflicting run_substeps rows already exist in shared DB for this project_id: "
                    f"{sample}. Resolve conflicts before importing."
                )

            before_count = shared_conn.execute(
                "SELECT COUNT(*) FROM tasks WHERE project_id = ?",
                (store._project_id,),
            ).fetchone()[0]
            max_imported_suffix = 0
            prefix_token = f"{project_prefix}-"
            for row in local_task_rows:
                task_id = str(row["id"])
                if not task_id.startswith(prefix_token):
                    continue
                suffix = task_id[len(prefix_token):]
                if not suffix.isdigit():
                    continue
                max_imported_suffix = max(max_imported_suffix, int(suffix))

            shared_conn.execute("BEGIN")
            try:
                shared_conn.execute(
                    f"""
                    INSERT OR IGNORE INTO tasks (
                        project_id, {task_import_columns_sql}
                    )
                    SELECT
                        ?, {task_source_select_sql_qualified}
                    FROM legacy_local.tasks
                    """,
                    (store._project_id,),
                )

                if _table_exists(local_conn, "task_tags"):
                    shared_conn.execute(
                        """
                        INSERT OR IGNORE INTO task_tags(project_id, task_id, tag)
                        SELECT ?, task_id, tag FROM legacy_local.task_tags
                        """,
                        (store._project_id,),
                    )
                if _table_exists(local_conn, "run_steps"):
                    run_step_rows = shared_conn.execute(
                        """
                        SELECT id, run_id, step_index, step_id, provider, message_role, message_text,
                               started_at, completed_at, outcome, summary, legacy_turn_id, legacy_event_id
                        FROM legacy_local.run_steps
                        ORDER BY id
                        """
                    ).fetchall()
                    run_step_id_map: dict[int, int] = {}
                    for row in run_step_rows:
                        old_step_id = int(row["id"])
                        insert_cur = shared_conn.execute(
                            """
                            INSERT OR IGNORE INTO run_steps(
                                project_id, run_id, step_index, step_id, provider, message_role, message_text,
                                started_at, completed_at, outcome, summary, legacy_turn_id, legacy_event_id
                            )
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            """,
                            (
                                store._project_id,
                                row["run_id"],
                                row["step_index"],
                                row["step_id"],
                                row["provider"],
                                row["message_role"],
                                row["message_text"],
                                row["started_at"],
                                row["completed_at"],
                                row["outcome"],
                                row["summary"],
                                row["legacy_turn_id"],
                                row["legacy_event_id"],
                            ),
                        )
                        if insert_cur.rowcount > 0 and insert_cur.lastrowid is not None:
                            run_step_id_map[old_step_id] = int(insert_cur.lastrowid)
                            continue

                        existing = shared_conn.execute(
                            """
                            SELECT id
                            FROM run_steps
                            WHERE project_id = ? AND run_id = ? AND step_index = ? AND step_id = ?
                            """,
                            (store._project_id, row["run_id"], row["step_index"], row["step_id"]),
                        ).fetchone()
                        if existing is None:
                            raise ValueError(
                                "Failed to resolve imported run_steps row for "
                                f"run_id={row['run_id']} step_id={row['step_id']}"
                            )
                        run_step_id_map[old_step_id] = int(existing["id"])
                else:
                    run_step_id_map = {}
                if _table_exists(local_conn, "run_substeps"):
                    run_substep_rows = shared_conn.execute(
                        """
                        SELECT id, run_id, step_id, substep_index, substep_id, type, source, call_id,
                               payload_json, timestamp, legacy_turn_id, legacy_event_id
                        FROM legacy_local.run_substeps
                        ORDER BY id
                        """
                    ).fetchall()
                    for row in run_substep_rows:
                        legacy_step_id = int(row["step_id"])
                        mapped_step_id = run_step_id_map.get(legacy_step_id)
                        if mapped_step_id is None:
                            raise ValueError(
                                "Legacy run_substeps row references unknown run_steps.id "
                                f"{legacy_step_id} in local DB import."
                            )
                        step_row = shared_conn.execute(
                            "SELECT project_id, run_id FROM run_steps WHERE id = ?",
                            (mapped_step_id,),
                        ).fetchone()
                        if step_row is None or str(step_row["project_id"]) != store._project_id:
                            raise ValueError(
                                "Imported run_substeps row could not be linked to a project-scoped run_steps row "
                                f"(legacy step_id={legacy_step_id}, mapped step_id={mapped_step_id})."
                            )
                        if str(step_row["run_id"]) != str(row["run_id"]):
                            raise ValueError(
                                "Imported run_substeps row run_id mismatch after step remap: "
                                f"substep run_id={row['run_id']} step run_id={step_row['run_id']}"
                            )
                        shared_conn.execute(
                            """
                            INSERT OR IGNORE INTO run_substeps(
                                project_id, run_id, step_id, substep_index, substep_id, type, source, call_id,
                                payload_json, timestamp, legacy_turn_id, legacy_event_id
                            )
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            """,
                            (
                                store._project_id,
                                row["run_id"],
                                mapped_step_id,
                                row["substep_index"],
                                row["substep_id"],
                                row["type"],
                                row["source"],
                                row["call_id"],
                                row["payload_json"],
                                row["timestamp"],
                                row["legacy_turn_id"],
                                row["legacy_event_id"],
                            ),
                        )
                if _table_exists(local_conn, "task_comments"):
                    shared_conn.execute(
                        """
                        INSERT INTO task_comments(project_id, task_id, content, source, author, created_at, resolved_at)
                        SELECT ?, c.task_id, c.content, c.source, c.author, c.created_at, c.resolved_at
                        FROM legacy_local.task_comments c
                        WHERE NOT EXISTS (
                            SELECT 1
                            FROM task_comments d
                            WHERE d.project_id = ?
                              AND d.task_id = c.task_id
                              AND d.content = c.content
                              AND d.source = c.source
                              AND COALESCE(d.author, '') = COALESCE(c.author, '')
                              AND d.created_at = c.created_at
                        )
                        """,
                        (store._project_id, store._project_id),
                    )
                if max_imported_suffix > 0:
                    shared_conn.execute(
                        """
                        INSERT INTO project_sequences(project_id, prefix, next_seq)
                        VALUES (?, ?, ?)
                        ON CONFLICT(project_id) DO UPDATE SET
                            prefix = excluded.prefix,
                            next_seq = MAX(project_sequences.next_seq, excluded.next_seq)
                        """,
                        (store._project_id, store._prefix, max_imported_suffix),
                    )

                shared_conn.execute("COMMIT")
            except Exception:
                shared_conn.execute("ROLLBACK")
                raise
            finally:
                shared_conn.execute("DETACH DATABASE legacy_local")

            after_count = shared_conn.execute(
                "SELECT COUNT(*) FROM tasks WHERE project_id = ?",
                (store._project_id,),
            ).fetchone()[0]

        _write_shared_import_marker(config.project_dir, local_db, shared_db)
    finally:
        local_conn.close()

    return {
        "status": "imported",
        "local_db_path": str(local_db),
        "shared_db_path": str(shared_db),
        "project_id": store._project_id,
        "tasks_imported": max(0, int(after_count) - int(before_count)),
    }


# === Module-level convenience functions ===

def _default_store() -> "SqliteTaskStore":
    """Create a SqliteTaskStore using config-derived db_path."""
    return SqliteTaskStore.default()


def _task_to_dict(task: "Task") -> dict:
    """Convert a Task to a JSON-serializable dict."""
    return {
        "id": task.id,
        "prompt": task.prompt,
        "status": task.status,
        "task_type": task.task_type,
        "task_id": task.slug,
        "branch": task.branch,
        "log_file": task.log_file,
        "report_file": task.report_file,
        "based_on": task.based_on,
        "has_commits": task.has_commits,
        "duration_seconds": task.duration_seconds,
        "num_steps_reported": task.num_steps_reported,
        "num_steps_computed": task.num_steps_computed,
        "num_turns_reported": task.num_turns_reported,
        "num_turns_computed": task.num_turns_computed,
        "attach_count": task.attach_count,
        "attach_duration_seconds": task.attach_duration_seconds,
        "cost_usd": task.cost_usd,
        "input_tokens": task.input_tokens,
        "output_tokens": task.output_tokens,
        "created_at": task.created_at.isoformat() if task.created_at else None,
        "started_at": task.started_at.isoformat() if task.started_at else None,
        "completed_at": task.completed_at.isoformat() if task.completed_at else None,
        "group": task.group,
        "tags": list(task.tags),
        "depends_on": task.depends_on,
        "spec": task.spec,
        "create_review": task.create_review,
        "create_pr": task.create_pr,
        "same_branch": task.same_branch,
        "task_type_hint": task.task_type_hint,
        "output_content": task.output_content,
        "session_id": task.session_id,
        "pr_number": task.pr_number,
        "pr_state": task.pr_state,
        "pr_last_synced_at": task.pr_last_synced_at.isoformat() if task.pr_last_synced_at else None,
        "model": task.model,
        "provider": task.provider,
        "provider_is_explicit": task.provider_is_explicit,
        "queue_position": task.queue_position,
        "merge_status": task.merge_status,
        "failure_reason": task.failure_reason,
        "skip_learnings": task.skip_learnings,
        "diff_files_changed": task.diff_files_changed,
        "diff_lines_added": task.diff_lines_added,
        "diff_lines_removed": task.diff_lines_removed,
        "review_cleared_at": task.review_cleared_at.isoformat() if task.review_cleared_at else None,
        "review_score": task.review_score,
        "log_schema_version": task.log_schema_version,
        "execution_mode": task.execution_mode,
    }


def get_task(task_id: str) -> dict:
    """Get a task by ID as a JSON-serializable dict.

    Uses config-derived DB resolution for the active project (shared or local).

    Raises:
        KeyError: If task_id is not found.
    """
    store = _default_store()
    task = store.get(task_id)
    if task is None:
        raise KeyError(f"Task {task_id} not found")
    return _task_to_dict(task)


def get_task_log_path(task_id: str) -> str | None:
    """Get the log_file path for a task.

    Uses config-derived DB resolution for the active project.
    Returns None if task not found or log_file is not set.
    """
    store = _default_store()
    task = store.get(task_id)
    if task is None:
        return None
    return task.log_file


def get_task_report_path(task_id: str) -> str | None:
    """Get the report_file path for a task.

    Uses config-derived DB resolution for the active project.
    Returns None if task not found or report_file is not set.
    """
    store = _default_store()
    task = store.get(task_id)
    if task is None:
        return None
    return task.report_file


def get_baseline_stats(limit: int = 20) -> dict:
    """Get average stats from the last N completed tasks.

    Uses config-derived DB resolution for the active project.

    Returns:
        Dict with keys: avg_steps, avg_turns, avg_duration, avg_cost
    """
    store = _default_store()
    with store._connect() as conn:
        cur = conn.execute(
            """
            SELECT
                round(avg(num_steps_reported), 1) as avg_steps,
                round(avg(num_turns_reported), 1) as avg_turns,
                round(avg(duration_seconds), 1) as avg_duration,
                round(avg(cost_usd), 4) as avg_cost
            FROM (
                SELECT * FROM tasks
                WHERE project_id = ? AND status = 'completed'
                ORDER BY completed_at DESC
                LIMIT ?
            )
            """,
            (store._project_id, limit),
        )
        row = cur.fetchone()
        return {
            "avg_steps": row["avg_steps"],
            "avg_turns": row["avg_turns"],
            "avg_duration": row["avg_duration"],
            "avg_cost": row["avg_cost"],
        }


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


# === Manual migration v25: INTEGER PK → TEXT base36 IDs ===

def check_migration_status(db_path: Path) -> dict:
    """Return the current schema version and any pending migrations.

    Returns:
        dict with keys:
            current_version: int — current schema version in the DB
            target_version: int — SCHEMA_VERSION constant
            pending_auto: list[int] — auto migration versions not yet applied
            pending_manual: list[int] — manual migration versions not yet applied
    """
    if not db_path.exists():
        return {
            "current_version": 0,
            "target_version": SCHEMA_VERSION,
            "pending_auto": [],
            "pending_manual": [],
        }
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        cur = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='schema_version'"
        )
        if cur.fetchone() is None:
            current = 0
        else:
            cur = conn.execute("SELECT version FROM schema_version LIMIT 1")
            row = cur.fetchone()
            current = row["version"] if row else 0
    finally:
        conn.close()

    pending_auto = [v for v, sql in _MIGRATIONS if v > current and v not in _MANUAL_MIGRATION_VERSIONS]
    pending_manual = [v for v, sql in _MIGRATIONS if v > current and v in _MANUAL_MIGRATION_VERSIONS]
    return {
        "current_version": current,
        "target_version": SCHEMA_VERSION,
        "pending_auto": pending_auto,
        "pending_manual": pending_manual,
    }


def run_v25_migration(db_path: Path, prefix: str) -> None:
    """Migrate database from v24 (INTEGER PKs) to v25 (TEXT base36 PKs).

    Steps:
    1. Create a backup of the DB file at <db_path>.backup.pre-v25.db
    2. Recreate all affected tables with TEXT PKs
    3. Convert old integer IDs to ``{prefix}-{base36(id)}``
    4. Rename the ``task_id`` column to ``slug``
    5. Populate ``project_sequences`` from the highest integer ID seen
    6. Update schema_version to 25 (v26 may run afterwards)

    This is idempotent if called on an already-v25 database.

    Raises:
        RuntimeError: if the DB is not at version 24 (or 0 for fresh empty DB).
    """
    import shutil

    # isolation_level=None puts the connection in autocommit mode so that the
    # backup check below (lines ~2668-2671) runs *outside* any transaction.
    # We then open an explicit BEGIN/COMMIT/ROLLBACK block ourselves to wrap the
    # destructive schema changes atomically.  This is intentional: autocommit +
    # manual BEGIN gives finer control over when the transaction starts than
    # Python's default deferred-transaction mode.
    conn = sqlite3.connect(db_path, isolation_level=None)
    conn.row_factory = sqlite3.Row
    try:
        cur = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='schema_version'"
        )
        if cur.fetchone() is None:
            current = 0
        else:
            cur = conn.execute("SELECT version FROM schema_version LIMIT 1")
            row = cur.fetchone()
            current = row["version"] if row else 0

        target_version = 25
        if current == target_version:
            return  # Already up-to-date
        if current > target_version:
            raise RuntimeError(f"DB is at v{current}, newer than v{target_version}")
        if current < 24 and current != 0:
            raise RuntimeError(
                f"DB is at v{current}. Auto-migrate to v24 first by opening the store."
            )

        # --- backup ---
        backup_path = db_path.with_suffix(".backup.pre-v25.db")
        if not backup_path.exists():
            shutil.copy2(db_path, backup_path)
            logger.info("v25 migration: backup written to %s", backup_path)

        def _id(old_id: int | str | None) -> str | None:
            if old_id is None:
                return None
            if isinstance(old_id, str):
                return old_id  # Already a prefixed string ID (idempotent)
            return f"{prefix}-{_encode_v25_base36(old_id)}"

        conn.execute("BEGIN")

        # --- project_sequences ---
        conn.execute("""
            CREATE TABLE IF NOT EXISTS project_sequences (
                prefix TEXT PRIMARY KEY,
                next_seq INTEGER NOT NULL DEFAULT 1
            )
        """)

        # Find max old integer id (if any tasks exist)
        try:
            cur = conn.execute("SELECT MAX(id) AS max_id FROM tasks")
            max_id_row = cur.fetchone()
            max_int_id = max_id_row["max_id"] if max_id_row and max_id_row["max_id"] else 0
        except sqlite3.OperationalError:
            max_int_id = 0

        if max_int_id and isinstance(max_int_id, int):
            # Use max_int_id (not max_int_id + 1) so that _next_id's increment
            # produces max_int_id + 1 as the first post-migration task ID (no gap).
            conn.execute(
                "INSERT INTO project_sequences (prefix, next_seq) VALUES (?, ?) "
                "ON CONFLICT(prefix) DO UPDATE SET next_seq = MAX(next_seq, ?)",
                (prefix, max_int_id, max_int_id),
            )

        # --- tasks table migration ---
        # Check if 'task_id' column exists (v24) or 'slug' (already migrated partially)
        cur = conn.execute("PRAGMA table_info(tasks)")
        col_names = {row["name"] for row in cur.fetchall()}
        slug_col = "slug" if "slug" in col_names else "task_id"

        conn.execute("""
            CREATE TABLE tasks_v25 (
                id TEXT PRIMARY KEY,
                prompt TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending',
                task_type TEXT NOT NULL DEFAULT 'implement',
                slug TEXT,
                branch TEXT,
                log_file TEXT,
                report_file TEXT,
                based_on TEXT REFERENCES tasks_v25(id),
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
                depends_on TEXT REFERENCES tasks_v25(id),
                spec TEXT,
                create_review INTEGER DEFAULT 0,
                create_pr INTEGER DEFAULT 0,
                same_branch INTEGER DEFAULT 0,
                task_type_hint TEXT,
                output_content TEXT,
                session_id TEXT,
                pr_number INTEGER,
                model TEXT,
                provider TEXT,
                provider_is_explicit INTEGER DEFAULT 0,
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
                log_schema_version INTEGER DEFAULT 1,
                cycle_id INTEGER,
                cycle_iteration_index INTEGER,
                cycle_role TEXT
            )
        """)

        cur = conn.execute("SELECT * FROM tasks")  # noqa: S608
        rows = cur.fetchall()
        for row in rows:
            old_id = row["id"]
            new_id = _id(old_id)
            based_on_new = _id(row["based_on"])
            depends_on_new = _id(row["depends_on"])
            slug_val = row[slug_col]
            conn.execute(
                """
                INSERT INTO tasks_v25 (
                    id, prompt, status, task_type, slug, branch, log_file, report_file,
                    based_on, has_commits, duration_seconds, num_steps_reported,
                    num_steps_computed, num_turns, num_turns_reported, num_turns_computed,
                    attach_count, attach_duration_seconds, cost_usd,
                    created_at, started_at, running_pid, completed_at,
                    "group", depends_on, spec, create_review, create_pr, same_branch, task_type_hint,
                    output_content, session_id, pr_number, model, provider, provider_is_explicit,
                    input_tokens, output_tokens, merge_status, merged_at, failure_reason,
                    skip_learnings, diff_files_changed, diff_lines_added, diff_lines_removed,
                    review_cleared_at, log_schema_version, cycle_id, cycle_iteration_index,
                    cycle_role
                ) VALUES (
                    ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?
                )
                """,
                (
                    new_id, row["prompt"], row["status"], row["task_type"], slug_val,
                    row["branch"], row["log_file"], row["report_file"],
                    based_on_new, row["has_commits"], row["duration_seconds"],
                    row["num_steps_reported"] if "num_steps_reported" in row.keys() else None,
                    row["num_steps_computed"] if "num_steps_computed" in row.keys() else None,
                    row["num_turns"] if "num_turns" in row.keys() else None,
                    row["num_turns_reported"] if "num_turns_reported" in row.keys() else None,
                    row["num_turns_computed"] if "num_turns_computed" in row.keys() else None,
                    row["attach_count"] if "attach_count" in row.keys() else None,
                    row["attach_duration_seconds"] if "attach_duration_seconds" in row.keys() else None,
                    row["cost_usd"], row["created_at"], row["started_at"],
                    row["running_pid"] if "running_pid" in row.keys() else None,
                    row["completed_at"],
                    row["group"],
                    depends_on_new,
                    row["spec"],
                    row["create_review"] if "create_review" in row.keys() else 0,
                    row["create_pr"] if "create_pr" in row.keys() else 0,
                    row["same_branch"] if "same_branch" in row.keys() else 0,
                    row["task_type_hint"] if "task_type_hint" in row.keys() else None,
                    row["output_content"] if "output_content" in row.keys() else None,
                    row["session_id"] if "session_id" in row.keys() else None,
                    row["pr_number"] if "pr_number" in row.keys() else None,
                    row["model"] if "model" in row.keys() else None,
                    row["provider"] if "provider" in row.keys() else None,
                    row["provider_is_explicit"] if "provider_is_explicit" in row.keys() else 0,
                    row["input_tokens"] if "input_tokens" in row.keys() else None,
                    row["output_tokens"] if "output_tokens" in row.keys() else None,
                    row["merge_status"] if "merge_status" in row.keys() else None,
                    row["merged_at"] if "merged_at" in row.keys() else None,
                    row["failure_reason"] if "failure_reason" in row.keys() else None,
                    row["skip_learnings"] if "skip_learnings" in row.keys() else 0,
                    row["diff_files_changed"] if "diff_files_changed" in row.keys() else None,
                    row["diff_lines_added"] if "diff_lines_added" in row.keys() else None,
                    row["diff_lines_removed"] if "diff_lines_removed" in row.keys() else None,
                    row["review_cleared_at"] if "review_cleared_at" in row.keys() else None,
                    row["log_schema_version"] if "log_schema_version" in row.keys() else 1,
                    row["cycle_id"] if "cycle_id" in row.keys() else None,
                    row["cycle_iteration_index"] if "cycle_iteration_index" in row.keys() else None,
                    row["cycle_role"] if "cycle_role" in row.keys() else None,
                ),
            )

        # --- task_cycles table migration ---
        conn.execute("""
            CREATE TABLE task_cycles_v25 (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                implementation_task_id TEXT NOT NULL REFERENCES tasks_v25(id),
                status TEXT NOT NULL DEFAULT 'active',
                max_iterations INTEGER NOT NULL DEFAULT 3,
                started_at TEXT NOT NULL,
                ended_at TEXT,
                stop_reason TEXT
            )
        """)
        try:
            cur = conn.execute("SELECT * FROM task_cycles")
            for row in cur.fetchall():
                conn.execute(
                    "INSERT INTO task_cycles_v25 (id, implementation_task_id, status, max_iterations, started_at, ended_at, stop_reason) VALUES (?,?,?,?,?,?,?)",
                    (row["id"], _id(row["implementation_task_id"]), row["status"],
                     row["max_iterations"], row["started_at"], row["ended_at"], row["stop_reason"]),
                )
        except sqlite3.OperationalError:
            logger.debug("v25 migration: task_cycles table did not exist, skipping")

        # --- task_cycle_iterations table migration ---
        conn.execute("""
            CREATE TABLE task_cycle_iterations_v25 (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                cycle_id INTEGER NOT NULL REFERENCES task_cycles_v25(id),
                iteration_index INTEGER NOT NULL,
                review_task_id TEXT REFERENCES tasks_v25(id),
                review_verdict TEXT,
                improve_task_id TEXT REFERENCES tasks_v25(id),
                state TEXT NOT NULL DEFAULT 'review_created',
                started_at TEXT NOT NULL,
                ended_at TEXT
            )
        """)
        try:
            cur = conn.execute("SELECT * FROM task_cycle_iterations")
            for row in cur.fetchall():
                conn.execute(
                    "INSERT INTO task_cycle_iterations_v25 (id, cycle_id, iteration_index, review_task_id, review_verdict, improve_task_id, state, started_at, ended_at) VALUES (?,?,?,?,?,?,?,?,?)",
                    (row["id"], row["cycle_id"], row["iteration_index"],
                     _id(row["review_task_id"]), row["review_verdict"],
                     _id(row["improve_task_id"]), row["state"], row["started_at"], row["ended_at"]),
                )
        except sqlite3.OperationalError:
            logger.debug("v25 migration: task_cycle_iterations table did not exist, skipping")

        # --- run_steps table migration ---
        conn.execute("""
            CREATE TABLE run_steps_v25 (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id TEXT NOT NULL REFERENCES tasks_v25(id),
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
                UNIQUE(run_id, step_id)
            )
        """)
        try:
            cur = conn.execute("SELECT * FROM run_steps")
            for row in cur.fetchall():
                conn.execute(
                    "INSERT INTO run_steps_v25 (id, run_id, step_index, step_id, provider, message_role, message_text, started_at, completed_at, outcome, summary, legacy_turn_id, legacy_event_id) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (row["id"], _id(row["run_id"]), row["step_index"], row["step_id"],
                     row["provider"], row["message_role"], row["message_text"],
                     row["started_at"], row["completed_at"], row["outcome"],
                     row["summary"], row["legacy_turn_id"], row["legacy_event_id"]),
                )
        except sqlite3.OperationalError:
            logger.debug("v25 migration: run_steps table did not exist, skipping")

        # --- run_substeps table migration ---
        conn.execute("""
            CREATE TABLE run_substeps_v25 (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id TEXT NOT NULL REFERENCES tasks_v25(id),
                step_id INTEGER NOT NULL REFERENCES run_steps_v25(id),
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
                UNIQUE(step_id, substep_id)
            )
        """)
        try:
            cur = conn.execute("SELECT * FROM run_substeps")
            for row in cur.fetchall():
                conn.execute(
                    "INSERT INTO run_substeps_v25 (id, run_id, step_id, substep_index, substep_id, type, source, call_id, payload_json, timestamp, legacy_turn_id, legacy_event_id) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                    (row["id"], _id(row["run_id"]), row["step_id"], row["substep_index"],
                     row["substep_id"], row["type"], row["source"], row["call_id"],
                     row["payload_json"], row["timestamp"], row["legacy_turn_id"],
                     row["legacy_event_id"]),
                )
        except sqlite3.OperationalError:
            logger.debug("v25 migration: run_substeps table did not exist, skipping")

        # --- drop old tables, rename new ones ---
        for tbl in ("run_substeps", "run_steps", "task_cycle_iterations", "task_cycles", "tasks"):
            conn.execute(f"DROP TABLE IF EXISTS {tbl}")
        for tbl in ("tasks", "task_cycles", "task_cycle_iterations", "run_steps", "run_substeps"):
            try:
                conn.execute(f"ALTER TABLE {tbl}_v25 RENAME TO {tbl}")
            except sqlite3.OperationalError:
                logger.debug("v25 migration: %s_v25 table did not exist, skipping rename", tbl)

        # --- recreate indexes ---
        for stmt in """
CREATE INDEX IF NOT EXISTS idx_tasks_status ON tasks(status);
CREATE INDEX IF NOT EXISTS idx_tasks_slug ON tasks(slug);
CREATE INDEX IF NOT EXISTS idx_tasks_created_at ON tasks(created_at);
CREATE INDEX IF NOT EXISTS idx_tasks_group ON tasks("group");
CREATE INDEX IF NOT EXISTS idx_tasks_depends_on ON tasks(depends_on);
CREATE INDEX IF NOT EXISTS idx_tasks_merge_status ON tasks(merge_status);
CREATE INDEX IF NOT EXISTS idx_run_steps_run_id ON run_steps(run_id);
CREATE INDEX IF NOT EXISTS idx_run_steps_step_index ON run_steps(run_id, step_index);
CREATE INDEX IF NOT EXISTS idx_run_substeps_run_id ON run_substeps(run_id);
CREATE INDEX IF NOT EXISTS idx_run_substeps_step_id ON run_substeps(step_id);
CREATE INDEX IF NOT EXISTS idx_task_cycles_impl_id ON task_cycles(implementation_task_id);
CREATE INDEX IF NOT EXISTS idx_task_cycles_status ON task_cycles(status);
CREATE INDEX IF NOT EXISTS idx_task_cycle_iterations_cycle_idx ON task_cycle_iterations(cycle_id, iteration_index);
CREATE UNIQUE INDEX IF NOT EXISTS uq_task_cycle_iterations_cycle_iter ON task_cycle_iterations(cycle_id, iteration_index);
CREATE INDEX IF NOT EXISTS idx_tasks_cycle_id ON tasks(cycle_id);
CREATE INDEX IF NOT EXISTS idx_tasks_type_based_on ON tasks(task_type, based_on);
""".strip().split(";"):
            stmt = stmt.strip()
            if stmt:
                try:
                    conn.execute(stmt)
                except sqlite3.OperationalError:
                    logger.debug("v25 migration: could not create index (table may not exist): %s", stmt[:60])

        # --- update schema version ---
        conn.execute("UPDATE schema_version SET version = ?", (target_version,))

        conn.execute("COMMIT")
        logger.info("v25 migration complete: %s", db_path)
    except Exception:
        conn.execute("ROLLBACK")
        raise
    finally:
        conn.close()


def run_v26_migration(db_path: Path) -> None:
    """Migrate database from v25 (base36 text IDs) to v26 (decimal text IDs).

    This migration is a pure ID-string rewrite across all task-ID columns.
    It is idempotent on v26 databases.
    """
    import shutil

    conn = sqlite3.connect(db_path, isolation_level=None)
    conn.row_factory = sqlite3.Row
    foreign_keys_original = 1
    in_transaction = False
    try:
        cur = conn.execute("PRAGMA foreign_keys")
        row = cur.fetchone()
        foreign_keys_original = int(row[0]) if row is not None else 1

        cur = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='schema_version'"
        )
        if cur.fetchone() is None:
            current = 0
        else:
            cur = conn.execute("SELECT version FROM schema_version LIMIT 1")
            row = cur.fetchone()
            current = row["version"] if row else 0

        target_version = 26
        if current == target_version:
            return
        if current != 25:
            raise RuntimeError(
                f"v26 migration requires DB at v25; found v{current}. Run prior migrations first."
            )

        backup_path = db_path.with_suffix(".backup.pre-v26.db")
        if not backup_path.exists():
            shutil.copy2(db_path, backup_path)
            logger.info("v26 migration: backup written to %s", backup_path)

        conn.execute("PRAGMA foreign_keys = OFF")
        conn.execute("BEGIN")
        in_transaction = True

        conn.execute("DROP TABLE IF EXISTS _v26_task_id_map")
        conn.execute("""
            CREATE TEMP TABLE _v26_task_id_map (
                old_id TEXT PRIMARY KEY,
                new_id TEXT NOT NULL UNIQUE
            )
        """)

        cur = conn.execute("SELECT id FROM tasks")
        prefix_to_max: dict[str, int] = {}
        for row in cur.fetchall():
            old_id = row["id"]
            if not isinstance(old_id, str) or "-" not in old_id:
                raise RuntimeError(f"Invalid task ID for v26 migration: {old_id!r}")
            prefix, old_suffix = old_id.rsplit("-", 1)
            decoded = _decode_base36(old_suffix)
            new_id = f"{prefix}-{decoded}"
            conn.execute(
                "INSERT INTO _v26_task_id_map (old_id, new_id) VALUES (?, ?)",
                (old_id, new_id),
            )
            prefix_to_max[prefix] = max(prefix_to_max.get(prefix, 0), decoded)

        # Rewrite all FK/reference columns first, then the tasks PK.
        conn.execute("""
            UPDATE tasks
            SET based_on = (SELECT m.new_id FROM _v26_task_id_map m WHERE m.old_id = tasks.based_on)
            WHERE based_on IN (SELECT old_id FROM _v26_task_id_map)
        """)
        conn.execute("""
            UPDATE tasks
            SET depends_on = (SELECT m.new_id FROM _v26_task_id_map m WHERE m.old_id = tasks.depends_on)
            WHERE depends_on IN (SELECT old_id FROM _v26_task_id_map)
        """)
        conn.execute("""
            UPDATE task_cycles
            SET implementation_task_id = (
                SELECT m.new_id FROM _v26_task_id_map m WHERE m.old_id = task_cycles.implementation_task_id
            )
            WHERE implementation_task_id IN (SELECT old_id FROM _v26_task_id_map)
        """)
        conn.execute("""
            UPDATE task_cycle_iterations
            SET review_task_id = (SELECT m.new_id FROM _v26_task_id_map m WHERE m.old_id = task_cycle_iterations.review_task_id)
            WHERE review_task_id IN (SELECT old_id FROM _v26_task_id_map)
        """)
        conn.execute("""
            UPDATE task_cycle_iterations
            SET improve_task_id = (SELECT m.new_id FROM _v26_task_id_map m WHERE m.old_id = task_cycle_iterations.improve_task_id)
            WHERE improve_task_id IN (SELECT old_id FROM _v26_task_id_map)
        """)
        conn.execute("""
            UPDATE run_steps
            SET run_id = (SELECT m.new_id FROM _v26_task_id_map m WHERE m.old_id = run_steps.run_id)
            WHERE run_id IN (SELECT old_id FROM _v26_task_id_map)
        """)
        conn.execute("""
            UPDATE run_substeps
            SET run_id = (SELECT m.new_id FROM _v26_task_id_map m WHERE m.old_id = run_substeps.run_id)
            WHERE run_id IN (SELECT old_id FROM _v26_task_id_map)
        """)
        conn.execute("""
            UPDATE tasks
            SET id = (SELECT m.new_id FROM _v26_task_id_map m WHERE m.old_id = tasks.id)
            WHERE id IN (SELECT old_id FROM _v26_task_id_map)
        """)

        # Rewrite slug-embedded lineage suffixes for derived review/implement/improve slugs.
        # These segments intentionally encode task-id suffixes in the pattern:
        # "<suffix>-{rev|impl|impr}-..."
        suffix_map: dict[str, str] = {}
        old_id_by_new_id: dict[str, str] = {}
        cur = conn.execute("SELECT old_id, new_id FROM _v26_task_id_map")
        for row in cur.fetchall():
            old_id = str(row["old_id"])
            new_id = str(row["new_id"])
            old_suffix = old_id.rsplit("-", 1)[-1]
            new_suffix = new_id.rsplit("-", 1)[-1]
            suffix_map[old_suffix] = new_suffix
            old_id_by_new_id[new_id] = old_id

        task_links: dict[str, tuple[str | None, str | None]] = {}
        cur = conn.execute("SELECT id, based_on, depends_on FROM tasks")
        for row in cur.fetchall():
            task_links[str(row["id"])] = (row["based_on"], row["depends_on"])

        lineage_suffix_cache: dict[str, set[str]] = {}

        def _collect_lineage_old_suffixes(task_id: str) -> set[str]:
            cached = lineage_suffix_cache.get(task_id)
            if cached is not None:
                return cached

            collected: set[str] = set()
            pending = [task_id]
            visited: set[str] = set()
            while pending:
                current_id = pending.pop()
                if current_id in visited:
                    continue
                visited.add(current_id)

                old_id = old_id_by_new_id.get(current_id)
                if old_id and "-" in old_id:
                    collected.add(old_id.rsplit("-", 1)[-1])

                based_on, depends_on = task_links.get(current_id, (None, None))
                if based_on:
                    pending.append(based_on)
                if depends_on:
                    pending.append(depends_on)

            lineage_suffix_cache[task_id] = collected
            return collected

        expected_marker_by_type = {"implement": "impl", "review": "rev", "improve": "impr"}

        def _rewrite_slug_lineage_suffixes(
            *,
            task_id: str,
            task_type: str,
            slug: str | None,
        ) -> str | None:
            if slug is None:
                return None

            slug_body = slug
            date_prefix = ""
            m = re.match(r"^(\d{8}-)(.+)$", slug)
            if m:
                date_prefix = m.group(1)
                slug_body = m.group(2)

            tokens = slug_body.split("-")
            if len(tokens) < 2:
                return slug

            old_task_id = old_id_by_new_id.get(task_id)
            if not old_task_id or "-" not in old_task_id:
                return slug
            old_self_suffix = old_task_id.rsplit("-", 1)[-1]
            expected_marker = expected_marker_by_type.get(task_type)
            if expected_marker is None:
                return slug
            if tokens[0] != old_self_suffix or tokens[1] != expected_marker:
                return slug

            lineage_old_suffixes = _collect_lineage_old_suffixes(task_id)
            changed = False
            i = 0
            while i + 1 < len(tokens):
                marker = tokens[i + 1]
                if marker not in {"rev", "impl", "impr"}:
                    break
                if tokens[i] not in lineage_old_suffixes:
                    break
                replacement = suffix_map.get(tokens[i])
                if replacement is None:
                    break
                if replacement != tokens[i]:
                    tokens[i] = replacement
                    changed = True
                i += 2

            if not changed:
                return slug
            return f"{date_prefix}{'-'.join(tokens)}"

        cur = conn.execute("SELECT id, task_type, slug FROM tasks WHERE slug IS NOT NULL")
        for row in cur.fetchall():
            new_slug = _rewrite_slug_lineage_suffixes(
                task_id=str(row["id"]),
                task_type=str(row["task_type"]),
                slug=row["slug"],
            )
            if new_slug != row["slug"]:
                conn.execute("UPDATE tasks SET slug = ? WHERE id = ?", (new_slug, row["id"]))

        # Heal project_sequences if it drifted from decoded task IDs.
        # Never decrease next_seq: task IDs must be monotonic and never reused.
        for prefix, max_decoded in prefix_to_max.items():
            conn.execute(
                "INSERT INTO project_sequences (prefix, next_seq) VALUES (?, ?) "
                "ON CONFLICT(prefix) DO UPDATE "
                "SET next_seq = MAX(project_sequences.next_seq, excluded.next_seq)",
                (prefix, max_decoded),
            )

        conn.execute("UPDATE schema_version SET version = ?", (target_version,))
        conn.execute("COMMIT")
        in_transaction = False
        logger.info("v26 migration complete: %s", db_path)
    except Exception:
        if in_transaction:
            conn.execute("ROLLBACK")
        raise
    finally:
        try:
            conn.execute(f"PRAGMA foreign_keys = {foreign_keys_original}")
        except Exception:
            pass
        conn.close()


def run_v27_migration(db_path: Path) -> None:
    """Migrate database from v26 to v27 by dropping TaskCycle bookkeeping."""
    import shutil

    conn = sqlite3.connect(db_path, isolation_level=None)
    conn.row_factory = sqlite3.Row
    foreign_keys_original = 1
    in_transaction = False
    try:
        cur = conn.execute("PRAGMA foreign_keys")
        row = cur.fetchone()
        foreign_keys_original = int(row[0]) if row is not None else 1

        cur = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='schema_version'"
        )
        if cur.fetchone() is None:
            current = 0
        else:
            cur = conn.execute("SELECT version FROM schema_version LIMIT 1")
            row = cur.fetchone()
            current = row["version"] if row else 0

        target_version = 27
        if current == target_version:
            return
        if current != 26:
            raise RuntimeError(
                f"v27 migration requires DB at v26; found v{current}. Run prior migrations first."
            )

        backup_path = db_path.with_suffix(".backup.pre-v27.db")
        if not backup_path.exists():
            shutil.copy2(db_path, backup_path)
            logger.info("v27 migration: backup written to %s", backup_path)

        conn.execute("PRAGMA foreign_keys = OFF")
        conn.execute("BEGIN")
        in_transaction = True

        conn.execute("""
            CREATE TABLE tasks_v27 (
                id TEXT PRIMARY KEY,
                prompt TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending',
                task_type TEXT NOT NULL DEFAULT 'implement',
                slug TEXT,
                branch TEXT,
                log_file TEXT,
                report_file TEXT,
                based_on TEXT REFERENCES tasks(id),
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
                depends_on TEXT REFERENCES tasks(id),
                spec TEXT,
                create_review INTEGER DEFAULT 0,
                create_pr INTEGER DEFAULT 0,
                same_branch INTEGER DEFAULT 0,
                task_type_hint TEXT,
                output_content TEXT,
                session_id TEXT,
                pr_number INTEGER,
                model TEXT,
                provider TEXT,
                provider_is_explicit INTEGER DEFAULT 0,
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
                log_schema_version INTEGER DEFAULT 1
            )
        """)

        legacy_create_pr_expr = "create_pr" if _table_has_column(conn, "tasks", "create_pr") else "0 AS create_pr"

        conn.execute(f"""
            INSERT INTO tasks_v27 (
                id, prompt, status, task_type, slug, branch, log_file, report_file,
                based_on, has_commits, duration_seconds, num_steps_reported,
                num_steps_computed, num_turns, num_turns_reported, num_turns_computed,
                attach_count, attach_duration_seconds,
                cost_usd, created_at, started_at, running_pid, completed_at, "group",
                depends_on, spec, create_review, create_pr, same_branch, task_type_hint,
                output_content, session_id, pr_number, model, provider,
                provider_is_explicit, input_tokens, output_tokens, merge_status,
                merged_at, failure_reason, skip_learnings, diff_files_changed,
                diff_lines_added, diff_lines_removed, review_cleared_at, log_schema_version
            )
            SELECT
                id, prompt, status, task_type, slug, branch, log_file, report_file,
                based_on, has_commits, duration_seconds, num_steps_reported,
                num_steps_computed, num_turns, num_turns_reported, num_turns_computed,
                attach_count, attach_duration_seconds,
                cost_usd, created_at, started_at, running_pid, completed_at, "group",
                depends_on, spec, create_review, {legacy_create_pr_expr}, same_branch, task_type_hint,
                output_content, session_id, pr_number, model, provider,
                provider_is_explicit, input_tokens, output_tokens, merge_status,
                merged_at, failure_reason, skip_learnings, diff_files_changed,
                diff_lines_added, diff_lines_removed, review_cleared_at, log_schema_version
            FROM tasks
        """)

        conn.execute("DROP TABLE IF EXISTS task_cycle_iterations")
        conn.execute("DROP TABLE IF EXISTS task_cycles")
        conn.execute("DROP TABLE tasks")
        conn.execute("ALTER TABLE tasks_v27 RENAME TO tasks")

        for stmt in """
CREATE INDEX IF NOT EXISTS idx_tasks_status ON tasks(status);
CREATE INDEX IF NOT EXISTS idx_tasks_slug ON tasks(slug);
CREATE INDEX IF NOT EXISTS idx_tasks_created_at ON tasks(created_at);
CREATE INDEX IF NOT EXISTS idx_tasks_group ON tasks("group");
CREATE INDEX IF NOT EXISTS idx_tasks_depends_on ON tasks(depends_on);
CREATE INDEX IF NOT EXISTS idx_tasks_merge_status ON tasks(merge_status);
CREATE INDEX IF NOT EXISTS idx_tasks_type_based_on ON tasks(task_type, based_on);
""".strip().split(";"):
            stmt = stmt.strip()
            if stmt:
                conn.execute(stmt)

        conn.execute("UPDATE schema_version SET version = ?", (target_version,))
        conn.execute("COMMIT")
        in_transaction = False
        logger.info("v27 migration complete: %s", db_path)
    except Exception:
        if in_transaction:
            conn.execute("ROLLBACK")
        raise
    finally:
        try:
            conn.execute(f"PRAGMA foreign_keys = {foreign_keys_original}")
        except Exception:
            pass
        conn.close()


def resolve_task_id(arg: str, project_prefix: str) -> str:
    """Resolve a user-supplied task ID argument to a canonical string ID.

    Accepts only full prefixed IDs: ``"{prefix}-{decimal_suffix}"``.
    Prefix matching is intentionally not enforced here; syntactically valid IDs
    with a different prefix are resolved and may fail later as "not found".

    The returned value is the string to pass to ``store.get()``.
    """
    arg = arg.strip()
    if not arg:
        raise InvalidTaskIdError(
            f"Invalid task ID {arg!r}. Use a full prefixed task ID like '{project_prefix}-1234'."
        )
    if not _FULL_TASK_ID_RE.match(arg):
        raise InvalidTaskIdError(
            f"Invalid task ID '{arg}'. Use a full prefixed task ID like '{project_prefix}-1234'."
        )
    return arg


class _MigrationPreview(TypedDict):
    task_count: int
    samples: list[tuple[int, str]]
    random_samples: list[tuple[int, str]]
    first_post_migration_id: str


def preview_v25_migration(
    db_path: Path,
    prefix: str,
    sample_limit: int = 10,
    random_sample_limit: int = 10,
) -> _MigrationPreview:
    """Return a preview of what run_v25_migration would do, without writing anything.

    Returns a TypedDict with keys:
    - ``task_count``: total number of tasks in the DB
    - ``samples``: list of ``(old_id, new_id)`` tuples for the first ``sample_limit`` tasks
    - ``random_samples``: list of ``(old_id, new_id)`` tuples for up to
      ``random_sample_limit`` tasks chosen at random from the tail beyond the
      first ``sample_limit``.  Useful for spot-checking conversions of
      higher-numbered IDs without dumping every row.  Uses SQLite's
      ``ORDER BY RANDOM()`` — non-deterministic across invocations.
    - ``first_post_migration_id``: the first ID that would be assigned to a new task
      after migration (i.e. ``{prefix}-{base36(max_id + 1)}``)

    Note: This function is only meaningful on pre-v25 databases.  On a database that has
    already been migrated to v25, all task IDs are TEXT strings so there are no integer
    IDs to convert — ``samples`` and ``random_samples`` will be empty and
    ``first_post_migration_id`` will be ``""`` to indicate the result is not applicable.
    Use :func:`check_migration_status` to determine whether v25 migration is pending
    before calling this function.
    """
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        # Determine the current schema version so we can short-circuit on already-migrated DBs.
        try:
            cur = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='schema_version'"
            )
            if cur.fetchone() is not None:
                cur = conn.execute("SELECT version FROM schema_version LIMIT 1")
                row = cur.fetchone()
                current_version = row["version"] if row else 0
            else:
                current_version = 0
        except sqlite3.OperationalError:
            current_version = 0

        target_version = 25
        if current_version >= target_version:
            # Already migrated — no integer IDs remain to convert.
            try:
                cur2 = conn.execute("SELECT COUNT(*) AS cnt FROM tasks")
                row2 = cur2.fetchone()
                task_count = row2["cnt"] if row2 else 0
            except sqlite3.OperationalError:
                task_count = 0
            return {
                "task_count": task_count,
                "samples": [],
                "random_samples": [],
                "first_post_migration_id": "",
            }

        def _format_sample(old_id: int) -> tuple[int, str]:
            return (old_id, f"{prefix}-{_encode_v25_base36(old_id)}")

        try:
            cur = conn.execute("SELECT id FROM tasks ORDER BY id ASC LIMIT ?", (sample_limit,))
            first_rows = [row["id"] for row in cur.fetchall() if isinstance(row["id"], int)]
            samples_raw = [_format_sample(old_id) for old_id in first_rows]

            # Random samples drawn from IDs beyond the first ``sample_limit``.
            # SQLite ORDER BY RANDOM() is sufficient for a spot-check — we don't
            # need cryptographic randomness, just a mix of high-numbered IDs so
            # the operator sees conversions at the tail, not just the head.
            random_samples_raw: list[tuple[int, str]] = []
            if first_rows and random_sample_limit > 0:
                cur3 = conn.execute(
                    "SELECT id FROM tasks WHERE id > ? ORDER BY RANDOM() LIMIT ?",
                    (first_rows[-1], random_sample_limit),
                )
                random_ids = sorted(
                    row["id"] for row in cur3.fetchall() if isinstance(row["id"], int)
                )
                random_samples_raw = [_format_sample(old_id) for old_id in random_ids]

            cur2 = conn.execute("SELECT COUNT(*) AS cnt, MAX(id) AS max_id FROM tasks")
            row = cur2.fetchone()
            task_count = row["cnt"] if row else 0
            max_id = row["max_id"] if row and row["max_id"] else 0
        except sqlite3.OperationalError:
            samples_raw = []
            random_samples_raw = []
            task_count = 0
            max_id = 0
    finally:
        conn.close()

    first_post = (
        f"{prefix}-{_encode_v25_base36(max_id + 1)}"
        if max_id
        else f"{prefix}-{_encode_v25_base36(1)}"
    )
    return {
        "task_count": task_count,
        "samples": samples_raw,
        "random_samples": random_samples_raw,
        "first_post_migration_id": first_post,
    }


class _MigrationV26Preview(TypedDict):
    task_count: int
    samples: list[tuple[str, str]]
    random_samples: list[tuple[str, str]]


def preview_v26_migration(
    db_path: Path,
    sample_limit: int = 10,
    random_sample_limit: int = 10,
) -> _MigrationV26Preview:
    """Return a preview of v26 ID rewrites (base36 text IDs -> decimal IDs)."""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        try:
            cur = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='schema_version'"
            )
            if cur.fetchone() is not None:
                cur = conn.execute("SELECT version FROM schema_version LIMIT 1")
                row = cur.fetchone()
                current_version = row["version"] if row else 0
            else:
                current_version = 0
        except sqlite3.OperationalError:
            current_version = 0

        if current_version != 25:
            try:
                cur2 = conn.execute("SELECT COUNT(*) AS cnt FROM tasks")
                row2 = cur2.fetchone()
                task_count = row2["cnt"] if row2 else 0
            except sqlite3.OperationalError:
                task_count = 0
            return {
                "task_count": task_count,
                "samples": [],
                "random_samples": [],
            }

        def _convert(old_id: str) -> tuple[str, str]:
            prefix, suffix = old_id.rsplit("-", 1)
            return (old_id, f"{prefix}-{_decode_base36(suffix)}")

        cur = conn.execute("SELECT id FROM tasks ORDER BY id ASC LIMIT ?", (sample_limit,))
        first_rows = [row["id"] for row in cur.fetchall() if isinstance(row["id"], str)]
        samples = [_convert(old_id) for old_id in first_rows]

        random_samples: list[tuple[str, str]] = []
        if first_rows and random_sample_limit > 0:
            cur2 = conn.execute(
                "SELECT id FROM tasks WHERE id NOT IN (SELECT id FROM tasks ORDER BY id ASC LIMIT ?) "
                "ORDER BY RANDOM() LIMIT ?",
                (sample_limit, random_sample_limit),
            )
            random_ids = sorted(
                row["id"] for row in cur2.fetchall() if isinstance(row["id"], str)
            )
            random_samples = [_convert(old_id) for old_id in random_ids]

        cur3 = conn.execute("SELECT COUNT(*) AS cnt FROM tasks")
        row3 = cur3.fetchone()
        task_count = row3["cnt"] if row3 else 0
        return {
            "task_count": task_count,
            "samples": samples,
            "random_samples": random_samples,
        }
    finally:
        conn.close()
