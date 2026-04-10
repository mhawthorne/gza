"""SQLite-based task storage."""

import json
import logging
import os
import re
import sqlite3
import subprocess
import tempfile
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, TypedDict

logger = logging.getLogger(__name__)

__all__ = [
    "KNOWN_FAILURE_REASONS",
    "ManualMigrationRequired",
    "Task",
    "TaskStats",
    "TaskCycle",
    "TaskCycleIteration",
    "SqliteTaskStore",
    "extract_failure_reason",
    "run_v25_migration",
    "preview_v25_migration",
    "check_migration_status",
    "resolve_task_id",
    "task_id_numeric_key",
]


# Known failure reason categories
KNOWN_FAILURE_REASONS = {"MAX_STEPS", "MAX_TURNS", "TEST_FAILURE", "TIMEOUT", "WORKER_DIED", "KILLED", "UNKNOWN"}

_FAILURE_MARKER_RE = re.compile(r"\[GZA_FAILURE:(\w+)\]")

# Base-36 alphabet (digits 0-9 then lowercase a-z)
_B36_CHARS = "0123456789abcdefghijklmnopqrstuvwxyz"


def _encode_base36(n: int) -> str:
    """Encode a positive integer to a base-36 string."""
    if n == 0:
        return "0"
    result: list[str] = []
    while n > 0:
        result.append(_B36_CHARS[n % 36])
        n //= 36
    return "".join(reversed(result))


def _decode_base36(s: str) -> int:
    """Decode a base-36 string to an integer."""
    return int(s, 36)


def task_id_numeric_key(task_id: str | None) -> int:
    """Return an integer sort key for a task ID that preserves creation order.

    Task IDs are ``{prefix}-{base36_seq}`` (e.g. ``"gza-1a2b"``).  Sorting
    them as plain strings is wrong: ``"gza-10"`` (decimal 36) sorts *before*
    ``"gza-2"`` (decimal 2) because ``"1" < "2"`` lexicographically.  This
    helper decodes the numeric suffix so comparisons are correct.

    Returns 0 for ``None``, empty, or IDs without a hyphen (e.g. legacy bare
    integers stored as strings), and 0 for suffixes that fail base-36 parsing.
    """
    if not task_id or "-" not in task_id:
        return 0
    suffix = task_id.rsplit("-", 1)[-1]
    try:
        return int(suffix, 36)
    except ValueError:
        return 0


class ManualMigrationRequired(Exception):
    """Raised when the DB needs a manual schema migration (e.g. v25).

    Callers should run ``gza migrate`` (or call :func:`run_v25_migration`)
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
    id: str | None  # None for unsaved tasks; project-prefixed base36 (e.g. "gza-1a2b")
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
    cost_usd: float | None = None
    input_tokens: int | None = None   # Total input tokens (including cache tokens)
    output_tokens: int | None = None  # Total output tokens
    created_at: datetime | None = None
    started_at: datetime | None = None
    running_pid: int | None = None
    completed_at: datetime | None = None
    # New fields for task import/chaining
    group: str | None = None  # Group name for related tasks
    depends_on: str | None = None  # Task ID this task depends on (string)
    spec: str | None = None  # Path to spec file for context
    create_review: bool = False  # Auto-create review task on completion
    same_branch: bool = False  # Continue on depends_on task's branch instead of creating new
    task_type_hint: str | None = None  # Explicit branch type hint (e.g., "fix", "feature")
    output_content: str | None = None  # Actual content of report/plan/review (for persistence)
    session_id: str | None = None  # Claude session ID for resume capability
    pr_number: int | None = None  # GitHub PR number
    model: str | None = None  # Per-task model override
    provider: str | None = None  # Per-task provider override
    provider_is_explicit: bool = False  # True when provider was explicitly set by user input
    merge_status: str | None = None  # None, 'unmerged', or 'merged'
    merged_at: datetime | None = None  # When merge_status was set to 'merged'
    failure_reason: str | None = None
    skip_learnings: bool = False
    diff_files_changed: int | None = None  # Files changed vs. main (v13)
    diff_lines_added: int | None = None    # Lines added vs. main (v13)
    diff_lines_removed: int | None = None  # Lines removed vs. main (v13)
    review_cleared_at: datetime | None = None  # When review state was cleared by an improve task (v14)
    log_schema_version: int = 1  # 1=legacy logs, 2=message-step logs
    cycle_id: int | None = None  # Cycle this task belongs to (v18); internal int PK of task_cycles
    cycle_iteration_index: int | None = None  # Iteration index within cycle (v18)
    cycle_role: str | None = None  # 'review' or 'improve' within a cycle (v18)

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


@dataclass
class TaskCycle:
    """A cycle of review/improve iterations for an implementation task."""
    id: int  # Internal integer PK (not exposed to users)
    implementation_task_id: str  # References tasks.id (project-prefixed base36)
    status: str  # active | approved | maxed_out | blocked
    max_iterations: int
    started_at: datetime
    ended_at: datetime | None
    stop_reason: str | None  # approved | max_iterations | needs_discussion | review_failed | improve_failed


@dataclass
class TaskCycleIteration:
    """A single iteration within a cycle (one review + optional improve)."""
    id: int  # Internal integer PK
    cycle_id: int  # References task_cycles.id (internal integer)
    iteration_index: int
    review_task_id: str | None  # References tasks.id (project-prefixed base36)
    review_verdict: str | None
    improve_task_id: str | None  # References tasks.id (project-prefixed base36)
    state: str  # review_created | review_completed | improve_created | improve_completed | terminal
    started_at: datetime
    ended_at: datetime | None


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

# Schema version for migrations
SCHEMA_VERSION = 25

# Migration versions that require manual intervention (gza migrate).
# These are NOT run automatically in _ensure_db.
_MANUAL_MIGRATION_VERSIONS: frozenset[int] = frozenset({25})

SCHEMA = """
CREATE TABLE IF NOT EXISTS schema_version (
    version INTEGER PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS project_sequences (
    prefix TEXT PRIMARY KEY,
    next_seq INTEGER NOT NULL DEFAULT 1
);

CREATE TABLE IF NOT EXISTS tasks (
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
    num_turns INTEGER,  -- kept for backward compat; use num_turns_reported instead
    num_turns_reported INTEGER,
    num_turns_computed INTEGER,
    cost_usd REAL,
    created_at TEXT NOT NULL,
    started_at TEXT,
    running_pid INTEGER,
    completed_at TEXT,
    -- Task import/chaining (v2+)
    "group" TEXT,
    depends_on TEXT REFERENCES tasks(id),
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
);

CREATE INDEX IF NOT EXISTS idx_tasks_status ON tasks(status);
CREATE INDEX IF NOT EXISTS idx_tasks_slug ON tasks(slug);
CREATE INDEX IF NOT EXISTS idx_tasks_created_at ON tasks(created_at);
CREATE INDEX IF NOT EXISTS idx_tasks_group ON tasks("group");
CREATE INDEX IF NOT EXISTS idx_tasks_depends_on ON tasks(depends_on);
CREATE INDEX IF NOT EXISTS idx_tasks_merge_status ON tasks(merge_status);

CREATE TABLE IF NOT EXISTS run_steps (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL REFERENCES tasks(id),
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
    run_id TEXT NOT NULL REFERENCES tasks(id),
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

CREATE TABLE IF NOT EXISTS task_cycles (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    implementation_task_id TEXT NOT NULL REFERENCES tasks(id),
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
    review_task_id TEXT REFERENCES tasks(id),
    review_verdict TEXT,
    improve_task_id TEXT REFERENCES tasks(id),
    state TEXT NOT NULL DEFAULT 'review_created',
    started_at TEXT NOT NULL,
    ended_at TEXT
);

CREATE INDEX IF NOT EXISTS idx_task_cycles_impl_id ON task_cycles(implementation_task_id);
CREATE INDEX IF NOT EXISTS idx_task_cycles_status ON task_cycles(status);
CREATE INDEX IF NOT EXISTS idx_task_cycle_iterations_cycle_idx ON task_cycle_iterations(cycle_id, iteration_index);
CREATE UNIQUE INDEX IF NOT EXISTS uq_task_cycle_iterations_cycle_iter ON task_cycle_iterations(cycle_id, iteration_index);
CREATE INDEX IF NOT EXISTS idx_tasks_cycle_id ON tasks(cycle_id);
CREATE INDEX IF NOT EXISTS idx_tasks_type_based_on ON tasks(task_type, based_on);
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
    run_id: str  # References tasks.id (project-prefixed base36)
    step_index: int
    step_id: str


@dataclass(frozen=True)
class RunStep:
    """Persisted top-level message step for a run."""

    id: int
    run_id: str  # References tasks.id (project-prefixed base36)
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
    run_id: str  # References tasks.id (project-prefixed base36)
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
    (25, None),  # Manual migration: INTEGER PK → TEXT base36 IDs (run via 'gza migrate')
]


class SqliteTaskStore:
    """SQLite-based task storage."""

    def __init__(self, db_path: Path, prefix: str = "gza"):
        self.db_path = db_path
        self._prefix = prefix
        self._ensure_db()

    @classmethod
    def default(cls, project_dir: Path | None = None) -> "SqliteTaskStore":
        """Create a store using the db_path derived from config.

        Args:
            project_dir: Project root. Defaults to cwd.
        """
        from .config import Config

        config = Config.load(project_dir or Path.cwd())
        return cls(config.db_path, prefix=config.project_prefix)

    def _ensure_db(self) -> None:
        """Ensure database exists and schema is current.

        Raises:
            ManualMigrationRequired: When the DB needs a manual migration (e.g. v25).
                The caller should run :func:`run_v25_migration` then re-open the store.
        """
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as conn:
            # Check if schema_version table exists
            cur = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='schema_version'"
            )
            if cur.fetchone() is None:
                # Fresh database - create full v25 schema directly
                conn.executescript(SCHEMA)
                conn.execute("INSERT INTO schema_version (version) VALUES (?)", (SCHEMA_VERSION,))
            else:
                # Check current version and migrate if needed
                cur = conn.execute("SELECT version FROM schema_version LIMIT 1")
                row = cur.fetchone()
                current_version = row["version"] if row else 0

                pending_manual: list[int] = []
                for target_version, migration_sql in _MIGRATIONS:
                    if current_version < target_version:
                        if target_version in _MANUAL_MIGRATION_VERSIONS:
                            pending_manual.append(target_version)
                            # Don't advance current_version; stop processing further
                            break
                        if migration_sql is not None:
                            for stmt in migration_sql.strip().split(";"):
                                stmt = stmt.strip()
                                if stmt:
                                    try:
                                        conn.execute(stmt)
                                    except sqlite3.OperationalError:
                                        # Column/table/index might already exist
                                        pass
                        conn.execute("UPDATE schema_version SET version = ?", (target_version,))
                        current_version = target_version

                if pending_manual:
                    raise ManualMigrationRequired(pending_manual)

                if row is None:
                    conn.execute("INSERT INTO schema_version (version) VALUES (?)", (SCHEMA_VERSION,))

    def _connect(self) -> sqlite3.Connection:
        """Create a database connection with auto-commit."""
        conn = sqlite3.connect(self.db_path, isolation_level=None, timeout=5)
        conn.row_factory = sqlite3.Row
        return conn

    def _row_to_task(self, row: sqlite3.Row) -> Task:
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
            cost_usd=row["cost_usd"],
            input_tokens=row["input_tokens"] if "input_tokens" in keys else None,
            output_tokens=row["output_tokens"] if "output_tokens" in keys else None,
            created_at=datetime.fromisoformat(row["created_at"]) if row["created_at"] else None,
            started_at=datetime.fromisoformat(row["started_at"]) if row["started_at"] else None,
            running_pid=row["running_pid"] if "running_pid" in keys else None,
            completed_at=datetime.fromisoformat(row["completed_at"]) if row["completed_at"] else None,
            group=row["group"],
            depends_on=row["depends_on"],
            spec=row["spec"],
            create_review=bool(row["create_review"]) if row["create_review"] is not None else False,
            same_branch=bool(row["same_branch"]) if row["same_branch"] is not None else False,
            task_type_hint=row["task_type_hint"] if "task_type_hint" in keys else None,
            output_content=row["output_content"] if "output_content" in keys else None,
            session_id=row["session_id"] if "session_id" in keys else None,
            pr_number=row["pr_number"] if "pr_number" in keys else None,
            model=row["model"] if "model" in keys else None,
            provider=row["provider"] if "provider" in keys else None,
            provider_is_explicit=bool(row["provider_is_explicit"]) if "provider_is_explicit" in keys and row["provider_is_explicit"] is not None else False,
            merge_status=row["merge_status"] if "merge_status" in keys else None,
            merged_at=datetime.fromisoformat(row["merged_at"]) if "merged_at" in keys and row["merged_at"] else None,
            failure_reason=row["failure_reason"] if "failure_reason" in keys else None,
            skip_learnings=bool(row["skip_learnings"]) if "skip_learnings" in keys and row["skip_learnings"] is not None else False,
            diff_files_changed=row["diff_files_changed"] if "diff_files_changed" in keys else None,
            diff_lines_added=row["diff_lines_added"] if "diff_lines_added" in keys else None,
            diff_lines_removed=row["diff_lines_removed"] if "diff_lines_removed" in keys else None,
            review_cleared_at=datetime.fromisoformat(row["review_cleared_at"]) if "review_cleared_at" in keys and row["review_cleared_at"] else None,
            log_schema_version=(
                row["log_schema_version"]
                if "log_schema_version" in keys and row["log_schema_version"] is not None
                else 1
            ),
            cycle_id=row["cycle_id"] if "cycle_id" in keys else None,
            cycle_iteration_index=row["cycle_iteration_index"] if "cycle_iteration_index" in keys else None,
            cycle_role=row["cycle_role"] if "cycle_role" in keys else None,
        )

    def _row_to_run_step(self, row: sqlite3.Row) -> RunStep:
        """Convert a database row to a RunStep."""
        return RunStep(
            id=row["id"],
            run_id=row["run_id"],
            step_index=row["step_index"],
            step_id=row["step_id"],
            provider=row["provider"],
            message_role=row["message_role"],
            message_text=row["message_text"],
            started_at=datetime.fromisoformat(row["started_at"]),
            completed_at=datetime.fromisoformat(row["completed_at"]) if row["completed_at"] else None,
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
            timestamp=datetime.fromisoformat(row["timestamp"]),
            legacy_turn_id=row["legacy_turn_id"],
            legacy_event_id=row["legacy_event_id"],
        )

    # === Task CRUD ===

    def _next_id(self, conn: sqlite3.Connection) -> str:
        """Allocate the next project-prefixed base36 task ID (within an open connection).

        Uses a single RETURNING statement so the increment and read are atomic —
        no concurrent writer can observe the same sequence value.
        """
        # INSERT path (fresh DB, no existing row): inserts next_seq=1 and
        # RETURNING returns 1 directly — no increment — so the first task is
        # {prefix}-1, not {prefix}-0.
        # ON CONFLICT path (row already exists): increments and returns the new
        # value.  After v25 migration the row is seeded at max_old_int_id, so
        # the first post-migration task gets max_old_int_id + 1.
        cur = conn.execute(
            "INSERT INTO project_sequences (prefix, next_seq) VALUES (?, 1) "
            "ON CONFLICT(prefix) DO UPDATE SET next_seq = next_seq + 1 "
            "RETURNING next_seq",
            (self._prefix,),
        )
        seq = int(cur.fetchone()["next_seq"])
        return f"{self._prefix}-{_encode_base36(seq)}"

    def add(
        self,
        prompt: str,
        task_type: str = "implement",
        based_on: str | None = None,
        group: str | None = None,
        depends_on: str | None = None,
        spec: str | None = None,
        create_review: bool = False,
        same_branch: bool = False,
        task_type_hint: str | None = None,
        model: str | None = None,
        provider: str | None = None,
        provider_is_explicit: bool | None = None,
        skip_learnings: bool = False,
    ) -> Task:
        """Add a new task. Returns the created Task with its generated string ID."""
        now = datetime.now(UTC).isoformat()
        if provider_is_explicit is None:
            provider_is_explicit = provider is not None
        with self._connect() as conn:
            new_id = self._next_id(conn)
            conn.execute(
                """
                INSERT INTO tasks (id, prompt, task_type, based_on, created_at, "group", depends_on, spec, create_review, same_branch, task_type_hint, model, provider, provider_is_explicit, skip_learnings)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (new_id, prompt, task_type, based_on, now, group, depends_on, spec, 1 if create_review else 0, 1 if same_branch else 0, task_type_hint, model, provider, 1 if provider_is_explicit else 0, 1 if skip_learnings else 0),
            )
            result = self.get(new_id)
            assert result is not None
            return result

    def get(self, task_id: str) -> Task | None:
        """Get a task by its string ID (e.g. 'gza-1a2b')."""
        with self._connect() as conn:
            cur = conn.execute("SELECT * FROM tasks WHERE id = ?", (task_id,))
            row = cur.fetchone()
            return self._row_to_task(row) if row else None

    def get_by_slug(self, slug: str) -> Task | None:
        """Get a task by slug (YYYYMMDD-... format, stored in the 'slug' DB column)."""
        with self._connect() as conn:
            cur = conn.execute("SELECT * FROM tasks WHERE slug = ?", (slug,))
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
                    same_branch = ?,
                    output_content = ?,
                    session_id = ?,
                    pr_number = ?,
                    model = ?,
                    provider = ?,
                    provider_is_explicit = ?,
                    merge_status = ?,
                    merged_at = ?,
                    failure_reason = ?,
                    skip_learnings = ?,
                    diff_files_changed = ?,
                    diff_lines_added = ?,
                    diff_lines_removed = ?,
                    review_cleared_at = ?,
                    log_schema_version = ?,
                    cycle_id = ?,
                    cycle_iteration_index = ?,
                    cycle_role = ?
                WHERE id = ?
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
                    task.cost_usd,
                    task.input_tokens,
                    task.output_tokens,
                    task.started_at.isoformat() if task.started_at else None,
                    task.running_pid,
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
                    1 if task.provider_is_explicit else 0,
                    task.merge_status,
                    task.merged_at.isoformat() if task.merged_at else None,
                    task.failure_reason,
                    1 if task.skip_learnings else 0,
                    task.diff_files_changed,
                    task.diff_lines_added,
                    task.diff_lines_removed,
                    task.review_cleared_at.isoformat() if task.review_cleared_at else None,
                    task.log_schema_version,
                    task.cycle_id,
                    task.cycle_iteration_index,
                    task.cycle_role,
                    task.id,
                ),
            )

    def delete(self, task_id: str) -> bool:
        """Delete a task by ID. Returns True if deleted."""
        with self._connect() as conn:
            cur = conn.execute("DELETE FROM tasks WHERE id = ?", (task_id,))
            return cur.rowcount > 0

    # === Query methods ===

    def get_next_pending(self) -> Task | None:
        """Get the next pending task (oldest first), skipping blocked tasks.

        A task is unblocked when its dependency is completed OR when its
        dependency is failed but a completed retry exists anywhere in the
        based_on chain.
        """
        with self._connect() as conn:
            cur = conn.execute(
                """
                WITH RECURSIVE successful_ancestors(id) AS (
                    SELECT id FROM tasks WHERE status = 'completed'
                    UNION ALL
                    SELECT t2.based_on FROM tasks t2
                    JOIN successful_ancestors sa ON t2.id = sa.id
                    WHERE t2.based_on IS NOT NULL
                )
                SELECT t.* FROM tasks t
                WHERE t.status = 'pending'
                AND t.task_type != 'internal'
                AND (
                    t.depends_on IS NULL
                    OR t.depends_on IN (SELECT id FROM successful_ancestors)
                )
                ORDER BY t.created_at ASC
                LIMIT 1
                """
            )
            row = cur.fetchone()
            return self._row_to_task(row) if row else None

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
                    WHERE id = ? AND status = 'pending'
                    """,
                    (started_at.isoformat(), pid, task_id),
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

    def get_pending(self, limit: int | None = None) -> list[Task]:
        """Get all pending tasks."""
        with self._connect() as conn:
            query = "SELECT * FROM tasks WHERE status = 'pending' ORDER BY created_at ASC"
            if limit is not None:
                query += f" LIMIT {limit}"
            cur = conn.execute(query)
            return [self._row_to_task(row) for row in cur.fetchall()]

    def get_in_progress(self) -> list[Task]:
        """Get all in-progress tasks, oldest first."""
        with self._connect() as conn:
            cur = conn.execute(
                "SELECT * FROM tasks WHERE status = 'in_progress' ORDER BY started_at ASC, created_at ASC"
            )
            return [self._row_to_task(row) for row in cur.fetchall()]

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
            where_clauses = []
            params = []

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

            return [self._row_to_task(row) for row in cur.fetchall()]

    def get_based_on_children(self, task_id: str) -> list[Task]:
        """Return tasks where based_on = task_id (direct lineage descendants)."""
        with self._connect() as conn:
            cur = conn.execute(
                "SELECT * FROM tasks WHERE based_on = ? ORDER BY created_at ASC",
                (task_id,),
            )
            return [self._row_to_task(row) for row in cur.fetchall()]

    def get_lineage_children(self, task_id: str) -> list[Task]:
        """Return direct lineage children linked by based_on or depends_on.

        This is the canonical query used by lineage tree construction.
        """
        with self._connect() as conn:
            cur = conn.execute(
                """
                SELECT * FROM tasks
                WHERE based_on = ? OR depends_on = ?
                ORDER BY created_at ASC
                """,
                (task_id, task_id),
            )
            return [self._row_to_task(row) for row in cur.fetchall()]

    def get_resumable_failed_tasks(self) -> list[Task]:
        """Return failed tasks that can be auto-resumed.

        A task is resumable if:
        - status = 'failed'
        - failure_reason IN ('MAX_STEPS', 'MAX_TURNS', 'TEST_FAILURE')
        - session_id IS NOT NULL
        """
        with self._connect() as conn:
            cur = conn.execute(
                """
                SELECT * FROM tasks
                WHERE status = 'failed'
                AND failure_reason IN ('MAX_STEPS', 'MAX_TURNS', 'TEST_FAILURE')
                AND session_id IS NOT NULL
                ORDER BY completed_at DESC, created_at DESC
                """
            )
            return [self._row_to_task(row) for row in cur.fetchall()]

    def count_resume_chain_depth(self, task_id: str) -> int:
        """Count consecutive failed ancestors with resumable failure reasons.

        Walks the based_on chain upward from task_id's parent, counting how many
        consecutive failed ancestors have failure_reason in ('MAX_STEPS', 'MAX_TURNS', 'TEST_FAILURE').
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
                "SELECT based_on FROM tasks WHERE id = ?",
                (task_id,),
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
                    "SELECT based_on, status, failure_reason FROM tasks WHERE id = ?",
                    (current_id,),
                )
                row = cur.fetchone()
                if row is None:
                    break
                based_on, status, failure_reason = row["based_on"], row["status"], row["failure_reason"]
                if status == "failed" and failure_reason in ("MAX_STEPS", "MAX_TURNS", "TEST_FAILURE"):
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
                WHERE status = 'completed'
                  AND task_type != 'internal'
                ORDER BY completed_at DESC, created_at DESC
                LIMIT ?
                """,
                (limit,),
            )
            return [self._row_to_task(row) for row in cur.fetchall()]

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
                WHERE merge_status = 'unmerged'
                AND (
                    task_type NOT IN ('improve', 'rebase')
                    OR based_on IS NULL
                )
                ORDER BY completed_at DESC
                """
            )
            return [self._row_to_task(row) for row in cur.fetchall()]

    def set_merge_status(self, task_id: str, merge_status: str | None) -> None:
        """Set the merge_status for a task. Records merged_at when setting to 'merged'."""
        merged_at = datetime.now(UTC).isoformat() if merge_status == "merged" else None
        with self._connect() as conn:
            conn.execute(
                "UPDATE tasks SET merge_status = ?, merged_at = ? WHERE id = ?",
                (merge_status, merged_at, task_id),
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
                "UPDATE tasks SET review_cleared_at = ? WHERE id = ?",
                (now, task_id),
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
                "UPDATE tasks SET review_cleared_at = NULL WHERE id = ?",
                (task_id,),
            )

    def set_log_schema_version(self, task_id: str, version: int) -> None:
        """Set the persisted log schema marker for a task/run."""
        with self._connect() as conn:
            conn.execute(
                "UPDATE tasks SET log_schema_version = ? WHERE id = ?",
                (version, task_id),
            )

    # === Cycle orchestration APIs (v18) ===

    def _row_to_task_cycle(self, row: sqlite3.Row) -> "TaskCycle":
        """Convert a database row to a TaskCycle."""
        return TaskCycle(
            id=row["id"],
            implementation_task_id=row["implementation_task_id"],
            status=row["status"],
            max_iterations=row["max_iterations"],
            started_at=datetime.fromisoformat(row["started_at"]),
            ended_at=datetime.fromisoformat(row["ended_at"]) if row["ended_at"] else None,
            stop_reason=row["stop_reason"],
        )

    def _row_to_task_cycle_iteration(self, row: sqlite3.Row) -> "TaskCycleIteration":
        """Convert a database row to a TaskCycleIteration."""
        return TaskCycleIteration(
            id=row["id"],
            cycle_id=row["cycle_id"],
            iteration_index=row["iteration_index"],
            review_task_id=row["review_task_id"],
            review_verdict=row["review_verdict"],
            improve_task_id=row["improve_task_id"],
            state=row["state"],
            started_at=datetime.fromisoformat(row["started_at"]),
            ended_at=datetime.fromisoformat(row["ended_at"]) if row["ended_at"] else None,
        )

    def start_cycle(self, impl_task_id: str, max_iterations: int = 3) -> "TaskCycle":
        """Start a new cycle for an implementation task.

        Raises ValueError if there is already an active cycle for this task.
        """
        now = datetime.now(UTC).isoformat()
        with self._connect() as conn:
            # Enforce uniqueness: only one active cycle per implementation
            cur = conn.execute(
                "SELECT id FROM task_cycles WHERE implementation_task_id = ? AND status = 'active'",
                (impl_task_id,),
            )
            existing = cur.fetchone()
            if existing:
                raise ValueError(
                    f"Task #{impl_task_id} already has an active cycle (#{existing['id']}). "
                    "Use --continue to resume it."
                )
            cur = conn.execute(
                """
                INSERT INTO task_cycles (implementation_task_id, status, max_iterations, started_at)
                VALUES (?, 'active', ?, ?)
                """,
                (impl_task_id, max_iterations, now),
            )
            cycle_id = cur.lastrowid
            assert cycle_id is not None
            cur = conn.execute("SELECT * FROM task_cycles WHERE id = ?", (cycle_id,))
            row = cur.fetchone()
            assert row is not None
            return self._row_to_task_cycle(row)

    def get_active_cycle_for_impl(self, impl_task_id: str) -> "TaskCycle | None":
        """Get the active cycle for an implementation task, if any."""
        with self._connect() as conn:
            cur = conn.execute(
                "SELECT * FROM task_cycles WHERE implementation_task_id = ? AND status = 'active'",
                (impl_task_id,),
            )
            row = cur.fetchone()
            return self._row_to_task_cycle(row) if row else None

    def get_cycles_for_impl(self, impl_task_id: str) -> "list[TaskCycle]":
        """Get all cycles for an implementation task, newest first."""
        with self._connect() as conn:
            cur = conn.execute(
                "SELECT * FROM task_cycles WHERE implementation_task_id = ? ORDER BY id DESC",
                (impl_task_id,),
            )
            return [self._row_to_task_cycle(row) for row in cur.fetchall()]

    def get_all_closed_cycles(self) -> "list[TaskCycle]":
        """Get all closed (non-active) cycles, newest first."""
        with self._connect() as conn:
            cur = conn.execute(
                "SELECT * FROM task_cycles WHERE status != 'active' ORDER BY id DESC",
            )
            return [self._row_to_task_cycle(row) for row in cur.fetchall()]

    def append_cycle_iteration(self, cycle_id: int, iteration_index: int) -> "TaskCycleIteration":
        """Create a new iteration record for a cycle."""
        now = datetime.now(UTC).isoformat()
        with self._connect() as conn:
            cur = conn.execute(
                """
                INSERT INTO task_cycle_iterations (cycle_id, iteration_index, state, started_at)
                VALUES (?, ?, 'review_created', ?)
                """,
                (cycle_id, iteration_index, now),
            )
            iter_id = cur.lastrowid
            assert iter_id is not None
            cur = conn.execute("SELECT * FROM task_cycle_iterations WHERE id = ?", (iter_id,))
            row = cur.fetchone()
            assert row is not None
            return self._row_to_task_cycle_iteration(row)

    def update_cycle_iteration(
        self,
        iteration_id: int,
        *,
        review_task_id: str | None = None,
        review_verdict: str | None = None,
        improve_task_id: str | None = None,
        state: str | None = None,
        ended_at: datetime | None = None,
    ) -> None:
        """Update fields on a cycle iteration row."""
        with self._connect() as conn:
            updates = []
            params: list[Any] = []
            if review_task_id is not None:
                updates.append("review_task_id = ?")
                params.append(review_task_id)
            if review_verdict is not None:
                updates.append("review_verdict = ?")
                params.append(review_verdict)
            if improve_task_id is not None:
                updates.append("improve_task_id = ?")
                params.append(improve_task_id)
            if state is not None:
                updates.append("state = ?")
                params.append(state)
            if ended_at is not None:
                updates.append("ended_at = ?")
                params.append(ended_at.isoformat())
            if not updates:
                return
            params.append(iteration_id)
            conn.execute(
                f"UPDATE task_cycle_iterations SET {', '.join(updates)} WHERE id = ?",
                params,
            )

    def get_cycle_iterations(self, cycle_id: int) -> "list[TaskCycleIteration]":
        """Get all iterations for a cycle, ordered by iteration_index."""
        with self._connect() as conn:
            cur = conn.execute(
                "SELECT * FROM task_cycle_iterations WHERE cycle_id = ? ORDER BY iteration_index ASC",
                (cycle_id,),
            )
            return [self._row_to_task_cycle_iteration(row) for row in cur.fetchall()]

    def close_cycle(self, cycle_id: int, status: str, stop_reason: str) -> None:
        """Close a cycle with the given status and stop reason."""
        now = datetime.now(UTC).isoformat()
        with self._connect() as conn:
            conn.execute(
                "UPDATE task_cycles SET status = ?, stop_reason = ?, ended_at = ? WHERE id = ?",
                (status, stop_reason, now, cycle_id),
            )

    def get_cycle_aggregate_stats(self) -> dict:
        """Get aggregate cycle stats across all closed approved cycles.

        Returns a dict with percentile distributions for:
        - improves_before_approval (improve task count before first approval in approved cycles)
        - reviews_per_cycle
        - improves_per_cycle
        - cycle_duration_seconds
        """
        cycles = self.get_all_closed_cycles()
        if not cycles:
            return {
                "total_cycles": 0,
                "approved_cycles": 0,
                "improves_before_approval": None,
                "reviews_per_cycle": None,
                "improves_per_cycle": None,
                "cycle_duration_seconds": None,
            }

        approved = [c for c in cycles if c.status == "approved"]
        improves_before_approval_vals: list[float] = []
        reviews_vals: list[float] = []
        improves_vals: list[float] = []
        duration_vals: list[float] = []

        for cycle in cycles:
            iters = self.get_cycle_iterations(cycle.id)
            reviews_vals.append(sum(1 for it in iters if it.review_task_id is not None))
            improves_vals.append(sum(1 for it in iters if it.improve_task_id is not None))
            if cycle.ended_at and cycle.started_at:
                duration_vals.append((cycle.ended_at - cycle.started_at).total_seconds())

        for cycle in approved:
            iters = self.get_cycle_iterations(cycle.id)
            improves_before_approval_vals.append(sum(1 for it in iters if it.improve_task_id is not None))

        return {
            "total_cycles": len(cycles),
            "approved_cycles": len(approved),
            "improves_before_approval": _compute_percentiles(improves_before_approval_vals) if improves_before_approval_vals else None,
            "reviews_per_cycle": _compute_percentiles(reviews_vals) if reviews_vals else None,
            "improves_per_cycle": _compute_percentiles(improves_vals) if improves_vals else None,
            "cycle_duration_seconds": _compute_percentiles(duration_vals) if duration_vals else None,
        }

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
                "SELECT COALESCE(MAX(step_index), 0) + 1 AS next_step FROM run_steps WHERE run_id = ?",
                (run_id,),
            )
            next_step = int(cur.fetchone()["next_step"])
            step_label = f"S{next_step}"
            cur = conn.execute(
                """
                INSERT INTO run_steps (
                    run_id, step_index, step_id, provider, message_role,
                    message_text, started_at, legacy_turn_id, legacy_event_id
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
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
                "SELECT run_id, step_index, step_id FROM run_steps WHERE id = ?",
                (step_ref.id,),
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
                "SELECT COALESCE(MAX(substep_index), 0) + 1 AS next_substep FROM run_substeps WHERE step_id = ?",
                (step_ref.id,),
            )
            next_substep = int(cur.fetchone()["next_substep"])
            substep_label = f"{step_ref.step_id}.{next_substep}"
            cur = conn.execute(
                """
                INSERT INTO run_substeps (
                    run_id, step_id, substep_index, substep_id, type, source, call_id,
                    payload_json, timestamp, legacy_turn_id, legacy_event_id
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
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
            row = conn.execute("SELECT * FROM run_substeps WHERE id = ?", (substep_row_id,)).fetchone()
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
                "SELECT run_id, step_index, step_id FROM run_steps WHERE id = ?",
                (step_ref.id,),
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
                WHERE id = ?
                """,
                (ts, outcome, summary, step_ref.id),
            )
            if cur.rowcount == 0:
                raise ValueError(f"Unknown step reference: {step_ref.id}")

    def count_steps(self, run_id: str) -> int:
        """Count the number of run_steps rows for a given run_id."""
        with self._connect() as conn:
            cur = conn.execute(
                "SELECT COUNT(*) AS cnt FROM run_steps WHERE run_id = ?",
                (run_id,),
            )
            row = cur.fetchone()
            return int(row["cnt"]) if row else 0

    def get_run_steps(self, run_id: str) -> list[RunStep]:
        """Get all stored run steps for a run, ordered by step index."""
        with self._connect() as conn:
            cur = conn.execute(
                "SELECT * FROM run_steps WHERE run_id = ? ORDER BY step_index ASC",
                (run_id,),
            )
            return [self._row_to_run_step(row) for row in cur.fetchall()]

    def get_run_substeps(self, step_ref: StepRef) -> list[RunSubstep]:
        """Get all stored substeps for a step, ordered by substep index."""
        with self._connect() as conn:
            cur = conn.execute(
                "SELECT run_id, step_index, step_id FROM run_steps WHERE id = ?",
                (step_ref.id,),
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
                WHERE run_id = ? AND step_id = ?
                ORDER BY substep_index ASC
                """,
                (step_ref.run_id, step_ref.id),
            )
            return [self._row_to_run_substep(row) for row in cur.fetchall()]

    def get_all(self) -> list[Task]:
        """Get all tasks."""
        with self._connect() as conn:
            cur = conn.execute("SELECT * FROM tasks ORDER BY created_at DESC")
            return [self._row_to_task(row) for row in cur.fetchall()]

    def get_impl_based_on_ids(self) -> set[str]:
        """Return the set of plan IDs that already have an implement task.

        Checks both based_on and depends_on, since implement tasks may
        reference their plan via either column.
        """
        with self._connect() as conn:
            cur = conn.execute(
                "SELECT DISTINCT based_on FROM tasks"
                " WHERE task_type = 'implement' AND based_on IS NOT NULL"
                " UNION"
                " SELECT DISTINCT depends_on FROM tasks"
                " WHERE task_type = 'implement' AND depends_on IS NOT NULL"
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
                WHERE task_type = 'review' AND depends_on = ?
                ORDER BY completed_at DESC NULLS LAST
                """,
                (task_id,),
            )
            return [self._row_to_task(row) for row in cur.fetchall()]

    def get_unlinked_reviews_for_slug(self, slug: str) -> list[Task]:
        """Get completed review tasks not linked via depends_on, matched by slug.

        This is a fallback for review tasks created manually (e.g., prompt starts
        with "review <slug>") without an explicit depends_on relationship.
        """
        with self._connect() as conn:
            cur = conn.execute(
                """
                SELECT * FROM tasks
                WHERE task_type = 'review'
                  AND status = 'completed'
                  AND depends_on IS NULL
                  AND (
                    slug LIKE ?
                    OR prompt LIKE ?
                  )
                ORDER BY completed_at DESC NULLS LAST
                """,
                (f"%review-{slug}%", f"review {slug}%"),
            )
            return [self._row_to_task(row) for row in cur.fetchall()]

    def get_improve_tasks_for(self, impl_task_id: str, review_task_id: str) -> list[Task]:
        """Get improve tasks that match the given implementation and review task IDs."""
        with self._connect() as conn:
            cur = conn.execute(
                """
                SELECT * FROM tasks
                WHERE task_type = 'improve' AND based_on = ? AND depends_on = ?
                ORDER BY created_at DESC
                """,
                (impl_task_id, review_task_id),
            )
            return [self._row_to_task(row) for row in cur.fetchall()]

    def get_improve_tasks_by_root(self, root_task_id: str) -> list[Task]:
        """Get all improve tasks whose based_on points to root_task_id.

        This remains for review/improve workflow logic; lineage display should
        use get_lineage_children() via gza.query.build_lineage_tree().
        """
        with self._connect() as conn:
            cur = conn.execute(
                """
                SELECT * FROM tasks
                WHERE task_type = 'improve' AND based_on = ?
                ORDER BY created_at DESC
                """,
                (root_task_id,),
            )
            return [self._row_to_task(row) for row in cur.fetchall()]

    def get_impl_tasks_by_depends_on_or_based_on(self, task_id: str) -> list[Task]:
        """Get implement tasks that depend on or are based on a given task.

        Kept for implementation-focused callers that only need implement tasks.
        Lineage display should use get_lineage_children().
        """
        with self._connect() as conn:
            cur = conn.execute(
                """
                SELECT * FROM tasks
                WHERE task_type = 'implement' AND (based_on = ? OR depends_on = ?)
                ORDER BY created_at ASC
                """,
                (task_id, task_id),
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
                    COUNT(*) FILTER (WHERE status = 'dropped') as dropped,
                    SUM(cost_usd) as total_cost,
                    SUM(duration_seconds) as total_duration,
                    SUM(COALESCE(num_steps_reported, num_steps_computed, num_turns_reported)) as total_steps,
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

    def _find_successful_retry(self, task_id: str) -> bool:
        """Check if any completed task exists in the retry chain rooted at task_id.

        Follows based_on links forward (task_id → retries → retries of retries)
        and returns True if any task in that tree has status='completed'.
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
                    "SELECT id, status FROM tasks WHERE based_on = ?",
                    (current_id,),
                )
                for row in cur.fetchall():
                    if row["status"] == "completed":
                        return True
                    queue.append(row["id"])
        return False

    def is_task_blocked(self, task: Task) -> tuple[bool, str | None, str | None]:
        """Check if a task is blocked by an incomplete dependency.

        When the direct dependency has failed, follows the retry chain (via
        based_on) to see if a subsequent retry succeeded.  If so, the task is
        treated as unblocked.

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

        if dep.status == "failed" and dep.id is not None and self._find_successful_retry(dep.id):
            return (False, None, None)

        return (True, dep.id, dep.status)

    def count_blocked_tasks(self) -> int:
        """Count pending tasks that are blocked by dependencies.

        A task is unblocked (and therefore not counted) if its dependency is
        completed OR if the dependency is failed but a completed retry exists
        anywhere in the based_on chain.

        NOTE: This SQL intentionally mirrors the Python logic in is_task_blocked():
        both treat only 'completed' (or 'failed' with a successful retry) as
        unblocking. A 'dropped' dependency is therefore counted as blocking here,
        consistent with is_task_blocked() returning (True, ...) for dropped deps.
        If the semantics of dropped-unblocks-dependents ever change, both sites
        must be updated together.
        """
        with self._connect() as conn:
            cur = conn.execute(
                """
                WITH RECURSIVE successful_ancestors(id) AS (
                    SELECT id FROM tasks WHERE status = 'completed'
                    UNION ALL
                    SELECT t2.based_on FROM tasks t2
                    JOIN successful_ancestors sa ON t2.id = sa.id
                    WHERE t2.based_on IS NOT NULL
                )
                SELECT COUNT(*) as count FROM tasks t
                WHERE t.status = 'pending'
                AND t.depends_on IS NOT NULL
                AND t.depends_on NOT IN (SELECT id FROM successful_ancestors)
                """
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
    depends_on: str | None = None,
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
        if based_on_slug:
            initial_content = f"Implement plan from task #{based_on}: {based_on_slug}"
        else:
            initial_content = f"Implement plan from task #{based_on}"

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
    based_on: str | None = None,
    spec: str | None = None,
    group: str | None = None,
    depends_on: str | None = None,
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


# === Module-level convenience functions ===

def _default_store() -> "SqliteTaskStore":
    """Create a SqliteTaskStore using config-derived db_path, with fallback."""
    try:
        return SqliteTaskStore.default()
    except Exception:
        return SqliteTaskStore(Path(".gza/gza.db"))


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
        "cost_usd": task.cost_usd,
        "input_tokens": task.input_tokens,
        "output_tokens": task.output_tokens,
        "created_at": task.created_at.isoformat() if task.created_at else None,
        "started_at": task.started_at.isoformat() if task.started_at else None,
        "completed_at": task.completed_at.isoformat() if task.completed_at else None,
        "group": task.group,
        "depends_on": task.depends_on,
        "spec": task.spec,
        "create_review": task.create_review,
        "same_branch": task.same_branch,
        "task_type_hint": task.task_type_hint,
        "output_content": task.output_content,
        "session_id": task.session_id,
        "pr_number": task.pr_number,
        "model": task.model,
        "provider": task.provider,
        "provider_is_explicit": task.provider_is_explicit,
        "merge_status": task.merge_status,
        "failure_reason": task.failure_reason,
        "skip_learnings": task.skip_learnings,
        "diff_files_changed": task.diff_files_changed,
        "diff_lines_added": task.diff_lines_added,
        "diff_lines_removed": task.diff_lines_removed,
        "review_cleared_at": task.review_cleared_at.isoformat() if task.review_cleared_at else None,
        "log_schema_version": task.log_schema_version,
    }


def get_task(task_id: str) -> dict:
    """Get a task by ID as a JSON-serializable dict.

    Auto-discovers the DB at .gza/gza.db relative to cwd.

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

    Auto-discovers the DB at .gza/gza.db relative to cwd.
    Returns None if task not found or log_file is not set.
    """
    store = _default_store()
    task = store.get(task_id)
    if task is None:
        return None
    return task.log_file


def get_task_report_path(task_id: str) -> str | None:
    """Get the report_file path for a task.

    Auto-discovers the DB at .gza/gza.db relative to cwd.
    Returns None if task not found or report_file is not set.
    """
    store = _default_store()
    task = store.get(task_id)
    if task is None:
        return None
    return task.report_file


def get_baseline_stats(limit: int = 20) -> dict:
    """Get average stats from the last N completed tasks.

    Auto-discovers the DB at .gza/gza.db relative to cwd.

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
                WHERE status = 'completed'
                ORDER BY completed_at DESC
                LIMIT ?
            )
            """,
            (limit,),
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
    6. Update schema_version to 25

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

        if current == SCHEMA_VERSION:
            return  # Already up-to-date
        if current > SCHEMA_VERSION:
            raise RuntimeError(f"DB is at v{current}, newer than v{SCHEMA_VERSION}")
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
            return f"{prefix}-{_encode_base36(old_id)}"

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
                cost_usd REAL,
                created_at TEXT NOT NULL,
                started_at TEXT,
                running_pid INTEGER,
                completed_at TEXT,
                "group" TEXT,
                depends_on TEXT REFERENCES tasks_v25(id),
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
                    cost_usd, created_at, started_at, running_pid, completed_at,
                    "group", depends_on, spec, create_review, same_branch, task_type_hint,
                    output_content, session_id, pr_number, model, provider, provider_is_explicit,
                    input_tokens, output_tokens, merge_status, merged_at, failure_reason,
                    skip_learnings, diff_files_changed, diff_lines_added, diff_lines_removed,
                    review_cleared_at, log_schema_version, cycle_id, cycle_iteration_index,
                    cycle_role
                ) VALUES (
                    ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?
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
                    row["cost_usd"], row["created_at"], row["started_at"],
                    row["running_pid"] if "running_pid" in row.keys() else None,
                    row["completed_at"],
                    row["group"],
                    depends_on_new,
                    row["spec"],
                    row["create_review"] if "create_review" in row.keys() else 0,
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
        conn.execute("UPDATE schema_version SET version = ?", (SCHEMA_VERSION,))

        conn.execute("COMMIT")
        logger.info("v25 migration complete: %s", db_path)
    except Exception:
        conn.execute("ROLLBACK")
        raise
    finally:
        conn.close()


def resolve_task_id(arg: str, project_prefix: str) -> str:
    """Resolve a user-supplied task ID argument to a canonical string ID.

    Handles three forms:
    - Full prefixed ID: ``"gza-3f"`` → ``"gza-3f"`` (returned as-is)
    - Bare decimal integer: ``"42"`` → ``"{prefix}-{base36(42)}"``
      Decimal integers are treated as legacy pre-migration integer IDs and
      converted to their base36 equivalent, so users with muscle memory of old
      integer IDs can still find their tasks after the v25 migration.
    - Bare base36 suffix: ``"3f"`` → ``"{prefix}-3f"``

    The returned value is the string to pass to ``store.get()``.

    Note: any hyphenated string (e.g. ``"feature-branch"``) is treated as a
    full prefixed ID and returned unchanged.  The caller is responsible for
    handling the resulting ``store.get()`` returning ``None``; the "not found"
    error message is more actionable than a format-validation error here.
    """
    arg = arg.strip()
    if not arg:
        raise ValueError("Empty task ID")
    # Full prefixed ID already (contains a hyphen) — returned as-is.
    # Callers handle store.get() returning None for unrecognised IDs.
    if "-" in arg:
        return arg
    # Bare decimal integer → interpret as legacy pre-migration integer ID
    if arg.isdigit() and int(arg) > 0:
        return f"{project_prefix}-{_encode_base36(int(arg))}"
    # Bare base36 suffix — prepend project prefix.
    # Note: an all-alpha string like "abc" becomes "{prefix}-abc", which looks
    # like a valid task ID but may not exist in the DB.  The caller is
    # responsible for handling store.get() returning None in that case.
    return f"{project_prefix}-{arg}"


class _MigrationPreview(TypedDict):
    task_count: int
    samples: list[tuple[int, str]]
    first_post_migration_id: str


def preview_v25_migration(db_path: Path, prefix: str, sample_limit: int = 10) -> _MigrationPreview:
    """Return a preview of what run_v25_migration would do, without writing anything.

    Returns a TypedDict with keys:
    - ``task_count``: total number of tasks in the DB
    - ``samples``: list of ``(old_id, new_id)`` tuples for the first ``sample_limit`` tasks
    - ``first_post_migration_id``: the first ID that would be assigned to a new task
      after migration (i.e. ``{prefix}-{base36(max_id + 1)}``)

    Note: This function is only meaningful on pre-v25 databases.  On a database that has
    already been migrated to v25, all task IDs are TEXT strings so there are no integer
    IDs to convert — ``samples`` will be empty and ``first_post_migration_id`` will be
    ``""`` to indicate the result is not applicable.  Use :func:`check_migration_status`
    to determine whether v25 migration is pending before calling this function.
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

        if current_version >= SCHEMA_VERSION:
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
                "first_post_migration_id": "",
            }

        try:
            cur = conn.execute("SELECT id FROM tasks ORDER BY id ASC LIMIT ?", (sample_limit,))
            samples_raw = [(row["id"], f"{prefix}-{_encode_base36(row['id'])}") for row in cur.fetchall()
                           if isinstance(row["id"], int)]
            cur2 = conn.execute("SELECT COUNT(*) AS cnt, MAX(id) AS max_id FROM tasks")
            row = cur2.fetchone()
            task_count = row["cnt"] if row else 0
            max_id = row["max_id"] if row and row["max_id"] else 0
        except sqlite3.OperationalError:
            samples_raw = []
            task_count = 0
            max_id = 0
    finally:
        conn.close()

    first_post = f"{prefix}-{_encode_base36(max_id + 1)}" if max_id else f"{prefix}-1"
    return {
        "task_count": task_count,
        "samples": samples_raw,
        "first_post_migration_id": first_post,
    }
