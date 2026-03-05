"""Task model and storage abstraction."""

from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Protocol


class TaskStatus(str, Enum):
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"
    UNMERGED = "unmerged"
    BLOCKED = "blocked"


@dataclass
class Task:
    """A task in the database."""
    id: int | None  # None for unsaved tasks
    prompt: str
    status: TaskStatus = TaskStatus.PENDING
    task_type: str = "task"  # task, explore, plan, implement, review
    task_id: str | None = None  # YYYYMMDD-slug format
    branch: str | None = None
    log_file: str | None = None
    report_file: str | None = None
    based_on: int | None = None  # Reference to parent task id
    has_commits: bool | None = None
    duration_seconds: float | None = None
    num_steps_reported: int | None = None  # Step count reported by the provider
    num_steps_computed: int | None = None  # Step count computed internally
    num_turns_reported: int | None = None  # Turn count reported by the provider
    num_turns_computed: int | None = None  # Turn count computed internally
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

    def is_explore(self) -> bool:
        """Check if this is an exploration task."""
        return self.task_type == "explore"

    def is_blocked(self) -> bool:
        """Check if this task is blocked by a dependency."""
        # This will be properly checked against the database in SqliteTaskStore
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


class TaskStore(Protocol):
    """Protocol for task storage backends."""

    def get_next_pending(self, tags: list[str] | None = None) -> Task | None:
        """Get the next pending task, optionally filtered by tags."""
        ...

    def mark_in_progress(self, task: Task) -> None:
        """Mark a task as in progress."""
        ...

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
        ...

    def mark_failed(
        self,
        task: Task,
        log_file: str | None = None,
        has_commits: bool = False,
        stats: TaskStats | None = None,
        branch: str | None = None,
    ) -> None:
        """Mark a task as failed."""
        ...

    def mark_unmerged(
        self,
        task: Task,
        branch: str | None = None,
        log_file: str | None = None,
        has_commits: bool = False,
        stats: TaskStats | None = None,
    ) -> None:
        """Mark a task as unmerged (completed but not merged to main)."""
        ...


