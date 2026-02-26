"""Task model and storage abstraction."""

import sys
from dataclasses import dataclass, field
from datetime import datetime, date, timezone
from enum import Enum
from pathlib import Path
from typing import Protocol

import yaml


class LiteralString(str):
    """String subclass that will be serialized as a YAML literal block scalar (|-)."""
    pass


def literal_string_representer(dumper: yaml.Dumper, data: LiteralString) -> yaml.ScalarNode:
    """YAML representer for LiteralString to output as literal block scalar."""
    return dumper.represent_scalar('tag:yaml.org,2002:str', str(data), style='|')


# Register the custom representer for LiteralString
yaml.add_representer(LiteralString, literal_string_representer)


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

    @classmethod
    def from_dict(cls, data: dict) -> "Task":
        """Create a Task from a dictionary (legacy YAML format)."""
        # Support both 'description' (legacy) and 'prompt' (new) field names
        prompt = data.get("prompt") or data.get("description", "")
        status = data.get("status", "pending")
        # Convert status string to TaskStatus if needed
        if isinstance(status, str):
            status = TaskStatus(status) if status in [s.value for s in TaskStatus] else TaskStatus.PENDING

        return cls(
            id=data.get("id"),
            prompt=prompt,
            status=status,
            task_type=data.get("type", data.get("task_type", "task")),
            task_id=data.get("task_id"),
            branch=data.get("branch"),
            log_file=data.get("log_file"),
            report_file=data.get("report_file"),
            based_on=data.get("based_on"),
            has_commits=data.get("has_commits"),
            duration_seconds=data.get("duration_seconds"),
            num_steps_reported=data.get("num_steps_reported"),
            num_steps_computed=data.get("num_steps_computed"),
            num_turns_reported=data.get("num_turns_reported", data.get("num_turns")),
            num_turns_computed=data.get("num_turns_computed"),
            cost_usd=data.get("cost_usd"),
            created_at=data.get("created_at"),
            started_at=data.get("started_at"),
            completed_at=data.get("completed_at"),
            group=data.get("group"),
            depends_on=data.get("depends_on"),
            spec=data.get("spec"),
            create_review=data.get("create_review", False),
            same_branch=data.get("same_branch", False),
            task_type_hint=data.get("task_type_hint"),
            output_content=data.get("output_content"),
        )

    def to_dict(self) -> dict:
        """Convert Task to a dictionary (for YAML serialization)."""
        # Use LiteralString for long (>50 chars) or multiline descriptions
        desc = self.prompt
        if len(desc) > 50 or '\n' in desc:
            desc = LiteralString(desc)

        result: dict[str, str | bool | float | int | datetime] = {
            "description": desc,
            "status": self.status.value if isinstance(self.status, TaskStatus) else self.status,
        }
        if self.task_type != "task":
            result["type"] = self.task_type
        if self.task_id:
            result["task_id"] = self.task_id
        if self.branch:
            result["branch"] = self.branch
        if self.log_file:
            result["log_file"] = self.log_file
        if self.report_file:
            result["report_file"] = self.report_file
        if self.has_commits:
            result["has_commits"] = self.has_commits
        if self.duration_seconds:
            result["duration_seconds"] = self.duration_seconds
        if self.num_steps_reported:
            result["num_steps_reported"] = self.num_steps_reported
        if self.num_steps_computed:
            result["num_steps_computed"] = self.num_steps_computed
        if self.num_turns_reported:
            result["num_turns_reported"] = self.num_turns_reported
        if self.num_turns_computed:
            result["num_turns_computed"] = self.num_turns_computed
        if self.cost_usd:
            result["cost_usd"] = self.cost_usd
        if self.completed_at:
            result["completed_at"] = self.completed_at
        return result


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


class YamlTaskStore:
    """YAML-based task storage."""

    def __init__(self, path: Path):
        self.path = path
        self._tasks: list[Task] = []
        self._load()

    def _load(self) -> None:
        """Load tasks from YAML file."""
        if not self.path.exists():
            self._tasks = []
            return

        try:
            with open(self.path) as f:
                data = yaml.safe_load(f) or {}
        except yaml.YAMLError as e:
            print(f"Error: Invalid YAML in {self.path}", file=sys.stderr)
            print(f"  {e}", file=sys.stderr)
            sys.exit(1)

        self._tasks = [Task.from_dict(t) for t in data.get("tasks", [])]

    def _save(self) -> None:
        """Save tasks to YAML file."""
        data = {"tasks": [t.to_dict() for t in self._tasks]}
        with open(self.path, "w") as f:
            yaml.dump(data, f, default_flow_style=False, sort_keys=False)

    def get_next_pending(self, tags: list[str] | None = None) -> Task | None:
        """Get the next pending task, optionally filtered by tags."""
        for task in self._tasks:
            if task.status != "pending":
                continue
            if tags is None:
                return task
            # If tags specified, task must have at least one matching tag (not currently supported in Task)
            # if any(t in task.tags for t in tags):
            #     return task
        return None

    def mark_in_progress(self, task: Task) -> None:
        """Mark a task as in progress."""
        task.status = TaskStatus.IN_PROGRESS
        self._save()

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
        task.status = TaskStatus.COMPLETED
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
            task.num_steps_reported = stats.num_steps_reported
            task.num_steps_computed = stats.num_steps_computed
            task.num_turns_reported = stats.num_turns_reported
            task.num_turns_computed = stats.num_turns_computed
            task.cost_usd = stats.cost_usd
        self._save()

    def mark_failed(
        self,
        task: Task,
        log_file: str | None = None,
        has_commits: bool = False,
        stats: TaskStats | None = None,
        branch: str | None = None,
    ) -> None:
        """Mark a task as failed."""
        task.status = TaskStatus.FAILED
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
        self._save()

    def mark_unmerged(
        self,
        task: Task,
        branch: str | None = None,
        log_file: str | None = None,
        has_commits: bool = False,
        stats: TaskStats | None = None,
    ) -> None:
        """Mark a task as unmerged (completed but not merged to main)."""
        task.status = TaskStatus.UNMERGED
        task.completed_at = datetime.now(timezone.utc)
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
        self._save()
