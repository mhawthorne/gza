"""Task model and storage abstraction."""

import sys
from dataclasses import dataclass, field
from datetime import date
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


@dataclass
class Task:
    description: str
    status: str = "pending"  # pending, in_progress, completed, failed, unmerged
    type: str = "task"  # task, explore (explore doesn't require code changes)
    tags: list[str] = field(default_factory=list)
    task_id: str | None = None  # Auto-generated: YYYYMMDD-slug format
    completed_at: date | None = None
    branch: str | None = None
    log_file: str | None = None
    report_file: str | None = None  # For explore tasks, stores findings
    based_on: str | None = None  # Reference to a report file for follow-up tasks
    has_commits: bool | None = None  # Whether the task made any git commits
    # Stats from task execution
    duration_seconds: float | None = None
    num_turns: int | None = None
    cost_usd: float | None = None

    def to_dict(self) -> dict:
        """Convert to dict for serialization, omitting None/empty values."""
        # Use LiteralString for multi-line or long descriptions
        description = self.description
        if len(description) > 50 or '\n' in description:
            description = LiteralString(description)

        d = {"description": description, "status": self.status}
        if self.type != "task":
            d["type"] = self.type
        if self.tags:
            d["tags"] = self.tags
        if self.task_id:
            d["task_id"] = self.task_id
        if self.completed_at:
            d["completed_at"] = self.completed_at.isoformat()
        if self.branch:
            d["branch"] = self.branch
        if self.log_file:
            d["log_file"] = self.log_file
        if self.report_file:
            d["report_file"] = self.report_file
        if self.based_on:
            d["based_on"] = self.based_on
        if self.has_commits is not None:
            d["has_commits"] = self.has_commits
        if self.duration_seconds is not None:
            d["duration_seconds"] = self.duration_seconds
        if self.num_turns is not None:
            d["num_turns"] = self.num_turns
        if self.cost_usd is not None:
            d["cost_usd"] = self.cost_usd
        return d

    @classmethod
    def from_dict(cls, d: dict) -> "Task":
        """Create Task from dict."""
        completed_at = None
        if d.get("completed_at"):
            completed_at = date.fromisoformat(d["completed_at"])
        return cls(
            description=d["description"],
            status=d.get("status", "pending"),
            type=d.get("type", "task"),
            tags=d.get("tags", []),
            task_id=d.get("task_id"),
            completed_at=completed_at,
            branch=d.get("branch"),
            log_file=d.get("log_file"),
            report_file=d.get("report_file"),
            based_on=d.get("based_on"),
            has_commits=d.get("has_commits"),
            duration_seconds=d.get("duration_seconds"),
            num_turns=d.get("num_turns"),
            cost_usd=d.get("cost_usd"),
        )

    def is_explore(self) -> bool:
        """Check if this is an exploration task."""
        return self.type == "explore"


@dataclass
class TaskStats:
    """Statistics from a task run."""
    duration_seconds: float | None = None
    num_turns: int | None = None
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
            # If tags specified, task must have at least one matching tag
            if any(t in task.tags for t in tags):
                return task
        return None

    def mark_in_progress(self, task: Task) -> None:
        """Mark a task as in progress."""
        task.status = "in_progress"
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
        task.status = "completed"
        task.completed_at = date.today()
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
        self._save()

    def mark_failed(
        self,
        task: Task,
        log_file: str | None = None,
        has_commits: bool = False,
        stats: TaskStats | None = None,
    ) -> None:
        """Mark a task as failed."""
        task.status = "failed"
        task.has_commits = has_commits
        if log_file:
            task.log_file = log_file
        if stats:
            task.duration_seconds = stats.duration_seconds
            task.num_turns = stats.num_turns
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
        task.status = "unmerged"
        task.completed_at = date.today()
        task.has_commits = has_commits
        if branch:
            task.branch = branch
        if log_file:
            task.log_file = log_file
        if stats:
            task.duration_seconds = stats.duration_seconds
            task.num_turns = stats.num_turns
            task.cost_usd = stats.cost_usd
        self._save()
