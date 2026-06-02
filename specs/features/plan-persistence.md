# Plan Persistence Design

## Problem

Plan tasks write their output to `.gza/plans/{task_id}.md` in the main project directory. When an implement task runs, it creates a fresh worktree from `origin/main` which does not contain this file. The implement task cannot access the plan.

Current flow:
1. Plan task runs in `config.project_dir` (main checkout)
2. Agent writes plan to `.gza/plans/123.md` (untracked file)
3. `mark_completed()` stores `report_file = ".gza/plans/123.md"`
4. Implement task creates worktree from `origin/main`
5. Worktree has no `.gza/plans/` directory - **file not found**

## Goals

1. Enable implement tasks to access plan content
2. Support both local (SQLite) and distributed (Postgres) modes
3. Keep plan files on disk for human review
4. Work with or without Docker

## Solution: Store Output Content in Database

Add an `output_content` column to the tasks table that stores the actual text content of plan/explore/review outputs.

### Schema Change

```sql
-- Migration v3 → v4
ALTER TABLE tasks ADD COLUMN output_content TEXT;
```

### Task Dataclass Update

```python
@dataclass
class Task:
    # ... existing fields ...
    report_file: str | None = None      # Rename from report_file to output_file (optional)
    output_content: str | None = None   # NEW: actual content stored in DB
```

### Storage Behavior

**Local mode (SQLite):**
- Plan file written to disk: `.gza/plans/123.md`
- `output_file` set to path (for human reference)
- `output_content` set to file contents

**Distributed mode (Postgres):**
- `output_file` = NULL (no shared filesystem)
- `output_content` = plan text

### Runner Changes

#### `_run_non_code_task()` - Store content on completion

```python
# After report file is created/verified (around line 397-406)
output_content = None
if report_path.exists():
    output_content = report_path.read_text()

store.mark_completed(
    task,
    branch=None,
    log_file=str(log_file.relative_to(config.project_dir)),
    report_file=report_file_relative,
    output_content=output_content,  # NEW
    has_commits=False,
    stats=stats,
)
```

#### `_build_context_from_chain()` - Read content from DB

```python
def _get_task_output(task: Task, project_dir: Path) -> str | None:
    """Get task output content, preferring DB over filesystem."""
    # Prefer DB content (works in distributed mode)
    if task.output_content:
        return task.output_content

    # Fall back to file (local mode, backward compat)
    if task.report_file:
        path = project_dir / task.report_file
        if path.exists():
            return path.read_text()

    return None
```

Then update `_build_context_from_chain()` to embed plan content directly in prompt:

```python
# For implement tasks, include plan content
if task.task_type == "implement" and task.based_on:
    plan_task = _find_task_of_type_in_chain(task.based_on, "plan", store)
    if plan_task:
        plan_content = _get_task_output(plan_task, project_dir)
        if plan_content:
            context_parts.append("## Plan to implement:\n")
            context_parts.append(plan_content)
```

## Database Backend Abstraction

To support both SQLite and Postgres, introduce a `TaskStore` protocol:

```python
from typing import Protocol

class TaskStore(Protocol):
    def add(self, prompt: str, task_type: str = "task", ...) -> Task: ...
    def get(self, task_id: int) -> Task | None: ...
    def get_next_pending(self) -> Task | None: ...
    def mark_in_progress(self, task: Task) -> None: ...
    def mark_completed(self, task: Task, output_content: str | None = None, ...) -> None: ...
    def is_task_blocked(self, task: Task) -> tuple[bool, int | None, str | None]: ...
    # ... other methods ...
```

Implementations:
- `SqliteTaskStore` - local file, single engineer
- `PostgresTaskStore` - remote DB, team/distributed

### Configuration

```yaml
# gza.yaml

# Local mode (default)
database:
  type: sqlite
  # path auto-derived: ~/.gza/{project}.db

# Team mode
database:
  type: postgres
  url: postgres://user:pass@host/gza_db
```

## Benefits

1. **Works locally** - SQLite stores content, no file coordination
2. **Works distributed** - Postgres stores content, any container can read
3. **No file copying** - Content flows through prompt
4. **Human-friendly** - Plan file still exists on disk for review
5. **Backward compatible** - Falls back to file read if `output_content` is NULL

## Migration Path

1. Add `output_content` column (nullable)
2. Update `mark_completed()` to populate it
3. Update `_build_context_from_chain()` to use `_get_task_output()`
4. Existing tasks without `output_content` fall back to file read
5. Later: Add PostgresTaskStore implementation

## Trade-offs

- **Larger DB** - Plans stored as text (typically 1-10KB each)
- **Prompt size** - Full plan embedded in prompt uses more tokens
- **Duplication** - Content in both file and DB (local mode)

These are acceptable given the benefit of reliable plan access across execution contexts.
