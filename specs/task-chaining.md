# Task Chaining and Planning Workflow

## Overview

This spec describes extensions to gza for supporting multi-phase development workflows where tasks can depend on each other, be grouped together, and automatically trigger follow-up tasks like reviews.

## Motivation

For complex work (like a large refactor), a single task isn't enough. The workflow looks like:

1. Agent creates a plan
2. Human reviews/approves the plan
3. Agent implements the plan
4. Agent reviews the implementation
5. Human decides next steps

This requires:
- Task dependencies (implementation waits for plan approval)
- Task grouping (see all related tasks at a glance)
- Automatic follow-up tasks (review after implementation)
- Per-task-type configuration (cheaper models for reviews)

## Task Types

Four task types, each with distinct behavior:

| Type | Purpose | Output |
|------|---------|--------|
| `task` | General work (default) | Code changes |
| `explore` | Research, no code expected | `.gza/explorations/{task_id}.md` |
| `plan` | Design/architecture | `.gza/plans/{task_id}.md` |
| `implement` | Build per a plan | Code changes on branch |
| `review` | Evaluate implementation (optional) | `.gza/reviews/{task_id}.md` with verdict |

## Groups

Groups are optional labels for organizing related tasks. They are purely for human comprehension - the `depends_on` field handles execution order separately.

A group can contain:
- Multiple parallel task chains (e.g., "plan transforms" and "plan builders" both in "tarantino-v2")
- Independent tasks that are conceptually related
- A mix of task types

Tasks without a group work exactly as they do today and appear under "(ungrouped)" in status output.

## Schema Changes

New fields on Task:

```python
@dataclass
class Task:
    # ... existing fields ...

    group: str | None = None        # Optional label for related tasks (e.g., "tarantino-v2")
    depends_on: int | None = None   # Task ID that must complete first
    create_review: bool = False     # Auto-create review task on completion
    same_branch: bool = False       # If True, continue on depends_on task's branch instead of creating new
    branch: str | None = None       # Git branch used/created by this task (set by runner)
```

SQL migration:

```sql
ALTER TABLE tasks ADD COLUMN group_name TEXT;
ALTER TABLE tasks ADD COLUMN depends_on INTEGER REFERENCES tasks(id);
ALTER TABLE tasks ADD COLUMN create_review INTEGER DEFAULT 0;
ALTER TABLE tasks ADD COLUMN same_branch INTEGER DEFAULT 0;
ALTER TABLE tasks ADD COLUMN branch TEXT;
```

## Configuration

Per-task-type configuration with fallback to defaults:

```yaml
defaults:
  model: opus
  max_turns: 50

task_types:
  plan:
    max_turns: 30
  implement:
    # inherits defaults
  review:
    model: sonnet
    max_turns: 10
```

## CLI Changes

### Creating tasks

```bash
# Simple task (unchanged)
gza add

# Plan task
gza add --type plan --group tarantino-v2

# Implementation task that depends on plan, with auto-review
gza add --type implement --based-on 1 --review

# Implementation task without review (review is optional)
gza add --type implement --based-on 1

# Explicit review task (alternative to --review flag)
gza add --type review --based-on 2

# Task that continues on the same branch as its dependency
# (for multi-step implementations that build on unmerged code)
gza add --based-on 1 --same-branch
```

Note: `--same-branch` requires `--based-on`. When set, the task will check out and continue working on the branch from the dependency task instead of creating a new branch. This is useful when multiple tasks need to build on each other's code changes before merging.

### Viewing status

```bash
# Show status of a group
gza status tarantino-v2

# Output:
# Group: tarantino-v2
#   ✓ 1. Plan ImageTransform system          completed  12/13
#   → 2. Implement ImageTransform            in_progress
#   ○ 3. Review ImageTransform impl          pending (blocked by #2)

# List all groups
gza groups

# Output:
# tarantino-v2      3 tasks (1 pending, 1 in_progress, 1 completed)
# auth-refactor     2 tasks (2 pending)
# (ungrouped)       5 tasks
```

### Editing tasks

```bash
# Move a task into a group
gza edit 5 --group tarantino-v2

# Remove a task from a group
gza edit 5 --group ""
```

### Listing pending tasks

```bash
gza next

# Output shows runnable tasks and blocked count:
# 1. [plan] Design ImageTransform system
# 3. [task] Fix stats bug
#
# (2 tasks blocked by dependencies)

# To see all pending tasks including blocked:
gza next --all

# Output:
# 1. [plan] Design ImageTransform system
# 2. [implement] Implement ImageTransform (blocked by #1)
# 3. [task] Fix stats bug
# 4. [review] Review ImageTransform (blocked by #2)
```

### Running tasks

```bash
# Run next available task
gza work

# Run a specific task
gza work 3
```

Behavior:
1. `get_next_pending()` skips tasks whose `depends_on` is not completed
2. `gza work <id>` errors if the task is blocked:
   ```
   $ gza work 2
   Error: Task #2 is blocked by task #1 (pending)
   ```
3. After completing an `implement` task with `create_review=True`:
   - Creates review task with `depends_on` set to the implement task
   - Inherits `group` from parent task
   - Immediately executes the review task (auto-run)

## Runner Behavior by Task Type

### Plan tasks

1. Agent receives prompt
2. Agent writes plan to `.gza/plans/{task_id}.md`
3. Task marked complete
4. Human reviews plan offline, then creates implement task if approved

### Implement tasks

1. Agent receives prompt + plan context (from `based_on` chain)
2. Branch handling:
   - If `same_branch=True`: check out the branch from `depends_on` task
   - Otherwise: create a new feature branch
3. Agent implements on branch
4. Task marked complete
5. If `create_review=True`:
   - Review task auto-created
   - Review task auto-executed

### Review tasks

1. Runner builds context:
   - Find implement task via `depends_on`
   - Get branch and compute `git diff main...{branch}`
   - Walk chain to find plan task, include plan contents
2. Agent reviews against plan
3. Agent writes to `.gza/reviews/{task_id}.md`
4. Review includes verdict: `APPROVED`, `CHANGES_REQUESTED`, or `NEEDS_DISCUSSION`
5. Human reads review, decides next steps manually

## Query Changes

`get_next_pending()` must respect dependencies:

```python
def get_next_pending(self) -> Task | None:
    with self._connect() as conn:
        cur = conn.execute(
            """
            SELECT * FROM tasks
            WHERE status = 'pending'
              AND (depends_on IS NULL
                   OR depends_on IN (
                       SELECT id FROM tasks WHERE status = 'completed'
                   ))
            ORDER BY created_at ASC
            LIMIT 1
            """
        )
        row = cur.fetchone()
        return self._row_to_task(row) if row else None
```

## Example Workflow

```bash
# 1. Create a plan task
$ gza add --type plan --group tarantino-v2
# Editor: "Design the ImageTransform system per specs/v2-design.md"

# 2. Run the plan
$ gza work
# Agent writes .gza/plans/20241213-imagetransform.md
# ✓ Task #1 completed

# 3. Human reviews plan offline...

# 4. Create implementation task with auto-review
$ gza add --type implement --based-on 1 --review --group tarantino-v2
# Editor: "Implement ImageTransform per the plan"

# 5. Run implementation (and auto-review)
$ gza work
# Agent implements on branch 20241213-imagetransform-impl
# ✓ Task #2 completed
# Running review task #3...
# ✓ Review completed

# 6. Check status
$ gza status tarantino-v2
# Group: tarantino-v2
#   ✓ 1. Plan ImageTransform system          completed
#   ✓ 2. Implement ImageTransform            completed
#   ✓ 3. Review ImageTransform impl          completed  APPROVED

# 7. Human merges branch, moves to next dimension
```

## Open Questions

1. **Review verdicts**: Should `CHANGES_REQUESTED` block merging somehow, or is it purely informational?
   - Decision: Informational only. Human decides.

2. **Failed reviews**: If the review task itself fails (agent error), what happens?
   - Suggestion: Mark review as failed, don't block. Human investigates.

3. **Re-reviews**: After making changes, how to re-run a review?
   - Suggestion: Create a new review task with `--based-on` pointing to same implement task.

4. **Plan approval gate**: Currently, creating the implement task is implicit approval. Should there be an explicit `gza approve <id>` command?
   - Suggestion: Keep it implicit for now. Explicit approval adds ceremony without clear benefit.
