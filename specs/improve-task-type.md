# Improve Task Type

## Overview

This spec describes the `improve` task type, which allows addressing feedback from a code review by making changes on the original implementation branch.

## Motivation

After a review task completes with `CHANGES_REQUESTED`, the current workflow requires manual intervention:

1. Read the review
2. Manually check out the implementation branch
3. Make changes
4. Optionally re-run a review

This spec introduces the `improve` task type to automate steps 2-3, creating a clear audit trail:

```
implement #29 → review #30 (CHANGES_REQUESTED) → improve #31 → review #32 (APPROVED)
```

## User Interface

### Creating an improve task

```bash
# Improve implementation task #29 based on its most recent review
gza improve 29

# Equivalent explicit form
gza add --type improve --based-on 29
```

The `gza improve <impl-id>` command:

1. Looks up task #29 (must be an `implement` task)
2. Finds the most recent `review` task where `depends_on = 29`
3. Creates an `improve` task with:
   - `depends_on` → the review task ID (for blocking and context)
   - `based_on` → the implementation task ID (for branch reference)
   - `same_branch: true` → continue on the implementation's branch
   - `group` → inherited from implementation task

### Error cases

```bash
$ gza improve 29
Error: Task #29 has no review. Run a review first:
  gza add --type review --depends-on 29

$ gza improve 30
Error: Task #30 is a review task. Provide the implementation task ID:
  gza improve 29

$ gza improve 99
Error: Task #99 not found
```

## Data Model

The `improve` task type uses existing fields:

```python
Task(
    prompt="Improve implementation based on review #30",
    task_type="improve",
    depends_on=30,        # The review task (blocks until review complete, provides context)
    based_on=29,          # The implementation task (provides branch)
    same_branch=True,     # Continue on implementation's branch
    group="feature-x",    # Inherited from implementation
)
```

### Relationship diagram

```
plan #28
    ↓ based_on
implement #29 (branch: gza/20260129-add-feature)
    ↓ depends_on
review #30 (CHANGES_REQUESTED)
    ↓ depends_on
improve #31 (same_branch=true, based_on=29)
    ↓ depends_on
review #32 (APPROVED)
```

## Runner Behavior

When running an `improve` task:

### 1. Context building

The runner builds context by walking the dependency chain:

```python
if task.task_type == "improve":
    # Get the review we're addressing
    if task.depends_on:
        review_task = store.get(task.depends_on)
        if review_task and review_task.task_type == "review":
            review_content = _get_task_output(review_task, project_dir)
            if review_content:
                context_parts.append("## Review feedback to address:\n")
                context_parts.append(review_content)

    # Get the original plan (via based_on chain)
    if task.based_on:
        impl_task = store.get(task.based_on)
        if impl_task and impl_task.based_on:
            plan_task = _find_task_of_type_in_chain(impl_task.based_on, "plan", store)
            if plan_task:
                plan_content = _get_task_output(plan_task, project_dir)
                if plan_content:
                    context_parts.append("\n## Original plan:\n")
                    context_parts.append(plan_content)
```

### 2. Branch handling

Since `same_branch=True`, the improve task checks out and works on the implementation's existing branch rather than creating a new one.

### 3. Commit strategy

New commits are added to the branch (not squashed). This preserves iteration history:

```
* abc123 Address review feedback: add test coverage
* def456 Address review feedback: validate volume format
* 789xyz Initial implementation of docker_volumes
```

Squashing happens at merge time via `gza merge --squash`, giving you:
- Full history on the feature branch (for debugging/learning)
- Clean single commit on main (for rollback/bisect)

### 4. Auto-review after improve

Like `implement`, the `improve` task can have `create_review=True`:

```bash
gza improve 29 --review
```

This creates a review task after the improve completes, enabling iteration loops.

## CLI Changes

### New command

```bash
gza improve <impl-task-id> [--review]
```

Options:
- `--review`: Auto-create and run a review task after improve completes

### Task type validation

Update valid task types:

```python
# In importer.py and cli.py
if task_type not in ("task", "explore", "plan", "implement", "review", "improve"):
    errors.append(...)
```

## Example Workflow

```bash
# 1. Implementation with auto-review
$ gza add "Add docker_volumes config" --type implement --review
# Creates task #29

$ gza work
# Runs implementation, auto-creates and runs review #30
# Review verdict: CHANGES_REQUESTED

# 2. Check the review
$ gza show 30
# Shows review content with required changes

# 3. Create improve task
$ gza improve 29 --review
# Creates task #31 (improve) with depends_on=#30

$ gza work
# Runs improve on same branch
# Makes changes based on review feedback
# Auto-creates and runs review #32
# Review verdict: APPROVED

# 4. Check final state
$ gza status
#   ✓ 29. implement Add docker_volumes      completed
#   ✓ 30. review                            completed  CHANGES_REQUESTED
#   ✓ 31. improve                           completed
#   ✓ 32. review                            completed  APPROVED
```

## Output Location

Improve tasks don't have a dedicated output directory. Like `implement` tasks, their output is:
- Code changes on the branch
- Optional summary in `.gza/summaries/{task_id}.md`

## Open Questions

### 1. Multiple reviews

If an implementation has multiple reviews (e.g., user ran review twice), which one does `gza improve` use?

**Decision**: Use the most recent review task where `depends_on = impl_id`, ordered by `created_at DESC`.

### 2. Improve without review

Should `gza improve 29` work if there's no review, just using the implementation context?

**Decision**: No. Require a review to exist. If the user wants to iterate without a review, they can use `gza retry 29` or create a new task with `--same-branch`.

### 3. Naming in status output

How should improve tasks appear in status?

**Decision**: Show as `improve #<impl-id>`:

```
✓ 31. improve #29                       completed
```

This makes it clear which implementation is being improved.
