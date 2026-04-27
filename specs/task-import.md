# Task Import

## Overview

Import tasks from a YAML file into gza. Useful for:
- Bulk-creating tasks from a spec or plan
- Sharing task templates across projects
- Generating tasks from external tools

## Usage

```bash
gza import <file>

# Preview without creating tasks
gza import <file> --dry-run

# Force re-import (skip duplicate detection)
gza import <file> --force
```

## File Format

```yaml
# Optional: default tags for all tasks in this file
tags: [task-chaining]

# Optional: spec file that provides context for plan/implement tasks
spec: specs/task-chaining.md

tasks:
  - prompt: |
      Design the schema changes for task chaining.
      Consider migration strategy and backwards compatibility.
    type: plan

  - prompt: |
      Implement schema changes per the plan.
      Add tags, depends_on, create_review fields to db.py.
    type: implement
    depends_on: 1
    review: true

  - prompt: |
      Update get_next_pending() to skip blocked tasks.
    type: task
    depends_on: 2

  - prompt: |
      Add gza tags command.
    type: task
    depends_on: 2
    tags: []  # override: no tags for this task
```

## Task Fields

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `prompt` | string | required | The task prompt |
| `type` | string | `task` | Task type: `task`, `explore`, `plan`, `implement`, `review` |
| `tags` | list[string] | file-level default | Flat tag list; use `[]` to override file default |
| `group` | string | none | Legacy alias converted to a one-item `tags` list |
| `depends_on` | int | none | Local index (1-based) of task this depends on |
| `review` | bool | `false` | Auto-create review task on completion |
| `spec` | string | file-level default | Path to spec file for context |

## File-Level Defaults

Fields at the root level apply to all tasks unless overridden:

```yaml
tags: [my-feature]     # all tasks get this tag
spec: specs/foo.md     # all tasks reference this spec

tasks:
  - prompt: "Task 1"
    # inherits tags: [my-feature], spec: specs/foo.md

  - prompt: "Task 2"
    tags: [other-group]  # override tags

  - prompt: "Task 3"
    tags: []            # no tags
    spec: null          # no spec context
```

## Dependency Resolution

The `depends_on` field uses 1-based local indices within the file:

```yaml
tasks:
  - prompt: "First task"      # index 1
  - prompt: "Second task"     # index 2
    depends_on: 1             # depends on "First task"
  - prompt: "Third task"      # index 3
    depends_on: 2             # depends on "Second task"
```

On import, gza:
1. Creates all tasks in order
2. Maps local indices to actual task IDs
3. Sets `depends_on` to the real IDs

Example: if "First task" gets ID #47, then "Second task" will have `depends_on: 47`.

## Duplicate Detection

By default, `gza import` checks for duplicate tasks based on normalized prompt content plus normalized tag set. If a matching pending task exists, it's skipped:

```
$ gza import tasks.yaml
Importing 5 tasks...
  ✓ Created: Design schema changes (#47)
  - Skipped: Implement schema (duplicate of #48)
  ✓ Created: Add tags command (#49)
  ...
Imported 4 tasks (1 skipped)
```

Use `--force` to skip duplicate detection and create all tasks.

## Dry Run

Preview what would be imported without creating tasks:

```
$ gza import tasks.yaml --dry-run
Would import 5 tasks:
  1. [plan] Design schema changes (tags: task-chaining)
  2. [implement] Implement schema (depends on #1, review: true)
  3. [task] Update get_next_pending (depends on #2)
  4. [task] Add groups command (depends on #2)
  5. [task] Add status command (depends on #2)
```

## Spec Context

When a task has a `spec` field (or inherits from file-level), the runner includes the spec file contents as context when executing the task.

For `plan` tasks: agent reads the spec to understand requirements.
For `implement` tasks: agent reads both the spec and the generated plan.
For `review` tasks: agent reads spec, plan, and implementation diff.

## Example: Full Workflow

```yaml
# specs/auth-feature.tasks.yaml
tags: [auth-feature]
spec: specs/auth-feature.md

tasks:
  # Phase 1: Planning
  - prompt: |
      Design the authentication system.
      Consider: session management, token storage, OAuth providers.
      Output detailed technical plan.
    type: plan

  # Phase 2: Implementation (with auto-review)
  - prompt: |
      Implement the auth system per the plan.
      Start with the core session management.
    type: implement
    depends_on: 1
    review: true

  - prompt: |
      Add OAuth provider support per the plan.
    type: implement
    depends_on: 2
    review: true

  # Phase 3: Integration
  - prompt: |
      Add auth middleware to API routes.
    type: task
    depends_on: 3

  - prompt: |
      Add login/logout UI components.
    type: task
    depends_on: 3
```

```bash
$ gza import specs/auth-feature.tasks.yaml
Importing 5 tasks...
  ✓ Created: Design auth system (#50, plan)
  ✓ Created: Implement core session (#51, depends on #50)
  ✓ Created: Add OAuth support (#52, depends on #51)
  ✓ Created: Add auth middleware (#53, depends on #52)
  ✓ Created: Add login UI (#54, depends on #52)
Imported 5 tasks

$ gza search --tag auth-feature
  ○ 50. Design auth system                    pending (plan)
  ○ 51. Implement core session                pending (blocked by #50)
  ○ 52. Add OAuth support                     pending (blocked by #51)
  ○ 53. Add auth middleware                   pending (blocked by #52)
  ○ 54. Add login UI                          pending (blocked by #52)
```

## Validation

Import validates the following before creating any tasks:

1. **YAML syntax** - File must be valid YAML
2. **Required fields** - Each task must have a `prompt`
3. **Spec files exist** - All referenced spec files (file-level and per-task) must exist
4. **Dependency indices valid** - `depends_on` must reference a valid task index (1 to N)
5. **No circular dependencies** - Task cannot depend on itself or create cycles

If validation fails, no tasks are created and the error is reported:

```
$ gza import tasks.yaml
Error: Spec file not found: specs/missing.md
  Referenced by: file-level default

$ gza import tasks.yaml
Error: Invalid depends_on: 5 (only 3 tasks in file)
  Task 2: "Implement schema changes"
```

## Design Decisions

1. **Append-only**: Import only creates new tasks. There's no update/sync capability since tasks lack a stable external ID to match on. Re-running import with `--force` creates duplicates.

2. **No export**: While `gza export --tag <tag>` could generate YAML from existing tasks, the complexity (converting IDs back to local indices, deciding what to include) isn't worth it for the limited use case. Task structures are typically project-specific.
