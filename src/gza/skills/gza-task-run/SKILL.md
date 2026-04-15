---
name: gza-task-run
description: Compatibility wrapper that routes inline task execution to the first-class gza run-inline command
allowed-tools: Bash(uv run:*), AskUserQuestion
version: 3.0.0
public: true
---

# Run Gza Task Inline (Compatibility)

Use the first-class runner command instead of reconstructing lifecycle behavior in this skill.

## Process

### Step 1: Get task ID

Require a full prefixed task ID (for example, `gza-1234`).

If no task ID is provided, list pending tasks and ask which one to run:

```bash
uv run gza next
```

### Step 2: Execute via first-class runner command

Run the task with `run-inline`:

```bash
uv run gza run-inline <TASK_ID>
```

Optional flags (only when the user explicitly requests them):

```bash
uv run gza run-inline <TASK_ID> --resume
uv run gza run-inline <TASK_ID> --no-docker
uv run gza run-inline <TASK_ID> --max-turns <N>
uv run gza run-inline <TASK_ID> --force
```

### Step 3: Report outcome

Report the command exit status and tell the user to use `gza log <TASK_ID>` if they want full runner/provider logs.

## Notes

- `run-inline` is runner-managed task execution (prompt building, worktree setup, provider launch, status transitions, artifacts, and provenance).
- Do not run `gza set-status`, `gza mark-completed`, synthetic `write_log_entry(...)`, or manual branch/log orchestration in this skill.
- Keep this skill as compatibility guidance only; lifecycle ownership stays in the runner.
