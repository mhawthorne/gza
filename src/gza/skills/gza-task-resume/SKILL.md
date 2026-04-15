---
name: gza-task-resume
description: Compatibility wrapper that routes task resumption to first-class gza CLI resume/run-inline workflows
allowed-tools: Bash(uv run:*), AskUserQuestion
version: 2.0.0
public: true
---

# Resume Gza Task (Compatibility)

Use first-class gza commands instead of manual inline reconstruction.

## Process

### Step 1: Get task ID

Require a full prefixed task ID (for example, `gza-1234`).

If no task ID is provided, list recent failed tasks and ask which one to resume:

```bash
uv run gza history --status failed --limit 10
```

### Step 2: Choose resume path

Preferred tracked resume flow:

```bash
uv run gza resume <TASK_ID>
```

If the user explicitly wants foreground inline execution through runner orchestration:

```bash
uv run gza run-inline <TASK_ID> --resume
```

Optional flags when requested:

```bash
uv run gza resume <TASK_ID> --no-docker
uv run gza resume <TASK_ID> --background
uv run gza run-inline <TASK_ID> --resume --no-docker
```

### Step 3: Report outcome

Report exit status and direct the user to `gza log <TASK_ID>` for full details.

## Notes

- Do not manually set task status/provenance for resume flows in this skill.
- Do not reconstruct runner lifecycle steps; let the CLI command own worktree, provider, and artifact handling.
