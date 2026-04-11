---
name: gza-task-resume
description: Resume a failed/max-turns gza task inline in the current conversation, picking up where the agent left off
allowed-tools: Read, Edit, Write, Glob, Grep, Bash(uv run:*), Bash(git:*), Bash(mkdir:*), Bash(ls:*), AskUserQuestion
version: 1.0.0
public: true
---

# Resume Failed Gza Task Inline

Resume a failed or max-turns gza task directly in the current conversation, picking up from the actual state of the branch.

## Process

### Step 1: Get task ID

The user should provide a full prefixed task ID (for example, `gza-1234`). Extract it from the input. If no task ID is provided, list recent failed tasks:

```bash
uv run python -c "
from gza.db import SqliteTaskStore
from gza.config import Config
config = Config.load()
store = SqliteTaskStore(config.db_path)
tasks = store.query(status=['failed', 'max_turns'], limit=10)
for t in tasks:
    print(f'#{t.id}  {t.status:<10}  {t.task_type:<10}  {t.prompt[:80]}')
"
```

Ask the user which task to resume.

### Step 2: Get task details

```bash
uv run python -c "from gza.db import get_task; import json; print(json.dumps(get_task(<ID>), indent=2, default=str))"
```

Verify the task status is `failed` or `max_turns`. If it's something else, warn the user and ask if they want to proceed.

Note these fields:
- `prompt` — the original task prompt
- `task_type` — task, implement, improve, explore, plan, review
- `branch` — the branch with partial work (may be NULL)
- `session_id` — the Claude session ID (for reference, we can't resume sessions inline)
- `log_file` — path to execution log

### Step 3: Build the full task prompt

Use `gza show --prompt` to get the complete prompt with all context injection:

```bash
uv run gza show --prompt <TASK_ID>
```

This outputs JSON with: `task_id`, `task_type`, `task_slug`, `branch`, `prompt`, `report_path`, `summary_path`, `verify_command`.

Save the `prompt`, `report_path`, `summary_path`, and `verify_command` for later.

### Step 4: Assess the actual state of the branch

If the task has a branch, you need to work from it. There are several possibilities:

1. **You're already on the branch** (e.g., in a worktree for it) — just proceed.
2. **You're on a different branch** — check it out: `git checkout <branch>`
3. **The branch doesn't exist locally** — the worktree may have been cleaned up. Check `git branch -a` for remote copies.

Determine which case applies:

```bash
git branch --show-current
```

Then assess what work was actually completed:

```bash
git log main..<branch> --oneline
git diff main..<branch> --stat
git status
git diff --stat
```

**This is the critical step.** Based on the git output — not any prior session claims — determine:
- What commits exist (work that was saved)
- What uncommitted changes exist (work in progress that survived)
- What the diff looks like compared to main

Report this assessment to the user before proceeding. For example:
> Branch `20260408-add-auth` has 3 commits ahead of main, touching 5 files. No uncommitted changes. The last commit message is "add JWT validation middleware". Based on the diff, token generation is done but the refresh flow is not implemented yet.

### Step 5: Read the log tail (optional but recommended)

If the task has a `log_file`, read the last ~100 lines to understand what the agent was doing when it stopped:

Read the log file tail to see:
- What was the agent working on last?
- Did it hit a specific error loop?
- Was it making progress or stuck?

Share a brief summary with the user:
> The agent was stuck in a loop trying to fix a mypy error in `src/auth/tokens.py` — it ran pytest 8 times with the same failure.

### Step 6: Confirm plan with user

Before executing, tell the user:
1. What work is already done (from Step 4)
2. What remains to be done (based on the original prompt vs completed work)
3. Any issues spotted in the logs (from Step 5)

Ask: "Should I continue with this, or would you like to adjust the approach?"

### Step 7: Mark the original task and create tracking

Mark the original task as failed (if it isn't already) and set the current one to in_progress:

```bash
uv run gza set-status <TASK_ID> in_progress
```

### Step 8: Execute the remaining work

Now **continue the task**. You already have:
- The full prompt from Step 3
- The actual state from Step 4
- Knowledge of what's done vs remaining

Follow the task-type-specific instructions from the built prompt, but skip work that's already completed based on your git assessment:

- For **task/implement/improve** types: Make the remaining code changes, run the verify command, write a summary to `summary_path`.
- For **explore** types: Continue research and write/update findings to `report_path`.
- For **plan** types: Continue the design and write/update the plan to `report_path`.
- For **review** types: Continue the review and write/update to `report_path`.

**Important**: If the previous agent was stuck in a loop on a specific issue, take a different approach rather than repeating the same pattern.

### Step 9: Commit your changes

Stage and commit all changes:

```bash
git add <changed_files>
git commit -m "<descriptive message>"
```

### Step 10: Run verification

If there's a `verify_command`, run it and fix any issues:

```bash
uv run <verify_command>
```

Iterate until verification passes.

### Step 11: Mark task as completed

```bash
uv run gza mark-completed <TASK_ID> --branch <BRANCH_NAME>
```

If the task produced a report or summary file, persist it:

```bash
uv run python -c "
from pathlib import Path
from gza.config import Config
from gza.db import SqliteTaskStore
config = Config.load()
store = SqliteTaskStore(config.db_path)
task = store.get(<TASK_ID>)
p = Path('<REPORT_OR_SUMMARY_PATH>')
if p.exists():
    task.report_file = str(p.relative_to(config.project_dir))
    task.output_content = p.read_text()
    store.update(task)
    print('Report persisted')
"
```

## Important notes

- **Trust git, not prior claims**: The previous session may have claimed work was done that wasn't actually committed. Always verify against git state.
- **No worktree creation**: Unlike background `gza resume` which creates a worktree for the branch, this runs in the current working tree. You may already be in a worktree for the task's branch, or you may need to `git checkout` the branch.
- **Different approach for loops**: If the log shows the agent was stuck in a loop, don't repeat the same strategy. Try a fundamentally different approach.
- **Verify command**: Always run verification before marking complete, even if the previous agent claimed tests passed.
- **Branch reuse**: This skill reuses the existing branch — it doesn't create a new one.
- **Session continuity**: We cannot resume the actual Claude session inline (that requires `--resume` flag in background mode). Instead, we pick up the work based on the git state.
