---
name: gza-task-run
description: Run a gza task inline in the current conversation, using the same prompt that background execution would use
allowed-tools: Read, Edit, Write, Glob, Grep, Bash(uv run:*), Bash(git:*), Bash(mkdir:*), Bash(ls:*), AskUserQuestion
version: 2.0.0
public: true
---

# Run Gza Task Inline

Run a pending gza task directly in the current conversation, using the same prompt that background execution would build.

## Process

### Step 1: Get task ID

The user should provide a full prefixed task ID (for example, `gza-1234`). Extract it from the input. If no task ID is provided, list pending tasks and ask:

```bash
uv run gza next
```

### Step 2: Build the task prompt

Use `gza show --prompt` to build the exact prompt that background execution would use:

```bash
uv run gza show --prompt <TASK_ID>
```

This outputs JSON with: `task_id`, `task_type`, `task_slug`, `branch`, `prompt`, `report_path`, `summary_path`, `verify_command`.

If you need to edit the task prompt before running, use `gza edit`:

```bash
uv run gza edit <TASK_ID> --prompt "updated prompt text"
```

Then re-run `gza show --prompt` to get the updated built prompt.

### Step 3: Mark task as in-progress

```bash
uv run gza set-status <TASK_ID> in_progress
```

### Step 4: Create a branch and execute the task

Create a branch for the task if one doesn't already exist:

```bash
git checkout -b <branch_name>
```

Use the branch name from the task if it has one, otherwise use the task slug (e.g., `20260326-task-slug`).

Before executing, capture the output path from Step 2:

- For **task/implement/improve/rebase** tasks, use `summary_path`.
- For **explore/plan/review** tasks, use `report_path`.

Create the parent directory first if needed:

```bash
mkdir -p "$(dirname <OUTPUT_PATH>)"
```

Now **execute the instructions from the built prompt**. The prompt from Step 2 contains the full task description and type-specific instructions. Follow them as if you were the agent running the task:

- For **task/implement/improve** types: Make the code changes described, run the verify command if one is specified, and write a summary to the `summary_path`.
- For **rebase** types: Perform the rebase task instructions and write a summary to the `summary_path` if one was provided.
- For **explore** types: Research the topic and write findings to the `report_path`.
- For **plan** types: Design the approach and write the plan to the `report_path`.
- For **review** types: Review the code and write the review to the `report_path` following the exact output format in the prompt.

Do not skip the output artifact. Inline runs do not automatically capture this conversation as task output; the file you write here is what later `gza show`, summaries, and follow-up automation can read.

### Step 5: Commit your changes

After completing the task, stage and commit all changes:

```bash
git add <changed_files>
git commit -m "<descriptive message>"
```

### Step 6: Persist task output and mark task as completed

Persist the output file you wrote in Step 4 before marking the task completed.

- If you wrote a summary file, store it as task output.
- If you wrote a report file, store it as both `report_file` and task output.

```bash
uv run python -c "
from pathlib import Path
from gza.config import Config
from gza.db import SqliteTaskStore

config = Config.load()
store = SqliteTaskStore(config.db_path)
task = store.get('<TASK_ID>')
p = Path('<OUTPUT_PATH>')
if not p.exists():
    raise SystemExit(f'Missing task output: {p}')

content = p.read_text()
rel = str(p.relative_to(config.project_dir))
task.output_content = content
if '<OUTPUT_KIND>' == 'report':
    task.report_file = rel
store.update(task)
print(f'Persisted {rel}')
"
```

Then mark the task completed:

```bash
uv run gza mark-completed <TASK_ID> --branch <BRANCH_NAME>
```

## Important notes

- **Same prompt as background**: `gza show --prompt` calls the same `build_prompt()` function that `gza run` uses. Identical instructions, context injection, and type-specific templates.
- **No worktree**: Unlike background execution, this runs directly on the current working tree. Changes are made in-place.
- **Output persistence is explicit**: Inline runs must write the summary/report file and persist it into the task record. The task log does not automatically contain this conversation.
- **Branch management**: Create a new branch for the task work, just like background execution would.
- **Editing prompts**: Use `gza edit <task_id> --prompt "..."` to modify a task's prompt before running. Supports `--prompt-file` for multi-line prompts and `--prompt -` to read from stdin.
- **Proper status tracking**: Uses `mark-completed` to ensure correct `merge_status` so tasks appear in `gza unmerged` and work with `gza advance`.
- **Failed tasks can be re-run**: Tasks with status "failed" can also be run inline — useful for debugging failures interactively.
- **Verify command**: For task/implement/improve types, the built prompt already includes the verify command instruction. Follow it.
