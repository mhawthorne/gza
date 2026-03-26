---
name: gza-task-run
description: Run a gza task inline in the current conversation, using the same prompt that background execution would use
allowed-tools: Read, Edit, Write, Glob, Grep, Bash(uv run:*), Bash(git:*), Bash(mkdir:*), Bash(ls:*), AskUserQuestion
version: 1.0.0
public: true
---

# Run Gza Task Inline

Run a pending gza task directly in the current conversation, using the same prompt that background execution would build.

## Process

### Step 1: Get task ID

The user should provide a task ID (numeric). Extract it from the input. If no task ID is provided, list pending tasks and ask:

```bash
uv run gza next
```

### Step 2: Build the task prompt

Run the following Python script to build the exact prompt that background execution would use. This calls the same `build_prompt()` function used by `gza run`:

```bash
uv run python -c "
import json, sys
from pathlib import Path
from gza.config import Config
from gza.db import SqliteTaskStore
from gza.runner import build_prompt, SUMMARY_DIR, DEFAULT_REPORT_DIR, PLAN_DIR, REVIEW_DIR, INTERNAL_DIR
from gza.git import Git

config = Config.load()
store = SqliteTaskStore(config.db_path)
task = store.get(<TASK_ID>)
if not task:
    print('ERROR: Task not found', file=sys.stderr)
    sys.exit(1)
if task.status not in ('pending', 'failed'):
    print(f'ERROR: Task is {task.status}, expected pending or failed', file=sys.stderr)
    sys.exit(1)

# Determine output paths based on task type (same logic as runner.py)
project_dir = config.project_dir
if task.task_type in ('explore',):
    report_dir = project_dir / DEFAULT_REPORT_DIR
elif task.task_type == 'plan':
    report_dir = project_dir / PLAN_DIR
elif task.task_type == 'review':
    report_dir = project_dir / REVIEW_DIR
elif task.task_type in ('internal', 'learn'):
    report_dir = project_dir / INTERNAL_DIR
else:
    report_dir = None

report_path = None
if report_dir and task.task_id:
    report_dir.mkdir(parents=True, exist_ok=True)
    report_path = report_dir / f'{task.task_id}.md'

summary_path = None
if task.task_type in ('task', 'implement', 'improve') and task.task_id:
    summary_dir = project_dir / SUMMARY_DIR
    summary_dir.mkdir(parents=True, exist_ok=True)
    summary_path = summary_dir / f'{task.task_id}.md'

git = Git(project_dir)
prompt = build_prompt(task, config, store, report_path=report_path, summary_path=summary_path, git=git)

# Output as JSON so we can parse it cleanly
print(json.dumps({
    'task_id': task.id,
    'task_type': task.task_type,
    'task_slug': task.task_id,
    'branch': task.branch,
    'prompt': prompt,
    'report_path': str(report_path) if report_path else None,
    'summary_path': str(summary_path) if summary_path else None,
    'verify_command': config.verify_command,
}))
"
```

Replace `<TASK_ID>` with the actual numeric task ID.

### Step 3: Mark task as in-progress

```bash
uv run python -c "
from gza.config import Config
from gza.db import SqliteTaskStore
from datetime import datetime, timezone
config = Config.load()
store = SqliteTaskStore(config.db_path)
task = store.get(<TASK_ID>)
task.status = 'in_progress'
task.started_at = datetime.now(timezone.utc)
store.update(task)
print('Task marked as in_progress')
"
```

### Step 4: Create a branch and execute the task

Create a branch for the task if one doesn't already exist:

```bash
git checkout -b <branch_name>
```

Use the branch name from the task if it has one, otherwise use the task slug (e.g., `20260326-task-slug`).

Now **execute the instructions from the built prompt**. The prompt from Step 2 contains the full task description and type-specific instructions. Follow them as if you were the agent running the task:

- For **task/implement/improve** types: Make the code changes described, run the verify command if one is specified, and write a summary to the summary_path.
- For **explore** types: Research the topic and write findings to the report_path.
- For **plan** types: Design the approach and write the plan to the report_path.
- For **review** types: Review the code and write the review to the report_path following the exact output format in the prompt.

### Step 5: Commit your changes

After completing the task, stage and commit all changes:

```bash
git add <changed_files>
git commit -m "<descriptive message>"
```

### Step 6: Mark task as completed

```bash
uv run python -c "
from pathlib import Path
from gza.config import Config
from gza.db import SqliteTaskStore
from datetime import datetime, timezone
config = Config.load()
store = SqliteTaskStore(config.db_path)
task = store.get(<TASK_ID>)
task.status = 'completed'
task.completed_at = datetime.now(timezone.utc)
task.has_commits = True
task.branch = '<BRANCH_NAME>'
store.update(task)
print('Task marked as completed')
"
```

If the task produced a report file, also persist it:

```bash
uv run python -c "
from gza.config import Config
from gza.db import SqliteTaskStore
config = Config.load()
store = SqliteTaskStore(config.db_path)
task = store.get(<TASK_ID>)
task.report_file = '<REPORT_PATH_RELATIVE>'
from pathlib import Path
report = Path('<REPORT_PATH_ABSOLUTE>')
if report.exists():
    task.output_content = report.read_text()
store.update(task)
print('Report persisted')
"
```

## Important notes

- **Same prompt as background**: The prompt is built using the exact same `build_prompt()` function that `gza run` uses. This ensures identical instructions, context injection, and type-specific templates.
- **No worktree**: Unlike background execution, this runs directly on the current working tree. Changes are made in-place.
- **Branch management**: Create a new branch for the task work, just like background execution would.
- **Task status tracking**: The task is properly marked as in_progress then completed in the database, so `gza status`, `gza history`, etc. reflect the work.
- **Failed tasks can be re-run**: Tasks with status "failed" can also be run inline — useful for debugging failures interactively.
- **Verify command**: For task/implement/improve types, the built prompt already includes the verify command instruction. Follow it.
