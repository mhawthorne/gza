---
name: gza-task-run
description: Run a gza task inline in the current conversation, using the same prompt that background execution would use
allowed-tools: Read, Edit, Write, Glob, Grep, Bash(uv run:*), Bash(git:*), Bash(mkdir:*), Bash(ls:*), AskUserQuestion
version: 2.2.0
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

### Step 4: Initialize runner-like log metadata for all task types

Before executing, initialize and persist task log metadata like the runner does:

- Create `.gza/logs/<task_slug>.log` under `config.log_path`.
- Persist `task.log_file` immediately so it exists even if the inline run is interrupted.
- Write synthetic JSONL provenance entries to the log.

```bash
uv run python -c "
import os
from pathlib import Path
from gza.config import Config
from gza.db import SqliteTaskStore
from gza.runner import get_effective_config_for_task, write_log_entry, write_worker_start_event

config = Config.load(Path.cwd())
store = SqliteTaskStore(config.db_path)
task = store.get('<TASK_ID>')
if task is None:
    raise SystemExit('Task not found')

config.log_path.mkdir(parents=True, exist_ok=True)
log_file = config.log_path / f'{task.slug}.log'
task.log_file = str(log_file.relative_to(config.project_dir))
store.update(task)

model, provider_name, _ = get_effective_config_for_task(task, config)
write_worker_start_event(log_file, resumed=False)
write_log_entry(log_file, {'type': 'gza', 'subtype': 'info', 'message': f'Task: {task.id} {task.slug}'})
write_log_entry(log_file, {'type': 'gza', 'subtype': 'branch', 'message': f'Branch: {task.branch or "<none>"}', 'branch': task.branch})
write_log_entry(log_file, {'type': 'gza', 'subtype': 'info', 'message': f'Provider: {provider_name}, Model: {model or "default"}'})
write_log_entry(log_file, {'type': 'gza', 'subtype': 'provenance', 'message': 'Execution mode: inline skill gza-task-run', 'skill': 'gza-task-run', 'inline': True, 'pid': os.getpid()})

print(task.log_file)
"
```

### Step 5: Type-specific execution setup

For **task/implement/improve/rebase** tasks only:

- Create or switch to the task branch.
- Persist `task.branch` in the DB only for these git-verified task types.

```bash
git checkout -b <branch_name>
```

Use the branch name from the task if it has one, otherwise use the task slug (e.g., `20260326-task-slug`).

```bash
uv run python -c "
from pathlib import Path
from gza.config import Config
from gza.db import SqliteTaskStore

config = Config.load(Path.cwd())
store = SqliteTaskStore(config.db_path)
task = store.get('<TASK_ID>')
if task is None:
    raise SystemExit('Task not found')

if '<BRANCH_NAME>' and task.branch != '<BRANCH_NAME>':
    task.branch = '<BRANCH_NAME>'
    store.update(task)
"
```

For **explore/plan/review** tasks:

- Do **not** create a new task branch.
- Do **not** persist a new `task.branch` value.
- Review-task branch context comes from the implementation branch/worktree being reviewed, not from creating a new review-task branch.

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

### Step 6: Commit code changes (code-task types only)

For **task/implement/improve/rebase** tasks, after completing the task, stage and commit all code changes:

For **explore/plan/review** tasks, do not create a task commit; write the report artifact and use status-only completion.

```bash
git add <changed_files>
git commit -m "<descriptive message>"
```

### Step 7: Persist task output, run completion checks, then finalize success outcome log

Persist the output file you wrote in Step 4 before marking the task completed.

- If you wrote a summary file, store it as task output.
- If you wrote a report file, store it as both `report_file` and task output.

Write artifact metadata to the synthetic log and keep `task.log_file` explicitly persisted.

```bash
uv run python -c "
from pathlib import Path
from gza.config import Config
from gza.db import SqliteTaskStore
from gza.runner import write_log_entry

config = Config.load(Path.cwd())
store = SqliteTaskStore(config.db_path)
task = store.get('<TASK_ID>')
if task is None:
    raise SystemExit('Task not found')

if not task.log_file:
    raise SystemExit('task.log_file is not set; initialize Step 4 first')

p = Path('<OUTPUT_PATH>')
if not p.exists():
    raise SystemExit(f'Missing task output: {p}')

content = p.read_text()
rel = str(p.relative_to(config.project_dir))
task.output_content = content
if '<OUTPUT_KIND>' == 'report':
    task.report_file = rel
store.update(task)

log_path = config.project_dir / task.log_file
write_log_entry(log_path, {'type': 'gza', 'subtype': 'artifact', 'message': f'Output artifact: {rel}', 'path': rel})

print(f'Persisted {rel}')
"
```

Then mark the task completed:

```bash
uv run gza mark-completed <TASK_ID>
```

For **explore/plan/review** tasks, `mark-completed` uses status-only completion by default.

Only after `mark-completed` succeeds, append a successful outcome entry:

```bash
uv run python -c "
from pathlib import Path
from gza.config import Config
from gza.db import SqliteTaskStore
from gza.runner import write_log_entry

config = Config.load(Path.cwd())
store = SqliteTaskStore(config.db_path)
task = store.get('<TASK_ID>')
if task and task.log_file:
    write_log_entry(config.project_dir / task.log_file, {'type': 'gza', 'subtype': 'outcome', 'message': 'Outcome: completed (inline skill)', 'exit_code': 0})
"
```

### Step 8: Failure-path consistency (if inline execution fails)

If the inline run cannot be completed, preserve diagnostic state instead of leaving a partially tracked task:

1. Append a failure `outcome` entry to `task.log_file` with a concise reason and `failure_reason` code.
2. Set task status back to `failed` and include `--reason` where possible.

Example:

```bash
uv run python -c "
from pathlib import Path
from gza.config import Config
from gza.db import SqliteTaskStore
from gza.runner import write_log_entry

config = Config.load(Path.cwd())
store = SqliteTaskStore(config.db_path)
task = store.get('<TASK_ID>')
if task and task.log_file:
    write_log_entry(config.project_dir / task.log_file, {'type': 'gza', 'subtype': 'outcome', 'message': 'Outcome: failed (inline skill)', 'failure_reason': '<FAILURE_REASON>'})
"
uv run gza set-status <TASK_ID> failed --reason <FAILURE_REASON>
```

## Important notes

- **Same prompt as background**: `gza show --prompt` calls the same `build_prompt()` function that `gza run` uses. Identical instructions, context injection, and type-specific templates.
- **No worktree**: Unlike background execution, this runs directly on the current working tree. Changes are made in-place.
- **Output persistence is explicit**: Inline runs must write the summary/report file and persist it into the task record. The task log does not automatically contain this conversation.
- **`log_file` persistence is explicit**: Set `task.log_file` before execution and keep it in DB updates so `gza log`, query views, and debugging flows behave consistently.
- **Synthetic provenance is intentional**: Inline execution cannot reproduce provider-native telemetry, but synthetic `gza` JSONL entries keep outcome and artifact traces discoverable.
- **Worker lifecycle line is conditional**: Inline runs always write the explicit synthetic `gza` entries shown above. `write_worker_start_event()` only emits `worker_lifecycle:start` when worker-mode env vars are present (`GZA_WORKER_MODE=1` and `GZA_WORKER_ID` set).
- **Branch and commit scope**: Branch creation, `task.branch` persistence, and commits apply only to git-verified types (`task`/`implement`/`improve`/`rebase`). `explore`/`plan`/`review` runs should persist log + output artifacts and finish with status-only completion.
- **Editing prompts**: Use `gza edit <task_id> --prompt "..."` to modify a task's prompt before running. Supports `--prompt-file` for multi-line prompts and `--prompt -` to read from stdin.
- **Proper status tracking**: For git-verified task types, Step 5 persists `task.branch`; then `mark-completed <TASK_ID>` reads that stored branch and applies normal completion logic. This keeps `merge_status` correct so tasks appear in `gza unmerged` and work with `gza advance`.
- **Expected warning behavior**: `mark-completed` may print a warning when status is not `failed`; this is expected for inline runs that set `in_progress` first and does not block completion.
- **Failed tasks can be re-run**: Tasks with status "failed" can also be run inline — useful for debugging failures interactively.
- **Verify command**: For task/implement/improve types, the built prompt already includes the verify command instruction. Follow it.
