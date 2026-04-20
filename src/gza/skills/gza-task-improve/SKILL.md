---
name: gza-task-improve
description: Address review comments for a gza task inline — reads the most recent review, checks out the branch, fixes must-fix items, runs verify, and commits
allowed-tools: Read, Edit, Write, Glob, Grep, Bash(uv run:*), Bash(git:*), Bash(mkdir:*), Bash(ls:*), Bash(cd:*), AskUserQuestion
version: 1.0.0
public: true
---

# Improve Gza Task Inline

Address review comments for a gza task directly in the current conversation. This is useful when a task has reached max review/improve cycles and needs human-guided fixes, or when you want to interactively resolve review feedback.

## Process

### Step 0: Capture the starting checkout

Before touching task state, capture where the user started:

```bash
git symbolic-ref --quiet --short HEAD || git rev-parse --short HEAD
```

Save this as `<START_CHECKOUT>`. You may switch to the implementation branch to make changes, but before finishing you must return the user to `<START_CHECKOUT>`. If `<START_CHECKOUT>` is a detached HEAD, restore it with `git checkout --detach <START_CHECKOUT>`.

### Step 1: Get task ID and find the review

The user should provide a full prefixed task ID (for example, `gza-1234`). If they provide a review task ID, resolve it to the implementation task. If no task ID is provided, ask the user.

Query the task and find its most recent review:

```bash
uv run python -c "
import json, sys
from pathlib import Path
from gza.config import Config
from gza.db import SqliteTaskStore

config = Config.load(Path.cwd())
store = SqliteTaskStore(config.db_path)
task = store.get(<TASK_ID>)
if not task:
    print('ERROR: Task not found', file=sys.stderr)
    sys.exit(1)

# If the user gave us a review task, resolve to its parent implementation
impl_task = task
if task.task_type == 'review' and task.depends_on:
    impl_task = store.get(task.depends_on)
elif task.task_type == 'improve' and task.based_on:
    impl_task = store.get(task.based_on)

# Find latest review
assert impl_task.id is not None
reviews = store.get_reviews_for_task(impl_task.id)
latest_review = reviews[0] if reviews else None

print(json.dumps({
    'impl_task_id': impl_task.id,
    'impl_task_type': impl_task.task_type,
    'impl_branch': impl_task.branch,
    'impl_prompt': impl_task.prompt,
    'review_task_id': latest_review.id if latest_review else None,
    'review_report_file': latest_review.report_file if latest_review else None,
    'review_output': latest_review.output_content if latest_review else None,
    'verify_command': config.verify_command,
}, default=str))
"
```

Replace `<TASK_ID>` with the actual full prefixed task ID.

### Step 2: Read the review

Read the review report file (from `review_report_file` in Step 1 output). If the report file doesn't exist on disk, use `review_output` from the database.

The review file follows a structured format with:
- **Must-Fix** items (M1, M2, etc.) — these are blockers that must be resolved
- **Suggestions** (S1, S2, etc.) — optional improvements
- **Questions/Assumptions** — may need user input

Present a summary of the must-fix items to the user before proceeding.

### Step 3: Check out the implementation branch

```bash
git checkout <impl_branch>
```

If the branch is checked out in another worktree, inform the user and ask how to proceed. Options:
- Work in the existing worktree path
- Create a new worktree

If `<START_CHECKOUT>` already equals `<impl_branch>`, do not switch away and back unnecessarily.

### Step 4: Address must-fix items

For each must-fix item in the review:

1. **Read the relevant source files** mentioned in the review
2. **Make the fix** as described in the review's "Required fix" section
3. **Mark progress** — tell the user which item you're working on (e.g., "Fixing M1: Missing logging import")

Focus on must-fix items first. Only address suggestions if the user asks.

### Step 5: Run verify command

If the task has a `verify_command` configured:

```bash
uv run gza config | grep verify_command
```

Run the verify command and fix any errors, up to 3 iterations (same as gza-test-and-fix):

1. Run the verify command
2. If errors, fix them in files on the branch
3. Repeat until clean or 3 iterations

### Step 6: Commit changes

Stage and commit all changes. A successful `/gza-task-improve` run always ends with a commit; do not leave review fixes uncommitted.

```bash
git add <changed_files>
git commit -m "Address review feedback for task #<IMPL_TASK_ID>

- M1: <brief description of fix>
- M2: <brief description of fix>
..."
```

### Step 7: Push the implementation branch

After the commit succeeds, push the implementation branch so the user does not need to manually publish the review fixes:

```bash
git push -u origin <impl_branch>
```

If the branch already has an upstream, a plain `git push` is fine. If the push fails, stop and tell the user exactly what happened. Do not report the improve workflow as fully complete when the changes are still only local.

### Step 8: Persist improve output and clear review state (required)

After a successful commit and push, always create a completed improve task row and summary artifact, then clear review state for the implementation task.

Use the `review_task_id` already resolved in Step 1, then call `gza show --prompt` on the newly created improve task ID to get the canonical `summary_path` (same source of truth as `get_task_output_paths()`), write the summary there with an origin header, persist `report_file` + `output_content`, and call `store.clear_review_state(<IMPL_TASK_ID>)`.

```bash
uv run python -c "
import json
from datetime import datetime, timezone
from pathlib import Path
from gza.config import Config
from gza.db import SqliteTaskStore
from gza.git import Git
from gza.runner import _compute_slug_override, generate_slug
import subprocess

config = Config.load(Path.cwd())
store = SqliteTaskStore(config.db_path)

origin_date = datetime.now(timezone.utc).strftime('%Y-%m-%d')
summary_body = '''Addressed <M_COUNT> must-fix items: <M_ITEMS_SUMMARY>
Verify: <VERIFY_RESULT>
Commit: <COMMIT_SHA>
Push: pushed to <IMPL_BRANCH>'''
summary_with_origin = f'<!-- origin: /gza-task-improve (manual, {origin_date}) -->\n' + summary_body

created = store.add(
    prompt='Manual improve via /gza-task-improve',
    task_type='improve',
    depends_on='<REVIEW_TASK_ID>',
    based_on='<IMPL_TASK_ID>',
)
assert created.id is not None

if created.slug is None:
    slug_override = _compute_slug_override(created, store)
    created.slug = generate_slug(
        created.prompt,
        existing_id=None,
        log_path=config.log_path,
        git=Git(config.project_dir),
        project_name=config.project_name,
        project_prefix=config.project_prefix,
        slug_override=slug_override,
        branch_strategy=config.branch_strategy,
        explicit_type=created.task_type_hint,
    )
    store.update(created)

prompt_json = subprocess.check_output(
    ['uv', 'run', 'gza', 'show', '--prompt', created.id],
    text=True,
)
prompt_data = json.loads(prompt_json)
summary_path = Path(prompt_data['summary_path'])
summary_path.parent.mkdir(parents=True, exist_ok=True)
summary_path.write_text(summary_with_origin)

created.report_file = str(summary_path.relative_to(config.project_dir))
created.status = 'completed'
created.completed_at = datetime.now(timezone.utc)
created.output_content = summary_body
store.update(created)
store.clear_review_state('<IMPL_TASK_ID>')
print(f'Improve saved as task #{created.id} ({created.report_file}); review state cleared')
"
```

### Step 9: Restore the starting checkout

After persisting the improve task, return the user to the checkout captured in Step 0:

```bash
git checkout <START_CHECKOUT>
```

If `<START_CHECKOUT>` was a detached HEAD, restore it with:

```bash
git checkout --detach <START_CHECKOUT>
```

If the restore fails, stop and tell the user exactly what checkout you left them on. Do not silently finish on the task branch.

## Important notes

- **Must-fix items are the priority** — address all must-fix items before considering suggestions.
- **Read before editing** — always read the source files before making changes, even if the review quotes code. The code may have changed since the review was written.
- **Verify the review's claims** — review comments can be wrong. If a review item doesn't match the current code state (e.g., the import already exists), skip it and note that to the user.
- **Scope to branch files** — only modify files that are part of the implementation branch's diff. Use `git diff --name-only main..HEAD` to check.
- **Commit and push are required** — a successful `/gza-task-improve` run should leave the implementation branch committed and pushed before you restore the user's original checkout.
- **Ask about suggestions** — don't automatically apply S1/S2/etc. suggestions. Ask the user which ones they want addressed.
- **Questions section** — if the review has questions, present them to the user for answers before making assumptions.
- **Restore the user's checkout before exit** — the skill may work on `<impl_branch>`, but the final state should return the user to `<START_CHECKOUT>` and the closing message should name both checkouts explicitly.
