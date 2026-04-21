---
name: gza-task-fix
description: Rescue a stuck gza task inline — diagnoses review/improve churn, verifies each blocker against current code, addresses the open ones, runs verify, and commits. Runs entirely in Claude Code, no background worker.
allowed-tools: Read, Edit, Write, Glob, Grep, Bash(uv run:*), Bash(git:*), Bash(mkdir:*), Bash(ls:*), Bash(cd:*), AskUserQuestion
version: 2.0.0
public: true
---

# Fix Stuck Gza Task Inline

Use this skill when an implementation task is stuck in review/improve churn — the same blockers keep reappearing, or a previous improve/fix pass failed to close them. `fix` is escalation: it **diagnoses why the loop is happening** before making any edits, then applies a bounded repair scoped strictly to blocker closure.

Unlike `/gza-task-improve`, this skill requires you to verify each blocker against the current code before deciding whether a change is needed. A stuck task often already has the fix on disk — in which case the answer is "no change, this was hallucinated-closure churn," not another edit pass.

This skill runs entirely inline in the current Claude Code session. Do not invoke `gza fix` or any background worker — that defeats the purpose of running here.

## Process

### Step 0: Capture the starting checkout

```bash
git symbolic-ref --quiet --short HEAD || git rev-parse --short HEAD
```

Save as `<START_CHECKOUT>`. You must return the user here before finishing. If detached, restore with `git checkout --detach <START_CHECKOUT>`.

### Step 1: Resolve the target task and recent review history

The user provides a full prefixed task ID (implementation, review, improve, or prior fix). Resolve to the implementation task and fetch the last three reviews so you can detect churn:

```bash
uv run python -c "
import json, sys
from pathlib import Path
from gza.config import Config
from gza.db import SqliteTaskStore

config = Config.load(Path.cwd())
store = SqliteTaskStore(config.db_path)
task = store.get('<TASK_ID>')
if not task:
    print('ERROR: task not found', file=sys.stderr); sys.exit(1)

impl = task
if task.task_type in ('review', 'fix') and task.depends_on:
    impl = store.get(task.depends_on)
    if impl and impl.task_type == 'review' and impl.depends_on:
        impl = store.get(impl.depends_on)
elif task.task_type == 'improve' and task.based_on:
    impl = store.get(task.based_on)

reviews = store.get_reviews_for_task(impl.id)[:3] if impl else []
print(json.dumps({
    'impl_id': impl.id if impl else None,
    'impl_branch': impl.branch if impl else None,
    'impl_prompt': impl.prompt if impl else None,
    'impl_group': impl.group if impl else None,
    'verify_command': config.verify_command,
    'reviews': [
        {'id': r.id, 'report_file': r.report_file, 'output_content': r.output_content, 'completed_at': str(r.completed_at)}
        for r in reviews
    ],
}, default=str))
"
```

The latest review is `reviews[0]`; prior reviews are `reviews[1:]`. If there are no reviews, there is no blocker set — stop and ask the user what they want rescued.

### Step 2: Diagnose the loop

Read the latest review's report file (fall back to `output_content` if the file is missing). Extract each Must-Fix item's **file:line evidence** and a short **blocker key** summarizing the topic (e.g., `improve-template-review-only-language`).

Then read the prior 1–2 reviews and look for repeated blocker keys or overlapping evidence. Classify the overall situation:

- **stale_review** — the latest review's claims don't match the current code (review is out of date).
- **prior_fix_missed** — same blocker key appears across 2+ reviews and earlier improve/fix commits didn't close it.
- **scope_creep** — review flags things not in the original task prompt.
- **genuine_open** — the blockers are valid and uncorrected.

Present a one-paragraph loop-diagnosis summary to the user. If the situation is `stale_review` or `scope_creep`, confirm with the user before making any changes — the right outcome may be zero code changes plus an explicit `diagnosed_no_change` handoff.

### Step 3: Check out the implementation branch

```bash
git checkout <impl_branch>
```

If the branch is checked out in another worktree, ask the user whether to work in that worktree path or switch this one. Do not create duplicate worktrees.

If `<START_CHECKOUT>` already equals `<impl_branch>`, do not switch away and back.

### Step 4: Verify each blocker against the current code before editing

For each Must-Fix item in the latest review:

1. **Read the cited file:line location** with the Read tool. The review may be wrong about the current state.
2. **Classify the blocker:**
   - `already_addressed` — code already satisfies the review's "Required fix." Record a file:line citation as evidence. No change.
   - `open` — code does not match. Plan the minimum change needed.
   - `ambiguous` — state unclear or genuinely contested. Stop and ask the user.
3. **For `open` items only, make the change** strictly as described in the review's "Required fix." Do not rename, refactor, or reorganize anything outside the blocker.
4. **Add or update the targeted regression test** the review names. Do not add extra tests.

If every blocker classifies as `already_addressed`, **do not make changes**. Skip to Step 7 with `fix_result: diagnosed_no_change`.

### Step 5: Run verify

If `verify_command` is configured, run it. Fix verification fallout only in files you already touched in Step 4 — do not broaden scope. Retry up to 3 iterations. If still failing, stop and report to the user with `fix_result: needs_user`.

### Step 6: Commit and push (only if changes were made)

```bash
git add <changed_files>
git commit -m "Rescue task #<IMPL_ID> (review #<LATEST_REVIEW_ID>)

- <blocker_key_1>: <brief fix>
- <blocker_key_2>: <brief fix>
"
git push -u origin <impl_branch>
```

If the push fails, stop and report. Do not treat the rescue as complete when changes are only local.

### Step 7: Emit the blocker ledger and persist a fix task row

Write the YAML ledger below, then persist a completed `fix` task row whose summary carries the ledger. Clear review state **only if changes were committed**.

Ledger format (preserve this schema — downstream tooling reads it):

```yaml
fix_result: repaired_pending_review | diagnosed_no_change | needs_user | blocked_external
blockers:
  - source_review_id: <review_task_id>
    blocker_key: <short_key>
    summary: <one-line description>
    status: addressed | already_addressed | deferred_to_user | out_of_scope
    closure_evidence: <file:line or commit sha proving the blocker is closed>
    verify_evidence: <verify command + brief outcome, or "n/a" for no-change passes>
    follow_up_review_required: true | false
```

**Ledger integrity rules:**
- `status: addressed` requires a commit in this pass. Cite the commit sha in `closure_evidence`.
- `status: already_addressed` requires a file:line citation to current code that satisfies the review. No commit.
- `follow_up_review_required: true` is permitted **only** when `fix_result: repaired_pending_review` and commits were made. Never request a review for a no-op pass.

Persist the fix task (mirrors how `/gza-task-improve` persists its row):

```bash
uv run python -c "
import json, subprocess
from datetime import datetime, timezone
from pathlib import Path
from gza.config import Config
from gza.db import SqliteTaskStore
from gza.git import Git
from gza.runner import _compute_slug_override, generate_slug

config = Config.load(Path.cwd())
store = SqliteTaskStore(config.db_path)

ledger_body = '''<LEDGER_YAML_FROM_ABOVE>'''
origin = datetime.now(timezone.utc).strftime('%Y-%m-%d')
summary_with_origin = f'<!-- origin: /gza-task-fix (manual, {origin}) -->\n' + ledger_body

created = store.add(
    prompt='Manual rescue via /gza-task-fix',
    task_type='fix',
    depends_on='<LATEST_REVIEW_ID_OR_NONE>',
    based_on='<IMPL_ID>',
    same_branch=True,
    group='<IMPL_GROUP_OR_NONE>',
)
assert created.id is not None

if created.slug is None:
    slug_override = _compute_slug_override(created, store)
    created.slug = generate_slug(
        created.prompt, existing_id=None,
        log_path=config.log_path,
        git=Git(config.project_dir),
        project_name=config.project_name,
        project_prefix=config.project_prefix,
        slug_override=slug_override,
        branch_strategy=config.branch_strategy,
        explicit_type=created.task_type_hint,
    )
    store.update(created)

prompt_data = json.loads(subprocess.check_output(
    ['uv', 'run', 'gza', 'show', '--prompt', created.id], text=True,
))
summary_path = Path(prompt_data['summary_path'])
summary_path.parent.mkdir(parents=True, exist_ok=True)
summary_path.write_text(summary_with_origin)

created.report_file = str(summary_path.relative_to(config.project_dir))
created.status = 'completed'
created.completed_at = datetime.now(timezone.utc)
created.output_content = ledger_body
store.update(created)

if <CHANGES_WERE_COMMITTED>:
    store.clear_review_state('<IMPL_ID>')
print(f'Fix saved as task #{created.id} ({created.report_file})')
"
```

### Step 8: Restore the starting checkout

```bash
git checkout <START_CHECKOUT>
```

If detached, use `git checkout --detach <START_CHECKOUT>`.

If the restore fails, stop and tell the user exactly what checkout you left them on. Do not silently finish on the implementation branch.

## Important notes

- **Diagnose before editing.** `fix` is escalation. Skipping to edits the way `improve` does reproduces the same loop.
- **Verify every blocker against the current code.** The failure mode we rescue from is hallucinated closure on one side — and stale-review false blockers on the other. Both are common; a Read is cheap.
- **Bounded scope.** Only touch files and tests named by the current review's Must-Fix items. No opportunistic cleanup, no drive-by refactors, no renames.
- **Ask the user before** broadening scope, deciding an `ambiguous` blocker, or committing to a `stale_review` / `scope_creep` diagnosis.
- **No review request without a commit.** `follow_up_review_required: true` is only valid when code changed in this pass. A no-op pass is an operational finding, not work for a reviewer.
- **Restore the user's checkout before exit.** Final state returns to `<START_CHECKOUT>`; closing message should name both checkouts explicitly.
- **Role separation.** `review` is the independent approval boundary. `improve` is the normal response to one review. `fix` is escalation for churn.
