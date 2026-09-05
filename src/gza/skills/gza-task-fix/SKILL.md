---
name: gza-task-fix
description: Rescue a stuck gza task inline — diagnoses review/improve churn, verifies each blocker against current code, addresses the open ones, runs verify, and commits. Runs entirely in Claude Code, no background worker.
allowed-tools: Read, Edit, Write, Glob, Grep, Bash(uv run:*), Bash(git:*), Bash(mkdir:*), Bash(ls:*), Bash(cd:*), AskUserQuestion
version: 2.2.0
public: true
---

# Fix Stuck Gza Task Inline

Use this skill when an implementation task is stuck in review/improve churn — the same blockers keep reappearing, or a previous improve/fix pass failed to close them. `fix` is escalation: it **diagnoses why the loop is happening** before making any edits, then applies a bounded repair scoped strictly to blocker closure.

Unlike `/gza-task-improve`, this skill requires you to verify each blocker against the current code before deciding whether a change is needed. A stuck task often already has the fix on disk — in which case the answer is "no change, this was hallucinated-closure churn," not another edit pass.

This skill runs entirely inline in the current Claude Code session. Do not invoke `gza fix` or any background worker — that defeats the purpose of running here.

## Process

### Step 0: Capture the starting directory

```bash
pwd
```

Save as `<START_DIR>`. This skill must never change the branch checked out in the directory the user invoked it from — all branch work happens in a separate worktree (Step 3). You return here at the end by `cd`, not by any `git checkout`.

### Step 1: Resolve the target task and recent review history

The user provides a full prefixed task ID (for example, `gza-1234`) — implementation, review, improve, or prior fix. Resolve to the implementation task and fetch the last three reviews so you can detect churn:

```bash
uv run python -c "
import json, sys
from pathlib import Path
from gza.config import Config
from gza.db import SqliteTaskStore

config = Config.load(Path.cwd())
store = SqliteTaskStore.from_config(config)
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
    'impl_tags': list(impl.tags) if impl else [],
    'verify_command': config.verify_command,
    'inner_verify_command': config.inner_verify_command,
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

### Step 3: Check out the implementation branch in an isolated worktree

**Never `git checkout <impl_branch>` in `<START_DIR>`.** That switches whatever branch is checked out there (often the shared main checkout) onto the task branch, and if this session ends before Step 8 runs, that checkout is left stranded on the task branch — with no automatic way back. This has happened in practice and is why this step exists.

1. Check whether the branch is already checked out somewhere:

```bash
git worktree list
```

2. If `<impl_branch>` appears there, `cd` into that existing path and continue. Do not create a duplicate worktree.
3. Otherwise, create a dedicated worktree for it rather than touching `<START_DIR>`:

```bash
git worktree add .gza-worktrees/<impl_branch> <impl_branch>
cd .gza-worktrees/<impl_branch>
```

Use a fixed, git-ignored parent directory (`.gza-worktrees/`) so repeat rescues of the same branch land in the same place instead of piling up duplicates. All remaining steps run from this worktree, not from `<START_DIR>`.

**Verify the new worktree resolves to the same task DB before running any `uv run` command in it.** A raw `git worktree add` only checks out tracked files — it does not carry over an untracked, project-relative task DB (e.g. `db_path: .gza/gza.db` in `gza.yaml`), so `SqliteTaskStore.from_config` can silently create a fresh, empty, throwaway DB in the new worktree instead of erroring. Any task lookup, rescue, or `clear_review_state` call made against that DB is a no-op on the real system and can be lost entirely.

```bash
uv run python -c "
from pathlib import Path
from gza.config import Config
print(Config.load(Path.cwd()).db_path)
"
```

Compare this to the same command run from `<START_DIR>`. If they differ and the DB path is project-relative (not an absolute path like `~/.gza/gza.db`), link the whole `.gza` directory in before doing anything else — do **not** hardlink only `gza.db`. SQLite keeps `-wal`/`-shm` files alongside the main `.db` file, and those are ordinary per-directory files, not covered by a hardlink of the main file alone. Hardlinking just `gza.db` gives the new worktree a private WAL that silently diverges from every other worktree's writes until someone manually checkpoints it — a write made here can be invisible everywhere else, and vice versa, with no error. Symlinking the whole directory instead makes `gza.db`, `gza.db-wal`, and `gza.db-shm` the literal same files everywhere, so there is only ever one WAL.

```bash
rm -rf .gza-worktrees/<impl_branch>/.gza   # only if it's an empty/throwaway DB this worktree just created
ln -s <START_DIR>/.gza .gza-worktrees/<impl_branch>/.gza
```

If a stray local DB was already created here (any `uv run gza`/`SqliteTaskStore` command ran before this check), the `rm -rf` above discards it — that's correct as long as you haven't already relied on data written to it. If you have, checkpoint it first (`PRAGMA wal_checkpoint(TRUNCATE)`) and inspect before deleting.

If the db paths already match (common when the project uses a global/absolute `db_path`), skip this — there is nothing to link.

### Step 4: Verify each blocker against the current code before editing

For each Must-Fix item in the latest review:

1. **Read the cited file:line location** with the Read tool. The review may be wrong about the current state.
2. **Classify the blocker:**
   - `already_addressed` — code already satisfies the review's "Required fix." Record a file:line citation as evidence. No change.
   - `open` — code does not match. Plan the minimum change needed.
   - `ambiguous` — state unclear or genuinely contested. Stop and ask the user.
3. **For `open` items only, make the change** strictly as described in the review's "Required fix." Do not rename, refactor, or reorganize anything outside the blocker.
4. **Add or update the targeted regression test** the review names. Do not add extra tests.

**Known recipe — installed skill out of sync after `src/gza/skills/` edit.**
If the blocker is a test failure where `.claude/skills/<name>/SKILL.md` differs from `src/gza/skills/<name>/SKILL.md` (e.g. `test_merge_first_docs_and_fix_skill_schema_stay_in_sync` or any `*sync*` test comparing the two paths), treat it as a **source-vs-install-state diagnosis**, not an automatic code edit. The bundled source under `src/gza/skills/` is the only committable source of truth; `.claude/skills/` is a gitignored install artifact. First verify whether the bundled source already satisfies the requested behavior. If it does, refresh the local installed copy only as verification/setup:

```bash
uv run gza skills-install --update --project .
```

Then re-run verify. Do **not** edit or stage `.claude/skills/` by hand — it is local install state, not source. If the only remaining mismatch is the installed copy, classify it as an environment/refresh issue rather than a review blocker. Only change code when the bundled source, installer behavior, or a test asserting the supported refresh path is actually wrong.

If every blocker classifies as `already_addressed`, **do not make changes**. Skip to Step 7 with `fix_result: diagnosed_no_change`.

### Step 5: Use fast checks while editing, then run the final gate

If `inner_verify_command` is configured, you may use it during Step 4 after a blocker-specific fix batch. Otherwise use targeted tests for touched files. Do not keep relaunching the full suite after each individual blocker edit.

After the last planned change, if `verify_command` is configured, run it once and time the full wall-clock duration with a monotonic clock. Record `verify.duration_seconds` whenever the full command actually runs. Fix verification fallout only in files you already touched in Step 4 — do not broaden scope. Retry the full gate up to 3 iterations. If still failing, stop and report to the user with `fix_result: needs_user`.

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
verify:
  command: <configured verify command or "">
  passed: true | false | null
  duration_seconds: <float or null>
  autonomous_verify_timeout_seconds: <int>
  review_verify_timeout_grace_seconds: <number >= 1>
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
- `verify.duration_seconds` is required whenever `verify_command` ran.
- `status: addressed` requires a commit in this pass. Cite the commit sha in `closure_evidence`.
- `status: already_addressed` requires a file:line citation to current code that satisfies the review. No commit.
- `follow_up_review_required: true` is permitted **only** when `fix_result: repaired_pending_review` and commits were made. Never request a review for a no-op pass.

Persist the fix task (mirrors how `/gza-task-improve` persists its row):

```bash
uv run python -c "
from datetime import datetime, timezone
from pathlib import Path
from gza.config import Config
from gza.db import SqliteTaskStore
from gza.git import Git
from gza.runner import _compute_slug_override, generate_slug, get_task_output_paths

config = Config.load(Path.cwd())
store = SqliteTaskStore.from_config(config)

ledger_body = '''<LEDGER_YAML_FROM_ABOVE>'''
origin = datetime.now(timezone.utc).strftime('%Y-%m-%d')
summary_with_origin = f'<!-- origin: /gza-task-fix (manual, {origin}) -->\n' + ledger_body

created = store.add(
    prompt='Manual rescue via /gza-task-fix',
    task_type='fix',
    depends_on='<LATEST_REVIEW_ID_OR_NONE>',
    based_on='<IMPL_ID>',
    same_branch=True,
    tags=<IMPL_TAGS>,
)
assert created.id is not None

try:
    if created.slug is None:
        slug_override = _compute_slug_override(created, store)
        created.slug = generate_slug(
            created.prompt, existing_id=None,
            log_path=config.log_path,
            git=Git(config.project_dir),
            store=store,
            exclude_task_id=created.id,
            project_name=config.project_name,
            project_prefix=config.project_prefix,
            slug_override=slug_override,
            branch_strategy=config.branch_strategy,
            explicit_type=created.task_type_hint,
        )
        store.update(created)

    _report_path, summary_path = get_task_output_paths(created, config.project_dir)
    assert summary_path is not None
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.write_text(summary_with_origin)

    created.report_file = str(summary_path.relative_to(config.project_dir))
    created.status = 'completed'
    created.completed_at = datetime.now(timezone.utc)
    created.output_content = ledger_body
    store.update(created)
except Exception:
    created.status = 'dropped'
    store.update(created)
    raise

if <CHANGES_WERE_COMMITTED>:
    store.clear_review_state('<IMPL_ID>')
print(f'Fix saved as task #{created.id} ({created.report_file})')
"
```

### Step 8: Return to the starting directory

```bash
cd <START_DIR>
```

`<START_DIR>`'s branch was never touched, so there is nothing to restore — this is just a `cd` back. The task worktree at `.gza-worktrees/<impl_branch>` is left in place (not removed) so a later rescue of the same branch can reuse it without recreating.

## Important notes

- **Diagnose before editing.** `fix` is escalation. Skipping to edits the way `improve` does reproduces the same loop.
- **Verify every blocker against the current code.** The failure mode we rescue from is hallucinated closure on one side — and stale-review false blockers on the other. Both are common; a Read is cheap.
- **Bounded scope.** Only touch files and tests named by the current review's Must-Fix items. No opportunistic cleanup, no drive-by refactors, no renames.
- **Ask the user before** broadening scope, deciding an `ambiguous` blocker, or committing to a `stale_review` / `scope_creep` diagnosis.
- **No review request without a commit.** `follow_up_review_required: true` is only valid when code changed in this pass. A no-op pass is an operational finding, not work for a reviewer.
- **Never check out the impl branch in `<START_DIR>`.** Always work in a separate worktree (Step 3) so the user's original checkout is never at risk of being left stranded on a task branch.
- **Return to the starting directory before exit.** Final state is `cd <START_DIR>`; closing message should name the worktree path used and confirm `<START_DIR>`'s branch was untouched.
- **Role separation.** `review` is the independent approval boundary. `improve` is the normal response to one review. `fix` is escalation for churn.
