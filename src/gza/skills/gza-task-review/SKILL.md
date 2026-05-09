---
name: gza-task-review
description: Run an interactive code review for a gza task's implementation branch, always running verify_command and producing structured review output compatible with gza-task-improve
allowed-tools: Bash(uv run:*), Bash(git:*), Bash(gh:*), Read, Glob, Grep, Agent, AskUserQuestion
version: 1.0.0
public: true
---

# Gza Task Review

Run an interactive code review for a specific gza task. Every review cycle must do both the normal code review work and an independent `verify_command` run from `gza.yaml`; verify is not a short-circuit. Produces structured review output that `/gza-task-improve` can consume. Use this when automated review cycles are exhausted, or when you want to review a task interactively.

## Inputs

- Required: full prefixed task ID (for example, `gza-1234`)
- Optional: `--pr` — also post the review as a PR comment

If the user did not provide a task ID, ask for it before proceeding.

Use the full prefixed task ID as provided.

## Process

### Step 0: Capture the starting checkout

Before reading task state, capture where the user started:

```bash
git symbolic-ref --quiet --short HEAD || git rev-parse --short HEAD
```

Save this as `<START_CHECKOUT>`. If you change checkouts at any point during the review, return to `<START_CHECKOUT>` before finishing. If `<START_CHECKOUT>` is a detached HEAD, restore it with `git checkout --detach <START_CHECKOUT>`.

### Step 1: Resolve the task

Query the task database to get task details and branch:

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

# If given a review or improve task, resolve to the implementation task
impl_task = task
if task.task_type == 'review' and task.depends_on:
    impl_task = store.get(task.depends_on)
elif task.task_type == 'improve' and task.based_on:
    impl_task = store.get(task.based_on)

# Check for existing reviews
assert impl_task.id is not None
reviews = store.get_reviews_for_task(impl_task.id)
latest_review = reviews[0] if reviews else None

print(json.dumps({
    'impl_task_id': impl_task.id,
    'impl_task_type': impl_task.task_type,
    'impl_branch': impl_task.branch,
    'impl_prompt': impl_task.prompt,
    'impl_status': impl_task.status,
    'impl_group': impl_task.group,
    'has_existing_review': latest_review is not None,
    'existing_review_id': latest_review.id if latest_review else None,
    'verify_command': config.verify_command,
}, default=str))
"
```

Replace `<TASK_ID>` with the actual full prefixed task ID.

If the task is not found, stop and tell the user.

### Step 2: Verify branch state and switch to the implementation checkout

Check that the implementation branch exists and has commits:

```bash
git log main..<impl_branch> --oneline
```

If no commits, stop — there's nothing to review.

If there's an existing review, inform the user and ask if they want to proceed with a fresh review.

To run `verify_command` against the actual implementation and let the reviewer inspect current source, make `<impl_branch>` the active checkout/worktree now:

```bash
git checkout <impl_branch>
```

If the branch is checked out in another worktree, inform the user and ask whether to use the existing worktree path or create a new worktree. If `<START_CHECKOUT>` already equals `<impl_branch>`, do not switch away and back unnecessarily.

### Step 3: Get task context

Gather additional context about the task:

```bash
uv run gza show <IMPL_TASK_ID>
uv run gza log <IMPL_TASK_ID>
```

### Step 4: Capture review context in the parent session

Capture one canonical ask section before spawning the reviewer:
- If the caller already provided exactly one canonical ask section (`## Original plan:` or `## Original request:`), pass that section through unchanged.
- Otherwise, derive ask context from the linked task chain using the context gathered in Step 3.
- If linked ask content exists but is unavailable on this machine, pass an explicit unavailable-content marker section (for example, `## Original plan:` followed by `(plan task <TASK_ID> exists but content unavailable on this machine - flag as blocker)`).
- If no retrievable plan or request exists for this task, pass no ask section and let the reviewer state: `No plan or request provided.`

### Step 5: Capture the committed diff

If the caller already provided diff context, use that as-is and do not reconstruct it.
Otherwise, collect the committed branch diff once in the parent session:
```bash
git diff main...<impl_branch>
```
Pass this diff to the subagent as `## Implementation diff context`.

### Step 6: Run verify_command

Run `verify_command` from `gza.yaml` as part of every review cycle. This is required even when the diff already has obvious code-review blockers; do not skip verify just because the code review may fail.

- If `verify_command` is empty or unset, note that verify is not configured and continue with the code review.
- If `verify_command` is configured, run it from the project root while `<impl_branch>` is checked out.
- Capture the exit status and keep a trimmed diagnostic excerpt for the reviewer. If the output is huge, keep the most useful failing excerpt (for example, the failing tool header plus the first ~120 lines and last ~40 lines).
- Do not fix the branch during review. The point here is to independently detect verify failures and fold them into the review findings.

Pass the result forward as a `## verify_command result` section. Include:
- The literal configured command
- Whether it passed or failed
- The exit status when it failed
- The trimmed stdout/stderr excerpt when it failed

### Step 7: Run the review

Spawn a **general-purpose Agent** subagent to perform the review. Give it this prompt:

---

You are reviewing a gza task's implementation. Your job is to read the project review guidelines, examine the diff, understand the task's intent, and produce a structured review.

**Task context:**
- Task ID: `<IMPL_TASK_ID>`
- Task type: `<impl_task_type>`
- Branch: `<impl_branch>`
- Group: `<impl_group>`

**Step 1**: Read `REVIEW.md` from the project root for review guidelines and criteria.

**Step 2**: Start with a repo-rules/learnings pass: compare the diff and behavior against AGENTS.md, REVIEW.md, project docs, and `.gza/learnings.md`; call out violations or regressions explicitly.

**Step 3**: The provided diff is authoritative - do not use git commands to reconstruct, re-derive, or expand it. You may read unchanged source files when surrounding context is needed to judge correctness.

**Step 3.5**: When you need to verify behavior that isn't visible in the diff (for example, whether a CLI command exists, how a called function works, or what a referenced method does), use Read, Grep, or Glob to check the current codebase. Do not guess.

**Step 4**: Review the diff against the provided ask context (`## Original plan:` or `## Original request:`). Evaluate whether the implementation actually achieves that ask, not just whether the code is clean.

**Step 4.5**: Independently evaluate the provided `## verify_command result` section in addition to the normal code review. Both signals matter every cycle.

- If verify passed, do not add findings just because verify ran.
- If verify failed, synthesize one or more blocking findings alongside the code-review findings. Clearly label each verify-driven blocker title with `verify_command failure` so humans and `/gza-task-improve` can distinguish them from code-quality blockers.
- Prefer one blocker per failing tool or distinct root cause when that makes the improve work clearer.
- Put the trimmed verify output in `Evidence:`. If the verify output already contains `path:line` locations (for example mypy/ruff/pytest file references), use those in `Open-state citation:`. If it does not, inspect the referenced current source and add current-source citations that prove the failure is still open.
- Treat verify failures as blocking even if the code review itself would otherwise approve the diff.

**Step 5**: Write a structured review with these sections:

```markdown
## Summary

<Provide 3-5 bullets summarizing the review>
<Then answer this checklist with exactly 6 bullets in `Yes/No - ...` form and one short evidence clause each:>
<- Did I check the diff against AGENTS.md and `.gza/learnings.md` and flag any violations/regressions?>
<- Did I check for silent broad-exception fallbacks that mask errors while changing user/agent-visible state?>
<- Did I check for misleading output (contradictory UI/prompt/context signals)?>
<- Was an `## Original plan:` or `## Original request:` section provided, and did I verify ask-adherence (plan decisions reflected in the diff, or request coverage) while calling out intentional deviations? If neither was provided, did I state "No plan or request provided."?>
<- Did I require targeted regression tests that match each failure mode (not generic "add tests")?>
<- If config, CLI, or operator-facing behavior changed, did I verify docs/help/release-note impact?>

## Blockers

<Use ### B1, ### B2, ... for blockers. If none, write "None.">
<Each blocker should include Evidence:, Open-state citation:, Impact:, Required fix:, Required tests:>
<Reserve BLOCKER for: correctness defects, behavior regressions, repository/rules violations, missing observability for user/agent-visible fallbacks, and misleading output/contradictory signals.>
<Treat unexplained deviations from the provided plan or request as BLOCKER.>
<Treat silent broad-exception fallbacks as BLOCKER when they can alter user/agent-visible state without clear warning/error surfacing.>
<Treat misleading output (UI/prompt/context contradictions) as BLOCKER when it can cause incorrect operator or agent decisions.>
<If config/CLI/operator-facing behavior changed, missing or incorrect docs/help/release-note updates are BLOCKER when they can mislead operators.>
<Use FOLLOWUP for actionable low-risk debt that should be tracked but should not block merge.>
<For each blocker, give a clear closure condition so an improve task can resolve all blockers in one pass.>
<Do not write a `BLOCKER` unless you can cite the current code or current diff proving the issue is still open.>
<Prior review text, improve lineage, or task history are not sufficient evidence for a blocker.>
<If `## verify_command result` shows a failed run, add one or more blocker items whose titles clearly include `verify_command failure`; use the trimmed failing output as Evidence and keep doing the normal code review in the same review.>
<If `## verify_command result` shows a passing run, do not add blocker text solely because verify ran.>
<Open-state citation must contain one or more current-source references in `path:line` or `path:start-end` form; backticked citations and comma-separated multiple citations are allowed.>

## Follow-Ups

<Use ### F1, ### F2, ... for non-blocking actionable follow-ups. If none, write "None.">
<Each follow-up should include Evidence:, Impact:, Recommended follow-up:, Recommended tests:>
<Do not include NIT findings in canonical output.>

## Questions / Assumptions

<Bullet list of open questions/assumptions. If none, write "None.">

## Verdict

<Brief justification>
<Verdict is blocking if either the code review or verify produced blocker findings.>
Verdict: APPROVED|APPROVED_WITH_FOLLOWUPS|CHANGES_REQUESTED|NEEDS_DISCUSSION
```

Do not rename, omit, or reorder these sections.

If a PR number is provided, post the review as a PR comment:
```bash
gh pr comment <PR_NUMBER> --body "<review content>"
```

Use a heredoc for the body to handle multi-line content properly.

If no PR number is provided, just output the review directly.

---

Pass the branch name, authoritative diff context, the `## verify_command result` section, and the canonical ask context section (exactly one of `## Original plan:` or `## Original request:` when available) to the subagent, plus PR number if `--pr` was used.

### Step 8: Persist review output (required)

After the review agent returns markdown, always persist it as a canonical review artifact and completed review task row.

Use `gza show --prompt` on the newly created review task ID to get the canonical `report_path` (same source of truth as `get_task_output_paths()`), write the file there, and persist `report_file` + `output_content`:

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

review_markdown = '''<REVIEW_CONTENT>'''
origin_date = datetime.now(timezone.utc).strftime('%Y-%m-%d')
origin_header = f'<!-- origin: /gza-task-review (manual, {origin_date}) -->\n'
file_content = origin_header + review_markdown

created = store.add(
    prompt='Manual review via /gza-task-review',
    task_type='review',
    depends_on='<IMPL_TASK_ID>',
    group=<impl_group_or_None>,
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
report_path = Path(prompt_data['report_path'])
report_path.parent.mkdir(parents=True, exist_ok=True)
report_path.write_text(file_content)

created.report_file = str(report_path.relative_to(config.project_dir))
created.status = 'completed'
created.completed_at = datetime.now(timezone.utc)
created.output_content = review_markdown
store.update(created)
print(f'Review saved as task #{created.id} ({created.report_file})')
"
```

### Step 9: Report back

After the subagent completes:
- Print the review verdict (APPROVED / APPROVED_WITH_FOLLOWUPS / CHANGES_REQUESTED / NEEDS_DISCUSSION)
- Print a brief summary of findings
- If changes were requested, tell the user: "Run `/gza-task-improve <IMPL_TASK_ID>` to address the blocker items."
- If a PR was used, include a link to it
- If you changed checkouts during the workflow, switch back to `<START_CHECKOUT>` before the final message and state explicitly which checkout is now active

## Important notes

- **Ask-adherence is mandatory** — use the Summary checklist item for `## Original plan:` or `## Original request:` to confirm the implementation matches the requested behavior, and treat unexplained deviations as blocker findings.
- **Verify is part of review, not a separate gate** — run `verify_command` every cycle in addition to the normal code review, and fold any failures into the same structured blocker list.
- **Structured output matters** — the review format (B1, B2, F1, F2) must be compatible with `/gza-task-improve` and follow-up automation.
- **Clearly label verify-driven blockers** — titles should include `verify_command failure`, and `Evidence:` should carry the trimmed failing output so improve can act on it without rerunning history reconstruction.
- **Don't duplicate existing reviews** — if there's already a recent review, inform the user and ask before creating another one.
- **Use authoritative diff context** — do not reconstruct or expand the diff in the reviewing subagent; only use provided diff context plus unchanged-source reads for verification.
- **Preserve the user's checkout** — `/gza-task-review` should be checkout-neutral. If you switch branches for any reason, restore the starting checkout before returning control to the user.
