---
name: gza-task-review
description: Run an interactive code review for a gza task's implementation branch, producing structured review output compatible with gza-task-improve
allowed-tools: Bash(uv run:*), Bash(git:*), Bash(gh:*), Read, Glob, Grep, Agent, AskUserQuestion
version: 1.0.0
public: true
---

# Gza Task Review

Run an interactive code review for a specific gza task. Produces structured review output that `/gza-task-improve` can consume. Use this when automated review cycles are exhausted, or when you want to review a task interactively.

## Inputs

- Required: full prefixed task ID (for example, `gza-1234`)
- Optional: `--pr` — also post the review as a PR comment

If the user did not provide a task ID, ask for it before proceeding.

Use the full prefixed task ID as provided.

## Process

### Step 1: Resolve the task

Query the task database to get task details and branch:

```bash
uv run python -c "
import json, sys
from gza.config import Config
from gza.db import SqliteTaskStore

config = Config.load()
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

### Step 2: Verify branch state

Check that the implementation branch exists and has commits:

```bash
git log main..<impl_branch> --oneline
```

If no commits, stop — there's nothing to review.

If there's an existing review, inform the user and ask if they want to proceed with a fresh review.

### Step 3: Get task context

Gather additional context about the task:

```bash
uv run gza show <IMPL_TASK_ID>
uv run gza log <IMPL_TASK_ID>
```

### Step 4: Run the review

Spawn a **general-purpose Agent** subagent to perform the review. Give it this prompt:

---

You are reviewing a gza task's implementation. Your job is to read the project review guidelines, examine the diff, understand the task's intent, and produce a structured review.

**Task context:**
- Task ID: `<IMPL_TASK_ID>`
- Task type: `<impl_task_type>`
- Task prompt: `<impl_prompt>`
- Branch: `<impl_branch>`
- Group: `<impl_group>`

**Step 1**: Read `REVIEW.md` from the project root for review guidelines and criteria.

**Step 2**: Start with a repo-rules/learnings pass: compare the diff and behavior against AGENTS.md, REVIEW.md, project docs, and `.gza/learnings.md`; call out violations or regressions explicitly.

**Step 3**: Get the diff to review:
```bash
git diff main...<impl_branch>
```

**Step 4**: Review the diff against the task prompt. The task prompt describes what was requested — evaluate whether the implementation actually achieves it, not just whether the code is clean.

**Step 5**: Write a structured review with these sections:

```markdown
## Summary

<Provide 3-5 bullets summarizing the review>
<Then answer this checklist with exactly 5 bullets in `Yes/No - ...` form and one short evidence clause each:>
<- Did I check the diff against AGENTS.md and `.gza/learnings.md` and flag any violations/regressions?>
<- Did I check for silent broad-exception fallbacks that mask errors while changing user/agent-visible state?>
<- Did I check for misleading output (contradictory UI/prompt/context signals)?>
<- Did I require targeted regression tests that match each failure mode (not generic "add tests")?>
<- If config, CLI, or operator-facing behavior changed, did I verify docs/help/release-note impact?>

## Must-Fix

<Use ### M1, ### M2, ... for blockers. If none, write "None.">
<Each blocker should include Evidence:, Impact:, Required fix:, Required tests:>
<Reserve Must-Fix for: correctness defects, behavior regressions, repository/rules violations, missing observability for user/agent-visible fallbacks, and misleading output/contradictory signals.>
<Treat silent broad-exception fallbacks as Must-Fix when they can alter user/agent-visible state without clear warning/error surfacing.>
<Treat misleading output (UI/prompt/context contradictions) as Must-Fix when it can cause incorrect operator or agent decisions.>
<If config/CLI/operator-facing behavior changed, missing or incorrect docs/help/release-note updates are Must-Fix when they can mislead operators.>
<Push style, cleanup, and non-risky refactors to Suggestions.>
<For each blocker, give a clear closure condition so an improve task can resolve all blockers in one pass.>

## Task Prompt Alignment

<Evaluate whether the implementation fulfills the task prompt.>
<Call out any requested behavior that is missing, partially implemented, or implemented differently than specified.>
<Call out any unrequested changes that add scope or risk.>

## Suggestions

<Use ### S1, ### S2, ... for non-blocking suggestions. If none, write "None.">
<Each suggestion should include Suggestion: and Why it helps:. Evidence: is optional but encouraged.>

## Questions / Assumptions

<Bullet list of open questions/assumptions. If none, write "None.">

## Verdict

<Brief justification>
Verdict: APPROVED|CHANGES_REQUESTED|NEEDS_DISCUSSION
```

Do not rename, omit, or reorder these sections.

If a PR number is provided, post the review as a PR comment:
```bash
gh pr comment <PR_NUMBER> --body "<review content>"
```

Use a heredoc for the body to handle multi-line content properly.

If no PR number is provided, just output the review directly.

---

Pass the branch name and PR number (if `--pr` was used) to the subagent.

### Step 5: Save the review to the task database (optional)

If the review found must-fix items (verdict is CHANGES_REQUESTED), ask the user if they want to record this as a review in the task database so `/gza-task-improve` can consume it:

```bash
uv run python -c "
import sys
from datetime import datetime, timezone
from gza.config import Config
from gza.db import SqliteTaskStore

config = Config.load()
store = SqliteTaskStore(config.db_path)

# Create a review task linked to the implementation
from gza.models import Task
review_task = Task(
    task_type='review',
    prompt='Interactive review via /gza-task-review',
    status='completed',
    depends_on=<IMPL_TASK_ID>,
    group=<impl_group_or_None>,
    output_content='''<REVIEW_CONTENT>''',
    completed_at=datetime.now(timezone.utc).isoformat(),
)
created = store.create(review_task)
print(f'Review saved as task #{created.id}')
"
```

### Step 6: Report back

After the subagent completes:
- Print the review verdict (APPROVED / CHANGES_REQUESTED / NEEDS_DISCUSSION)
- Print a brief summary of findings
- If changes were requested, tell the user: "Run `/gza-task-improve <IMPL_TASK_ID>` to address the must-fix items."
- If a PR was used, include a link to it

## Important notes

- **Task prompt alignment is key** — unlike generic code review, this skill evaluates whether the implementation matches what the task asked for.
- **Structured output matters** — the review format (M1, M2, S1, S2) must be compatible with `/gza-task-improve` so the improve workflow can consume it.
- **Don't duplicate existing reviews** — if there's already a recent review, inform the user and ask before creating another one.
- **Scope to branch files** — only review files in the diff between main and the implementation branch.
