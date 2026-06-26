---
name: gza-task-review
description: Run an interactive code review for a gza task's implementation branch, always running verify_command and producing structured review output compatible with gza-task-improve
allowed-tools: Bash(uv run:*), Bash(gh:*), Read, Glob, Grep, Agent, AskUserQuestion
version: 1.0.0
public: true
---

# Gza Task Review

Run an interactive code review for a specific gza task. Every review iteration must do both the normal code review work and an independent `verify_command` run from `gza.yaml`; verify is not a short-circuit. Produces structured review output that `/gza-task-improve` can consume. Use this when automated review iterations are exhausted, or when you want to review a task interactively.

## Inputs

- Required: full prefixed task ID (for example, `gza-1234`)
- Optional: `--pr` — also post the review as a PR comment

If the user did not provide a task ID, ask for it before proceeding.

Use the full prefixed task ID as provided.

## Process

### Step 0: Confirm review inputs without switching branches

Manual `/gza-task-review` must stay compatible with repositories that forbid ad hoc git commands. Do not run `git checkout`, `git switch`, or other manual branch-switching commands as part of this skill.

Before proceeding, make sure at least one of these is true:
- The current workspace/worktree was already prepared on the implementation checkout by Gza or another documented non-forbidden workflow, so running `verify_command` here will evaluate the intended implementation.
- The caller already provided authoritative diff context, so the review can use that diff even if this workspace is not on the implementation checkout.

If neither is true, stop and ask the user to either:
- rerun `/gza-task-review` from a prepared implementation checkout/worktree, or
- provide the authoritative implementation diff explicitly.

### Step 1: Resolve the task

Query the task database to get task details and branch:

```bash
uv run python -c "
import json, sys
from pathlib import Path
from gza.config import Config
from gza.db import SqliteTaskStore

config = Config.load(Path.cwd())
store = SqliteTaskStore.from_config(config)
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
    'impl_tags': list(impl_task.tags),
    'has_existing_review': latest_review is not None,
    'existing_review_id': latest_review.id if latest_review else None,
    'verify_command': config.verify_command,
}, default=str))
"
```

Replace `<TASK_ID>` with the actual full prefixed task ID.

If the task is not found, stop and tell the user.

### Step 2: Confirm the implementation context is reviewable

If there's an existing review, inform the user and ask if they want to proceed with a fresh review.

If the current workspace/worktree is not already prepared on `<impl_branch>` and no authoritative diff was supplied, stop and ask the user for one of those inputs. Do not try to make `<impl_branch>` active yourself.

### Step 3: Get task context

Gather additional context about the task:

```bash
uv run gza show <IMPL_TASK_ID>
uv run gza log <IMPL_TASK_ID>
```

### Step 4: Capture review context in the parent session

Capture canonical ask context before spawning the reviewer:
- If the caller already provided a `## Review scope:` section, pass it through unchanged, along with any `## Original plan context (out of scope except for the review scope):` section.
- Otherwise, if the caller already provided exactly one canonical ask section (`## Original plan:` or `## Original request:`), pass that section through unchanged.
- Otherwise, derive ask context from the linked task chain using the context gathered in Step 3.
- If linked ask content exists but is unavailable on this machine, pass an explicit unavailable-content marker section (for example, `## Original plan:` followed by `(plan task <TASK_ID> exists but content unavailable on this machine - flag as blocker)`).
- If no retrievable plan or request exists for this task, pass no ask section and let the reviewer state: `No plan or request provided.`

### Step 5: Capture the committed diff

If the caller already provided diff context, use that as-is and do not reconstruct it.
Otherwise, if the prepared review environment already includes authoritative diff context through Gza's review workflow, pass that through unchanged as `## Implementation diff context`.
If neither source exists, stop and ask the user for the authoritative implementation diff instead of using git commands to reconstruct it.

### Step 6: Run verify_command

Run `verify_command` from `gza.yaml` as part of every review iteration. This is required even when the diff already has obvious code-review blockers; do not skip verify just because the code review may fail.

- If `verify_command` is empty or unset, note that verify is not configured and continue with the code review.
- If `verify_command` is configured, run it from the project root only when this workspace/worktree was already prepared on `<impl_branch>` or equivalent implementation content. If the current checkout is not known to match the implementation, stop and ask the user for a prepared review environment rather than running verify against the wrong source tree.
- Capture the exit status and keep a trimmed diagnostic excerpt for the reviewer. If the output is huge, keep the most useful failing excerpt (for example, the failing tool header plus the first ~120 lines and last ~40 lines).
- If the verify run hangs, stop it after a bounded wait and pass the timeout forward as a failed `## verify_command result` with timeout evidence and any partial output captured so the review still happens in the same iteration.
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
- Tags: `<impl_tags>`

**Step 1**: Read `REVIEW.md` from the project root for review guidelines and criteria.

**Step 2**: Start with a repo-rules/learnings pass: compare the diff and behavior against AGENTS.md, REVIEW.md, project docs, and `.gza/learnings.md`; call out violations or regressions explicitly.

**Step 3**: The provided diff is authoritative - do not use git commands to reconstruct, re-derive, or expand it. You may read unchanged source files when surrounding context is needed to judge correctness.

**Step 3.5**: When you need to verify behavior that isn't visible in the diff (for example, whether a CLI command exists, how a called function works, or what a referenced method does), use Read, Grep, or Glob to check the current codebase. Do not guess.

**Step 4**: If `## Review scope:` is present, grade ask-adherence against that section only and use any original-plan-context section only for boundaries/contracts. Otherwise, review the diff against the provided ask context (`## Original plan:` or `## Original request:`). Evaluate whether the implementation actually achieves that ask, not just whether the code is clean.

**Step 4.5**: Independently evaluate the provided `## verify_command result` section in addition to the normal code review. Both signals matter every iteration.

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
<- Was a `## Review scope:` section provided, and if so did I grade ask-adherence against that scope while treating sibling slices as non-blocking unless they break an explicit contract? Otherwise, was an `## Original plan:` or `## Original request:` section provided, and did I verify ask-adherence against it while calling out intentional deviations? If neither was provided, did I state "No plan or request provided."?>
<- Did I require targeted regression tests that match each failure mode (not generic "add tests")?>
<- If config, CLI, or operator-facing behavior changed, did I verify docs/help/release-note impact?>

## Blockers

<Use ### B1, ### B2, ... for blockers. If none, write "None.">
<Each blocker should include Evidence:, Open-state citation:, Impact:, Required fix:, Required tests:>
<Class-of-issue enumeration: when one blocker is an instance of a repeated code-surface pattern (lookup table, classifier, dispatcher, schema/field mapping, multi-field validator, or parallel per-field/per-type handling), audit for analogous gaps before writing the blocker. The audit boundary is the affected file plus any other files in the same module (the same depth-3 path under `src/`) that were touched by the diff, plus any obvious same-module sibling of the affected file.>
<Report all still-open gaps for that same class in one blocker, with every affected `path:line` or `path:start-end` citation included in `Open-state citation:` regardless of file, and a `Required fix:` that closes the whole class.>
<Do not create one blocker per field, branch, case, table row, or file unless the required fixes are materially different.>
<Do not expand the audit beyond the same module, and do not expand isolated one-off defects - this rule applies only after you have found a repeated-pattern blocker shape.>
<Reserve BLOCKER for: correctness defects, behavior regressions, repository/rules violations, missing observability for user/agent-visible fallbacks, and misleading output/contradictory signals.>
<Treat unexplained deviations from the provided review scope, plan, or request as BLOCKER.>
<If `## Review scope:` is present, grade ask-adherence against that section only. Use any original plan context section only to understand boundaries and integration contracts.>
<Do not raise blockers solely because deferred sibling slices from the original plan are not implemented; only raise blockers when in-scope work is missing/broken or the diff violates an explicit integration contract described in the review scope or plan context.>
<Treat silent broad-exception fallbacks as BLOCKER when they can alter user/agent-visible state without clear warning/error surfacing.>
<Treat misleading output (UI/prompt/context contradictions) as BLOCKER when it can cause incorrect operator or agent decisions.>
<If config/CLI/operator-facing behavior changed, missing or incorrect docs/help/release-note updates are BLOCKER when they can mislead operators.>
<Use FOLLOWUP for actionable low-risk debt that should be tracked but should not block merge.>
<For each blocker, give a clear closure condition so an improve task can resolve all blockers in one pass.>
<For class-of-issue blockers, the closure condition must cover every enumerated instance across all cited paths, not just the first example.>
<Every BLOCKER must be falsifiable: `Evidence:` and `Open-state citation:` must show the current still-open state, and `Required fix:` must describe the concrete change needed to close it.>
<Do not write a `BLOCKER` unless you can cite the current code or current diff proving the issue is still open.>
<Prior review text, improve lineage, or task history are not sufficient evidence for a blocker.>
<If `## verify_command result` shows a failed or timed-out run, add one or more blocker items whose titles clearly include `verify_command failure`; use the trimmed failing output as Evidence and keep doing the normal code review in the same review.>
<If `## verify_command result` shows a passing run, do not add blocker text solely because verify ran.>
<Severity shorthand: `BLOCKER` means merge-blocking; `FOLLOWUP` means non-gating but task-worthy; `NIT` is omitted from canonical output.>
<Do not add a per-finding `Severity:` line; the `## Blockers` and `## Follow-Ups` sections are the severity field.>
<Derive the final verdict from the findings:>
<cannot classify safely -> `NEEDS_DISCUSSION`>
<Borderline cases must include a one-sentence rubric justification in `Impact:`, `Required fix:`, or `Recommended follow-up:`>
<A broad exception that can mask visible state or swallow a user/agent-visible failure is a `BLOCKER`.>
<An adjacent-path coverage sweep that would strengthen confidence without proving the current slice unsafe is a `FOLLOWUP`.>
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
<Verdict is derived from the findings: no blockers/no follow-ups -> APPROVED; no blockers/at least one follow-up -> APPROVED_WITH_FOLLOWUPS; any blocker -> CHANGES_REQUESTED; cannot classify safely -> NEEDS_DISCUSSION.>
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

Pass the branch name, authoritative diff context, the `## verify_command result` section, the `## Review scope:` section when available, otherwise the canonical ask context section (exactly one of `## Original plan:` or `## Original request:` when available), plus PR number if `--pr` was used.

### Step 8: Persist review output (required)

After the review agent returns markdown, always persist it as a canonical review artifact and completed review task row.

Compute the canonical `report_path` via `get_task_output_paths(created, config.project_dir)`, write the file there, and persist `report_file` + `output_content`. If any persistence step after `store.add(...)` fails, mark the created review task as `dropped` before re-raising so no runnable orphan stays `pending`:

```bash
uv run python -c "
from datetime import datetime, timezone
from pathlib import Path
from gza.config import Config
from gza.db import SqliteTaskStore
from gza.runner import _compute_slug_override, generate_slug, get_task_output_paths

config = Config.load(Path.cwd())
store = SqliteTaskStore.from_config(config)

review_markdown = '''<REVIEW_CONTENT>'''
origin_date = datetime.now(timezone.utc).strftime('%Y-%m-%d')
origin_header = f'<!-- origin: /gza-task-review (manual, {origin_date}) -->\n'
file_content = origin_header + review_markdown

created = store.add(
    prompt='Manual review via /gza-task-review',
    task_type='review',
    depends_on='<IMPL_TASK_ID>',
    tags=<impl_tags>,
)
assert created.id is not None

try:
    if created.slug is None:
        slug_override = _compute_slug_override(created, store)
        created.slug = generate_slug(
            created.prompt,
            existing_id=None,
            log_path=config.log_path,
            git=None,
            store=store,
            exclude_task_id=created.id,
            project_name=config.project_name,
            project_prefix=config.project_prefix,
            slug_override=slug_override,
            branch_strategy=config.branch_strategy,
            explicit_type=created.task_type_hint,
        )
        store.update(created)

    report_path, _summary_path = get_task_output_paths(created, config.project_dir)
    assert report_path is not None
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(file_content)

    created.report_file = str(report_path.relative_to(config.project_dir))
    created.status = 'completed'
    created.completed_at = datetime.now(timezone.utc)
    created.output_content = review_markdown
    store.update(created)
except Exception:
    created.status = 'dropped'
    store.update(created)
    raise
print(f'Review saved as task #{created.id} ({created.report_file})')
"
```

### Step 9: Report back

After the subagent completes:
- Print the review verdict (APPROVED / APPROVED_WITH_FOLLOWUPS / CHANGES_REQUESTED / NEEDS_DISCUSSION)
- Print a brief summary of findings
- If changes were requested, tell the user: "Run `/gza-task-improve <IMPL_TASK_ID>` to address the blocker items."
- If a PR was used, include a link to it

## Important notes

- **Ask-adherence is mandatory** — use the Summary checklist item for `## Review scope:` when present, otherwise `## Original plan:` or `## Original request:`, to confirm the implementation matches the requested behavior, and treat unexplained deviations as blocker findings.
- **Verify is part of review, not a separate gate** — run `verify_command` every iteration in addition to the normal code review, and fold any failures into the same structured blocker list.
- **Structured output matters** — the review format (B1, B2, F1, F2) must be compatible with `/gza-task-improve` and follow-up automation.
- **Clearly label verify-driven blockers** — titles should include `verify_command failure`, and `Evidence:` should carry the trimmed failing output so improve can act on it without rerunning history reconstruction.
- **Don't duplicate existing reviews** — if there's already a recent review, inform the user and ask before creating another one.
- **Use authoritative diff context** — do not reconstruct or expand the diff in the reviewing subagent; only use provided diff context plus unchanged-source reads for verification.
- **Stay checkout-neutral** — `/gza-task-review` must not instruct agents to switch branches manually. Review only from a prepared implementation checkout/worktree or from authoritative diff context that the caller already supplied.
- **Don't review installed skill artifacts** — files under `.claude/skills/` and `~/.codex/...` are install outputs, not source. Source of truth for `gza-*` skills is `src/gza/skills/<name>/SKILL.md` (see AGENTS.md and `docs/skills.md`). `.claude/skills/` is gitignored, so a discrepancy between an installed copy and its source can never be committed; never raise it as a blocker. Review the source file instead.
