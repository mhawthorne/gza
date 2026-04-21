---
name: gza-code-review-interactive
description: Review changes on current branch and output a structured review. Optionally post to PR with --pr flag.
allowed-tools: Bash(git:*), Bash(gh:*), Bash(uv run:*), Read, Agent, AskUserQuestion
version: 2.3.0
public: true
---

# Interactive Code Review

Review committed changes on the current feature branch and output a structured review.

Requires being on a non-main branch with commits ahead of main. If not, stop and tell the user:
"Switch to a feature branch with commits to review. This skill reviews committed changes on feature branches (git diff main...HEAD)."

## Arguments

- `--pr` — Post the review as a PR comment (requires an existing PR on the branch)
- No arguments — Just output the review locally, no PR interaction

## Process

### Step 1: Verify branch state

1. Check current branch: `git branch --show-current`
   - If on `main` or `master`, stop and tell the user to switch to a feature branch
2. Check for uncommitted changes: `git status --porcelain`
   - If there are uncommitted changes, warn the user but proceed with reviewing committed changes
3. Check if branch has commits ahead of main: `git log main..HEAD --oneline`
   - If no commits ahead, stop and tell the user there's nothing to review

### Step 2: Find PR (only if --pr flag is set)

1. Look up existing PR: `gh pr view --json number,url,title 2>/dev/null`
2. If no PR exists, stop and tell the user to create one first (do NOT create a PR automatically)
3. Capture the PR number and URL

### Step 3: Capture review context in the parent session

Capture one canonical ask section before spawning the reviewer:
- If the caller already provided exactly one canonical ask section (`## Original plan:` or `## Original request:`), pass that section through unchanged.
- Otherwise, try to resolve ask context from the branch's linked gza task chain (`uv run gza show <TASK_ID>` / `uv run gza log <TASK_ID>` is preferred once you identify the task for this branch).
- If linked ask content exists but is unavailable on this machine, pass an explicit unavailable-content marker section (for example, `## Original plan:` followed by `(plan task <TASK_ID> exists but content unavailable on this machine - flag as blocker)`).
- If no retrievable plan or request exists for this branch, pass no ask section and let the reviewer state: `No plan or request provided.`

Then capture the committed diff:
- If the caller already provided diff context, use that as-is and do not reconstruct it.
Otherwise, collect the committed branch diff once in the parent session:
```bash
git diff main...HEAD
```
Pass this diff to the subagent as `## Implementation diff context`.

### Step 4: Run the review

Spawn a **general-purpose Agent** subagent to perform the review. Give it this prompt (include the captured diff context):

---

You are reviewing a pull request. Your job is to read the project review guidelines, examine the diff, and produce a structured review.

**Step 1**: Read `REVIEW.md` from the project root for review guidelines and criteria.

**Step 2**: Start with a repo-rules/learnings pass: compare the diff and behavior against AGENTS.md, REVIEW.md, project docs, and `.gza/learnings.md`; call out violations or regressions explicitly.
Keep this review stack-agnostic. If project verification instructions are missing, state that explicitly in assumptions/risks.

**Step 3**: The provided diff is authoritative - do not use git commands to reconstruct, re-derive, or expand it. You may read unchanged source files when surrounding context is needed to judge correctness.

**Step 3.5**: When you need to verify behavior that isn't visible in the diff (e.g., whether a CLI command exists, how a called function works, what a referenced method does), use the Read, Grep, or Glob tools to check the current codebase. Do not guess or assume — verify.

**Step 3.7**: Review the diff against the provided canonical ask context (`## Original plan:` or `## Original request:`) when present. If ask content is marked unavailable, call that out as a blocker. If neither ask section is provided, state `No plan or request provided.`

**Step 4**: Write a structured review with these sections:

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
<Each blocker should include Evidence:, Impact:, Required fix:, Required tests:>
<Reserve BLOCKER for: correctness defects, behavior regressions, repository/rules violations, missing observability for user/agent-visible fallbacks, and misleading output/contradictory signals.>
<Treat unexplained deviations from the provided plan or request as BLOCKER.>
<Treat silent broad-exception fallbacks as BLOCKER when they can alter user/agent-visible state without clear warning/error surfacing.>
<Treat misleading output (UI/prompt/context contradictions) as BLOCKER when it can cause incorrect operator or agent decisions.>
<If config/CLI/operator-facing behavior changed, missing or incorrect docs/help/release-note updates are BLOCKER when they can mislead operators.>
<Use FOLLOWUP for actionable low-risk debt that should be tracked but should not block merge.>
<For each blocker, give a clear closure condition so an improve task can resolve all blockers in one pass.>

## Follow-Ups

<Use ### F1, ### F2, ... for non-blocking actionable follow-ups. If none, write "None.">
<Each follow-up should include Evidence:, Impact:, Recommended follow-up:, Recommended tests:>
<Do not include NIT findings in canonical output.>

## Questions / Assumptions

<Bullet list of open questions/assumptions. If none, write "None.">

## Verdict

<Brief justification>
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

Pass the authoritative diff context (`## Implementation diff context`), canonical ask context section (exactly one of `## Original plan:` or `## Original request:` when available), and the PR number (if `--pr` was used and a PR was found) to the subagent.

### Step 5: Report back

After the subagent completes:
- Print the review verdict (APPROVED / APPROVED_WITH_FOLLOWUPS / CHANGES_REQUESTED / NEEDS_DISCUSSION)
- Print a brief summary of findings
- If changes were requested, tell the user: "Fix the issues above, commit, push, then run `/gza-code-review-interactive` again."
- If a PR was used, include a link to it
