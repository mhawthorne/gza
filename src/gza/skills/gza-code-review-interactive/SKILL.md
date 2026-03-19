---
name: gza-code-review-interactive
description: Review changes on current branch and output a structured review. Optionally post to PR with --pr flag.
allowed-tools: Bash(git:*), Bash(gh:*), Read, Agent, AskUserQuestion
version: 2.0.0
public: true
---

# Interactive Code Review

Review committed changes on the current feature branch and output a structured review.

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

### Step 3: Run the review

Spawn a **general-purpose Agent** subagent to perform the review. Give it this prompt:

---

You are reviewing a pull request. Your job is to read the project review guidelines, examine the diff, and produce a structured review.

**Step 1**: Read `REVIEW.md` from the project root for review guidelines and criteria.

**Step 2**: Start with a repo-rules/learnings pass: compare the diff and behavior against AGENTS.md, REVIEW.md, project docs, and `.gza/learnings.md`; call out violations or regressions explicitly.
Keep this review stack-agnostic. If project verification instructions are missing, state that explicitly in assumptions/risks.

**Step 3**: Get the diff to review:
```bash
git diff main...HEAD
```

**Step 4**: Write a structured review with these sections:

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

Pass the PR number (if `--pr` was used and a PR was found) or nothing to the subagent.

### Step 5: Report back

After the subagent completes:
- Print the review verdict (APPROVED / CHANGES_REQUESTED / NEEDS_DISCUSSION)
- Print a brief summary of findings
- If changes were requested, tell the user: "Fix the issues above, commit, push, then run `/gza-code-review-interactive` again."
- If a PR was used, include a link to it
