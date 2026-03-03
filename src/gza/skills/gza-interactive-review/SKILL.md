---
name: gza-interactive-review
description: Review changes on current branch, create/update PR, and post review comments
allowed-tools: Bash(git:*), Bash(gh:*), Read, Agent, AskUserQuestion
version: 1.0.0
public: false
---

# Interactive Code Review

Review committed changes on the current feature branch, ensure a PR exists, and post a structured review as a PR comment.

## Prerequisites

- You must be on a feature branch (not main/master)
- Changes must be committed and pushed

## Process

### Step 1: Verify branch state

1. Check current branch: `git branch --show-current`
   - If on `main` or `master`, stop and tell the user to switch to a feature branch
2. Check for uncommitted changes: `git status --porcelain`
   - If there are uncommitted changes, ask the user if they want to proceed anyway or commit first
3. Check if branch is pushed: `git log @{u}..HEAD 2>/dev/null`
   - If there are unpushed commits, tell the user to push first: `git push -u origin <branch>`

### Step 2: Ensure a PR exists

1. Try to view existing PR: `gh pr view --json number,url,title 2>/dev/null`
2. If no PR exists, create one:
   ```bash
   gh pr create --fill --draft
   ```
3. Capture the PR number and URL for later use

### Step 3: Run the review

Spawn a **general-purpose Agent** subagent to perform the review. Give it this prompt:

---

You are reviewing a pull request. Your job is to read the project review guidelines, examine the diff, and produce a structured review.

**Step 1**: Read `REVIEW.md` from the project root for review guidelines and criteria.

**Step 2**: Get the diff to review:
```bash
git diff main...HEAD
```

**Step 3**: Write a structured review with these sections:

```markdown
## Review Summary

<1-2 sentence overview of the changes>

## Must-Fix Issues

<Numbered list of issues that MUST be addressed before merging. If none, write "None found.">

## Suggestions

<Numbered list of non-blocking suggestions for improvement. If none, write "None.">

## Verdict

<One of: APPROVED, CHANGES_REQUESTED, or NEEDS_DISCUSSION>
<Brief justification>
```

**Step 4**: Post the review as a PR comment:
```bash
gh pr comment <PR_NUMBER> --body "<review content>"
```

Use a heredoc for the body to handle multi-line content properly.

---

Pass the PR number to the subagent so it can post the comment.

### Step 4: Report back

After the subagent completes:
- Print the review verdict (APPROVED / CHANGES_REQUESTED / NEEDS_DISCUSSION)
- Print a brief summary of findings
- If changes were requested, tell the user: "Fix the issues above, commit, push, then run `/gza-interactive-review` again."
- Include a link to the PR
