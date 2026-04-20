---
name: gza-task-fix
description: Run a stuck-task rescue workflow inline using gza fix context, bounded repair, verification, and closure ledger handoff
allowed-tools: Read, Edit, Write, Glob, Grep, Bash(uv run:*), Bash(git:*), Bash(mkdir:*), Bash(ls:*), AskUserQuestion
version: 1.0.0
public: true
---

# Fix Stuck Gza Task Inline

Use this skill when an implementation task is stuck in review/improve churn and needs a focused rescue pass with explicit closure tracking.

## Process

### Step 1: Get the target task

The user should provide a full prefixed task ID (for example, `gza-1234`).
The ID may be an implementation, review, improve, or existing fix task ID.

If no ID is provided, ask the user and optionally show unresolved work:

```bash
uv run gza incomplete --last 10
```

### Step 2: Start the first-class fix workflow

Run the built-in command so gza resolves lineage and assembles rescue context:

```bash
uv run gza fix <TASK_ID>
```

Use `--queue` only when the user explicitly wants to defer execution.

### Step 3: Follow rescue guardrails

During the fix run:
- Diagnose repeated blockers from recent reviews.
- Produce a bounded repair plan before editing.
- Restrict changes to blocker closure and verification fallout.
- Avoid opportunistic cleanup or unrelated refactors.
- Ask the user before broadening scope beyond blocker closure.

### Step 4: Verify and record closure

Ensure the task output includes a machine-readable blocker ledger section with:
- `fix_result`
- blocker entries (`source_review_id`, `blocker_key`, `summary`, `status`, `closure_evidence`, `verify_evidence`, `follow_up_review_required`)

Treat this ledger as operational tracking, not merge approval.

### Step 5: Handoff behavior

After a fix run:
- If code changed, require a fresh independent review.
- If no code changed, end with an explicit non-review handoff (`diagnosed_no_change`, `needs_user`, or `blocked_external`).

Prefer the command-driven flow above over ad hoc manual recovery.

## Notes

- `review` remains the independent approval boundary.
- `improve` remains the normal response to one review.
- `fix` is escalation for stuck loops/churn.
