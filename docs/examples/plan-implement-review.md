# Plan → Implement → Review Workflow

A multi-phase workflow for larger features requiring design review.

## Phase 1: Create a plan task

```bash
$ gza add --type plan --group auth-refactor \
  "Design a new authentication system using JWT tokens. Consider:
   - Token refresh strategy
   - Secure storage on client
   - Session invalidation
   - Migration path from current cookie-based auth"

Created task: 20260108-design-a-new-authentication (plan)
Group: auth-refactor
```

## Run the plan task in background

```bash
$ gza work --background
Started worker: w-20260108-143022
Task: 20260108-design-a-new-authentication
```

## Monitor running workers

```bash
$ gza ps
WORKER ID          PID    STATUS   TASK                              DURATION
w-20260108-143022  48291  running  20260108-design-a-new-authenti…   2m 15s
```

## Tail the logs

```bash
$ gza log -w w-20260108-143022 -f
[14:30:22] Starting task: 20260108-design-a-new-authentication
[14:30:25] Reading existing auth code...
[14:31:02] Analyzing current session handling...
[14:32:18] Writing plan to .gza/plans/20260108-design-a-new-authentication.md
...
```

Press `Ctrl+C` to stop following.

## Review the plan

Once complete, a copy of the plan is saved to `.gza/plans/` for human review:

```bash
$ cat .gza/plans/20260108-design-a-new-authentication.md
```

The plan content is also stored in the database. When an implement task runs, it reads the plan from the database (not the file), so the plan is available even when the implement task runs in a fresh worktree.

## Phase 2: Create implementation task

After reviewing and approving the plan:

```bash
$ gza add --type implement --based-on 20260108-design-a-new-authentication \
  --review --group auth-refactor \
  "Implement the JWT authentication system per the plan"

Created task: 20260108-implement-the-jwt-authentication (implement)
Group: auth-refactor
Depends on: 20260108-design-a-new-authentication
Review task: 20260108-review-implement-the-jwt (auto-created)
```

The `--review` flag automatically creates a review task that will run after implementation.

## Run the implementation

```bash
$ gza work --background
Started worker: w-20260108-151033
Task: 20260108-implement-the-jwt-authentication
```

## Check group status

```bash
$ gza status auth-refactor
Group: auth-refactor

  ✓ 20260108-design-a-new-authentication (plan)
      completed - 8m 12s

  ● 20260108-implement-the-jwt-authentication (implement)
      in_progress - depends on: 20260108-design-a-new-authentication

  ○ 20260108-review-implement-the-jwt (review)
      pending - depends on: 20260108-implement-the-jwt-authentication
```

## View the review

After implementation completes, the review task runs automatically:

```bash
$ cat .gza/reviews/20260108-review-implement-the-jwt.md

# Review: 20260108-implement-the-jwt-authentication

## Verdict: APPROVED

## Summary
Implementation follows the plan correctly...

## Findings
- Token refresh logic is correct
- Recommend adding rate limiting to refresh endpoint
...
```

## Create PRs for the work

```bash
$ gza unmerged
Unmerged branches:

  20260108-design-a-new-authentication
    Branch: gza/20260108-design-a-new-authentication
    Commits: 1 ahead of main

  20260108-implement-the-jwt-authentication
    Branch: feature/implement-the-jwt-authentication
    Commits: 8 ahead of main

$ gza pr 20260108-implement-the-jwt-authentication
PR created: https://github.com/myorg/myapp/pull/143
```
