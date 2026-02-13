# Plan → Implement → Review Workflow

A multi-phase workflow for larger features requiring design review.

## Phase 1: Create and run a plan

```bash
$ gza add --type plan --group auth-refactor
# Opens $EDITOR to write the prompt
```

Or provide the prompt directly:

```bash
$ gza add --type plan --group auth-refactor \
  "Design a new authentication system using JWT tokens. Consider:
   - Token refresh strategy
   - Secure storage on client
   - Session invalidation
   - Migration path from current cookie-based auth"

Created task #1: 20260108-design-a-new-authentication (plan)
Group: auth-refactor
```

> **Note:** The `--group` flag is optional. Groups make it easier to track the status of related tasks with `gza status <group>`.

Run the plan task:

```bash
$ gza work 1
=== Task: Design a new authentication system... ===
    ID: #1 20260108-design-a-new-authentication
    Type: plan
...
=== Done ===
Stats: Runtime: 8m 12s | Turns: 15 | Cost: $0.42
```

> **Tip:** If you don't provide a task ID, `gza work` runs the next pending task.

## Review the plan

The plan is saved to `.gza/plans/` for human review:

```bash
$ cat .gza/plans/20260108-design-a-new-authentication.md
```

The plan content is also stored in the database, so it's available to dependent tasks even in fresh worktrees.

## Phase 2: Implement based on the plan

After reviewing and approving the plan, create an implementation task:

```bash
$ gza add --type implement --based-on 1 --group auth-refactor \
  "Implement the JWT authentication system per the plan"

Created task #2: 20260108-implement-the-jwt-authentication (implement)
Group: auth-refactor
Based on: #1
```

The `--based-on` flag takes a task ID (not a slug) and links the implementation to the plan, providing context to the AI.

Run the implementation:

```bash
$ gza work 2
=== Task: Implement the JWT authentication system... ===
    ID: #2 20260108-implement-the-jwt-authentication
    Type: implement
...
=== Done ===
Stats: Runtime: 12m 45s | Turns: 32 | Cost: $1.23
Branch: feature/implement-the-jwt-authentication
```

## Phase 3: Review the implementation

Create and run a review task:

```bash
$ gza review 2 --run
✓ Created review task #3
=== Task: Review implementation... ===
    ID: #3 20260108-review-implementation
    Type: review
...
=== Done ===
Stats: Runtime: 3m 18s | Turns: 8 | Cost: $0.28
```

> **Alternative:** You can use `--review` with `gza add` to auto-create a review task upfront:
> ```bash
> $ gza add --type implement --based-on 1 --review "Implement..."
> ```

View the review:

```bash
$ cat .gza/reviews/20260108-review-implementation.md

# Review: 20260108-implement-the-jwt-authentication

## Verdict: CHANGES_REQUESTED

## Summary
Implementation follows the plan but needs improvements...

## Findings
- Token refresh logic is correct
- Missing: rate limiting on refresh endpoint
- Missing: input validation on token claims
```

## Phase 4: Address review feedback

If the review requests changes, create an improve task:

```bash
$ gza improve 2

Created task #4: 20260108-improve-implementation (improve)
Based on: #2
```

> **Tip:** Add `--review` if you want automatic follow-up reviews after each improvement iteration.

Run the improve task (continues on the same branch):

```bash
$ gza work 4
=== Task: Improve implementation based on review... ===
    ID: #4 20260108-improve-implementation
    Type: improve
...
=== Done ===
Stats: Runtime: 5m 22s | Turns: 14 | Cost: $0.45
```

Run a follow-up review to verify the changes:

```bash
$ gza review 2 --run
✓ Created review task #5
=== Task: Review implementation... ===
    ID: #5 20260108-review-implementation
    Type: review
...
=== Done ===
```

Check the new review verdict:

```bash
$ cat .gza/reviews/20260108-review-implementation-2.md

# Review: 20260108-implement-the-jwt-authentication

## Verdict: APPROVED

All requested changes have been addressed.
```

## Phase 5: Create PR and merge

Check the group status:

```bash
$ gza status auth-refactor
Group: auth-refactor

  ✓ #1 20260108-design-a-new-authentication (plan)
      completed - 8m 12s

  ✓ #2 20260108-implement-the-jwt-authentication (implement)
      completed - 12m 45s

  ✓ #3 20260108-review-implementation (review)
      completed - CHANGES_REQUESTED

  ✓ #4 20260108-improve-implementation (improve)
      completed - 5m 22s

  ✓ #5 20260108-review-implementation (review)
      completed - APPROVED
```

Create and merge the PR:

```bash
$ gza pr 2
PR created: https://github.com/myorg/myapp/pull/143

# After PR approval, merge locally
$ gza merge 2 --squash
Merged: feature/implement-the-jwt-authentication → main (squashed)
```

## Summary

The complete workflow:

1. **Plan** - `gza add --type plan` → `gza work <id>` → review `.gza/plans/`
2. **Implement** - `gza add --type implement --based-on <plan_id>` → `gza work <id>`
3. **Review** - `gza review <impl_id> --run`
4. **Improve** (if needed) - `gza improve <impl_id>` → `gza work <id>` → `gza review <impl_id> --run`
5. **Merge** - `gza pr <impl_id>` → `gza merge <impl_id> --squash`
