# Plan → Implement → Review Workflow

A multi-phase workflow for larger features requiring design review.

## Phase 1: Create and run a plan

```bash
$ uv run gza add --type plan --tag auth-refactor
# Opens $EDITOR to write the prompt
```

Or provide the prompt directly:

```bash
$ uv run gza add --type plan --tag auth-refactor \
  "Design a new authentication system using JWT tokens. Consider:
   - Token refresh strategy
   - Secure storage on client
   - Session invalidation
   - Migration path from current cookie-based auth"

Created task gza-1: 20260108-design-a-new-authentication (plan)
Tags: auth-refactor
```

> **Note:** The `--tag` flag is optional. Tags make it easier to track related tasks with commands such as `uv run gza search --tag auth-refactor`.

Run the plan task:

```bash
$ uv run gza work gza-1
=== Task: Design a new authentication system... ===
    ID: gza-1 20260108-design-a-new-authentication
    Type: plan
...
=== Done ===
Stats: Runtime: 8m 12s | Turns: 15 | Cost: $0.42
```

> **Tip:** If you don't provide a task ID, `uv run gza work` runs the next pending task.

## Review the plan

The plan is saved to `.gza/plans/` for inspection:

```bash
$ cat .gza/plans/20260108-design-a-new-authentication.md
```

The plan content is also stored in the database, so it's available to dependent tasks even in fresh worktrees.

## Phase 2: Run the automated plan-review gate

For unattended lifecycle progress, create or run the plan-review task:

```bash
$ uv run gza plan-review gza-1
✓ Created plan review task gza-2
  Plan source: gza-1

Running plan review task gza-2...
```

If the plan is approved, the report contains a machine-readable slice manifest. You can inspect it with:

```bash
$ uv run gza show gza-2
```

If you need to correct the reviewed slicing before materializing tasks:

```bash
$ uv run gza plan-review gza-2 --edit-slices
$ uv run gza plan-review gza-2 --materialize
✓ Materialized implementation slices for plan review gza-2
```

## Phase 3: Implement reviewed slices

The normal manual approval path is now `uv run gza implement <plan-id>`. When an approved valid plan-review manifest exists, it materializes the reviewed slices instead of creating one monolithic implement task:

```bash
$ uv run gza implement gza-1
✓ Created implement task gza-3
  Plan source: gza-1
  Plan review: gza-2
```

If no approved plan review exists yet, `gza implement <plan-id>` preserves the legacy single-implement fallback and warns that the automated lifecycle prefers plan review and slicing.

Run the first materialized implementation slice:

```bash
$ uv run gza work gza-3
=== Task: Implement approved plan-review slice S1... ===
    ID: gza-3 20260108-implement-the-jwt-authentication
    Type: implement
...
=== Done ===
Stats: Runtime: 12m 45s | Turns: 32 | Cost: $1.23
Branch: feature/implement-the-jwt-authentication
```

## Phase 4: Review the implementation

Create and run a review task:

```bash
$ uv run gza review gza-2
✓ Created review task gza-3
=== Task: Review implementation... ===
    ID: gza-3 20260108-review-implementation
    Type: review
...
=== Done ===
Stats: Runtime: 3m 18s | Turns: 8 | Cost: $0.28
```

> **Alternative:** You can use `--review` with `uv run gza add` to auto-create a review task upfront:
> ```bash
> $ uv run gza add --type implement --based-on gza-1 --review "Implement..."
> ```
>
> Add `--pr` as well if you want the implementation to request PR creation or reuse after it completes successfully. That request is evaluated at completion time and skipped without failing when PRs are unavailable, so later `uv run gza review` runs can post PR comments automatically when a PR exists:
> ```bash
> $ uv run gza add --type implement --based-on gza-1 --review --pr "Implement..."
> ```

View the review:

```bash
$ cat .gza/reviews/20260108-review-implementation.md

# Review: 20260108-implement-the-jwt-authentication

## Summary
Implementation follows the plan but needs improvements...

## Blockers
### B1
Evidence: Missing rate limiting on refresh endpoint.
Impact: Allows brute-force refresh abuse and can degrade service.
Required fix: Add request throttling for refresh attempts.
Required tests: Add a targeted test proving rate limits trigger on repeated refresh calls.

## Follow-Ups
### F1
Evidence: Token claim validation could be hardened for malformed optional claims.
Impact: Low-risk hardening opportunity; supported path remains correct.
Recommended follow-up: Add stricter optional-claim normalization and validation.
Recommended tests: Add malformed-claim regression cases.

## Questions / Assumptions
None.

## Verdict
Blocking security issue exists.
Verdict: CHANGES_REQUESTED
```

## Phase 5: Address review feedback

If the review requests changes, create and run an improve task (runs immediately by default):

```bash
$ uv run gza improve gza-2
✓ Created improve task gza-4
=== Task: Improve implementation based on review... ===
    ID: gza-4 20260108-improve-implementation
    Type: improve
...
=== Done ===
Stats: Runtime: 5m 22s | Turns: 14 | Cost: $0.45
```

> **Tip:** Add `--review` if you want automatic follow-up reviews after each improvement iteration.
> Use `--queue` to add to the queue without running immediately.

Run a follow-up review to verify the changes:

```bash
$ uv run gza review gza-2
✓ Created review task gza-5
=== Task: Review implementation... ===
    ID: gza-5 20260108-review-implementation
    Type: review
...
=== Done ===
```

Check the new review verdict:

```bash
$ cat .gza/reviews/20260108-review-implementation-2.md

# Review: 20260108-implement-the-jwt-authentication

## Summary

- Follow-up changes address prior blockers.
- Tests cover the updated auth path.
- No new regressions found in touched areas.

## Blockers

None.

## Follow-Ups

None.

## Questions / Assumptions

None.

## Verdict

All requested changes have been addressed.
Verdict: APPROVED
```

## Phase 6: Create PR and merge

Check the related tasks by tag:

```bash
$ uv run gza search --tag auth-refactor

  ✓ gza-1 20260108-design-a-new-authentication (plan)
      completed - 8m 12s

  ✓ gza-2 20260108-implement-the-jwt-authentication (implement)
      completed - 12m 45s

  ✓ gza-3 20260108-review-implementation (review)
      completed - CHANGES_REQUESTED

  ✓ gza-4 20260108-improve-implementation (improve)
      completed - 5m 22s

  ✓ gza-5 20260108-review-implementation (review)
      completed - APPROVED
```

Create and merge the PR:

```bash
$ uv run gza pr gza-2
PR created: https://github.com/myorg/myapp/pull/143

# After PR approval, merge locally
$ uv run gza merge gza-2 --squash
Merged: feature/implement-the-jwt-authentication → main (squashed)

# Daily merge-truth check: what still needs to be merged?
$ uv run gza unmerged
No unmerged tasks

# Explicit PR reconciliation: refresh cached PR state and close stale open PRs if origin proves the merge landed
$ uv run gza sync gza-2
feature/implement-the-jwt-authentication | merge=merged | pr=#143:closed
```

## Summary

The complete workflow:

1. **Plan** - `uv run gza add --type plan` → `uv run gza work <task_id>`
2. **Plan review** - `uv run gza plan-review <plan_id>` → inspect `uv run gza show <plan_review_id>` → optionally `--edit-slices` / `--materialize`
3. **Implement** - `uv run gza implement <plan_id>` (materializes approved slices when present) → `uv run gza work <task_id>`
4. **Review** - `uv run gza review <impl_id>`
5. **Improve** (if needed) - `uv run gza improve <task_id>` → `uv run gza review <task_id>` (accepts implement, improve, or review ID — auto-resolves)
6. **Merge** - `uv run gza pr <impl_id>` → `uv run gza merge <impl_id> --squash` → `uv run gza sync <impl_id>`
7. **Daily reconciliation** - `uv run gza unmerged` answers the default-branch merge-truth question; use `uv run gza sync` when you explicitly want broader branch and PR refresh
