# Plan → Implement → Review Workflow

A multi-phase workflow for larger features requiring design review.

## Phase 1: Create and run a plan

```bash
$ gza add --type plan --tag auth-refactor
# Opens $EDITOR to write the prompt
```

Or provide the prompt directly:

```bash
$ gza add --type plan --tag auth-refactor \
  "Design a new authentication system using JWT tokens. Consider:
   - Token refresh strategy
   - Secure storage on client
   - Session invalidation
   - Migration path from current cookie-based auth"

Created task gza-1: 20260108-design-a-new-authentication (plan)
Tags: auth-refactor
```

> **Note:** The `--tag` flag is optional. Tags make it easier to track related tasks with commands such as `gza search "" --tag auth-refactor`.

Run the plan task:

```bash
$ gza work gza-1
=== Task: Design a new authentication system... ===
    ID: gza-1 20260108-design-a-new-authentication
    Type: plan
...
=== Done ===
Stats: Runtime: 8m 12s | Turns: 15 | Cost: $0.42
```

> **Tip:** If you don't provide a task ID, `gza work` runs the next pending task.

## Review the plan

The plan is saved to `.gza/plans/` for inspection:

```bash
$ cat .gza/plans/20260108-design-a-new-authentication.md
```

The plan content is also stored in the database, so it's available to dependent tasks even in fresh worktrees.

## Phase 2: Run the automated plan-review gate

For unattended lifecycle progress, create or run the plan-review task:

```bash
$ gza plan-review gza-1 --run
✓ Created plan review task gza-2
  Plan source: gza-1

Running plan review task gza-2...
```

If the plan is approved, the report contains a machine-readable slice manifest. You can inspect it with:

```bash
$ gza show gza-2
```

If you need to correct the reviewed slicing before materializing tasks:

```bash
$ gza plan-review gza-2 --run --edit-slices
$ gza plan-review gza-2 --run --materialize
✓ Materialized implementation slices for plan review gza-2
```

## Phase 3: Implement reviewed slices

The normal manual approval path is now `gza implement <plan-id>`. Bare `implement` queues the work; add `--run` if you want it to start immediately. When an approved valid plan-review manifest exists, it materializes the reviewed slices instead of creating one monolithic implement task:

```bash
$ gza implement gza-1
✓ Created implement task gza-3
  Plan source: gza-1
  Plan review: gza-2
```

If no approved plan review exists yet, `gza implement <plan-id>` preserves the legacy single-implement fallback and warns that the automated lifecycle prefers plan review and slicing.

Run the first materialized implementation slice:

```bash
$ gza work gza-3
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
$ gza review gza-3 --run
✓ Created review task gza-4
=== Task: Review implementation... ===
    ID: gza-4 20260108-review-implementation
    Type: review
...
=== Done ===
Stats: Runtime: 3m 18s | Turns: 8 | Cost: $0.28
```

> **Alternative:** You can use `--review` with `gza add` to auto-create a review task upfront:
> ```bash
> $ gza add --type implement --based-on gza-1 --review "Implement..."
> ```
>
> If `gza-1` is a completed plan still held for review, this command is rejected on purpose.
> Release the plan first with `gza implement gza-1` (queues by default) or
> `gza edit gza-1 --no-hold-for-review`, then create follow-up implementation work.
>
> Add `--pr` as well if you want the implementation to request PR creation or reuse after it completes successfully. That request is evaluated at completion time and skipped without failing when PRs are unavailable, so later `gza review` runs can post PR comments automatically when a PR exists:
> ```bash
> $ gza add --type implement --based-on gza-1 --review --pr "Implement..."
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

If the review requests changes, create and run an improve task with `--run`:

```bash
$ gza improve gza-3 --run
✓ Created improve task gza-5
=== Task: Improve implementation based on review... ===
    ID: gza-5 20260108-improve-implementation
    Type: improve
...
=== Done ===
Stats: Runtime: 5m 22s | Turns: 14 | Cost: $0.45
```

> **Tip:** Add `--review` if you want automatic follow-up reviews after each improvement iteration.
> Use `--queue` to add to the queue without running immediately.

Run a follow-up review to verify the changes:

```bash
$ gza review gza-3 --run
✓ Created review task gza-6
=== Task: Review implementation... ===
    ID: gza-6 20260108-review-implementation
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
$ gza search "" --tag auth-refactor

completed   gza-1 (2026-01-08 09:14) Design a new authentication...
    [plan]
    stats: 8m12s | 2026-01-08

completed   gza-2 (2026-01-08 09:30) Plan review: authentication...
    [plan_review]
    stats: APPROVED | 2026-01-08

completed   gza-3 (2026-01-08 09:52) Implement the JWT authentication...
    [implement]
    stats: 12m45s | 2026-01-08

completed   gza-4 (2026-01-08 10:20) Review implementation...
    [review]
    stats: CHANGES_REQUESTED | 2026-01-08

completed   gza-5 (2026-01-08 10:41) Improve implementation...
    [improve]
    stats: 5m22s | 2026-01-08

completed   gza-6 (2026-01-08 11:03) Review implementation...
    [review]
    stats: APPROVED | 2026-01-08
```

Create and merge the PR (target the implementation task, `gza-3`):

```bash
$ gza pr gza-3
PR created: https://github.com/myorg/myapp/pull/143

# After PR approval, merge locally
$ gza merge gza-3 --squash
Merged: feature/implement-the-jwt-authentication → main (squashed)

# Daily merge-truth check: what still needs to be merged?
$ gza unmerged
No unmerged tasks

# Explicit PR reconciliation: refresh cached PR state and close stale open PRs if origin proves the merge landed
$ gza sync gza-3
feature/implement-the-jwt-authentication | merge=merged | pr=#143:closed
```

## Summary

The complete workflow:

1. **Plan** - `gza add --type plan` → `gza work <task_id>`
2. **Plan review** - `gza plan-review <plan_id>` queues by default; use `--run` to execute immediately → inspect `gza show <plan_review_id>` → optionally `--run --edit-slices` / `--run --materialize`
3. **Implement** - `gza implement <plan_id>` (materializes approved slices when present) → `gza work <task_id>`
4. **Review** - `gza review <impl_id> --run`
5. **Improve** (if needed) - `gza improve <task_id> --run` → `gza review <task_id> --run` (accepts implement, improve, or review ID — auto-resolves)
6. **Merge** - `gza pr <impl_id>` → `gza merge <impl_id> --squash` → `gza sync <impl_id>`
7. **Daily reconciliation** - `gza unmerged` answers the default-branch merge-truth question; use `gza sync` when you explicitly want broader branch and PR refresh
