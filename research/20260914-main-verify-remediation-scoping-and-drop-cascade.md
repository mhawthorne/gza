# Main-verify remediation: scoping gap and drop-cascade gap

Date: 2026-09-14
Related: gza-10886 (dropped), gza-10889 (dropped), gza-10888 (plan)

## What happened

1. Main went red on two independent verify phases at once: `ruff` (import
   order) and `unit` (two tests raising `RetryTargetLineageResolvedError`).
2. Watch's main-verify remediation only detects the **first** failing phase
   in the verify output and builds a fix task scoped to just that phase.
   It filed gza-10886, scoped only to `ruff`.
3. gza-10886 correctly fixed `ruff`, but its own verify run then hit the
   still-broken `unit` phase — out of its stated scope — so it parked
   unmerged instead of merging.
4. Watch's `verify_fix` machinery then filed gza-10889, a child task
   `based_on` gza-10886, whose job is to fix whatever is still failing
   verify on that branch. This is a second, slower hop at fixing the same
   "main is red" problem — implement (scoped) -> verify_fix (unscoped) ->
   merge, instead of one task fixing everything up front.
5. Meanwhood, a human fixed both `ruff` and the `unit` regression directly
   on main (see commit 3334361fa) faster than the task chain could land.
6. gza-10886 was dropped as redundant. gza-10889 had already independently
   completed and verified green, so it was **not** touched by the parent's
   drop — it sat as "completed, ready to merge" a fix that was already
   redundant. It required a second, separate manual drop.

## Root causes

### 1. First-failing-phase scoping (see gza-10888 for the fix plan)

`_verify_failure_phase_name` in `src/gza/main_integration_verify.py:448-454`
returns the first failing phase it finds and stops — it never collects all
currently-failing phases. The resulting single-phase `signature`
(`phase:X`) flows into the spawned task's prompt and into watch's
dedup/reuse keying, so remediation is always scoped to one phase at a time,
sequentially, even when main is red on multiple phases simultaneously.
Multi-phase failures currently require this: partial fix -> park -> child
verify_fix task -> maybe fixes the rest -> merge. Slow, and via
`verify_fix` this ad hoc "fix the whole failure class" step already exists
implicitly — it should be scoped correctly up front instead.

### 2. Drop doesn't cascade to scope-completing children

When a task is dropped as redundant/superseded, nothing propagates that to
a child task whose sole purpose is completing that parent's unresolved
work (e.g. a `verify_fix` child, or any `based_on` child filed only to get
the parent mergeable). If that child already finished independently before
the drop, it's left behind as an orphaned "ready to merge" task that a
human has to separately notice and drop.

## Fix direction

Both issues are folded into gza-10888 (plan, tagged `system`, `v0.5.1`):

- Collect **all** failing verify phases and scope one remediation task to
  fix all of them (not just the first), removing the need for a follow-on
  `verify_fix` hop in the common case.
- When a task is dropped, cascade the drop to children that exist only to
  extend/complete its scope (verify_fix children in particular), so a
  human doesn't have to manually clean up orphaned "ready to merge" tasks
  after fixing the underlying problem another way.
