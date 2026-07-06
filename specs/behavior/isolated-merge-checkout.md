# Isolated merge checkout

> **Status: Draft.** This document owns the contract for watch-time default-branch merge
> staging when `main_checkout_isolate` is enabled: the detached integration checkout, the
> promotion of a successful staged merge onto the real default-branch ref, and how an
> attached default-branch checkout is treated during that promotion.

## What this owns

This file specifies the isolation boundary for watch-time merges onto the default branch.
It answers:

- where the staged merge runs;
- when the real default-branch ref advances;
- what MUST happen when another checkout has the default branch attached and contains
  tracked local edits.

It does **not** own general watch-cycle ordering or merge eligibility. Those belong to
[watch-supervisor.md](watch-supervisor.md) and [00-overview.md](00-overview.md).

## Contract

When `main_checkout_isolate` is enabled:

1. Automated default-branch merge flows that enable `main_checkout_isolate` MUST stage
   each merge attempt in a dedicated detached integration checkout that is separate from
   any operator-attached default-branch checkout.
2. When a configured local-target verify gate exists, the exact staged candidate tree
   MUST verify green before promotion. A staged merge MUST count as landed only after the
   real default-branch ref is advanced to that verified detached merge result.
3. If some attached checkout currently has the default branch checked out, watch MUST
   reset that checkout to the newly advanced default-branch tip after promotion so the
   checkout does not drift behind the moved ref.
4. Tracked local edits in that attached default-branch checkout MUST NOT block promotion.
   Before resetting the checkout, watch MUST stash those tracked edits, advance the real
   default-branch ref, and then attempt to restore the stash onto the new tip.
5. Untracked files in the attached default-branch checkout MAY remain in place; they are
   not a promotion blocker and do not need to be stashed.
6. If the stashed tracked edits restore cleanly onto the new tip, watch MUST leave the
   checkout with those edits applied on top of the new default-branch commit.
7. If the stashed tracked edits do not restore cleanly, watch MUST leave the checkout
   clean at the new default-branch tip and MUST preserve the stash for manual recovery.
   Watch MUST emit an operator-visible warning that names the preserved stash.
8. A conflicting tracked-edit restore MUST NOT roll back a successful ref promotion and
   MUST NOT re-dirty the attached checkout in a way that can block later isolated merges.
9. If promotion later fails after the stash was already restored cleanly, rollback MUST
   NOT re-apply or drop some older shifted stash entry by reusing the saved `stash@{n}`
   ordinal. Any cleanup failure that leaves the operator stash parked or unrestored MUST
   be surfaced in the promotion failure.
10. If candidate verify blocks promotion after the detached checkout was mutated, the next
    automated merge attempt in the same command cycle MUST first refresh or rebuild that
    checkout back to the canonical target, or the merge lane MUST stop for the cycle.

## Rationale

The point of isolation is that operator working state in the attached default-branch
checkout does not gate automated landing. Parking a conflicting stash preserves the
operator's work without turning one local conflict into a standing merge-stall for the
entire watch queue.
