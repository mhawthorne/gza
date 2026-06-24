# Main verify self-heal contract

> **Status: Draft north-star contract.** This document defines the required convergence
> behavior for a red local-target integration verify verdict. The implement tasks
> (`gza-5778`, `gza-5856`, the checkpoint-TTL task, and the deterministic-red repair
> task) realize this contract over time; until then, code/spec mismatches are behavior
> findings, not license to weaken the contract.

## What this owns

This document owns one question:

- When the shared local-target integration verify gate goes red, how must automation
  recover or escalate so the pipeline still converges?

It does **not** own:

- The ordinary lifecycle transition table. That lives in
  [lifecycle-engine.md](lifecycle-engine.md).
- The watch-loop phase ordering, capacity accounting, or restart-safe no-progress
  mechanics. Those live in [watch-supervisor.md](watch-supervisor.md).

This file is the north-star contract that those two documents must apply whenever the
local-target verify gate is not green.

## Terms

- **Red verdict** — any configured local-target integration verify result whose status is
  not `passed`.
- **Checkpoint** — the durable recorded result reused to decide whether more merges onto
  the canonical local target are allowed.
- **Merge freeze** — the state where automation halts further merges onto the canonical
  local target because the checkpoint is red or freshness is unproven.
- **Launch stall** — downstream work stops launching or making progress for reasons that
  are only an artifact of the merge freeze rather than the work's own state.

## Contract

### MV1 — Red verify state MUST converge

A configured local-target integration verify gate MUST NOT leave the system in an
unbounded freeze. A merge freeze MAY stop further merges onto the canonical local target,
but it MUST always converge by one of these bounded outcomes:

1. the gate reruns and turns green;
2. the gate reruns and confirms a deterministic red, which then enters bounded repair
   plus visible alerting; or
3. the gate becomes a visible human-required condition with an explicit bounded reason.

A merge stall MUST NOT convert into a launch stall.

### MV2 — Red verdicts MUST be re-verified before automation acts on them

Before automation reuses a red checkpoint to keep merges halted, park work, or emit a
durable red-main attention row, it MUST rerun the local-target verify gate against the
current canonical local-target tree.

- A stale red verdict MUST NOT be reused indefinitely.
- A flake that passes on rerun MUST self-clear: the prior red checkpoint is replaced, the
  merge freeze ends, and normal merge planning resumes without requiring a human to
  manually delete or override the old red state.
- The rerun freshness proof MUST be against the exact current local-target tree. If exact
  tree freshness cannot be proven, automation MUST fail closed but treat that as a
  freshness problem to be refreshed again, not as permanent proof that the old red
  verdict remains valid forever.

### MV3 — Red checkpoints MUST have a bounded lifetime even on an unchanged tree

The durable checkpoint for a red local-target verify result MUST auto-expire after a
bounded TTL, even when the local-target tree fingerprint and verify-gate identity are
unchanged.

- The bound itself is policy; the existence of the bound and its enforcement are
  contract.
- After the TTL expires, the next lifecycle decision that would reuse that red checkpoint
  MUST rerun the local-target verify gate and replace the checkpoint with fresh evidence.
- Automation MUST NOT treat "same tree, same gate, same old red checkpoint" as sufficient
  reason to freeze merges forever.

### MV4 — Confirmed deterministic red MUST trigger bounded repair plus alert

When rerun verify confirms a real deterministic failure on the current canonical
local-target tree, automation MUST halt further merges onto that target and MUST trigger
both:

- one visible durable alert naming the red-main condition; and
- one bounded automatic repair path aimed at restoring a green local target or reaching a
  clear human-required stop.

That repair path MUST itself be bounded. It MUST NOT silently freeze the merge lane
without either making bounded repair attempts or surfacing a human-required condition.

### MV5 — Red merge freezes MUST NOT hard-park downstream work

A merge freeze caused by red local-target verify MUST NOT hard-park downstream tasks only
because merges are currently halted.

- Work that is otherwise runnable MUST remain runnable.
- Work whose next meaningful action is blocked by the freeze MAY remain waiting, but it
  MUST stay visible and re-evaluable rather than being converted into a permanent parked
  state solely because the target is red.
- The shared no-progress backstop MUST count only actually executed unchanged actions. It
  MUST NOT count repeated evaluation of a blocked merge lane, skipped launches, or
  capacity-denied actions as "no progress."

This is what prevents a merge stall from cascading into a launch stall.

## Cross-document requirements

- [lifecycle-engine.md](lifecycle-engine.md) MUST own the action semantics for the
  `main-integration-verify-red` attention path without weakening MV1-MV5.
- [watch-supervisor.md](watch-supervisor.md) MUST own the loop-level freshness checks,
  rerun timing, and no-progress accounting without weakening MV1-MV5.
- Future behavior-check findings against this area MUST classify implementation drift
  against **this** document as the source contract, not treat the current implementation
  as normative.

## Implementation note

The intended realization is split deliberately:

- `gza-5778` supplies rerun-before-reuse so flakes self-clear.
- The checkpoint-TTL task bounds red lifetime on unchanged trees.
- The deterministic-red repair task supplies bounded auto-repair plus alerting.
- `gza-5856` ensures merge freezes do not cascade into watch no-progress launch stalls.
