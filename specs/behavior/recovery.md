# Recovery semantics — resume, retry, and manual escalation

> **Status: Draft.** This document is the prescriptive contract for failed-task recovery:
> when gza MUST resume an existing provider session, when it MUST start a fresh retry,
> when an empty merge unit is moot versus still recoverable, and when automation MUST
> stop for a human.
>
> Read [00-overview.md](00-overview.md) for lifecycle states,
> [lifecycle-engine.md](lifecycle-engine.md) for where recovery is ordered into the
> engine, and [lineage.md](lineage.md) for the recovery-chain and merge-unit substrate
> this policy evaluates.

## What this owns

This spec owns the **policy layer** for failed-task recovery.

- `lineage.md` owns the mechanics of recovery chains, merge-unit attachment, and "which
  task represents this work unit".
- `lifecycle-engine.md` owns when the engine consults recovery policy.
- This document owns the decision boundary: **resume**, **retry**, **manual stop**, or
  **moot/no recovery**.

The same shared policy MUST be reused by `advance`, `iterate`, `watch`, queue/query
surfaces, and recovery dry-run output. Recovery semantics MUST NOT fork by command.

## Principles

- **R1 — Resume and retry are different actions.** `resume` continues an existing
  provider session/thread. `retry` starts a fresh execution attempt. The two MUST NOT be
  conflated.
- **R2 — Empty is not enough.** An `empty` merge unit alone MUST NOT decide whether a
  failed task is moot or recoverable. The policy MUST inspect recorded provider execution
  evidence first.
- **R3 — Fail closed for lost-work risk.** When evidence is incomplete, the policy MUST
  prefer "attempt recovery once" over "declare moot and silently drop work".
- **R4 — Recovery is bounded.** Automatic recovery MUST stop at a named budget and then
  park for a human; it MUST NOT loop forever.
- **R5 — Already-landed work is not recoverable.** Once a failed task has a valid landed
  or completed representative, recovery MUST suppress the older failed row instead of
  re-queueing it.

## Decision model

### 1. Empty merge units split into moot vs recoverable

When a failed task's active merge unit state is `empty`, the policy MUST evaluate a
single shared predicate:

- `empty_task_requires_recovery(task)` is **true** iff all of the following hold:
  - the task status is `failed`
  - a provider `session_id` is present
  - recorded execution evidence proves or may have proved actual provider work:
    `num_steps_computed >= 1`, or `num_steps_reported >= 1`, or `output_tokens > 0`
- If a `session_id` exists but some or all step/token fields are missing, the predicate
  MUST evaluate **true** (fail closed).
- Only an explicit all-zero record is sufficient proof that the task never actually ran
  and is therefore moot.

Consequences:

- `empty` + predicate **false** = **moot**. The task MUST stay out of failed-task
  recovery queues and dry-run recovery surfaces.
- `empty` + predicate **true** = **recoverable**. The task MUST stay eligible for the
  normal recovery policy and MUST NOT be suppressed merely because the branch currently
  has no commits to land.

### 2. Resume vs retry vs manual

- Timeout-style or otherwise resumable failures with a recoverable preserved session MUST
  choose `resume` on the first automatic recovery attempt.
- Retryable provider/infrastructure failures that should not reuse the same execution
  thread MUST choose `retry`.
- Manual-only failures MUST park for a human and MUST NOT be auto-resumed or auto-retried.
- A fresh retry MUST create a new execution attempt. A resume MUST preserve the provider
  session/thread being continued.

### 3. Recovery suppression after valid resolution

`empty_task_requires_recovery(task)` MUST evaluate false once the failed task is already
resolved by a valid representative, including:

- a completed recovery descendant for the same failed chain
- a completed automatic sibling recovery that already resolved the replaced attempt
- another valid landed representative for the same code by an independent path

The same failed task being `empty` on its own MUST NOT count as proof of resolution.

## Operator-visible behavior

- Operator wording MUST distinguish **moot empty work** from **empty but resumable
  failed work**.
- `iterate` MUST check the shared empty-recovery predicate before printing an `empty` /
  "no remaining commits to land" terminal message.
- Failed-task recovery queues and dry-run reports MUST omit moot empty tasks and retain
  recoverable empty tasks.
- `watch`, `advance`, `iterate`, and query/recovery-lane surfaces MUST agree on the same
  recovery decision for the same task.

## Policy knobs

| Knob | Default | Governs |
|------|---------|---------|
| `recovery attempts` | bounded | Automatic resume/retry budget before escalation |
| provider resumability classification | provider-owned | Which failure reasons are resume-capable vs retry-only vs manual |

## Open questions

- **OQ1 — Empty dependency semantics after recoverable failure.** If a failed
  implementation is `empty` but still recoverable, should downstream merge-required
  dependencies remain blocked until recovery resolves, or is `empty` still sufficient to
  satisfy those dependencies immediately? The current contract for empty dependencies
  lives in `lineage.md`; recovery-aware dependency semantics may need a follow-up spec.
- **OQ2 — Provider evidence fidelity.** The fail-closed rule assumes step/token capture
  can be incomplete. If providers become fully reliable here, the predicate may be
  tightened later at this single policy point.
