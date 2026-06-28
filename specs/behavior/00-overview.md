# Lifecycle state machine — overview

> **Status: Draft.** This file owns the shared model for `specs/behavior/`: vocabulary,
> system-wide invariants, the lifecycle diagram, and the consolidated human-escalation
> table. Core invariants and the five lifecycle decisions were ratified 2026-06-01 (see
> *Ratified decisions* in [lifecycle-engine.md](lifecycle-engine.md)); detailed transition
> rules remain draft pending a conformance pass against the code.

## What this models

gza turns a request into landed code by moving a **unit of work** through a lifecycle —
implement, review, improve, rebase, merge — spawning AI workers for each step and
escalating to a human only when automation cannot safely proceed.

This document specifies that lifecycle as a state machine. It is the answer to:

- When does work move from implement → review → improve?
- When do rebases happen?
- When does a merge happen?
- **When must a human get involved, and how do they clear it?**

It does **not** specify the long-running runtime loop that drives those decisions. Cycle
cadence, slot accounting, detached-worker adoption, drift restart, and pass ordering live
in [watch-supervisor.md](watch-supervisor.md). Read this overview plus
[lifecycle-engine.md](lifecycle-engine.md) for the pure per-work-unit decision function;
read [watch-supervisor.md](watch-supervisor.md) for the operational contract that drives
those decisions continuously.

## Vocabulary (the data model, abstractly)

The contract is defined over these concepts, independent of how they are stored.

- **Task** — one unit of agent execution with a type and an execution status. Tasks are
  the *atoms*; the engine spawns them and reads their results.
  - **Types:** `plan`, `plan_review`, `plan_improve`, `explore`, `implement`, `review`, `improve`, `rebase`.
  - **Execution status:** `pending` → `in_progress` → `completed` | `failed`.
- **Work unit** (a.k.a. *merge unit* / *implementation lineage*) — the *molecule*: the
  set of related tasks that together produce one mergeable change on one branch. This is
  the thing that has a lifecycle and a merge state. A work unit MUST have exactly one
  canonical merge target branch.
- **Merge state of a work unit:** `unmerged` | `merged`. Authoritative answer to "has
  this landed?" It MUST be decided from recorded lifecycle state, **not** from strict
  git ancestry (a squash-merged branch fails ancestry but is merged). See the four
  merge-state axes in `docs/internal/task-model-canonical.md`.
- **Review verdict** (on a completed `review` task): `APPROVED` |
  `APPROVED_WITH_FOLLOWUPS` | `CHANGES_REQUESTED` | `NEEDS_DISCUSSION`. Any other value
  is treated as *unknown* and escalates.
- **The engine** — the transition function. Each pass, for every unresolved work unit,
  it reads the current state and selects exactly one next action. It MUST be
  **idempotent**: running it repeatedly with no external change produces the same
  decision and never duplicates in-flight work.

## Layered state

There are two FSMs, and they MUST NOT be conflated:

1. **Task execution FSM** (per task): `pending → in_progress → completed | failed`. This
   is owned by the worker that runs the task.
2. **Work-unit lifecycle FSM** (per work unit): the diagram below. This is owned by the
   engine and is *derived* from the tasks in the unit plus git/merge state. The engine
   does not store the lifecycle state as a column; it recomputes it each pass. This is
   what makes the engine idempotent and safe to interrupt.

## The work-unit lifecycle

```mermaid
stateDiagram-v2
    [*] --> Planned: plan task completes
    [*] --> Implementing: implement task created

    Planned --> PlanReviewing: auto_implement\n(create/run plan_review)
    Planned --> AwaitingHuman: plan held\n(auto_implement = false)
    AwaitingHuman --> Planned: APPROVED valid plan_review\n(release hold only)
    PlanReviewing --> SlicingMaterializing: APPROVED valid manifest
    SlicingMaterializing --> Implementing: materialize reviewed slices
    PlanReviewing --> PlanImproving: CHANGES_REQUESTED
    PlanReviewing --> HumanParked: invalid manifest / unknown verdict / needs discussion
    PlanImproving --> PlanReviewing: revised plan completes

    Implementing --> Implemented: implement completes\n(branch exists)
    Implementing --> Recovering: implement fails

    Implemented --> ScopeParked: touches out-of-scope paths\n(not tagged cross-project)
    Implemented --> Reviewing: review required,\nno valid review
    Implemented --> Mergeable: review not required

    Reviewing --> Mergeable: APPROVED (review still valid)
    Reviewing --> MergeableWithFollowups: APPROVED_WITH_FOLLOWUPS
    Reviewing --> Improving: CHANGES_REQUESTED\n(cycles < limit)
    Reviewing --> HumanParked: unknown / inconsistent verdict,\ncurrent-head max cycles, duplicate blocker,\nverify-blocked

    Improving --> Reviewing: improve changed code\n(fresh review)
    Improving --> Recovering: improve fails
    Improving --> HumanParked: no-op cycles >= limit

    Mergeable --> Rebasing: selected for merge,\nconflicts with target
    Recovering --> Rebasing: recovery preflight,\nbranch lacks target tip
    Rebasing --> Reviewing: rebased, resolution delta changed\n(resolution review required)
    Rebasing --> Mergeable: rebased, implementation patch preserved\n(prior approval carried)
    Rebasing --> HumanParked: rebase failed / circuit breaker /\ndid not unblock merge
    MergeableWithFollowups --> Merged: file follow-ups, then merge
    Mergeable --> Merged: merge succeeds

    Recovering --> Implementing: resume / retry (within bounds)
    Recovering --> Improving: resume / retry (within bounds)
    Recovering --> HumanParked: retry limit / ambiguous recovery

    AwaitingHuman --> Implementing: human approves / re-enables
    HumanParked --> Reviewing: human resolves & re-advances
    ScopeParked --> Implemented: human tags cross-project\nor fixes branch

    Merged --> [*]
```

`AwaitingHuman`, `ScopeParked`, and `HumanParked` are the only states that require a
person. They are not failures — they are deliberate stops where automation declined to
guess. Everything else MUST progress without human input.

## Core invariants (the load-bearing rules)

These hold across the whole machine; the detailed rules in
[lifecycle-engine.md](lifecycle-engine.md) MUST NOT contradict them.

1. **Idempotent & interruptible.** Re-running the engine never duplicates in-flight work
   and never double-merges. In-progress tasks cause a wait, not a respawn.
   Singleton derived-child creation applies to `review`, `rebase`, and review-backed
   `improve` tasks: each parent MUST have at most one active direct `based_on` child of
   that kind at a time, where active means `pending` or `in_progress`. This invariant
   does not apply to non-singleton derived fan-out such as follow-up `implement` tasks,
   and it does not block comments-only `improve` refreshes when newer unresolved
   feedback needs a fresh pass.
2. **Bounded loops, always.** Every cycle (review→improve, rebase, recovery, no-op
   improve) MUST have a hard bound. When the bound is hit, the unit goes to a human
   state — it MUST NOT loop forever and MUST NOT silently give up. The *existence and
   enforcement* of each bound is invariant; the specific bound *values* are tunable
   policy knobs, not contract (see [lifecycle-engine.md](lifecycle-engine.md)).
3. **Review is the universal pre-merge checkpoint** (policy `require_review_before_merge`,
   default on). When on, an implementation work unit MUST have a current, valid review
   *whose verdict permits merge* before it can merge. The **verdict is the gate**:
   `CHANGES_REQUESTED` blocks unless every current blocker is later cleared by a valid
   derived path (for example a later review, verify-only provenance clearing, superseding
   review output, or adjudication marking a disputed blocker `INVALID` for lifecycle
   purposes). That clearance is derived state, not a historical verdict rewrite or merge
   bypass. `APPROVED` and `APPROVED_WITH_FOLLOWUPS` permit merge — the latter meaning the
   reviewer judged the code mergeable now, with the follow-ups as non-blocking later
   work. When a verdict carries follow-ups, those follow-ups MUST be durably recorded as
   tracked work *before* the merge completes, so nothing is lost. This is the one
   human-or-agent quality gate the whole pipeline is built around.
4. **The local target branch is canonical.** Merge-ness MUST be proven against the local
   target branch, never against `origin/<target>`. The engine MUST NOT push the target
   branch as a side effect of merging.
5. **Never destroy work to make progress.** The engine MUST NOT delete branches and MUST
   NOT discard a human's uncommitted work. Branch cleanup is an operator concern.
6. **No orphans left pending.** Work that can never progress (moot, superseded, orphaned)
   MUST be surfaced for an explicit drop decision, not left silently pending — pending
   work gets run.

The pass-ordering invariant "land fresh code first" is owned by
[watch-supervisor.md](watch-supervisor.md), because it constrains the supervisor's cycle
execution order rather than the engine's per-work-unit decision function.

## Human-escalation table

Every state/reason that requires a human. This is the contract's most important table:
each row is a place we chose *not* to automate. The goal is to shrink this table over
time, so each row names what would let us remove it.

| State / reason | Trigger (intent) | How a human clears it | Path to removing the stop |
|----------------|------------------|------------------------|---------------------------|
| `AwaitingHuman` — plan held | A plan completed but auto-implement is off for this lineage. | Review the plan; start the implement task, or re-enable automatic follow-up. | Per-lineage policy: trusted plans MAY auto-implement. |
| `HumanParked` — manual plan-review creation | A completed plan needs automated plan review, but `advance_create_plan_reviews` is off and no review exists yet. | Create a `plan_review` manually or re-enable automatic plan-review creation. | Re-enable auto-creation once the project trusts the plan-review gate. |
| `HumanParked` — invalid plan-review slices | A plan review said `APPROVED`, but the slice manifest was missing, malformed, oversized, cyclic, ambiguous, or otherwise invalid. | Fix the plan review output or use `uv run gza plan-review <review-id> --edit-slices`, then materialize again. | Stronger structured prompting and deterministic validation feedback. |
| `HumanParked` — plan review needs discussion | The plan review explicitly concluded that automation cannot safely approve or revise the plan on its own. | Resolve the design ambiguity, revise the plan, then re-run plan review. | Better plan prompts and richer source context. |
| `HumanParked` — unknown plan-review verdict | The plan-review verdict could not be classified. | Re-run or correct the plan-review output. | More reliable plan-review verdict extraction. |
| `HumanParked` — plan-review cycle limit | `plan_review` → `plan_improve` loops hit `max_plan_review_cycles` without approval. | Take over the planning work, fix the design gaps, then restart the review loop. | Better plan-improve quality and better escalation hints. |
| `needs_discussion` — explore dangling | An explore task completed with no plan/implement follow-up. | Decide: drop it, or spawn follow-up work. | Auto-summarize explore output and propose next work. |
| `ScopeParked` — out of scope | The branch diff touches paths outside the work unit's declared project scope and it is not tagged `cross-project`. | Tag `cross-project` and re-advance if intended, or fix the branch. | Clearer per-task scope declaration up front. |
| `needs_discussion` — scope unverifiable | The scope of the diff could not be checked reliably (bad ref/diff). | Fix the ref/diff problem, or tag `cross-project` if the wide scope is intended. | More robust diff inspection. |
| `needs_discussion` — rebase failed | A rebase task failed and no later proof shows the work already landed. | Resolve the conflict manually, then re-advance. | Better autonomous conflict resolution. |
| `needs_discussion` — rebase did not unblock | A rebase completed but the branch still cannot merge. | Decide manually; don't let the engine re-queue an identical rebase. | Detect why the rebase was a no-op. |
| `needs_discussion` — rebase circuit breaker | Repeated rebase attempts (default bound) with no intervening progress. | Resolve manually. | Same as autonomous conflict resolution. |
| `needs_discussion` — incomplete lineage, rebase moot | The branch already contains the target tip but the lineage is still unresolved. | Inspect the lineage; resolve the real blocker. | Tighten lineage-resolution detection. |
| `needs_discussion` — review refresh blocked | A completed rebase changed the implementation patch or conflict-resolution delta after the latest completed review, so a narrower refresh review is required, but auto-review creation is off. Target movement alone does not trigger this row. | Refresh the review manually, then merge. | Re-enable auto-review creation for the lineage. |
| `needs_discussion` — review freshness unverified | The engine could not verify whether the latest completed review still matches the current implementation head after a code-changing lineage event, so freshness is unknown. Target movement alone does not trigger this row. | Fix the branch-head probe problem or refresh review state manually, then re-advance. | More robust git freshness probing and better operator diagnostics. |
| `needs_discussion` — inconsistent review | Verdict `APPROVED_WITH_FOLLOWUPS` but zero parsed follow-ups (self-contradictory output). | Re-review / correct the review output. | More reliable verdict extraction. |
| `needs_discussion` — verify-blocked | Review keeps failing only because the verify step times out, not on code issues, once timeout-only reviews hit the threshold and no runner-owned review-fail → no-op-improve-pass transition at the same branch head has already cleared the review. Ordinary verify-failure-only stale reviews are handled by that same-head evidence-clear path or, failing that proof, by the generic no-op improve stop instead of this timeout-specific park. | Fix the environment/verify config, then re-advance. | Separate "verify infra failed" from "code rejected." |
| `max_cycles_reached` — review churn | Review→improve cycles within the current durable-progress epoch hit the bound (`max_review_cycles`) and no stale-review refresh path is available. Historical churn from older reviewed heads does not count once fresh durable progress has produced a new epoch. | Take over: review and fix inline, or redirect the work. | Better improve quality; raise/redesign the bound. |
| `needs_discussion` — blocker adjudication needed | A disputed non-verify CODE blocker reached independent adjudication, but the adjudicator returned `NEEDS_HUMAN`, failed, or produced an unsafe/unparseable result. | Review the blocker, the dispute evidence, and the adjudication output; then fix, override, or restate the blocker explicitly. | Reliable adjudication worker plus durable blocker-resolution state. |
| `needs_discussion` — duplicate blocker | The same primary blocker repeats across cycles (default bound) with no progress. | Resolve the underlying issue the agent keeps missing. | Detect and break the repeat earlier. |
| `needs_discussion` — no-op improves | Improve completed without changing code, repeatedly (`max_noop_improve_cycles`). A verify-origin review does not park when lifecycle first classifies the blocker set as verify-only and runner-owned verify evidence shows the review FAILED and the no-op improve later PASSED at the same branch head, including ordinary verify failures as well as timeouts, and including after a review-preserved rebase that refreshes that evidence onto the rewritten head. Disputed non-verify CODE blockers instead route to adjudication first; remaining no-op cases still park. | Decide whether the feedback is actionable; fix or drop. | Detect un-actionable feedback up front. |
| `needs_discussion` — unknown verdict | The review verdict could not be classified. | Re-review or correct the output. | More reliable verdict extraction. |
| `HumanParked` — recovery exhausted | Automatic resume/retry hit its limit or the recovery situation is ambiguous. | Diagnose the failure; resume, redirect, or drop. | Better failure classification & recovery. |

**Reason codes are contract; messages are not.** Several rows share one state
(`HumanParked`) and differ only by *reason*. Each parked action MUST carry a
machine-readable **reason code** drawn from a stable, enumerated set (see *Parked reason
codes* in [lifecycle-engine.md](lifecycle-engine.md)); automation MAY branch on the code,
and adding a new code is a spec change. The human-facing **message** that accompanies a
code is free text and MAY be reworded at any time.
</content>
