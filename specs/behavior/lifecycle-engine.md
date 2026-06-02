# Lifecycle engine — transition rules

> **Status: Draft.** The prescriptive transition function: given a work unit's current
> state, which single action the engine selects. Read
> [00-overview.md](00-overview.md) first for the state machine and vocabulary. The five
> *Ratified decisions* at the end were settled 2026-06-01 and are contract; the rules
> themselves remain draft pending a conformance pass against the code.
>
> *Implementation note (non-normative): today this logic lives in the shared rule
> evaluator behind `gza advance`, and is reused by `gza iterate` and `gza watch`. The
> rules below are the intended behavior; the code is the thing measured against them.*

## How the engine decides

The engine evaluates an **ordered list of rules; first match wins.** For each unresolved
work unit it selects exactly one action, then executes selected actions for the pass.

This ordering is itself part of the contract — the rules are written so that earlier
rules are *safety gates* (don't act on out-of-scope or conflicted code) and later rules
are *progress* (review, improve, merge). Reordering changes behavior.

### Action vocabulary

- **Worker-spawning** (subject to batch limits): create/run a `review`, `improve`,
  `rebase`, `implement`, resume, or retry task.
- **Direct** (not batch-limited): `merge`, `merge_with_followups`.
- **Wait**: an expected task is in progress; do nothing and re-evaluate next pass.
- **Stop-for-human**: `awaiting_human`, `needs_discussion`, `max_cycles_reached` (see the
  escalation table in the overview).
- **Skip**: nothing to do for this unit.

The engine MUST distinguish *task created/selected* from *worker failed to start* in its
output: a creation success followed by a launch failure MUST NOT be reported as a plain
failure to create.

## Principles these rules must satisfy

These restate the core invariants for this layer; no rule below may contradict them.

- **P1 — Idempotent.** A `pending`/`in_progress` child for the needed step MUST cause a
  *wait*, never a duplicate spawn. Already-merged units MUST be invisible to the engine.
- **P2 — Terminate.** Every loop is bounded (§5, §6, §7). Hitting a bound MUST escalate to
  a human, never loop and never silently abandon.
- **P3 — Fail closed.** When a required fact cannot be established safely (scope,
  merge-ness, verdict, ref state), the engine MUST stop for a human rather than guess in
  a way that could merge wrong or unreviewed code.
- **P4 — Canonical local target.** Merge-ness and conflict checks MUST resolve against the
  work unit's canonical local target branch. The engine MUST NOT prove merge-ness against
  `origin/<target>` and MUST NOT push the target branch.
- **P5 — Minimize human stops.** Every stop-for-human MUST be a deliberate, named choice
  with a clearing path (overview escalation table), not an accident of missing logic.

## Policy knobs

Each is a single named switch with a conservative default. Defaults lean toward *stop and
let a human decide*; the intent is that each can be flipped toward automation in one place
as confidence grows.

| Knob | Default | Governs |
|------|---------|---------|
| `require_review_before_merge` | on | Whether an implementation unit needs a valid review before merge (§4, §8). |
| `advance_create_reviews` | on | Whether the engine auto-creates needed reviews, vs parking for a manual review (§4, §8). |
| `auto_implement` (per lineage) | — | Whether a completed plan auto-creates its implement, vs holding for a human (§1). |
| `max_review_cycles` | 3 | Bound on review→improve cycles before escalation (§6). |
| `max_noop_improve_cycles` | 2 | Bound on consecutive improves that change nothing (§6). |
| rebase-failure circuit breaker | 3 | Bound on repeated failed rebases with no progress (§5). |
| duplicate-blocker bound | 3 | Bound on the same blocker repeating across reviews (§6). |
| recovery attempts | bounded | Automatic resume/retry budget before escalation (§7). |
| `merge_squash_threshold` | off | Auto-squash branches at/above N commits on merge (§8). |

The *values* above are non-normative defaults. Only the **existence and enforcement** of
each corresponding bound/gate is contract (P2); an operator changing a value is
configuration, not a spec violation.

## The rules, in order

### §1 — Plan and explore intake

- `auto_implement` defaults **on**. A completed `plan` with no implement child MUST create
  and run its `implement` unless holding was explicitly chosen at plan-creation time. This
  keeps `awaiting_human` rare: the plan stage is not a routine human checkpoint — the
  review-before-merge gate is (P-overview-4).
- A completed `plan` explicitly held for review (`auto_implement` off) MUST go to
  `awaiting_human`.
- A completed `explore` with no plan/implement follow-up MUST go to `needs_discussion`
  (decide: drop or spawn follow-up). The engine MUST NOT silently leave it pending (P5,
  no-orphans).

> **Planned (aspirational — not yet contract):** an *automatic* plan-review / plan-refine
> step will run on every plan before its implement — an agent gate analogous to code
> review, **not** a human stop. Whether it is a review→improve loop or a single refine
> pass, and its exact task type/name, are TBD and will be decided when the work is done.
> This is distinct from the interactive `/gza-plan-review` skill (a human-driven gate).

### §2 — No actionable branch

- A completed task with no branch (nothing to land) MUST `skip`.
- A non-completed task with no branch MUST `skip` (no merge action is possible yet).

### §3 — Strict project scope gate (safety, runs before any code action)

Before queuing rebase, review, improve, or merge for a code-changing branch, the engine
MUST verify the branch diff stays within the work unit's declared project scope.

- If the diff touches any path outside scope and the unit is not tagged `cross-project`,
  the engine MUST `needs_discussion` (ScopeParked): list the offending paths; instruct to
  tag `cross-project` and re-advance, or fix the branch.
- If the diff cannot be inspected reliably, the engine MUST `needs_discussion` and stop
  all automation for the unit until the ref/diff problem is fixed (P3, fail closed).

### §4 — Conflict & rebase gate

Conflict is decided against the canonical local target (P4).

- Branch cannot merge AND a rebase child is `pending`/`in_progress` → `skip` (P1).
- Branch cannot merge AND no rebase child AND the branch does not already contain the
  target tip → create a `rebase` task (`needs_rebase`).
- Local branch and `origin/<branch>` have diverged → reconcile the source ref directly
  (publish the strictly-ahead or patch-equivalent local side; otherwise fetch and
  mechanically rebase onto the remote side, then publish). A genuine host-side conflict
  here MUST be parked as `needs_discussion`, **not** delegated to a sandboxed rebase task
  — task sandboxes cannot reach remote-tracking refs.
- Branch cannot merge AND the latest rebase child `failed`, with no later proof the work
  landed → `needs_discussion` (rebase-failed). The proof set is intentionally narrow: the
  merge unit is recorded `merged`, the branch tip equals the target tip, or the branch
  already contains the target tip.
- Branch cannot merge AND a same-branch rebase already `completed` → `needs_discussion`
  (reason `rebase-did-not-unblock-merge`). The engine MUST NOT re-queue an identical
  rebase (P2).
- Repeated rebase failures reach the **circuit-breaker bound** with no intervening success,
  review, or code change → `needs_discussion` (reason `rebase-failure-circuit-breaker`).
- Branch already contains the target tip but the lineage is still unresolved →
  `needs_discussion` (surface the real blocker rather than spawn a guaranteed-no-op
  rebase).

A failed rebase is **not** cleared merely because the tip became mergeable again; the
engine MUST keep surfacing the rebase blocker until a later approved/cleared review or one
of the narrow local proofs exists.

**Rebase outcome → review impact.** A completed rebase records whether it changed the
normalized implementation patch. If unchanged, a prior approval MUST be carried across the
rebase. If changed (or equivalence cannot be proven), prior review evidence MUST be
treated as stale (§5). Recovered/resumed rebases MUST fail closed and be treated as
changed.

### §5 — Post-rebase review invalidation

- If `require_review_before_merge` is off → fall through to the no-review merge path; the
  engine MUST NOT create or wait on a refresh review.
- A rebase that changed code and is newer than the latest review, with
  `advance_create_reviews` on → `create_review`.
- Same condition with `advance_create_reviews` off → `needs_discussion` (park for a manual
  review refresh before merge).

### §6 — Review state

When a current review exists for the implementation lineage:

- Latest review `pending` → `run_review`. Latest review `in_progress` → `wait_review`.
  (P1.)
- Verdict `APPROVED` and still valid for the current mergeable diff → `merge`.
- Verdict `APPROVED_WITH_FOLLOWUPS` with ≥1 parsed follow-up, review still valid →
  `merge_with_followups` (create/reuse follow-up implement tasks, then merge). The
  follow-up tasks MUST be durably recorded *before* the merge completes (overview
  invariant 4); the merge MUST NOT proceed if its follow-ups could not be persisted.
- Verdict `APPROVED_WITH_FOLLOWUPS` with **zero** parsed follow-ups → `needs_discussion`
  (P3: self-contradictory output; do not guess).
- Verdict `CHANGES_REQUESTED`:
  - An improve is `in_progress` → `wait_improve`; `pending` → `run_improve`. (P1.)
  - No improve yet, and no bound is tripped → create an `improve` task.
- Unresolved review comments newer than the latest completed review MUST be addressed via
  the improve flow **before** any merge, even on an approved verdict.
- Verdict is unknown / unclassifiable → `needs_discussion` (P3).

**Bounds (P2), each a policy knob:**

- Review→improve cycles reach `max_review_cycles` → `max_cycles_reached`.
- Consecutive no-op improves reach `max_noop_improve_cycles` (unit not tagged
  `allow-noop-improve`) → `needs_discussion` (reason `improve-no-op`).
- The same primary blocker repeats across the duplicate-blocker bound of consecutive
  review cycles with no progress → `needs_discussion` (reason
  `duplicate-blocker-no-progress`). The streak resets on any completed rebase between the
  compared reviews, any non-`CHANGES_REQUESTED` review, or a changed blocker.
- Last reviews fail only on verify timeout (no code issues) → `needs_discussion` (reason
  `verify-blocked-no-code-issues`); do not keep spawning improves that cannot help.

**Improve chain invariant (load-bearing; source of past bugs).** An (implementation,
review) pair can spawn a *chain* of improves (the original plus retries/resumes). To find
all improves for that pair, queries MUST follow the *review* link, not the implementation
link — filtering by the implementation link finds only first-generation improves and
misses every retry/resume. Side effects that target "the implementation this improve
belongs to" MUST walk the chain to the nearest non-improve ancestor.

### §7 — Failure recovery

Failed tasks are evaluated by the same ordered engine, through one shared recovery policy
(so `advance`, `iterate`, and `watch` agree on one resume/retry/manual boundary).

- Recovery policy says `resume` → create a resume task and run it.
- Recovery policy says `retry` → create a retry task and run it.
- Recovery is disabled (attempt budget = 0) → stop; surface that automatic recovery is
  off.
- Recovery limit reached, ambiguous, or a terminal manual situation (e.g. failed resume
  descendants, dropped recovery terminal) → `needs_discussion` / manual review (P2, P5).
- A failed task whose work has *already landed* by another path (merged sibling in the
  same lineage, branch reachable from target) MUST be omitted silently — there is nothing
  to recover.

Recovery and lifecycle progress are independent: a unit that carries both a recovered
failure *and* actionable merge/review work remains eligible for the latter.

### §8 — Merge

- Reviews all cleared/addressed, with no newer rebase or closing-review requirement
  invalidating that state → `merge`.
- A non-implementation unit, or a unit that does not require review → `merge`.
- An implementation unit with no review and `require_review_before_merge` on →
  `create_review` (never merge unreviewed). With it off → `merge`.
- Merge executes against the canonical local target (P4), respects
  `merge_squash_threshold`, and MUST NOT push the target branch as a side effect.

## Parked reason codes

Every stop-for-human action MUST carry one machine-readable **reason code** from this
closed set (overview escalation table). Automation MAY branch on the code; adding a code
is a spec change. The accompanying human message is free text.

| Reason code | State | Trigger (rule) |
|-------------|-------|----------------|
| `plan-held` | AwaitingHuman | §1 plan completed, `auto_implement` off |
| `explore-dangling` | needs_discussion | §1 explore with no follow-up |
| `scope-out-of-scope` | ScopeParked | §3 diff touches paths outside scope |
| `scope-unverifiable` | needs_discussion | §3 diff could not be inspected |
| `rebase-failed-needs-manual-resolution` | HumanParked | §4 rebase failed, no landing proof |
| `rebase-did-not-unblock-merge` | HumanParked | §4 rebase completed, still conflicts |
| `rebase-failure-circuit-breaker` | HumanParked | §4 repeated rebase failures, no progress |
| `rebase-moot-incomplete-lineage` | needs_discussion | §4 branch has target tip, lineage unresolved |
| `review-refresh-blocked` | needs_discussion | §5 stale-after-rebase, auto-review off |
| `review-inconsistent-followups` | needs_discussion | §6 `APPROVED_WITH_FOLLOWUPS`, zero parsed |
| `verify-blocked-no-code-issues` | needs_discussion | §6 reviews fail only on verify timeout |
| `review-max-cycles-reached` | max_cycles_reached | §6 cycles ≥ `max_review_cycles` |
| `duplicate-blocker-no-progress` | needs_discussion | §6 same blocker repeats across cycles |
| `improve-no-op` | needs_discussion | §6 no-op improves ≥ bound |
| `review-verdict-unknown` | needs_discussion | §6 verdict unclassifiable |
| `recovery-retry-limit-reached` | HumanParked | §7 recovery attempts exhausted |
| `recovery-manual-review-required` | HumanParked | §7 terminal manual recovery situation |
| `recovery-ambiguous` | HumanParked | §7 recovery situation ambiguous |

*Implementation note (non-normative): the code today emits some of these as shorter
strings (e.g. `retry-limit-reached`, `manual-review-required`). Reconciling the emitted
strings to this canonical set is a conformance task; until then, treat the table as the
target vocabulary.*

## Ratified decisions

Settled 2026-06-01 (previously open questions). These are now contract; the rationale is
kept for future readers.

1. **`APPROVED_WITH_FOLLOWUPS` merges, then files follow-ups** (§6, invariant 4). The
   verdict *is* the gate; the reviewer chose the non-blocking door. No behavior change —
   invariant 4's wording was sharpened, with a new MUST that follow-ups are persisted
   before the merge completes. Best serves minimizing human involvement.
2. **`auto_implement` defaults on** (§1). Holding is a manual opt-in at plan creation; the
   plan stage is not a routine human checkpoint. *Forward-looking:* an automatic
   plan-review/refine step (agent gate, not human) is planned before implement — loop vs
   single-pass and naming TBD (see §1 note).
3. **Reason codes are a stable enumerated contract; messages are free text** (§ Parked
   reason codes; overview escalation table). Legitimizes `watch` branching on codes such
   as recovery stops.
4. **Bound existence is contract; bound values are tunable knobs** (§ Policy knobs, P2).
   Conformance verifies a loop cannot run unbounded, not the specific number.
5. **One batch slot per `iterate` chain is intended** (to be detailed in the future
   concurrency doc). The batch limit bounds concurrent worker *processes*; `iterate`
   drives a unit through its review/improve chain to completion within its slot.
   *Rationale:* the goal is to finish a unit as fast as possible. Step-at-a-time
   interleaving would not make any individual unit finish sooner, and at small batch sizes
   could leave a unit hours from merge. **Interleaving fairness is explicitly not the
   optimization target;** revisit only if large-batch under-utilization is measured.
</content>
