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
> Cycle cadence, slot accounting, detached-worker adoption, and watch-process restart are
> intentionally out of scope here; see [watch-supervisor.md](watch-supervisor.md). How a
> selected action's task acquires its isolated worktree at start (and when an existing
> worktree may be reclaimed) is specified in [worktree-reclaim.md](worktree-reclaim.md).

## How the engine decides

The engine evaluates an **ordered list of rules; first match wins.** For each unresolved
work unit it selects exactly one action, then executes selected actions for the pass.

This ordering is itself part of the contract — the rules are written so that earlier
rules are *safety gates* (don't act on out-of-scope or conflicted code) and later rules
are *progress* (review, improve, merge). Reordering changes behavior.

### Action vocabulary

- **Worker-spawning** (subject to batch limits): create/run a `review`, `verify_fix`,
  `improve`, `rebase`, `implement`, resume, or retry task.
- **Direct** (not batch-limited): `merge`, `merge_with_followups`,
  `materialize_plan_slices`, and other non-worker lifecycle actions such as direct
  branch-divergence reconciliation.
- **Wait**: an expected task is in progress; do nothing and re-evaluate next pass.
- **Stop-for-human**: `awaiting_human`, `needs_discussion`, `max_cycles_reached` (see the
  escalation table in the overview).
- **Skip**: nothing to do for this unit.

**Derived-task tag inheritance invariant.** When the engine or a direct command creates a
derived task (`implement`, `review`, `improve`, `rebase`, follow-up `implement`,
resume, or retry), the new task MUST inherit all parent task tags by default. If the
creation path provides explicit tags, those explicit tags replace the inherited set,
including the explicit empty set. This rule is forward-only: creating or reusing a later
derived task MUST NOT retroactively mutate tags on existing children.

The engine MUST distinguish *task created/selected* from *worker failed to start* in its
output: a creation success followed by a launch failure MUST NOT be reported as a plain
failure to create.

## Shared model

Shared vocabulary and system-wide invariants are defined in
[00-overview.md](00-overview.md). The rules below MUST apply that model and MUST NOT
contradict it.

## Policy knobs

Each is a single named switch with a conservative default. Defaults lean toward *stop and
let a human decide*; the intent is that each can be flipped toward automation in one place
as confidence grows.

| Knob | Default | Governs |
|------|---------|---------|
| `require_review_before_merge` | on | Whether an implementation unit needs a valid review before merge (§4, §8). |
| `advance_create_reviews` | on | Whether the engine auto-creates needed reviews, vs parking for a manual review (§4, §8). |
| `advance_off_topic_verify_unblock` | off | Whether the narrow legacy compatibility lane for verify-only review blockers MAY clear through the audited off-topic-failure contract instead of parking (§6, [off-topic-verify-failures.md](off-topic-verify-failures.md)). |
| `auto_implement` (per lineage) | — | Whether a completed plan auto-creates its implement, vs holding for a human (§1). |
| `max_review_cycles` | 3 | Bound on review→improve cycles before escalation (§6). |
| `max_noop_improve_cycles` | 1 | Bound on consecutive improves that change nothing (§6). |
| plan-review failure circuit breaker | 3 | Bound on repeated failed automated `plan_review` attempts for one plan source before escalation (§1). |
| rebase-failure circuit breaker | 3 | Bound on repeated failed rebases with no progress (§5). |
| duplicate-blocker bound | 3 | Bound on the same blocker repeating across reviews (§6). |
| recovery attempts | bounded | Automatic resume/retry budget before escalation (§7). |
| `merge_squash_threshold` | off | Auto-squash branches at/above N commits on merge (§8). |

The *values* above are generally non-normative defaults. Only the **existence and
enforcement** of each corresponding bound/gate is contract (see
[00-overview.md](00-overview.md#core-invariants-the-load-bearing-rules), invariant 2); an
operator changing a value is configuration, not a spec violation. The exception is any
knob whose focused contract explicitly makes its default part of the safety boundary. In
this table, `advance_off_topic_verify_unblock` is one such exception because
[off-topic-verify-failures.md](off-topic-verify-failures.md) requires the knob to exist
and default to **off**.

## The rules, in order

### §1 — Plan and explore intake

- `auto_implement` defaults **on**. A completed non-held `plan` (or completed `plan_improve`)
  with no implementation follow-up MUST enter automated `plan_review` first when
  `require_plan_review_before_implement` is on. The engine MUST create/run a `plan_review`,
  then materialize bounded implementation slices only after an approved valid manifest.
  Approved-slice materialization MUST create one `implement` task per slice as a distinct
  merge unit on its own branch. Cross-slice ordering is expressed with `depends_on`;
  materialization MUST NOT reuse `same_branch=True` to stack distinct slices onto one
  branch or merge unit.
  Unambiguous integer-like persisted `schema_version` representations such as string `"1"`
  and float `1.0` MUST be normalized through the shared manifest validator and MAY proceed
  through approved-manifest materialization. If an approved manifest instead fails
  validation because `schema_version` is missing or not an unambiguous integer
  representation, the engine MUST re-run `plan_review` to re-derive the manifest; it MUST
  NOT park that format-only failure as `plan-review-invalid-slices`.
  If the same plan source is explicitly held (`auto_implement` off), an approved valid
  latest completed `plan_review` MUST first release that hold through the shared
  `auto_implement=true` transition, without materializing slices in the same action; the
  next evaluation pass MUST then reuse the existing approved-manifest materialization path
  unchanged.
  `gza iterate <plan>` MUST reuse this same intake path for completed plan sources, and
  `gza iterate <failed-plan> --resume|--retry` MUST re-enter the same plan loop only
  after the failed plan source itself has been restarted through the shared failed-task
  recovery policy.
  Repeated failed automated `plan_review` attempts for the same plan source MUST be bounded by
  a circuit breaker; once the failed-attempt cap is reached, the engine MUST park with
  `plan-review-repeatedly-failed` instead of spawning another review.
  `max_plan_review_cycles` bounds only repeated `CHANGES_REQUESTED` plan-review churn on the
  current plan revision chain. When that bound is reached, lifecycle MUST accept the latest
  completed plan revision for lifecycle purposes and continue through the shared
  direct-implement path; it MUST NOT park waiting for a human to re-enable automation.
  If implement descendants exist for an approved manifest but the durable materialization
  record is missing, incomplete, or already complete while stale extra pending duplicate
  slice descendants remain outside the recorded set, the engine MUST first attempt
  deterministic repair when the current descendants can be proven to be an unstarted safe
  pending subset of that same validated manifest. The matched slice `trigger_source` used
  to prove that candidate MUST be carried into the repair action and revalidated before
  any mutation.
  The repair MUST either recreate one complete durable materialization record for that
  manifest or leave the prior state unchanged and fall through to fail-closed parking.
  The engine MUST park with `plan-review-materialization-repair-needed` only when the
  partial materialization state is ambiguous or unsafe; it MUST NOT silently treat a
  partial prefix as a complete materialization.
  If a completed plan already has a non-dropped implement descendant but no recorded
  approved-slice materialization, `iterate` MAY still exit 0, but it MUST report a
  neutral skip such as `already_has_implement`; it MUST NOT claim the plan is already
  materialized unless the durable materialization state proves that approved slices
  were fully materialized.
  The legacy single-implement path is allowed only when
  `require_plan_review_before_implement` is off.
  Once iterate materializes approved slices, it MUST stop at that materialization result;
  it MUST NOT continue by iterating the newly created implement children in the same run.
- A completed `plan` explicitly held for review (`auto_implement` off) MUST go to
  `awaiting_human` with parked reason `awaiting-human-review` unless its latest completed
  `plan_review` is `APPROVED` and the manifest validates, in which case lifecycle MUST
  release the hold first and only then fall through to normal approved-slice
  materialization on the next pass.
  Operators MUST NOT pre-create `implement` dependents for that held plan via
  `gza add --type implement --depends-on <plan-id>` or a `--based-on` lineage rooted at the
  held plan; those creation/edit attempts MUST fail with explicit release guidance directing
  the operator to `uv run gza implement <plan-id>` or
  `uv run gza edit <plan-id> --no-hold-for-review`.
- A completed `explore` with no plan/implement follow-up MUST go to `needs_discussion`
  (decide: drop or spawn follow-up). The engine MUST NOT silently leave it pending (see
  [00-overview.md](00-overview.md#core-invariants-the-load-bearing-rules), invariant 6).

### §2 — No actionable branch

- A completed task with no branch (nothing to land) MUST `skip`.
- A completed branch-backed task whose authoritative merge-unit state is `empty` or
  `redundant` is also terminal no-action work: it MUST `skip` merge/review creation, and
  any merge-required dependent MUST treat it as satisfied under `lineage.md` L1.
- A non-completed task with no branch MUST `skip` (no merge action is possible yet).

### §3 — Strict project scope gate (safety, runs before any code action)

Before queuing rebase, review, improve, or merge for a code-changing branch, the engine
MUST verify the branch diff stays within the work unit's declared project scope.

- If the diff touches any path outside scope and the unit is not tagged `cross-project`,
  the engine MUST `needs_discussion` (ScopeParked): list the offending paths; instruct to
  tag `cross-project` and re-advance, or fix the branch.
- If the diff cannot be inspected reliably, the engine MUST `needs_discussion` and stop
  all automation for the unit until the ref/diff problem is fixed (fail closed; see
  [00-overview.md](00-overview.md#core-invariants-the-load-bearing-rules), invariant 4).

### §4 — Conflict & rebase gate

Conflict is decided against the canonical local target (see
[00-overview.md](00-overview.md#core-invariants-the-load-bearing-rules), invariant 4).

- Ordinary queue-wide lifecycle projection MUST evaluate unresolved work units with
  `selected_for_merge = false` by default. Under that ordinary projection, a branch that
  merely conflicts with the current local target MUST NOT emit `needs_rebase` yet. It
  remains on its review/improve/merge lane until a narrower rebase-owning path below
  applies.
- Conflict-driven `needs_rebase` is merge-selection scoped. The engine MUST emit it only
  when the unit has already been selected for merge in the current cycle, or when the
  shared failed-task recovery policy requires a recovery-preflight rebase before a
  `resume`/`retry` can safely proceed.
- A selected merge candidate that reprojects to `needs_rebase` MUST be selected and
  reported under that final action's worker-slot and merge-lane gates. A cycle with no
  worker capacity, halted merges, or an unavailable merge lane MUST NOT preview or start a
  merge-selection rebase for that candidate.
- A selected-for-merge branch that cannot merge AND already has a rebase child
  `pending`/`in_progress` → `skip` (see
  [00-overview.md](00-overview.md#core-invariants-the-load-bearing-rules), invariant 1).
- Singleton derived-child creation applies to `review`, `rebase`, and review-backed
  `improve` tasks: each parent MUST have at most one active direct `based_on` child of
  that kind at a time. Lifecycle planning MUST honor that invariant by not emitting
  `needs_rebase` when an active rebase child already exists, while non-singleton fan-out
  such as follow-up `implement` children and comments-only `improve` refreshes remains
  allowed.
- A selected-for-merge branch that cannot merge, has no rebase child, and does not
  already contain the local target tip → create a `rebase` task (`needs_rebase`). The
  action's machine-readable reason slug MUST distinguish this merge-lane path from the
  recovery-preflight path; `merge-selection-conflict-rebase` is the canonical slug.
- A recovery-preflight rebase MUST remain lifecycle-owned around recovery policy. When
  recovery would otherwise choose `resume` or `retry`, but the branch does not contain the
  local target tip, lifecycle MUST emit `needs_rebase` first instead of spawning the
  recovery action. That `needs_rebase` action MUST carry a stable machine-readable reason
  slug, `recovery-preflight-rebase`, plus metadata identifying the deferred recovery
  action to resume on the next pass. Recovery policy owns deciding **whether** the failed
  task is recoverable; lifecycle owns this local-target rebase preflight around that
  policy decision.
- Local branch and `origin/<branch>` have diverged → reconcile publication host-side.
  The engine MAY inspect, fetch, and publish the unit's own `origin/<branch>` ref to
  decide whether the local side is strictly ahead, patch-equivalent, or otherwise safe to
  republish. But merge/rebase correctness MUST still be proven against the canonical
  local target branch, never any `origin/*` ref: if direct publication is not enough, the
  mechanical fallback MUST rebase onto that local target branch and then publish. A
  genuine host-side conflict in that local-target rebase MUST be parked as
  `needs_discussion`, **not** delegated to a sandboxed rebase task — task sandboxes
  cannot reach remote-tracking refs, and worker rebase targets MUST stay local.
- Branch cannot merge AND the latest rebase child `failed`, with no later proof the work
  landed, AND shared recovery classification says the failure is manual (for example a
  real `REBASE_CONFLICT`) → `needs_discussion` (rebase-failed). The proof set is
  intentionally narrow: the merge unit is recorded `merged`, the branch tip equals the
  target tip, or the branch already contains the target tip.
- Branch cannot merge AND the latest rebase child `failed`, but shared recovery
  classification says the failure is retryable/transient (for example `WORKER_DIED`,
  `NO_ACTIVITY`, or infrastructure-normalized `GIT_ERROR`) → follow the shared recovery
  decision first. Lifecycle MAY still require the local-target-only
  `recovery-preflight-rebase` before that recovery action when the branch does not yet
  contain the current target tip, but it MUST NOT park that transient failed rebase as
  `rebase-failed-needs-manual-resolution`.
- Branch cannot merge AND a same-branch rebase already `completed`, the branch still
  conflicts, AND the branch already contains the current local target tip →
  `needs_discussion` (reason `rebase-did-not-unblock-merge`). This park rule applies
  only when the completed rebase already includes the current target tip, so a fresh
  same-target rebase is already proved futile. A selected merge candidate with only a
  stale completed rebase and no current-target-tip containment remains eligible for
  `merge-selection-conflict-rebase` above. The engine MUST NOT re-queue an identical
  rebase (see [00-overview.md](00-overview.md#core-invariants-the-load-bearing-rules), invariant 2).
- Repeated rebase failures reach the **circuit-breaker bound** with no intervening success,
  review, or code change → `needs_discussion` (reason `rebase-failure-circuit-breaker`).
- Branch already contains the target tip but the lineage is still unresolved →
  `needs_discussion` (surface the real blocker rather than spawn a guaranteed-no-op
  rebase).

A manual/conflict failed rebase is **not** cleared merely because the tip became
mergeable again; the engine MUST keep surfacing the rebase blocker until a later
approved/cleared review or one of the narrow local proofs exists. Retryable/transient
failed rebases remain governed by the shared recovery decision before any manual park is
considered.

**Rebase outcome → review impact.** A completed rebase records whether it changed the
normalized implementation patch. If unchanged, a prior approval MUST be carried across the
rebase, and target movement alone MUST NOT stale that review. If changed (or equivalence
cannot be proven), prior whole-task review evidence MUST be treated as stale for merge,
but the required refresh is a **resolution-scoped review** of the conflict-resolution
delta, not a generic full-task review refresh (§5). Recovered/resumed rebases MUST fail
closed and be treated as changed.

### §5 — Stale review invalidation

- If `require_review_before_merge` is off → fall through to the no-review merge path; the
  engine MUST NOT create or wait on a refresh review.
- A rebase that changed code and is newer than the latest review, with
  `advance_create_reviews` on → `create_review`, but that created review MUST be marked
  and described as a **resolution review** limited to the rebase-introduced
  conflict-resolution delta.
- Resolution review scope MUST be narrow by default: reviewers re-check only the delta
  introduced while rebasing an already-reviewed branch, not the whole implementation,
  except where broader context is required to understand the resolution hunks.
- Target-branch movement alone MUST NOT invalidate a valid review. A later local target
  tip MAY force a merge-lane or recovery-preflight rebase (§4), but it MUST NOT by itself
  trigger stale review refresh while the implementation patch is still preserved.
- The current implementation branch/merge-unit head differing from the latest completed
  review's recorded `review_verify_head_sha` is stale-review evidence only when lifecycle
  can tie that head change to an implementation-changing event (for example a changed
  rebase, code-changing improve, or other durable lineage change). Known target movement
  alone MUST NOT be treated as branch-head stale review.
- If the live branch-head probe fails while checking that freshness, the engine MUST fail
  closed: it MUST NOT treat cached merge-unit head metadata as proof that the latest
  completed review is current, and it MUST surface a stop-for-human action instead of
  merge, stale-refresh, or `review_max_cycles` decisions that assume freshness is known.
- If both stale-review sources are true, operator-facing stale-review descriptions MUST
  prefer the rebase-specific reason over the generic branch-head-advanced wording.
- Either stale-review condition with `advance_create_reviews` off → `needs_discussion`
  (park for a manual review refresh before merge).
- Missing `review_verify_head_sha` evidence MUST fail closed for freshness: the engine
  MUST NOT infer stale branch-head advancement from absence alone.
- If persisted metadata for a required resolution review is missing, stale, or inconsistent
  with the authoritative post-rebase context, lifecycle MUST first try to re-derive the
  resolved post-rebase head/target SHAs from the live rebase branch head and the current or
  persisted merge target, then lazily repair the review task's persisted resolution-review
  metadata from that shared context before evaluating merge eligibility.
- If lifecycle still cannot resolve or validate the metadata that defines a required
  resolution review after that re-derivation-and-repair attempt, it MUST fail closed and
  park the lineage with `resolution-review-metadata-invalid`. It MUST NOT silently preserve
  the old approval, and it MUST NOT silently widen that refresh into a generic whole-task
  review.
- Stale-review refresh rules MUST run before `review_max_cycles` evaluation.
- `max_review_cycles` MUST count only completed review/improve cycles inside the current
  durable-progress epoch. The epoch resets only when persisted evidence shows a new
  reviewed head or other durable branch progress boundary; historical pre-boundary churn
  MUST NOT keep poisoning the lineage after that progress.

### §5a — Pre-review verify gate

Before lifecycle creates a first review or refreshes a stale review for an implementation
owner, it MUST evaluate the runner-owned verify gate for that owner's current verify
epoch.

- Missing or stale verify evidence for the current owner epoch MUST select `verify_gate`
  first. Lifecycle MUST rerun verify before it creates a review for that head.
- Current red verify evidence before review MUST route into the `verify_fix` lane, not the
  review/improve lane. Lifecycle MUST create, reuse, run, or wait on one same-branch
  `verify_fix` task keyed by the exact current verify epoch and implementation owner.
- If a same-epoch `verify_fix` is already `pending`, lifecycle MUST `run_verify_fix`. If
  it is already `in_progress`, lifecycle MUST `wait_verify_fix`.
- If one same-epoch `verify_fix` attempt already completed and the current verify gate is
  still red for that epoch, lifecycle MUST park with reason `verify-fix-failed` instead
  of spawning another `verify_fix`.
- If the current pre-review verify gate is unavailable and lifecycle cannot safely route
  through `verify_fix`, it MUST park with `verify-unavailable`. If that same unavailable
  state persists after one completed same-epoch `verify_fix`, it MUST park with
  `verify-unavailable-after-fix`.

### §6 — Review state

When a current review exists for the implementation lineage:

- Latest review `pending` → `run_review`. Latest review `in_progress` → `wait_review`.
  (See [00-overview.md](00-overview.md#core-invariants-the-load-bearing-rules), invariant 1.)
- Verdict `APPROVED` and still valid for the current mergeable diff → if the current
  pre-merge verify gate is green, `merge`; otherwise lifecycle MUST route through the
  shared `verify_gate` / same-epoch `verify_fix` handling before merge.
- Verdict `APPROVED_WITH_FOLLOWUPS` with ≥1 parsed follow-up, review still valid →
  if the current pre-merge verify gate is green, `merge_with_followups` (create/reuse
  follow-up implement tasks, then merge); otherwise lifecycle MUST route through the
  shared `verify_gate` / same-epoch `verify_fix` handling before creating follow-ups and
  merging. The follow-up tasks MUST be durably recorded *before* the merge completes
  (overview invariant 3); the merge MUST NOT proceed if its follow-ups could not be
  persisted.
- Verdict `APPROVED_WITH_FOLLOWUPS` with **zero** parsed follow-ups → `needs_discussion`
  (self-contradictory output; do not guess. See
  [00-overview.md](00-overview.md#core-invariants-the-load-bearing-rules), invariant 4.)
- Verdict `CHANGES_REQUESTED`:
  - An improve is `in_progress` → `wait_improve`; `pending` → `run_improve`. (See
    [00-overview.md](00-overview.md#core-invariants-the-load-bearing-rules), invariant 1.)
  - No improve yet, and no bound is tripped → create an `improve` task.
  - Improve work is atomic over the full current blocker/comment set for that pass. This
    specification file is the behavior owner for that observable contract. The improve
    worker MUST re-read all current feedback before editing, inventory every current
    review blocker and unresolved feedback comment, treat grouped blocker classes as
    grouped work, plan one shared fix set, re-check the full initial inventory after
    meaningful edit batches and again after the last edit, and run the configured final
    full verify gate after any targeted inner-loop checks before reporting closure.
  - Improve reports MUST include a machine-readable `## Blocker Closure Ledger (Machine
    Readable)` section covering every in-scope blocker/comment, including disputed no-op
    entries. They MUST also include an explicit closure matrix for every current
    blocker/comment plus a short anti-regression statement covering the full initial
    inventory, so operators can audit closure evidence separately from free-form
    narrative.
  - A completed no-op improve MAY dispute a non-verify CODE blocker only by supplying
    structured current-state evidence that the blocker is unreproducible, stale, already
    satisfied, out of scope, or otherwise invalid. Prior review text or task history
    alone is not enough; the dispute MUST cite the current still-open-or-cleared state.
  - Improve-lineage context in later reviews MAY be used only as a pointer to inspect
    current code/diff for repeated blocker shapes the latest improve was expected to
    close. It is never standalone blocker evidence; renewed blockers still require
    current-source proof on the reviewed diff or code.
- Unresolved `feedback` comments newer than the latest completed review MUST be addressed
  via the improve flow **before** any merge, even on an approved verdict.
- Unresolved comments of other kinds (for example `review_scope`) MUST remain visible to
  operators but MUST NOT create, reuse, resume, wait on, or freshness-block an improve task.
- When review scope is needed for a completed or otherwise non-pending implementation, the
  authoritative resolution order is: persisted `review_scope` task field first, latest
  typed `review_scope` comment next, then legacy sliced-prompt parsing, then a
  conservative plan-backed fallback derived from the linked plan identity plus the
  implementation prompt metadata.
  A created review MUST persist that resolved scope on its own row so later scope comments
  do not silently rewrite an existing review's gradeable contract.
- When a resolved review scope exists, that scope is the only gradeable ask for review.
  Linked plan text MUST be rendered only as labeled background context and MUST NOT
  widen the contract beyond the resolved review scope.
- Verdict is unknown / unclassifiable → `needs_discussion` (see
  [00-overview.md](00-overview.md#core-invariants-the-load-bearing-rules), invariant 4).

**Bounds (see [00-overview.md](00-overview.md#core-invariants-the-load-bearing-rules), invariant 2), each a policy knob:**

- Review→improve cycles reach `max_review_cycles` within the current durable-progress
  epoch → `max_cycles_reached`.
- **A. Ordinary no-op improves do not bypass the two-gate model.** A no-op improve does
  not, by itself, authorize merge. If code changed, both the review gate and verify gate
  become stale and MUST be re-run in the normal order: verify first, then review.
- **A2. Legacy verify-only compatibility lane.** Historical review rows that still carry
  verify-only `CHANGES_REQUESTED` blockers MAY remain supported through a narrow
  compatibility path, including the opt-in audited off-topic contract in
  [off-topic-verify-failures.md](off-topic-verify-failures.md). That lane exists only to
  adjudicate persisted historical review state; it MUST NOT be treated as the ordinary
  merge rule for new two-gate work, and it MUST NOT replace the pre-review or pre-merge
  verify gates in §5a and §8.
- **B. Disputed non-verify CODE blocker adjudication.** When the latest
  `CHANGES_REQUESTED` review carries a non-verify CODE blocker and the latest completed
  improve for that `(implementation, review)` pair is a no-op with structured dispute
  evidence, lifecycle MUST treat the blocker as adjudication-eligible once
  `max_noop_improve_cycles` is reached. This adjudication route MUST run before the
  generic `improve-no-op`, `duplicate-blocker-no-progress`, and `review-max-cycles`
  parks. The adjudication output is strict:
  - `INVALID` clears that blocker for lifecycle purposes only; historical review output
    is preserved.
  - `VALID` keeps the blocker open and returns the lineage to the normal improve flow.
  - `NEEDS_HUMAN`, failed adjudication, or unparseable adjudication output MUST park with
    reason `review-blocker-adjudication-needed` and include the dispute/adjudication
    evidence.
  The same adjudication lane also applies when the same non-verify CODE blocker repeats
  across the duplicate-blocker bound of consecutive review cycles with no completed
  rebase boundary between them. In that repeated-review case lifecycle MUST synthesize
  dispute metadata from the repeated blocker evidence and the current reviewed branch
  state, then run the same strict `VALID | INVALID | NEEDS_HUMAN` adjudication before
  the generic `duplicate-blocker-no-progress` or `review-max-cycles` parks.
  This lane applies only to non-verify CODE blockers. Verify-only review rows
  remain governed by the narrow compatibility lane above; ordinary two-gate merge
  eligibility does not flow through that compatibility path.
- Otherwise, consecutive no-op improves reach `max_noop_improve_cycles` (unit not tagged
  `allow-noop-improve`) → `needs_discussion` (reason `improve-no-op`). This generic
  no-op park applies only after ruling out rule B adjudication-eligible disputed
  non-verify CODE blockers. A no-op improve does not create new merge authority by
  itself: if lifecycle still lacks a current merge-permitting review plus current passing
  verify evidence for the same head, the no-op improve limit MUST park rather than
  auto-clear. If lifecycle cannot resolve the current branch head while checking
  freshness, it MUST still fail closed but surface that probe failure in the parked
  result instead of silently degrading to a generic no-op loop. When parallel sibling
  reviews exist on one implementation, lifecycle MUST attribute this park to the review
  whose feedback actually remains unresolved and MUST still park instead of merging while
  an older sibling CODE review remains unresolved.
- The same primary blocker repeats across the duplicate-blocker bound of consecutive
  review cycles with no progress after rule B has already been exhausted or the
  adjudication result was `NEEDS_HUMAN` → `needs_discussion` (reason
  `duplicate-blocker-no-progress`). The streak resets on any completed rebase between the
  compared reviews, any non-`CHANGES_REQUESTED` review, or a changed blocker.
- Verify-only reviews that fail only on verify timeout (no code issues) MAY still park
  with `needs_discussion` (reason `verify-blocked-no-code-issues`) on the legacy
  compatibility lane. Ordinary two-gate work MUST route current red verify
  evidence into `verify_fix` before review instead of converting it into a review-state
  timeout policy.

**Improve chain invariant (load-bearing; source of past bugs).** An (implementation,
review) pair can spawn a *chain* of improves (the original plus retries/resumes). To find
all improves for that pair, queries MUST follow the *review* link, not the implementation
link — filtering by the implementation link finds only first-generation improves and
misses every retry/resume. Side effects that target "the implementation this improve
belongs to" MUST walk the chain to the nearest non-improve ancestor.

### §7 — Failure recovery

The shared recovery policy referenced in this section is specified in
[recovery.md](recovery.md).

Failed tasks are evaluated by the same ordered engine, through one shared recovery policy
(so `advance`, `iterate`, and `watch` agree on one resume/retry/manual boundary).

- Recovery policy says `resume` → create a resume task and run it, unless §4 first emits a
  `recovery-preflight-rebase`.
- Recovery policy says `retry` → create a retry task and run it, unless §4 first emits a
  `recovery-preflight-rebase`.
- Recovery is disabled (attempt budget = 0) → stop; surface that automatic recovery is
  off.
- Recovery limit reached, ambiguous, or a terminal manual situation (e.g. failed resume
  descendants, dropped recovery terminal) → `needs_discussion` / manual review (see
  [00-overview.md](00-overview.md#core-invariants-the-load-bearing-rules), invariants 2 and 6).
- Before treating merge-unit state `empty` or `redundant` as a terminal "nothing left to
  do" outcome for a failed task, the engine MUST apply the shared recovery predicate from
  [recovery.md](recovery.md). A failed task with terminal no-work merge state but
  recoverable session-backed execution evidence MUST continue through recovery instead of
  being suppressed as moot.
- A failed task whose work has *already landed* by an independent valid path — a completed
  recovery descendant, or a merged sibling/lineage member that actually contributed the commits —
  MUST be omitted silently; there is nothing to recover. **Branch reachability from the target is
  not, by itself, proof of landing.** A branch is a landed representative only if it contributed
  **at least one commit that is now contained in the target**. A branch whose tip is merely an
  ancestor of the target with **no unique commits** is split by task provenance: no task
  commits means `empty`; task commits already represented on target means `redundant`.
  Neither state is landed by itself, and both MUST be routed through the shared recovery
  predicate ([recovery.md](recovery.md) §1), never silently omitted by this clause.

For any failed task with a recoverable failure — timeout-style resumable failures *and* retryable
failures (e.g. `WORKER_DIED`) alike — the engine MUST prefer the shared recovery decision
(`resume`, `retry`, bounded retry, or manual stop) **before any reachability- or merge-style
suppression**. The "already landed" exception only applies when the work landed by an independent
valid path, such as a completed recovery descendant or a different merged lineage member that
contributed commits. The same failed task being reachable-from-target, marked merged, `empty`,
or `redundant` on its own MUST never satisfy that exception.

Recovery and lifecycle progress are independent: a unit that carries both a recovered
failure *and* actionable merge/review work remains eligible for the latter.

### §8 — Merge

- A completed `implement` task with no task commits, or with merge-unit state `empty` or
  `redundant`, is terminal moot: it MUST NOT create, run, wait on, or require a review,
  and it MUST remain absent from actionable `unmerged` and lifecycle-`incomplete`
  surfaces.
- Canonical host-side reconciliation MUST still re-validate stored terminal `empty` /
  `redundant` merge units that retain a recorded `head_sha` against that **recorded
  head SHA** and the unit's own target branch. If recorded-head patch proof positively
  shows missing work, the unit MUST be restored to `unmerged` so normal lifecycle
  progress resumes. If recorded-head proof is unavailable (for example the commit is no
  longer resolvable), reconciliation MUST leave the terminal state unchanged and log the
  degraded proof. This healing pass MUST be idempotent and fail closed.
- Reviews all cleared/addressed, with no newer rebase or closing-review requirement
  invalidating that state → if the current pre-merge verify gate is green, `merge`;
  otherwise lifecycle MUST route through the shared `verify_gate` / same-epoch
  `verify_fix` handling before merge.
- A non-implementation unit → `merge`.
- For implementation-owned units whose review gate is enabled, merge eligibility remains
  the ordinary two-gate rule even after an approved review: automation MUST have both a
  merge-permitting current review verdict and current passing lifecycle-owned verify
  evidence for the current implementation head/verify epoch. If the verify gate is
  missing or stale, automation MUST rerun it; if it is red or unavailable, automation
  MUST block merge and follow the shared verify-gate handling instead of merging on
  review alone.
- An implementation unit with no review and `require_review_before_merge` on →
  `create_review` when `advance_create_reviews` is on, otherwise `needs_discussion` with
  reason `review-needs-manual-creation` (never merge unreviewed). With
  `require_review_before_merge` off → if the current pre-merge verify gate is green,
  `merge`; otherwise lifecycle MUST route through the shared `verify_gate` / same-epoch
  `verify_fix` handling before merge. This review-disabled branch is the only exception
  to the ordinary implementation two-gate merge rule from
  [00-overview.md](00-overview.md#core-invariants-the-load-bearing-rules).
- A failed implementation task is never mergeable. Timeout-style failed implementations
  with a resumable `session_id` MUST stay in recovery until that recovery resolves to a
  valid completed representative, exhausts its bounded policy, or is parked for manual
  intervention.
- Merge executes against the canonical local target (see
  [00-overview.md](00-overview.md#core-invariants-the-load-bearing-rules), invariant 4), respects
  `merge_squash_threshold`, and MUST NOT push the target branch as a side effect. Direct
  mark-merged paths and post-promotion bookkeeping are part of the same precondition: they
  MUST reject merge representatives whose execution status is not `completed` or
  `unmerged`.
- Manual `gza merge` retains a narrower human-override path than automation. Automated
  lifecycle actions (`advance`/`watch`) MUST still merge only review-cleared work under
  the rules above; they MUST NOT auto-merge `CHANGES_REQUESTED` reviews by deferring
  blockers. Manual `gza merge` MUST refuse a latest completed `CHANGES_REQUESTED` review
  that still has any open non-verify `BLOCKER` finding unless the operator passes
  `--defer-blockers`.
- For manual `gza merge`, when the latest completed `CHANGES_REQUESTED` review is a
  verify-only compatibility case blocked only by verify failures/timeouts, the
  command MAY auto-defer those blockers without a flag. Every blocker bypassed by either
  that legacy verify-only path or `--defer-blockers` MUST
  create or reuse a persisted deferred-blocker `implement` task before merge success or
  `--mark-only` merged-state mutation is recorded. If that persistence fails, the merge
  MUST fail closed.
- Manual `gza merge --no-followups` remains scoped to ordinary `FOLLOWUP` findings only.
  It MUST NOT suppress mandatory deferred-blocker tasks created for bypassed `BLOCKER`
  findings.
- After a merge lands on the canonical local target, and whenever automation can prove the
  local target's HEAD changed since the last successful or failed target-level verify
  fingerprint, watch/advance MUST rerun the configured verify gate against that local
  target tree before allowing more same-cycle merge work onto it. That checkpoint also
  becomes stale when the configured gate identity changes on the same tree: at minimum the
  normalized `verify_command` and the gate-enabled/no-gate state are part of freshness, and
  the current implementation also keys freshness on the resolved automation timeout
  settings. Independently of tree change, configured-gate checkpoints that are not
  `passed` MUST also expire after a bounded configured TTL and be rerun on that cadence so
  a stale red/unavailable result cannot park merges indefinitely on an unchanged tree. If
  the live local-target checkout cannot produce an exact tree fingerprint for that
  freshness proof, automation MUST fail closed instead of reusing `HEAD` equality alone:
  it MUST rerun the verify gate, and if exact-tree freshness still cannot be established
  it MUST persist an operator-visible unavailable proof state that halts merges for the
  cycle. More generally, if that target-level verify is not `passed`,
  automation MUST halt further merges for the cycle and surface one durable
  needs-attention signal with reason `main-integration-verify-red` that names the failing
  local target SHA and, when structured phase output exists, the failing phase. Projects
  must apply the convergence contract in
  [main-verify-self-heal.md](main-verify-self-heal.md) when that red state is reused,
  refreshed, repaired, or escalated. Projects
  with no configured
  `verify_command` are an explicit no-gate exception: they MAY persist an `unavailable`
  checkpoint with `exit_status="not configured"` for visibility, but that checkpoint MUST
  NOT halt merges or emit the red-main attention signal.
- When that red-main path would hand off to automatic remediation, lifecycle MUST keep the
  shared representativeness gate intact: remediation metadata includes the observed verify
  environment identity, and watch MUST park with durable attention instead of queueing a
  non-representative or unknown/unavailable worker environment for ordinary remediation.
- When automation uses an isolated host merge checkout to stage a merge before updating
  the canonical local target, that isolated checkout becomes the authoritative
  pre-promotion verify subject. With a configured verify gate, the shared merge executor
  MUST run candidate verify on the exact staged candidate tree before updating the
  canonical target ref, MUST fail closed when the required isolated checkout is
  unavailable, and MUST block promotion when the candidate result is red or exact-tree
  freshness cannot be proven. The canonical checkpoint MAY be copied forward from that
  candidate evidence only when the exact verified candidate tree is the one promoted onto
  the canonical target. Before any later merge attempt in the same command cycle, the
  caller MUST refresh or rebuild that isolated checkout back to the canonical target, or
  stop the merge lane for the cycle; later candidates MUST NOT run on top of a blocked
  candidate tree. Watch's caller surface MUST preserve that blocked-candidate outcome
  distinctly, surfacing it as candidate-verify attention for the merge subject rather
  than collapsing it into a generic merge failure.

Note: the "implementation unit with no review" rule above applies only when the
implementation still has reviewable commits or diff against the target. Terminal
empty/redundant implementations are covered by the moot rule and do not require review
creation.

### §9 — PR publication for completed code tasks

When a code task completes with PR creation requested (`create_pr`), the work is published
by pushing the unit's source branch to `origin` and opening a PR. Publication is a
*completion-time* step, distinct from the §8 merge into the canonical local target; it MUST
NOT be conflated with merge-ness. Overview invariant 4 forbids pushing the *target* branch — it does **not**
forbid publishing the unit's own source branch to `origin`.

Publication has two failure modes with different outcomes, decided by **whether the branch
push succeeded**:

- **Push succeeded, PR creation failed** (host unavailable, auth/token expired, API/rate
  limit) → the unit is **completed**. The branch is already on `origin` and visible; only
  the PR wrapper is missing. The engine MUST record the missing PR as a *non-fatal*,
  surfaced note (watch log) and MUST NOT mark the unit `failed`. A unit completed this way
  stays eligible for the normal merge path (§8); the absent PR never blocks merge.
- **Push failed** (the branch could not be published — e.g. local diverged from
  `origin/<branch>`) → the unit is **failed** with the recoverable reason
  `BRANCH_UNPUSHABLE` ([recovery.md](recovery.md) §2). This is *not* a manual stop. Its
  prescribed next action is to make the branch pushable via the §4 reconcile/rebase
  machinery. The reason MUST be distinct and countable so publication-blocked frequency is
  observable (an invisible "completed" branch is a real hazard, not a silent success).

**Recovery and continuation.** A `BRANCH_UNPUSHABLE` unit
routes into §4: benign/mechanical divergence (including superseded gza WIP savepoints) is
reconciled automatically (publish the strictly-ahead or patch-equivalent local side;
otherwise fetch, mechanically rebase onto the canonical local target branch, then
publish); only a genuine host-side conflict in that local-target rebase parks for a human
(the existing §4 reconcile / merge-source manual codes). Once reconcile or rebase makes
the branch pushable, if `create_pr` is set and no PR yet exists, the engine
MUST publish and create the PR, then proceed to the §8 merge gate — closing
push → PR → merge end-to-end with no human step on the mechanical path.

`PR_REQUIRED` is retired as a single terminal/manual outcome: a publication problem is now
either non-terminal (push succeeded, §9 first bullet) or the recoverable `BRANCH_UNPUSHABLE`
(push failed).

## Parked reason codes

Every stop-for-human action MUST carry one machine-readable **reason code** from this
closed set (overview escalation table). Automation MAY branch on the code; adding a code
is a spec change. The accompanying human message is free text.

| Reason code | State | Trigger (rule §) |
|-------------|-------|------------------|
| `awaiting-human-review` | awaiting_human | §1 completed held plan, no implement follow-up |
| `plan-review-needs-manual-creation` | needs_discussion | §1 completed non-held plan needs plan review, but auto-creation is off |
| `plan-review-invalid-slices` | needs_discussion | §1 approved plan review has no valid effective slice manifest |
| `plan-review-needs-discussion` | needs_discussion | §1 completed plan review returned `NEEDS_DISCUSSION` |
| `plan-review-unknown-verdict` | needs_discussion | §1 completed plan review verdict missing or unparseable |
| `plan-review-repeatedly-failed` | needs_discussion | §1 failed automated plan-review attempts reached the configured cap |
| `plan-review-materialization-repair-needed` | needs_discussion | §1 approved manifest has an ambiguous or unsafe partial materialization state that cannot be auto-repaired safely |
| `explore-needs-follow-up-decision` | needs_discussion | §1 completed explore, no plan/implement follow-up |
| `project-scope-violation` | ScopeParked | §3 diff touches paths outside scope, not tagged `cross-project` |
| `project-scope-unverified` | needs_discussion | §3 diff could not be inspected (fail closed) |
| `merge-source-needs-manual-resolution` † | HumanParked | §4 host-side merge-source divergence needs manual resolution |
| `reconcile-needs-manual-resolution` † | HumanParked | §4 execution-time reconcile outcome needs manual resolution |
| `rebase-failed-needs-manual-resolution` | HumanParked | §4 manual/conflict rebase failed, no landing proof after shared recovery classification |
| `rebase-did-not-unblock-merge` | HumanParked | §4 rebase completed, still conflicts |
| `rebase-failure-circuit-breaker` | HumanParked | §4 repeated rebase failures, no progress |
| `branch-already-rebased-lineage-incomplete` | needs_discussion | §4 branch contains target tip, lineage unresolved |
| `stale-review-needs-manual-refresh` | needs_discussion | §5 code-changing stale review requires a manual refresh or manual resolution review, `advance_create_reviews` off |
| `review-freshness-unverified` | needs_discussion | §5 live branch-head probe failed while checking whether a code-changing event made the latest completed review stale |
| `resolution-review-metadata-invalid` | needs_discussion | §5 required resolution-review metadata is still missing, malformed, or inconsistent after live SHA re-derivation |
| `closing-review-needs-manual-refresh` † | needs_discussion | §6/§8 closing-review requirement, manual refresh |
| `verify-failed-needs-fix` | needs_discussion | §5a verify gate is red before review can proceed, but lifecycle cannot safely create/continue the current `verify_fix` lane |
| `verify-fix-failed` | needs_discussion | §5a current verify gate is still red after one completed same-epoch `verify_fix` |
| `verify-unavailable` | needs_discussion | §5a verify gate is unavailable and lifecycle cannot safely route through `verify_fix` |
| `verify-unavailable-after-fix` | needs_discussion | §5a verify gate remains unavailable after one completed same-epoch `verify_fix` |
| `verify-blocked-no-code-issues` | needs_discussion | §6 legacy compatibility park for timeout-only verify-coupled reviews |
| `improve-no-op` | needs_discussion | §6 consecutive no-op improves ≥ bound after adjudication/compatibility handling is exhausted |
| `review-blocker-adjudication-needed` | needs_discussion | §6 adjudication for a disputed non-verify CODE blocker returned `NEEDS_HUMAN`, failed, or could not be parsed safely |
| `duplicate-blocker-no-progress` | needs_discussion | §6 same primary blocker repeats across cycles |
| `review-max-cycles-reached` | max_cycles_reached | §6 current-head review→improve cycles ≥ `max_review_cycles` with no stale-review refresh available |
| `review-verdict-needs-manual-attention` | needs_discussion | §6 verdict unclassifiable, or `APPROVED_WITH_FOLLOWUPS` with zero parsed follow-ups |
| `review-needs-manual-creation` | needs_discussion | §8 implementation-owned lineage requires review, no review exists, `advance_create_reviews` off |
| `main-integration-verify-red` | needs_discussion | §8 local target verify failed after target HEAD changed; halt further merges until it is green again |
| `automatic-recovery-disabled` | HumanParked | §7 recovery attempt budget = 0 |
| `retry-limit-reached` | HumanParked | §7 recovery attempts exhausted or terminal manual-review recovery stop |
| `retryable-provider-error` | HumanParked | §7 fresh retry consumed for a retryable provider failure; completed implementations with retryable terminal failures recommend `uv run gza unstick <owner-id> --reason retry-limit` (optionally `--run`) |
| `recovery-ambiguous` | HumanParked | §7 recovery situation ambiguous |
| `manual-failure-reason` † | HumanParked | §7 failure flagged for manual handling |
| `newer-recovery-descendant-needs-attention` † | HumanParked | §7 newer unresolved recovery descendant |
| `no-descendant-on-the-impl-branch` † | needs_discussion | projected lineage attention: no descendant remains on the implementation branch |

**†** Names a behavior whose *producing rule* is not yet written in §1–§8. Adding the code
reconciles the vocabulary; specifying the rule that emits it is a tracked follow-up gap.

Primary lifecycle code MUST attach `needs_attention_reason` explicitly via
`with_needs_attention(...)` or the equivalent execution-time needs-attention result.
`needs-discussion` and `max-improve-attempts-reached` remain accepted legacy compatibility
fallback slugs, but new rules MUST NOT rely on bare action-type fallback to produce them.
`manual-review-required` is not a recovery parked reason code; recovery paths use
`retry-limit-reached` and `retryable-provider-error`. CLI attention surfaces MUST route
completed implementations with retryable terminal failures to the shared rearm handoff
(`uv run gza unstick <owner-id> --reason retry-limit`, optionally `--run`), while the
shared `gza fix` handoff remains reserved for review/content churn and completed-
implementation failed recovery whose terminal failure category is not retryable. If the
implementation never completed and is merely parked/failed, operators must be directed to
retry or re-implement instead of creating a fix task.

Manual operator semantics for `uv run gza unstick` are intentionally narrow. The
command may target parked owners with reason class `backstop`
(`watch-no-progress-backstop`), `retry-limit` (`retry-limit-reached`), or `reconcile`
(`reconcile-needs-manual-resolution`).

- For `backstop` and `reconcile`, it MUST clear only the watch-owned exclusion state that
  kept the owner out of normal watch/advance pickup.
- For `retry-limit`, it MUST record one durable manual-rearm epoch for the parked subject
  and reason so the next shared recovery evaluation measures retry budget from that epoch
  instead of lifetime history.
- Plain `uv run gza unstick` MUST remain clear-only and MUST NOT start workers itself.
- `uv run gza unstick --run` MAY immediately dispatch only the owners it just cleared, but
  it MUST do so by reusing the shared scoped watch dispatch path, shared slot ceiling,
  `max_concurrent`, and launch-permit rules rather than owning a second executor.
- In all cases it MUST NOT downgrade landed/moot guards.

If the selected owner is already merged, terminal `empty`/`redundant`, branch-missing
and therefore unprovable, or otherwise not currently parked, the command MUST skip it
with an operator-visible reason instead of forcing it back into the actionable set. A
second `retry-limit` clear after the owner is no longer parked MUST therefore report
`not currently parked`, not create another effective reset.

*Status: reconciled to the strings the engine actually emits as of the 2026-06-02
behavior-check (`reviews/20260602003648-behavior-check.md`), spec-follows-code. Remaining
open work is limited to the **†** rows whose producing rules still need to be specified in
§1–§8.*

## Ratified decisions

Settled 2026-06-01 (previously open questions). These are now contract; the rationale is
kept for future readers.

1. **`APPROVED_WITH_FOLLOWUPS` merges, then files follow-ups** (§6, invariant 3). The
   verdict *is* the gate; the reviewer chose the non-blocking door. No behavior change —
   invariant 3's wording was sharpened, with a new MUST that follow-ups are persisted
   before the merge completes. Best serves minimizing human involvement.
2. **`auto_implement` defaults on** (§1). Holding is a manual opt-in at plan creation; the
   plan stage is not a routine human checkpoint. *Forward-looking:* an automatic
   plan-review/refine step (agent gate, not human) is planned before implement — loop vs
   single-pass and naming TBD (see §1 note).
3. **Reason codes are a stable enumerated contract; messages are free text** (§ Parked
   reason codes; overview escalation table). Legitimizes `watch` branching on codes such
   as recovery stops.
4. **Bound existence is contract; bound values are tunable knobs** (§ Policy knobs; see
   [00-overview.md](00-overview.md#core-invariants-the-load-bearing-rules), invariant 2).
   Conformance verifies a loop cannot run unbounded, not the specific number.
5. **One batch slot per `iterate` chain is intended** (to be detailed in the future
   concurrency doc). The batch limit bounds concurrent worker *processes*; `iterate`
   drives a unit through its review/improve chain to completion within its slot.
   *Rationale:* the goal is to finish a unit as fast as possible. Step-at-a-time
   interleaving would not make any individual unit finish sooner, and at small batch sizes
   could leave a unit hours from merge. **Interleaving fairness is explicitly not the
   optimization target;** revisit only if large-batch under-utilization is measured.
</content>
