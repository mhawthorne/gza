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

`gza advance <task-id> --repeat` MUST remain a thin task-scoped loop over the same
engine and executor: resolve the named task's current lifecycle action, execute that
action through the shared advance execution path, re-resolve, and repeat until the task
is merged, parked for human attention, skipped with no further progress, stopped by a
bounded iteration cap, or stopped by a no-progress backstop. It MUST be rejected without
an explicit task ID. It MUST perform merge actions instead of stopping at merge-ready,
honor `--dry-run`, `--auto`/`-y`, and existing `--force` semantics, and acquire capacity
through the same launch-permit system used by advance/watch before any live cycle
executes so task-scoped draining does not exceed the configured concurrency cap. Each
numbered repeat cycle MUST execute at most one selected lifecycle action and emit at
most one outcome line; when a failed merge changes the next action to `needs_rebase`,
that rebase dispatch MUST happen only after the next cycle re-resolves lifecycle state.
That single live repeat registration MUST remain visible for the drain lifetime and cover
direct actions such as `merge` and `verify_gate`; same-process foreground child workers
MUST reuse that slot rather than requiring or counting a second one. `--dry-run` repeat
MUST NOT run verification, merge, create tasks or artifacts, update worker registry
state, or mutate Git state; merge previews may inspect only the persisted
main-integration checkpoint and MUST stop at the execution boundary when proving
freshness would require running the gate.

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
| `max_review_cycles` | 3 | Bound on review→improve cycles before exhaustion handling (§6). |
| `on_max_cycles` | `park` | Exhaustion policy for implementation review→improve cycles: `park` emits manual attention; opt-in `merge_and_defer` may emit an audited annotated merge only under §6/§8 prerequisites. |
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

- If the diff touches any path outside scope and the unit is not explicitly or implicitly
  cross-project,
  the engine MUST `needs_discussion` (ScopeParked): list the offending paths; instruct to
  tag `cross-project` and re-advance, or fix the branch.
- A unit is implicitly cross-project when its project config sets
  `default_cross_project: true`. This MUST behave like the reserved `cross-project` tag
  for both strict-scope exemption and per-affected-project verify fan-out. It MUST NOT
  behave like `enforce_project_scope: false`, which disables scope parking without
  enabling cross-project verification fan-out.
- Cross-project scope exemption MUST still fail closed if any changed path falls outside
  all discovered project roots and branch-declared project roots. When the current
  project is nested under another project config, ancestor configs MUST be available for
  path attribution so parent-owned paths can resolve cleanly. Most-specific attribution
  MUST prevent an ancestor project from being selected only because a descendant-owned
  path changed.
- If the diff cannot be inspected reliably, the engine MUST fail closed with
  `project-scope-unverified`. For a unit that is not explicitly or implicitly
  cross-project, the guidance MAY tell the operator to tag `cross-project` when wider
  scope is intended. For a unit that is already explicitly or implicitly cross-project,
  the guidance MUST direct the operator to repair the diff/ref inspection failure
  without asking for a redundant tag.
- If the diff cannot be inspected reliably, the engine MUST `needs_discussion` and stop
  all automation for the unit until the ref/diff problem is fixed (fail closed; see
  [00-overview.md](00-overview.md#core-invariants-the-load-bearing-rules), invariant 4).

### §4 — Conflict & rebase gate

Conflict is decided against the canonical local target (see
[00-overview.md](00-overview.md#core-invariants-the-load-bearing-rules), invariant 4).

- Ordinary queue-wide lifecycle projection MUST run a single rebase-before-dispatch gate
  after it has selected the next action for a merge unit and before that action is
  dispatched. The gate applies to actions against the unit's branch, including review,
  improve, verify-fix, verify-gate, and merge actions; idle rows and needs-attention rows
  with no action about to be dispatched MUST NOT be rebased.
- The pre-dispatch gate emits `needs_rebase` when the local merge source does not already
  contain the current local target tip and either the source cannot merge or the source is
  positively known to be behind the target. Recovery-preflight rebase remains lifecycle
  owned: when failed-task recovery would otherwise `resume` or `retry`, the same gate
  emits `needs_rebase` unless target-tip containment is already proven.
- If the source is cleanly mergeable but target freshness cannot be verified because the
  behind-count probe fails, returns no result, or returns malformed data, lifecycle MUST
  fail closed with `needs_discussion`
  (`pre-dispatch-target-freshness-unverified`) instead of dispatching the selected branch
  action.
- A merge-unit action that reprojects to `needs_rebase` MUST be reported under that final
  action's worker-slot gates. A cycle with no worker capacity MUST NOT preview or start a
  rebase for that candidate.
- A branch that needs the pre-dispatch rebase and already has a rebase child
  `pending`/`in_progress` → `skip` (see
  [00-overview.md](00-overview.md#core-invariants-the-load-bearing-rules), invariant 1).
- A branch that needs the pre-dispatch rebase and has unresolved failed-rebase evidence
  MUST apply the same failed-rebase and circuit-breaker policy independent of `can_merge`.
  Three qualifying failed same-branch rebase attempts with no intervening reset proof emit
  `needs_discussion` (`rebase-failure-circuit-breaker`) before a new rebase can be
  planned; one unresolved manual/conflict failure emits `needs_discussion`
  (`rebase-failed-needs-manual-resolution`).
- Singleton identity and duplicate-capacity behavior are owned by
  [00-overview.md](00-overview.md#core-invariants-the-load-bearing-rules), invariant 1.
  For this gate, an active rebase for the source branch MUST make the row `skip` without
  consuming worker capacity.
- A branch that needs the pre-dispatch rebase, has no active rebase child, and still has a
  resolvable local merge source → create a `rebase` task (`needs_rebase`). The action's
  machine-readable reason slug MUST distinguish this general dispatch path from the
  recovery-preflight path; `pre-dispatch-rebase` is the canonical slug. Every
  `needs_rebase` action emitted by this gate MUST identify the canonical rebase parent
  task id and source branch that the gate evaluated, and executors MUST create or dedupe
  the rebase under that canonical parent rather than the evaluated descendant row.
- If a branch lacks any resolvable local merge source while persisted merge state is still
  non-terminal, lifecycle MUST fail closed with `needs_discussion`
  (`merge-source-needs-manual-resolution`) instead of planning or continuing rebase
  automation from a remote-only/deleted source ref.
- A recovery-preflight rebase MUST remain lifecycle-owned around recovery policy. When
  recovery would otherwise choose `resume` or `retry`, but the branch does not contain the
  local target tip, lifecycle MUST emit `needs_rebase` first instead of spawning the
  recovery action. That `needs_rebase` action MUST carry a stable machine-readable reason
  slug, `recovery-preflight-rebase`, plus metadata identifying the deferred recovery
  action to resume on the next pass. Recovery policy owns deciding **whether** the failed
  task is recoverable; lifecycle owns this local-target rebase preflight around that
  policy decision.
- Lifecycle merge-source proof MUST use confirmed local refs only. If the implementation
  branch exists locally, that branch is the merge source for advance/watch mergeability,
  diff/spec gates, already-merged checks, and merge execution. Remote-only or divergent
  `origin/<branch>` state MUST NOT count as merge-source proof.
- Local branch and `origin/<branch>` have diverged during explicit host-side publication
  reconcile → reconcile publication host-side. The engine MAY inspect, fetch, and publish
  the unit's own `origin/<branch>` ref to decide whether the local side is strictly
  ahead, patch-equivalent, or otherwise safe to republish. But merge/rebase correctness
  MUST still be proven against the canonical local target branch, never any `origin/*`
  ref: if direct publication is not enough, the mechanical fallback MUST rebase onto that
  local target branch and then publish. A genuine host-side conflict in that local-target
  rebase MUST be parked as `needs_discussion`, **not** delegated to a sandboxed rebase
  task — task sandboxes cannot reach remote-tracking refs, and worker rebase targets MUST
  stay local.
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
  stale completed rebase and no current-target-tip containment remains eligible for the
  shared `pre-dispatch-rebase` gate before its next branch action. The engine MUST NOT
  re-queue an identical rebase (see
  [00-overview.md](00-overview.md#core-invariants-the-load-bearing-rules), invariant 2).
- Repeated rebase failures reach the **circuit-breaker bound** with no intervening success,
  review, review-clear proof, or code change → `needs_discussion` (reason
  `rebase-failure-circuit-breaker`).
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
- If a completed changed-diff rebase row has lost or partially lost its persisted
  pre/post-rebase provenance, lifecycle MUST first try to re-derive that provenance from
  local git refs and reflogs. Writable lifecycle paths MUST persist the repaired rebase
  metadata before evaluating dependent resolution-review state. Read-only/query lifecycle
  paths MAY apply the proven repair in memory for that evaluation, MUST NOT write the
  repaired metadata, and MUST leave durable repair for the next write-capable lifecycle
  path.
- If persisted metadata for a required resolution review is missing, stale, or inconsistent
  with the authoritative post-rebase context, lifecycle MUST then try to re-derive the
  resolved post-rebase head/target SHAs from the repaired rebase provenance plus the live
  rebase branch head and the current or persisted merge target. Writable lifecycle paths
  MUST repair the review task's persisted resolution-review metadata from that shared
  context before evaluating merge eligibility. Read-only/query lifecycle paths MAY apply
  the proven review-scope repair in memory for that evaluation, MUST NOT write it, and
  MUST leave durable repair for the next write-capable lifecycle path.
- Recovered rebase-diff provenance is usable for a narrow resolution review when all
  three pre-rebase SHAs and the resolved post-rebase head/target SHAs are populated.
  `Recovered baseline: yes` records how the baseline was obtained; it MUST NOT by itself
  make otherwise complete provenance invalid.
- Provenance is an optimization for a narrow resolution review, not the correctness
  floor for merge. If lifecycle still cannot resolve or validate the metadata that
  defines a required resolution review after the re-derivation-and-repair attempt, it
  MUST NOT silently preserve the old approval and MUST NOT park permanently only because
  resolution-review metadata is unavailable. Instead, when the live implementation head
  is known, lifecycle MUST degrade to the coarser head-SHA freshness rule: require a
  plain full review whose stored `review_verify_head_sha` equals the current live branch
  head, plus current green verify evidence. Until such a full review exists, lifecycle
  MUST select the normal pre-review verify / `create_review` path for a full review at
  the current head, and operator-facing descriptions MUST distinguish unavailable
  resolution-review metadata from actual reviewed-head/live-head advancement. A pending
  plain full-review row MAY be reused only when its stored `review_verify_head_sha`
  already equals the proven live head and the executor enforces that selected head before
  launch. A full-review approval or pending full-review row recorded against an older
  head MUST NOT satisfy the merge gate. Pending malformed resolution-review refresh rows
  MAY be dropped before that fallback is selected, but valid spec-coherence reviews MUST
  remain governed by the spec-coherence gate and an `in_progress` malformed refresh row
  MUST terminalize before lifecycle selects a replacement review that could race it.
  When the live implementation head cannot be proven, lifecycle MUST fail closed with
  `review-freshness-unverified`; cached merge-unit head metadata is not live proof.
- When valid resolution-review metadata exists for a real resolution review, lifecycle
  MUST keep the narrow resolution-review path and precise rebase-delta match. Plain
  non-resolution reviews MUST NOT be repaired into resolution reviews merely because
  they completed after a changed-diff rebase; they are eligible only through the full
  live-head fallback above.
- Stale-review refresh rules MUST run before `review_max_cycles` evaluation.
- With current default `on_max_cycles=park`, capped review/improve churn remains a
  `review-max-cycles-reached` manual-attention stop.
- With opt-in `on_max_cycles=merge_and_defer`, a capped current ordinary plain-full or
  resolution `CHANGES_REQUESTED` review MAY enter the existing pre-merge verify
  gate before `review_max_cycles` handling only after local merge-source proof
  and readable deterministic blocker content are established. This candidate
  MUST still be current for the live implementation head and outside the plan
  cap, verify exhaustion, no-op improve, adjudication, duplicate-blocker, and
  spec-coherence lanes. Missing merge source or unavailable/invalid review
  content MUST surface its own attention reason before verify handling; when
  both source proof and review content are unavailable, missing merge source
  MUST win with `merge-source-needs-manual-resolution`. Missing or stale verify
  evidence MUST run the normal pre-merge verify path; red or unavailable
  evidence MUST remain on the existing verify-fix or attention path.
- `max_review_cycles` MUST count completed review/improve cycles for the merge unit,
  not just cycles since the last commit, and MUST apply to ordinary and
  `spec_coherence` review modes through the same boundary-scoped count. Existing
  pending or in-progress improves for a current `CHANGES_REQUESTED` review MUST be
  allowed to run or finish before the cap parks the lineage, but lifecycle MUST NOT
  create another improve after the count reaches the bound. Ordinary improve, fix,
  or reviewed-head advancement MUST NOT reset the counter. The counter MAY reset
  only at a deliberate scope/base boundary, currently persisted as a completed
  rebase with affirmative changed-diff boundary proof over the exact immutable
  target SHA consumed by the rebase operation and used for the post-rebase
  comparison. Historical pre-boundary churn MUST NOT keep poisoning the lineage
  after that boundary. Review rounds belong to a boundary epoch by durable
  attempt-start time, so a review created before a changed-diff rebase MUST NOT
  count as a post-boundary round merely because it completed afterward. Recovered,
  missing-ref, moving-target, or comparison-error rebases MAY invalidate review
  because equivalence is unproven, but MUST NOT reset cycle accounting or be
  labeled as proven changed-diff boundaries.
- Review evidence for an implementation lineage MUST include direct implementation-linked
  reviews, merge-unit-attached reviews, and review recovery descendants whose `based_on`
  chain stays on the automatic review recovery path. Strict exact legacy unlinked
  slug reviews MAY be fallback lifecycle evidence only when linked or
  merge-unit-attached review evidence is absent. The review-cycle accounting
  population MAY independently include those eligible legacy rows so historical
  rounds contribute to the cap without changing authoritative latest-review,
  merge, finalization, or query decisions. A manual same-type follow-up on a
  review MUST NOT silently count as review evidence for merge or closing-review
  invariants.
- Legacy unlinked review rows MAY contribute to review evidence and review-cycle
  accounting only when their prompt or task slug exactly names the implementation slug.
  Overlapping slugs such as `foo` and `foo-bar` MUST remain isolated. When historical
  implementations reuse the same semantic slug, dated review slugs MUST match the
  implementation's dated slug identity when available; prompt-only rows MUST be scoped
  by durable review attempt-start time to the matching implementation epoch and MUST NOT
  be attributed to multiple merge units.

### §5a — Pre-review verify gate

Before lifecycle creates a first review or refreshes a stale review for an implementation
owner, it MUST evaluate the runner-owned verify gate for that owner's current verify
epoch.

- If the current implementation lineage has no resolvable local merge source (`merge_source_ref`
  is absent, there is no explicit merge-source warning, and persisted merge state is not
  terminal), lifecycle MUST fail closed with `needs_discussion`
  (`merge-source-needs-manual-resolution`) before any pre-review `verify_gate`,
  `create_review`, or `run_review` automation.
- Missing or stale verify evidence for the current owner epoch MUST select `verify_gate`
  first. Lifecycle MUST rerun verify before it creates a review for that head.
- Current red verify evidence before review MUST route into the `verify_fix` lane, not the
  review/improve lane, unless it is a budget-only timeout. Lifecycle MUST create, reuse,
  run, or wait on one same-branch `verify_fix` task keyed by the exact current verify
  epoch and implementation owner.
- If the current verify evidence has `failure_origin == "timeout"` and persisted phase
  diagnostics show no failed phase, lifecycle MUST park with
  `verify-budget-exceeded` instead of creating a `verify_fix`. The gate remains
  blocking; this only classifies the condition as wall-clock budget exhaustion rather
  than a code defect. The attention payload SHOULD include completed phase names and
  known not-started phase names when available. This classification MUST happen before
  looking up or acting on any existing same-epoch `verify_fix`; existing task rows are
  left untouched.
- If the current evidence is not a budget-only timeout and a same-epoch `verify_fix`
  is already `pending`, lifecycle MUST `run_verify_fix`; if it is already
  `in_progress`, lifecycle MUST `wait_verify_fix`.
- If a same-epoch `verify_fix` is `failed`, lifecycle MUST apply the shared failed-task
  recovery policy to that `verify_fix` before parking the implementation owner. A retry
  decision with remaining attempts MUST dispatch/reuse the verify-fix retry; only
  non-retry decisions or exhausted retry attempts may park with `verify-failed-needs-fix`,
  and the park message MUST distinguish those cases.
- If one same-epoch `verify_fix` attempt completed without source changes and the current
  red verify evidence is structurally classified as timeout-origin, lifecycle MUST rerun
  verification for the exact same head once before treating that `verify_fix` as
  terminally complete for the epoch. That rerun MUST persist SHA-keyed green evidence
  only when the tested tracked tree is clean for the recorded head and the verify-fix
  task is proven not to have restored, edited, or committed source relative to that head.
  During verify-fix completion, if the rerun cannot be performed, cannot be persisted, or
  does not produce green evidence, lifecycle MUST leave the `verify_fix`
  non-completed/retryable rather than completing against the timeout-origin red. For an
  already-completed same-epoch `verify_fix` that has durable no-source proof, a
  persisted completion head SHA, and no consumed recovery rerun, lifecycle MUST select an
  exact-head rerun action before parking the epoch as terminally failed. A completed
  legacy row missing the structured outcome MAY be upgraded to that durable no-source
  outcome only when lifecycle can prove the live branch head exactly equals the verify
  epoch head and the worktree is clean. A present-but-invalid canonical completion
  outcome, including malformed JSON, wrong kind, or unsupported schema version, MUST NOT
  fall back to legacy columns and MUST fail closed with actionable proof-unavailable
  recovery guidance instead of scheduling a rerun; otherwise legacy upgrade failures MUST
  fail closed with actionable recovery guidance. Once an already-completed verify-fix
  recovery rerun persists any
  non-green result, including another timeout, lifecycle MUST record the rerun as consumed
  and the next decision MUST park rather than rerun indefinitely.
- Latest same-head verify evidence wins inside a verify epoch: a newer persisted green
  result for the same reviewed branch and reviewed head SHA supersedes older same-head
  red evidence. The normalized verify command and timeout settings are run provenance,
  not verify freshness identity; changing only `verify_command`,
  `autonomous_verify_timeout_seconds`, or `review_verify_timeout_grace_seconds` MUST NOT
  be the supported way to clear or stale a verify gate.
- When the newest current same-head verify evidence in a merge unit is credited to a
  contributor instead of the canonical owner, writable lifecycle execution MUST recredit
  that evidence to the canonical owner by copying the selected artifact's original
  source task, verify epoch run settings, structured provenance, and aggregate details,
  and MUST record the reconciliation itself as separate metadata. It MUST then
  reevaluate before routing red, missing, or unavailable owner evidence to verify-gate
  reruns, `verify_fix`, or park states. Legacy evidence without resolvable source
  provenance MAY use an explicit safe fallback source, but valid source provenance MUST
  NOT be rewritten to the artifact-holder row. Any no-merge-unit compatibility copy that
  attaches canonical owner evidence to a prepared holder row MUST use the same
  provenance-preserving copy semantics.
- If one same-epoch `verify_fix` attempt already completed and the latest current verify
  gate remains red for that epoch, lifecycle MUST park with reason `verify-fix-failed`
  instead of spawning another `verify_fix`. Deterministic test failures and unknown
  structured failure origins are not timeout-origin and MUST remain blocking unless a
  later source-changing fix or valid same-head green verify result replaces them. A
  manual `iterate --force` or `unstick --reason verify-fix-failed` rearm MAY bypass only
  this park and MUST first run a fresh verify gate for the current head; if that fresh
  verify is still red, cannot be executed, or cannot be persisted, lifecycle MUST remain
  blocked and surface an actionable direct diagnostic rather than continuing to review or
  merge.
- `gza verify <task_id>` is a manual verify-gate refresh surface for a resolved
  merge unit. It MUST run the same lifecycle verify executor and persist the same
  `verify_gate_result` evidence for the current epoch; it MUST NOT bypass,
  suppress, or fake green evidence. If the current epoch is already green, it MUST
  avoid rerunning unless `--force` is supplied. The command MUST select the same
  effective newest current same-epoch merge-unit evidence as lifecycle before
  reporting `--dry-run`, no-op, or refresh outcomes. A writable no-force invocation
  MUST recredit non-owner current merge-unit evidence to the canonical owner. After
  that reconciliation, it MAY return without rerunning only when the refreshed
  effective state is green; if the refreshed effective state is red, unavailable,
  stale, or missing, it MUST continue through the shared explicit-refresh
  `verify_gate` action and report that fresh outcome. It MUST NOT hide newer red
  contributor evidence behind older owner green evidence. `--dry-run` MUST report
  that effective epoch and verdict without creating, attaching, synchronizing,
  dual-writing, or otherwise mutating merge-unit or verify evidence. If query-only
  merge-unit planning encounters cyclic branchless-review lineage, dry-run and writable
  invocations MUST return non-zero with a concise diagnostic before mutating merge-unit
  or verify evidence. A forced rerun MUST return success only when the invoked
  verify action succeeds and
  produces fresh current green evidence. If the forced action persists fresh
  current red or unavailable evidence, the command MUST render that fresh evidence
  normally and return non-zero.
  Setup, unsupported-action, execution-before-persistence, or persistence failures
  that leave no newly selected current result MUST return non-zero and label any old
  verdict as pre-existing evidence.
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
- A completed recovered review descendant that resolves to the current implementation
  lineage counts as review evidence for this section and for the closing-review invariant
  in §8. Once such a recovered review exists, lifecycle MUST route on its verdict or park
  fail-closed; it MUST NOT keep selecting a fresh same-head `create_review` only because
  the recovered row is linked through a failed review ancestor instead of directly to the
  implementation root.
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

- Review→improve cycles reach `max_review_cycles` within the current merge-unit
  scope/base boundary → `max_cycles_reached` under current default
  `on_max_cycles=park`; under opt-in `on_max_cycles=merge_and_defer`, eligible
  ordinary current-head candidates first prove local merge source and readable
  deterministic blocker content, then traverse the pre-merge verify gate above,
  and emit the existing `merge` action annotated with max-cycle deferral metadata
  only when fresh green verify evidence and validated persisted `BLOCKER` metadata
  are available. Missing merge source or unavailable/invalid review content
  surfaces its own attention reason instead of falling through to the generic cap
  park; if both are present, missing merge source wins. Plan-review cap exhaustion
  is separate: it accepts the latest plan revision and moves to implementation, and
  it does not grant code-merge authority. Verify exhaustion states, including red
  verify, unavailable verify, failed verify-fix, and unavailable-after-fix, cannot
  satisfy the merge-and-defer precondition.
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
- The closing-review requirement after a newer completed code change is satisfied by any
  follow-on review evidence for that implementation, including an eligible completed
  review recovery descendant. Failed closing-review attempts do not satisfy the invariant,
  and their bounded retry accounting MUST follow the same logical review recovery chain
  rather than restarting from zero on each retry/resume descendant.
- A non-implementation unit, or a unit that does not require review → `merge`.
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
  `verify_fix` handling before merge. This review-disabled branch is one ordinary
  automated lifecycle exception to the implementation two-gate merge rule from
  [00-overview.md](00-overview.md#core-invariants-the-load-bearing-rules); the other is
  the narrow capped-review `on_max_cycles=merge_and_defer` path defined below. The
  separate exact-state guarded-landing exception named there is operator-triggered only
  and belongs to §8a; `advance` and `watch` MUST NOT use it.
- A selected `create_review` action has epoch identity, and automation MUST NOT treat an
  older review row or mismatched active duplicate as the selected action. Ordinary
  branch-head and closing-review refreshes match the implementation and selected reviewed
  head when that head is known; resolution reviews match implementation, rebase task,
  resolved head SHA, and resolved target SHA; behavior-spec coherence reviews match
  implementation, reviewed head SHA, and changed behavior-spec paths. A mismatched active
  review is a conflict to surface or wait on, not a review task to run or account as the
  selected action.
- A failed implementation task is never mergeable. Timeout-style failed implementations
  with a resumable `session_id` MUST stay in recovery until that recovery resolves to a
  valid completed representative, exhausts its bounded policy, or is parked for manual
  intervention.
- Merge executes against the canonical local target (see
  [00-overview.md](00-overview.md#core-invariants-the-load-bearing-rules), invariant 4), respects
  `merge_squash_threshold`, and MUST NOT push the target branch as a side effect. Direct
  mark-merged paths and post-promotion bookkeeping are part of the same precondition: they
  MUST prove the owning merge unit is independently unmerged and MUST reject merge
  representatives whose task execution status is neither canonical `completed` nor the
  compatibility task execution status `unmerged` defined in
  [00-overview.md](00-overview.md#vocabulary-the-data-model-abstractly). Pending,
  in-progress, and failed representatives MUST be rejected even when their owning merge
  unit is unmerged.
- Automated merge execution MUST distinguish pre-promotion source/proof refusals from
  post-promotion proof or state failures. If the authorized source ref is unavailable,
  pre-merge proof cannot be established, or the source ref changes after mandatory
  child materialization but before merge, `advance`, `repeat`, and `watch` MUST report
  that the target is unchanged, preserve any created or reused child IDs in output, and
  MUST NOT invoke conflict assessment or create a rebase child.
- Manual `gza merge` retains a narrower human-override path than automation. Automated
  lifecycle actions (`advance`/`watch`) MUST still merge only review-cleared work under
  the rules above, except for the narrow `on_max_cycles=merge_and_defer` capped-review
  path defined in §6: an eligible ordinary plain-full or resolution review may produce an
  annotated `merge` action only when the review content is unchanged, the live source ref
  still equals the reviewed head, the deterministic persisted `BLOCKER` payload is
  validated, and current lifecycle-owned verify evidence is fresh and passing for that
  same head. The executor MUST create or reuse every deferred-blocker task before
  promotion, already-merged mutation, or merge-unit finalization records success.
  Missing or stale verify evidence MUST run the normal pre-merge verify path before
  eligibility is reconsidered; spec-coherence reviews, red or unavailable verify gates,
  no-op/adjudication lanes, and other parked gates remain non-deferable. Automation MUST
  NOT otherwise auto-merge `CHANGES_REQUESTED` reviews by deferring blockers, and it MUST
  NOT bypass parked lifecycle `needs_attention` / `needs_discussion` merge gates.
  Manual `gza merge --force` MAY override those parked
  lifecycle gates for the local merge path only, but it MUST still refuse any real git
  conflict and MUST leave the unit's persisted provenance distinguishable from an
  ordinary manual merge. Manual `gza merge` is a non-rebase merge command: standalone
  task-backed rebase orchestration belongs to `gza rebase <task-id> --run`, and full
  rebase/review/judgment/merge orchestration belongs to `gza land <task-id>`. The removed
  merge flags `--rebase`, `--remote`, and `--resolve` have no compatibility surface and
  MUST be rejected by the CLI parser rather than parsed as aliases. Manual `gza merge`
  MUST refuse a latest completed
  plain-full or resolution `CHANGES_REQUESTED` review that still has any open non-verify
  `BLOCKER` finding unless the operator passes `--defer-blockers`. Behavior-spec
  coherence `CHANGES_REQUESTED` reviews are not eligible for `--defer-blockers` and MUST
  remain refused by the manual merge path. Current red verify-gate actions are a stricter manual exception:
  `create_verify_fix`, `rerun_completed_verify_fix`, and `needs_discussion` with reason
  `verify-fix-failed` for the same current verify epoch MUST NOT be bypassed by `--force`
  alone. They MAY be bypassed only when the operator passes both `--force` and
  `--ignore-verify-gate`; a successful bypass MUST print a loud warning that includes the
  failing epoch head and verify command, and MUST persist `manual_force` merge provenance.
  Pending and in-progress same-epoch verify-fix work (`run_verify_fix` and
  `wait_verify_fix`) MUST fail closed even with both red-gate bypass flags because the
  live recovery task may still mutate the source branch. Unavailable verify evidence,
  unavailable epochs, invalid verify-fix proof, stopped verify-fix tasks, and other
  non-red attention states MUST stay distinct from current red evidence and MUST NOT be
  accepted by the red-gate bypass predicate. Manual `gza merge` MUST refuse any real git conflict even
  with both red-gate bypass flags.
- For the automated `on_max_cycles=merge_and_defer` exception, every parsed open
  `BLOCKER` finding from the capped review MUST have a corresponding urgent
  `implement` follow-up created or idempotently found before git promotion,
  mark-merged reconciliation, or merge-unit `state=merged` persistence. Each deferred
  task MUST be based on and depend on the implementation being merged, MUST carry
  deterministic audit identity for the implementation, review, finding, and
  `review-max-cycles` reason, MUST preserve the complete persisted review output in
  its durable prompt payload, and MUST include the reserved
  `deferred-review-blocker` tag plus inherited and active positive scope tags.
  Successful capped merges MUST persist distinct `max_cycles_deferred` merge
  provenance for ordinary merge, already-merged reconciliation, and isolated
  finalization paths. Child tags identify the outstanding debt population; merge
  provenance identifies the automated merge event even after children finish.
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
- When that red-main path hands off to automatic remediation, remediation metadata MUST
  include the observed verify environment identity as prompt context, but that metadata
  MUST NOT block watch from creating or requeueing the bounded remediation task.
- When automation uses an isolated host merge checkout to stage a merge before updating
  the canonical local target, that isolated checkout becomes the authoritative
  pre-promotion verify subject. With a configured verify gate, the shared merge executor
  MUST run candidate verify on the exact staged candidate tree before updating the
  canonical target ref, MUST fail closed when the required isolated checkout is
  unavailable, and MUST block promotion when the candidate result is red or exact-tree
  freshness cannot be proven. The canonical checkpoint MAY be copied forward from that
  candidate evidence only when the exact verified candidate tree is the one promoted onto
  the canonical target. Already-merged reconciliation may mutate merge state immediately
  only when the source is contained in the live pre-batch target; containment introduced
  only by earlier staged candidate entries MUST wait for verified promotion. Before any later merge attempt in the same command cycle, the
  caller MUST refresh or rebuild that isolated checkout back to the canonical target, or
  stop the merge lane for the cycle; later candidates MUST NOT run on top of a blocked
  candidate tree. Watch's caller surface MUST preserve that blocked-candidate outcome
  distinctly, surfacing it as candidate-verify attention for the merge subject rather
  than collapsing it into a generic merge failure.
- An ordinary merge or rebase operation failure emitted by the shared merge executor's
  non-resolve failure block MUST identify the merge subject task ID and source branch on
  that block's primary error line, abort line, and abort-warning line, while preserving
  the underlying git error text.

Note: the "implementation unit with no review" rule above applies only when the
implementation still has reviewable commits or diff against the target. Terminal
empty/redundant implementations are covered by the moot rule and do not require review
creation.

### §8a — Operator-triggered land

`uv run gza land <task-id>` is a manual command over one canonical merge unit, not an
ordinary lifecycle action selected by `advance` or `watch`. The command MUST accept one
selected task, resolve it through canonical merge-unit membership to the unit owner,
representative, local source ref, and canonical local target branch, and re-read that
state after every mutating step. If authoritative merge-unit reconciliation proves the
unit is already `merged`, `land` MUST finish idempotently without rebasing, reviewing,
judging, deferring blockers, or merging again.

Landing phase order is part of the safety contract. Writable `land` MUST execute or stop
in this order, re-reading durable state between phases and after every source-head
change: resolve identity and active merge unit; prove dependency readiness; prove project
scope from reliable changed-path inspection; prove source/target heads and checkout
cleanliness; run or exact-reuse the one required rebase when the source lacks the target
tip; re-read and re-prove dependency readiness and project scope for the new source head;
acquire or exact-reuse current lifecycle source verify; acquire or exact-reuse required
spec-coherence evidence; acquire or exact-reuse the required code/resolution review;
evaluate strict or guarded policy; perform final preflight; materialize ordinary
follow-ups and, for guarded escalation, deferred blockers; merge or mark merged; then
refresh/reuse the post-merge target verification checkpoint. Out-of-scope and
scope-unverifiable branches MUST stop before rebase, verify, review, judgment, or any
provider-backed work launches. If a rebase or any other source-head change introduces an
out-of-scope path, unverifiable changed-path set, newly unresolved dependency, or
dependency proof that can no longer be trusted, `land` MUST stop before verify, review,
judgment, follow-up/deferred-task materialization, or merge.

`land` has one per-run policy: `guarded` by default, or `strict`. `strict` MUST obey the
ordinary §8 merge gates exactly: it MUST perform or exact-reuse deterministic
prerequisites such as the required same-source rebase, current verify acquisition, and
required current reviews whenever acquisition is enabled and identity proof is available,
but it MUST stop on any parked lifecycle gate or open blocker that ordinary automation
would not merge. When `require_review_before_merge=false`, `strict` MUST preserve §8's
explicit no-review merge path: current green source verify plus all other non-review
merge prerequisites are sufficient and no code-review acquisition is required. `guarded`
does not support review-disabled escalation in this version; if
`require_review_before_merge=false` and strict prerequisites are otherwise satisfied, it
MUST use the same non-escalated no-review path and record `manual_land`, and if escalation
would be needed it MUST stop with a review-gate-disabled blocking fact rather than ask a
landing judge to waive an absent review. `guarded` MAY choose the equivalent of manual
force/deferred-blocker merge only after every deterministic gate below passes and the
durable landing judgment authorizes escalation for the exact current state. Neither
policy is a config knob, and neither changes unattended `advance`/`watch` behavior.

#### Deterministic prerequisites

Before any semantic landing judgment, `land` MUST fail closed unless all facts below are
proven from current state:

- The selected ID resolves to exactly one active merge unit whose canonical execution
  representative has task execution status `completed` or compatibility task execution
  status `unmerged`, whose owning merge unit is independently in merge state `unmerged`,
  has a resolvable local source ref, and targets the currently checked-out canonical
  local target branch. A pending, in-progress, failed, missing, or ambiguous
  representative is rejected even when the merge unit is unmerged.
- All merge-required dependencies are proven satisfied by authoritative merged or
  terminal no-work merge-unit state before any code action. Missing, stale, ambiguous, or
  still-unresolved dependency proof is nonforceable and MUST be recomputed after every
  rebase or other source-head-changing step.
- Project scope is proven before any rebase, verify, review, judgment, merge, or other
  provider-backed work. The proof MUST come from reliable changed-path inspection for the
  exact live source head and target head being considered, applying §3's cross-project
  and project-root attribution rules. Out-of-scope paths and
  `project-scope-unverified` stop before launching any rebase/provider work. The
  changed-path set and scope decision MUST be recomputed after every rebase or other
  source-head-changing step before verify, spec-coherence, code review, judgment,
  follow-up/deferred-task materialization, merge, or mark-merged can continue.
- The tracked checkout is clean, the live source head and target head are known, and a
  final pre-merge proof shows the source cleanly merges into that local target after any
  completed rebase. Missing ancestry, missing merge proof, remote-only source proof, or
  unresolved/failed conflict resolution is nonforceable.
- If the source does not contain the target tip, `land` MUST run or exact-reuse exactly
  one task-backed rebase onto the canonical local target when rebase acquisition is
  enabled, the shared launch route has capacity/permission, and exact identity proof is
  available. It MUST refuse only for an enumerated inability: acquisition disabled,
  source/target or active-work identity conflict, launch/capacity failure, terminal
  worker failure, unavailable ancestry/rebase proof, incompatible exact reuse, an exact
  in-progress rebase that must be waited on, or exhausted invocation budget. If
  ancestry/behind proof is unavailable, or the rebase fails, is unresolved, is superseded
  without exact target-tip containment proof, or still leaves the source unmergeable, the
  command MUST stop.
- Current lifecycle verify evidence is green for the exact final live source head/tree
  and current verify-gate identity. If evidence is absent or stale, writable `land` MUST
  run or exact-reuse the shared direct verify acquisition path when acquisition is
  enabled and identity proof is available. It MUST refuse only for an enumerated
  inability: verify acquisition disabled, source/epoch or active-work identity conflict,
  launch/capacity failure, terminal worker failure, unavailable proof, exact
  in-progress verify work that must be waited on, incompatible exact reuse, or exhausted
  invocation budget. Provider-side rebase verification is not a substitute for canonical
  lifecycle verify evidence. Red, unavailable, malformed, stale, stopped, or still
  missing verify evidence after the acquisition/reuse attempt is nondeferrable.
- If the ordinary §8 gate requires a review, the latest relevant code review is current,
  parseable, and either plain-full or resolution mode. All code/resolution-review
  acquisition, reuse, waiting, budget, and verdict rules in this section apply only to
  these review-enabled lineages. If the review gate is disabled, `strict` and
  non-escalated `guarded` landing use the §8 no-review path; guarded escalation is
  rejected as described above.
- Spec-coherence has an explicit phase between current source-verify acquisition and the
  invocation-local code/resolution-review phase. If spec-coherence is disabled or the
  current changed-path set does not touch configured spec-coherence paths, `land` skips
  this phase. If touched paths require evidence and no exact spec-coherence task or
  artifact exists, writable `land` MUST create/run the exact task through the shared
  launch route when acquisition is enabled and identity proof is available; it MUST NOT
  proceed as though the gate were optional. If an exact pending spec-coherence task
  exists, `land` MUST run or exact-reuse that task unless an enumerated inability applies.
  If an exact in-progress spec-coherence task exists, `land` MUST wait or
  stop rather than create a duplicate. If exact current evidence is terminal `APPROVED`,
  `land` may proceed to the code/resolution-review phase. Every other terminal verdict
  (`CHANGES_REQUESTED`, `NEEDS_DISCUSSION`, failed, malformed, unknown, or unavailable)
  and every stale, head-mismatched, path-mismatched, or identity-mismatched task/artifact
  MUST stop before code-review judgment or deferred blocker materialization. Mandatory
  spec-coherence work is independent of the invocation-local one code/resolution-review
  budget; creating, running, reusing, or waiting on spec-coherence work MUST NOT consume
  that budget, and spending the code-review budget MUST NOT skip or satisfy a required
  spec-coherence review.
- There is no unresolved dependency, out-of-project path gate, missing source/target
  proof, freshness uncertainty, failed implementation/rebase/verify-fix recovery,
  pending or in-progress verify-fix work, unavailable verify epoch, or other actionable
  lifecycle work that must run before merge.

Missing or stale required code/resolution review evidence in a review-enabled lineage is
not a generic prerequisite-missing stop. Writable `land` MUST create/run or exact-reuse
the required plain-full or resolution review through the shared route when acquisition is
enabled, exact identity proof is available, and the invocation-local review budget has
not been spent. It MUST refuse only for an enumerated inability: review acquisition
disabled, source/head/mode/rebase-provenance or active-work identity conflict,
launch/capacity failure, terminal worker failure, unavailable proof, exact in-progress
review work that must be waited on, incompatible exact reuse, or exhausted invocation
budget.

The only parked reason classes `guarded` may consider overriding are review-churn
exhaustion states: `review-max-cycles-reached`, `duplicate-blocker-no-progress`, and
`improve-no-op`. `review-blocker-adjudication-needed` is eligible only when the landing
judgment receives the complete current dispute and adjudication evidence. `guarded` MUST
NOT override a review verdict of `NEEDS_DISCUSSION`, project-scope parks, recovery parks,
verify parks, rebase parks, malformed/unknown review parks, missing manual-review parks,
or unrelated needs-attention classes merely because `gza merge --force` has a narrower
human escape hatch.

#### Post-rebase review bound

For review-enabled lineages, `land` MUST capture the pre-rebase context and enforce an
invocation-local budget of at most one post-rebase code/resolution review. A mechanical
rebase that proves `changed_diff == false` and did not require provider/AI conflict
resolution MAY preserve an eligible current review. The rebase outcome MUST be durable,
structured, bound to the attempted source head and target head, and must classify the
execution as `mechanical`, `no-op`, or `provider-resolved`. Missing, malformed,
head-mismatched, unsupported, or prose-only rebase outcome proof MUST fail closed and
MUST be included in the coordinator's reuse and non-progress fingerprint identity. `land`
MUST NOT infer mechanical safety or provider resolution from task output prose or logs.
Every durable `no-op` subtype MUST carry explicit proof and be classified before any
review evidence is carried forward:

- `no-op:already-contained` MAY preserve an eligible current review only when the outcome
  proves exact attempted source-head identity, exact attempted target-head identity, exact
  target-tip containment in the live source head, `changed_diff == false`, and
  `provider_conflict_resolution == false`.
- `no-op:superseded-contained` MAY proceed without another rebase only when the outcome
  proves the live source head now exactly contains the attempted target tip, the live
  source head is the head re-read by the coordinator, and no provider conflict resolution
  occurred. It MAY preserve existing review evidence only when the same exact source-head
  identity and `changed_diff == false` proof also holds; otherwise it MUST obtain the
  single current-head review required by this section.
- `no-op:unchanged-target`, `no-op:moot`, or any other unchanged no-op subtype MAY carry
  review evidence forward only with exact source/target identity, target-tip containment,
  `changed_diff == false`, and proof that no provider conflict resolution occurred.
- A no-op outcome with missing, mismatched, prose-only, unsupported, or ambiguous proof
  MUST fail closed with a named `LandBlocked` reason if the coordinator cannot safely
  acquire a current review. If current review acquisition is enabled and budget remains,
  it MUST refresh through the single current-head review path instead of preserving old
  review evidence.
Any `provider-resolved` outcome requires one current resolution review even when
`changed_diff == false`, because conflict resolution is itself an unreviewed code
decision. A changed diff, unknown diff, recovered/resumed rebase, or unrepaired
resolution-review provenance also requires exactly one current-head review: prefer a
resolution review when exact provenance can be bound, otherwise fall back to one plain
full review bound to the proven live head.

For review-disabled lineages, the same provider-resolved, changed/unknown-diff,
unrepaired-provenance, and recovered/resumed rebase outcomes MUST NOT create, run, reuse,
or wait on a code review or resolution review, and MUST NOT create or run a landing
judgment. After any rebase or source-head-changing step, both `strict` and
non-escalated `guarded` landing MUST obtain current canonical green lifecycle verify
evidence for the exact final live source head/tree and current gate identity, satisfy all
other non-review prerequisites and final preflight rules, and then continue through
ordinary no-review landing with `manual_land` provenance.

After every rebase or any other source-head-changing step in a review-enabled lineage,
`land` MUST first obtain or reuse current canonical green lifecycle verify evidence for
the new live source head and current gate identity before it creates, runs, reuses, or
waits on the invocation's post-rebase code/resolution review. Red, unavailable,
malformed, stale, or missing verify evidence after that acquisition attempt MUST stop
without consuming the one post-rebase review budget and without creating or running a
landing judgment.

An exact matching pending review MAY be reused only when its mode and reviewed-head
identity match the required review. A genuinely in-progress matching review is a wait/stop
condition, not permission to create a duplicate. `APPROVED` and valid
`APPROVED_WITH_FOLLOWUPS` remain merge-permitting. `CHANGES_REQUESTED` under `strict`
blocks immediately; under `guarded` it may proceed only to the single landing judgment.
`land` MUST NOT create, run, wait on, or resume an `improve` task, and MUST NOT start a
second review in the same invocation after a post-rebase `CHANGES_REQUESTED` verdict.
Failed, malformed, unknown, or `NEEDS_DISCUSSION` review output stops the command.

#### Guarded landing judgment

`guarded` escalation requires a bounded independent landing judgment with strict
structured output. The judgment prompt MUST include the authoritative review scope and
linked request/plan context, source and target heads, current diff context, the current
review and parsed blocker records, current green verify evidence, the exact current
blocker-resolution, dispute, and adjudication artifact identities and their normalized
decision content, and the guarded policy definition.

The judgment schema MUST be versioned and produce exactly one overall result:
`LAND`, `BLOCK`, or `NEEDS_HUMAN`. `LAND` is valid only when it states that the original
graded ask is satisfied and every current blocker is `DEFERABLE`. A blocker is
deferrable only when it is adjacent to or beyond the authoritative scope and safe as an
urgent follow-up. Correctness regressions, repository-rule violations, integration
contract defects, unsafe conflict-resolution defects, behavior-spec coherence findings,
verify failures, source/target proof failures, and dependency/scope gates are
nondeferrable regardless of judge text. Missing findings, malformed output, ambiguous or
uncited evidence, `NEEDS_HUMAN`, or any `REQUIRED` blocker MUST block landing.

The judgment MUST be persisted as an auditable artifact keyed at minimum by policy/schema
version, implementation ID, merge-unit ID, review ID and reviewed head, live source head,
target head, verify artifact or epoch identity, authoritative review scope, and normalized
current blocker fingerprints plus the current blocker-resolution, dispute, and
adjudication artifact identities and normalized decision content. It MAY be reused only on
an exact key match. Any source, target, review, verify, blocker, resolution/dispute/
adjudication artifact, scope, or policy change invalidates the prior judgment. The
judgment reuse key and the coordinator visited fingerprint are one exact-identity class:
any input that can affect a landing decision MUST appear in at least one of those
identities, and any durable evidence phase that can make progress MUST appear in the
visited fingerprint.

#### Deferred blockers, provenance, and final preflight

Immediately before any durable follow-up/deferred-task materialization, mark-merged
mutation, or merge mutation, every `strict`, `guarded`, escalated, and non-escalated
landing path MUST run the same final preflight: recheck the exact source head, exact
target head, tracked checkout cleanliness, and clean-merge proof that the preceding
verify/review/judgment evidence was bound to. If either head moved, the checkout dirtied,
or clean-merge proof changed, it MUST stop with that single blocking fact, perform no
merge mutation, create no follow-up or deferred-blocker tasks, and MUST NOT spend another
rebase, review, or judgment budget in the same invocation. Only after this final preflight
may the command create or reuse all ordinary `FOLLOWUP` tasks from the current review and,
for guarded escalation, urgent PR-required deferred-blocker `implement` tasks for every
deferred `BLOCKER`. Every landing path, including strict, non-escalated guarded,
escalated guarded, merge, and mark-merged success, MUST create or reuse the deterministic
ordinary follow-up task set before recording success whenever the current review contains
`FOLLOWUP` findings. Ordinary `FOLLOWUP` findings materialized from a guarded
`CHANGES_REQUESTED` escalation are part of the same safety handoff as deferred blockers:
each such follow-up MUST be urgent and PR-required before merge or mark-merged success.
Exact-key reuse of a pre-existing ordinary follow-up from that guarded escalation MUST
validate both properties and reconcile them before merge when reconciliation is available;
if either property is missing and cannot be reconciled, landing MUST refuse before any
merge-state mutation. Ordinary follow-up semantics for non-escalated
`APPROVED_WITH_FOLLOWUPS` paths remain unchanged unless another owning contract requires
urgent or PR-required handling. Guarded deferred-blocker tasks MUST preserve both urgent
handling and PR-required semantics. Any follow-up or deferred-blocker creation failure,
reuse-validation failure, property-reconciliation failure, or persistence failure MUST
block landing and MUST occur before any merge-state mutation.

A successful non-escalated land MUST record `manual_land` merge provenance. A successful
guarded escalation that deferred blockers or overrode an eligible churn park MUST record
`manual_land_escalated` merge provenance and link the judgment artifact plus follow-up and
deferred task IDs so the override is directly auditable. Successful output SHOULD name the
canonical owner, target branch, whether rebase/review/judgment were used, any follow-up
task IDs, any deferred task IDs, and the final merge provenance. After merge, the command
MUST refresh or reuse the same
canonical post-merge target verification checkpoint required by §8 before reporting the
authoritative merged result. Configured-gate completion means fresh or reused `passed`
evidence for the exact final target tree and current gate identity. Projects with no
configured `verify_command` keep §8's explicit no-gate exception. If the merge mutation
already occurred but the post-merge checkpoint is red, unavailable, malformed, stale, or
missing after the refresh/reuse attempt, `land` MUST return non-success with wording that
truthfully states the merge occurred but integration verification failed; it MUST NOT use
the pre-merge `Cannot land ...` refusal template for that post-merge state.

#### Dry run, idempotency, and refusal output

`land --dry-run` is query-only. It MUST NOT create tasks or artifacts, run providers,
run verification, rewrite refs, update worker registry state, create follow-ups or
deferred blockers, repair durable metadata, merge, or mark anything merged. It MUST show
the resolved owner, local source, canonical target, current evidence known from queryable
state, and the ordered conditional phases available before execution. Where a future fact
requires executing a rebase, review, verify, judgment, task materialization, or merge, dry
run MUST explicitly label that phase as conditional or unknown and MUST stop prediction at
the first execution-required boundary instead of synthesizing later outcomes.

Writable `land` MUST be bounded and idempotent. It MUST enforce a named, swappable
per-invocation maximum-transition policy, `LandingTransitionLimitPolicy`, in addition to
the visited-state detector. The visited-state detector is the earlier non-progress guard:
if an exact fingerprint repeats before the transition cap is exhausted, `land` MUST stop
on that repeat. If the transition count is exhausted first, `land` MUST stop with exactly
one precedence-compatible `LandBlocked` fact for the bounded-attempt refusal, regardless
of whether every observed fingerprint was distinct. After either boundedness refusal, the
coordinator MUST NOT launch later provider work, create or reuse follow-up or deferred
tasks, run a landing judgment, merge, or mark anything merged in that invocation.

The fingerprint MUST include merge-unit state, source and target SHAs, latest relevant
review ID/verdict/reviewed head, verify epoch/verdict, durable rebase outcome
ID/status/`changed_diff`/resolution kind/source head/target head, blocker fingerprints,
landing-judgment identity, the blocker-resolution/dispute/adjudication artifact
identities and normalized decision content that feed the judgment, and the relevant
spec-coherence task/evidence identity, status, verdict, reviewed head, and normalized
changed-path set. Every
live worker/provider phase launched by `land` (rebase, verify, code review,
spec-coherence review, and landing judgment) MUST use the shared launch-permit and
execution route. Foreground `land` owns one foreground slot and MUST transfer/reuse that
slot for child worker phases rather than acquiring extra capacity for each child. Exact
matching active rebase, review, spec-coherence review, or judgment work MAY be reused or
waited on only when its identity exactly matches the current source head, target head,
review mode, changed paths, gate identity, policy schema, and blocker fingerprint
required by the selected phase. Mismatched active work MUST fail closed without creating
duplicate tasks or launching a provider, and after any ownership change or active-work
terminalization, `land` MUST re-resolve canonical merge-unit, source, target, review, and
verify state before continuing. Exact matching rebase, review, judgment, and
deferred-blocker artifacts MUST be reused; successful rerun after merge MUST reconcile
and report the already-merged state without another merge. Already-merged reconciliation
MUST still refresh or reuse the configured post-merge target checkpoint for the exact
merged target tree and current gate identity before returning success, without rerunning
earlier rebase, source-verify, review, judgment, or merge phases.

Every pre-merge terminal refusal MUST be represented as one stable `LandBlocked` result
with at least:

- `reason_code`: one of the stable codes in the total precedence list below;
- `fact`: one concise human-readable fact that explains the selected blocking condition;
- `evidence_refs`: one or more durable references such as task IDs, artifact IDs, review
  IDs, verify epochs, source/target SHAs, changed-path proof IDs, or persistence error
  records that justify the reason.

When multiple refusal facts are simultaneously true, `land` MUST select exactly one
`LandBlocked` result by this total precedence order and MUST perform no lower-precedence
side effects after selecting it:

1. `identity-proof-unavailable`: selected task, active merge unit, representative,
   dependency readiness, project-scope proof, source ref, target ref, source head, target
   head, or exact active-work identity proof is missing, ambiguous, out of scope, stale,
   or mismatched.
2. `dirty-checkout`: tracked checkout cleanliness proof failed.
3. `rebase-or-conflict`: ancestry proof, required rebase acquisition/reuse, rebase
   terminal result, durable rebase outcome proof, target-tip containment, or final
   clean-merge proof failed.
4. `verify-unavailable-or-red`: required source verify evidence is missing, stale, red,
   malformed, unavailable, stopped, or could not be acquired/reused through the shared
   direct verify path.
5. `required-review-unavailable`: required spec-coherence, code, or resolution review
   evidence is missing, stale, mismatched, failed, malformed, unknown, unavailable,
   disabled, `NEEDS_DISCUSSION`, identity-mismatched, in progress without wait
   completion, or could not be created/run/reused. A current, parseable plain-full or
   resolution `CHANGES_REQUESTED` review is usable guarded-escalation evidence, not
   unavailable, when the guarded judgment path is otherwise eligible.
6. `nondeferrable-blocker`: strict mode sees any current plain-full or resolution
   `CHANGES_REQUESTED` review blocker, or guarded policy sees an open blocker that is not
   deferrable under this contract. Guarded nondeferrable blockers stop here before any
   landing judgment.
7. `policy-or-judge-refused`: guarded escalation is unavailable, disabled by review-gate
   shape, malformed, stale, exact-key invalid, returns `BLOCK`/`NEEDS_HUMAN`, omits
   required blocker decisions, or fails reuse validation. A valid guarded `LAND` judgment
   permits continuing to materialization and final preflight; guarded `BLOCK` and
   `NEEDS_HUMAN` stop here.
8. `materialization-or-persistence-failed`: required ordinary follow-up or deferred task
   creation failed; exact-key reuse validation failed; guarded follow-up urgent/PR
   property reconciliation failed; durable judgment, provenance, merge-unit, task, or
   artifact persistence failed; or follow-up/deferred materialization could not be
   recorded before merge.
9. `bounded-attempt-exhausted`: the visited fingerprint repeated, the
   `LandingTransitionLimitPolicy` transition cap was exhausted, or the invocation budget
   for rebase, review, verify, spec-coherence, judgment, or materialization was spent
   before the command could reach a merge decision.
10. `merge-failed`: final merge, mark-merged, or pre-merge merge-state mutation failed
    before success was recorded.

Repeated-state, transition-cap, follow-up/deferred materialization, reuse-validation, and
persistence failures are therefore first-class refusals, not generic "prerequisite
missing" stops. The final user-facing pre-merge refusal line MUST be exactly one
sentence:
`Cannot land <task-id>: <fact>.` Normal phase progress may precede it and detailed
diagnostics may live in task/ops logs, but the terminal refusal MUST NOT print competing
error paragraphs or a list of suggested commands.

A post-merge checkpoint failure is a separate non-success result, not `LandBlocked`, and
it MUST NOT participate in the pre-merge refusal precedence. Its terminal user-facing
line MUST be exactly one truthful sentence stating that the merge occurred and that
integration verification failed, including the single checkpoint fact; it MUST NOT use
the `Cannot land <task-id>: <fact>.` template because that would falsely imply no merge
mutation occurred.

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
| `pre-dispatch-target-freshness-unverified` | needs_discussion | §4 behind-count probe could not prove a cleanly mergeable branch is current |
| `rebase-failed-needs-manual-resolution` | HumanParked | §4 manual/conflict rebase failed, no landing proof after shared recovery classification |
| `rebase-did-not-unblock-merge` | HumanParked | §4 rebase completed, still conflicts |
| `rebase-failure-circuit-breaker` | HumanParked | §4 repeated rebase failures, no progress |
| `branch-already-rebased-lineage-incomplete` | needs_discussion | §4 branch contains target tip, lineage unresolved |
| `stale-review-needs-manual-refresh` | needs_discussion | §5 code-changing stale review requires a manual refresh or manual resolution review, `advance_create_reviews` off |
| `review-freshness-unverified` | needs_discussion | §5 live branch-head probe failed while checking whether a code-changing event made the latest completed review stale |
| `resolution-review-metadata-invalid` | needs_discussion | §5 required resolution-review metadata is still missing, malformed, or inconsistent after live SHA re-derivation |
| `closing-review-needs-manual-refresh` † | needs_discussion | §6/§8 closing-review requirement, manual refresh |
| `verify-budget-exceeded` | needs_discussion | §5a verify gate timed out with no failed phase in persisted phase diagnostics |
| `verify-failed-needs-fix` | needs_discussion | §5a verify gate is red before review can proceed, but lifecycle cannot safely create/continue the current `verify_fix` lane |
| `verify-fix-failed` | needs_discussion | §5a current verify gate is still red after one completed same-epoch `verify_fix` |
| `verify-unavailable` | needs_discussion | §5a verify gate is unavailable and lifecycle cannot safely route through `verify_fix` |
| `verify-unavailable-after-fix` | needs_discussion | §5a verify gate remains unavailable after one completed same-epoch `verify_fix` |
| `verify-blocked-no-code-issues` | needs_discussion | §6 legacy compatibility park for timeout-only verify-coupled reviews |
| `improve-no-op` | needs_discussion | §6 consecutive no-op improves ≥ bound after adjudication/compatibility handling is exhausted |
| `review-blocker-adjudication-needed` | needs_discussion | §6 adjudication for a disputed non-verify CODE blocker returned `NEEDS_HUMAN`, failed, or could not be parsed safely |
| `duplicate-blocker-no-progress` | needs_discussion | §6 same primary blocker repeats across cycles |
| `review-max-cycles-reached` | max_cycles_reached | §6 merge-unit review→improve cycles since the current scope/base boundary ≥ `max_review_cycles` with no stale-review refresh available under current default `on_max_cycles=park`; opt-in `merge_and_defer` candidates first prove local merge source and readable deterministic blocker content, then traverse pre-merge verify gating and emit annotated `merge` only after green verify plus validated persisted blocker metadata |
| `review-max-cycles-review-content-unavailable` | needs_discussion | §6 `merge_and_defer` candidate review content is missing, blank, unreadable, or undecodable, so deferred blocker tasks cannot preserve authoritative review text |
| `review-max-cycles-review-content-invalid` | needs_discussion | §6 `merge_and_defer` candidate review content is readable but parsed blocker identity, blocker summary, or capped-review validation is nondeterministic or unsafe |
| `review-verdict-needs-manual-attention` | needs_discussion | §6 verdict unclassifiable, or `APPROVED_WITH_FOLLOWUPS` with zero parsed follow-ups |
| `review-needs-manual-creation` | needs_discussion | §8 implementation-owned lineage requires review, no review exists, `advance_create_reviews` off |
| `main-integration-verify-red` | needs_discussion | §8 local target verify failed after target HEAD changed; halt further merges until it is green again |
| `main-integration-verify-launch-failed` | needs_discussion | §8 local target verify tool could not launch because the environment is misconfigured; surface operator attention without marking main red, halting merges, or creating code remediation |
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
(`watch-no-progress-backstop`), `retry-limit` (`retry-limit-reached`), `reconcile`
(`reconcile-needs-manual-resolution`), or `verify-fix-failed`
(`verify-fix-failed`).

- For `backstop` and `reconcile`, it MUST clear only the watch-owned exclusion state that
  kept the owner out of normal watch/advance pickup.
- For `retry-limit`, it MUST record one durable manual-rearm epoch for the parked subject
  and reason so the next shared recovery evaluation measures retry budget from that epoch
  instead of lifetime history.
- For `verify-fix-failed`, it MUST record one durable manual-rearm epoch for the owner so
  the next shared lifecycle evaluation reruns the verify gate at the current head before
  considering the completed same-epoch `verify_fix` terminal again.
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
