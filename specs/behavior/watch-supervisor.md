# Watch supervisor — runtime contract

> **Status: Draft.** This document is the prescriptive contract for the runtime loop that
> drives the lifecycle engine. It is written as the intended north-star behavior for
> `gza watch`; code may still lag parts of it pending conformance work.
>
> Read [00-overview.md](00-overview.md) for the lifecycle state machine and
> [lifecycle-engine.md](lifecycle-engine.md) for the pure per-work-unit decision
> function. This document owns the *loop that drives those decisions*: cycle cadence,
> worker accounting, restart/adoption, and process-level ordering.

## What this models

gza has two distinct layers that MUST NOT be conflated:

- The **lifecycle engine** decides, for one unresolved work unit, what the next action is
  (`merge`, `create_review`, `needs_rebase`, `wait`, `needs_discussion`, and so on).
- The **watch supervisor** decides when to run those decisions, how many workers may run
  at once, what order cycle phases execute in, and how a long-running watch process
  survives interruption, restart, and installed-code drift.

Worktree acquisition and reclaim is **not** owned here: it is a step of task start performed
by the task runner for every caller (watch, manual, inline, recovery), specified in
[worktree-reclaim.md](worktree-reclaim.md).

This document answers questions the engine spec intentionally does not:

- What is one watch cycle?
- How is available concurrency computed?
- When MUST watch wait instead of spawning?
- How does a restarted watch adopt already-running detached workers?
- When does watch re-exec itself to load new code?
- When does watch stop, back off, or require a human?

## Principles this layer must satisfy

- **S1 — The supervisor drives; the engine decides.** The supervisor MUST reuse the
  shared lifecycle/recovery decision machinery. It MUST NOT fork command-specific
  transition rules for `watch`.
- **S2 — Process-level idempotency.** Re-running or restarting `watch` with no external
  state change MUST NOT double-spawn workers, duplicate recovery, or double-merge work.
- **S3 — Interruptible and restart-safe.** `watch` MUST be safe to stop and restart at any
  cycle boundary. Detached workers MUST continue independently, and the next `watch`
  process MUST adopt them instead of respawning equivalent work.
- **S4 — Land fresh code first.** Within a cycle, direct landing work MUST execute before
  new worker spawns, so later worker starts evaluate against the freshest landed target.
- **S5 — Scope is explicit.** When tag filters are active, watch MUST act only on
  in-scope work: merges, recovery, new starts, queue pickup, and operator summaries.
- **S6 — Human-required states are standing operator signals.** Repeated failures,
  backoff, drift restart, idle exit, and human-needed parked states MUST surface explicit
  operator signals. For every watch cycle, `watch` MUST emit an operator-visible `Needs
  attention` signal for every in-scope **lineage owner / merge unit** that contains a
  failed task whose shared recovery decision parks it for human intervention. The failed
  leaf ID is detail within that owner's signal, never a separate top-level entry. This
  set, compared by **owner / merge-unit ID**, MUST be identical to the set surfaced by
  `gza incomplete` from the same shared failed-task recovery computation for the same
  store and tag filters. `--restart-failed` and `--show-skipped` MUST NOT control whether
  these human-required owners are visible. No failure reason, empty-branch state,
  landed-lineage state, or lack of an in-session status transition may remove a
  human-required owner from this surface. Watch MUST NOT silently stall.
- **S7 — Watch owns bounded stateful work creation.** `watch` is not only a runner of
  existing actions; it is the top-level stateful executor that MAY create new work, but
  only for explicitly bounded supervisor-owned surfaces. Those surfaces currently include
  lifecycle follow-on materialization and local-target verify remediation. When watch
  creates such work, it MUST do so through deduped supervisor rules, not ad hoc per-pass
  task creation.

## Core invariants

### 1. One cycle has a fixed order

Each watch cycle MUST execute these phases in order:

1. **Reconcile runtime state.** Reap or reconcile stale in-progress state, then discover
   live detached workers and live in-progress tasks.
2. **Compute capacity.** Derive current `running` and `slots`.
3. **Evaluate direct lifecycle work first.** Execute merge-ready and every other
   actionable non-worker lifecycle action selected by the shared lifecycle gate before
   spawning any new workers (S4). This includes direct follow-on creation such as
   approved-plan materialization; watch MUST NOT maintain a watch-only allowlist that can
   diverge from `advance`. When a watch-managed merge action succeeds, watch MUST emit
   exactly one structured
   `MERGE <owner-task-id> -> <target>` line for the landed merge unit at merge time,
   before any same-cycle worker starts, queue pickup, or informational summary output
   that follows from the fresher target-branch state. The logged task ID is the
   merge-unit owner/leader, not every credited member. During this same phase, watch MUST
   reuse the shared local-target integration verify gate: when the canonical local target's
   fingerprint differs from the last verified fingerprint, when the configured verify-gate
   identity changes on the same tree, and after each successful merge onto that target,
   watch MUST rerun the configured verify gate against the local target tree. Freshness is
   keyed at least by the normalized `verify_command` plus gate-enabled/no-gate state, and
   the current implementation also includes the resolved automation timeout settings.
   Independently of tree change, a configured-gate checkpoint that is not `passed` MUST
   expire after a bounded configured TTL and be rerun on that cadence so red/unavailable
   results cannot persist indefinitely on an unchanged target tree. If the current
   default-branch checkout cannot produce an exact tree fingerprint before or after that
   verify, watch MUST treat freshness as unproven instead of reusing `HEAD` equality
   alone; it MUST halt further merges for the current cycle and surface one visible
   durable attention row explaining that exact-tree freshness could not be proven. More
   generally, if that verify is not `passed`, watch MUST first perform the bounded
   rerun-before-halt sequence owned by
   [main-verify-self-heal.md](main-verify-self-heal.md). A flaky red that turns green in
   that sequence MUST clear the halt for the current cycle and MUST create or reuse
   exactly one open de-flake remediation task for that failure identity. A deterministic
   red that stays red across the full bounded sequence MUST halt further merges for the
   current cycle, MUST create or reuse exactly one open fix-remediation task for that
   failure identity, and MUST emit one visible durable attention row with reason
   `main-integration-verify-red` naming the failing target SHA and, when structured phase
   output exists, the failing phase. The convergence requirements for how that red state
   self-heals or escalates over time are owned by
   [main-verify-self-heal.md](main-verify-self-heal.md). If no `verify_command` is
   configured for the project, that is an explicit no-gate
   exception: watch MAY record an `unavailable` checkpoint with
   `exit_status="not configured"` but MUST NOT halt merges or emit red-main attention for it.
   For this supervisor-owned remediation lane, dedup is by failure identity: normalized
   failure signature only. The exact local-target tree fingerprint from bounded rerun
   evidence remains prompt context, but watch MUST reuse one open remediation task for
   that signature even when the fingerprint changes, becomes available later, or is
   unavailable on one observation. Reusing an existing open remediation task MUST still
   bump it to the front of the runnable queue. If the current bounded rerun evidence
   changes the required remediation kind for that same identity (for example `deflake`
   to `fix`) or improves the fingerprint/evidence context, watch MUST rewrite the reused
   task so its prompt and purpose match the current classification before queue-bumping
   it. Reused or newly created remediation tasks in this lane MUST also carry the distinctive tag
   `system-main-verify` in addition to `system` and inherited watch scope tags.
   `advance` MAY surface the red-main condition from the shared state, but it MUST NOT
   create these remediation tasks itself.
4. **Blind parked auto-rearm phase.** After the direct non-worker lifecycle phase has
   reconciled the freshest target state for this cycle, watch MAY run one conservative
   parked-owner auto-rearm pass before any worker dispatch. This phase MUST stay
   supervisor-owned and MUST reuse the shared parked clear service; it MUST NOT create a
   judge task, inspect per-merge relevance, or fork a second lifecycle policy. For each
   currently parked subject/reason candidate, watch MUST apply these gates in order:
   feature enabled, budget remaining, cooldown elapsed, and target branch advanced when
   `watch.parked_auto_rearm.require_target_advanced` is true. A failed gate MUST leave the
   parked row untouched and MUST NOT spend an attempt. In particular, an unchanged target
   SHA under `require_target_advanced` spends no attempt and performs no clear. A
   successful blind auto-rearm MUST clear only the shared parked exclusion state, persist
   the current target SHA plus attempt timestamp, increment the per-subject/per-reason
   blind auto-attempt count, and then return the owner to the same cycle's ordinary watch
   planning. Slot use is unchanged: the rearm phase itself consumes no worker slot, and any
   follow-on recovery or lifecycle work spawned because of that clear MUST reuse the same
   remaining worker-slot accounting as every other same-cycle dispatch. Cooldown identity
   is subject plus parked reason, so a burst of watch cycles or merges inside one cooldown
   window yields at most one blind auto-rearm attempt for that pair.
5. **Spend slots on worker-consuming actions.** Use remaining capacity for recovery and
   lifecycle worker starts selected by the shared engine. Recovery allocation is not a
   pending leftover: the supervisor MUST reserve worker-consuming recovery capacity before
   pending pickup, and `--recovery-only` MUST gate pending pickup entirely while any
   actionable in-scope recovery remains, even if that recovery action is direct and does
   not consume a worker slot.
   Before pending pickup begins, the supervisor MUST examine pending work in the same
   priority order the pickup lane would use if quiet-period holds were ignored. If the
   first otherwise-pickable pending task is currently held only by the quiet-period
   policy, watch MUST emit at most one operator-visible `SKIP` for that hold window and
   MUST NOT emit separate quiet `SKIP` lines for lower-priority quiet tasks in the same
   cycle. The dedupe identity for that quiet `SKIP` MUST include the task and its
   current hold-until time so a later meaningful edit that moves the quiet window causes
   exactly one new `SKIP`.
6. **Observe outcomes.** Emit operator-visible events for starts, merges, waits, skips,
   parked states, recovery decisions, and failures. Snapshot-based transition detection
   remains responsible for repaired or otherwise out-of-band merge transitions, but it
   MUST emit at most one `MERGE` line per merge unit per cycle and MUST NOT duplicate a
   `MERGE` line that was already emitted inline for the same merge unit owner when the
   direct merge action landed. A `START` event MUST be emitted only once the launched
   task reaches `in_progress` or a live worker is confirmed under the same live-running
   accounting used for supervisor capacity, never merely because a spawn call returned
   success. Recovery launches first registered during a cycle MUST get that cycle's
   end-of-cycle observation and, if still unconfirmed, the next cycle's start-of-cycle
   observation before watch declares a no-show. A launch that never reaches
   `in_progress` within that window and remains pending/non-live MUST surface an explicit
   operator warning rather than a clean `START`; terminal outcomes observed before then
   stand on their own and MUST NOT also emit a contradictory no-show warning. This is
   required by invariant S6's outcome-over-launch rule.
7. **Decide the next boundary.** Stop, back off, re-exec, idle-exit, or sleep until the
   next poll interval.

The supervisor MUST NOT reorder these phases in a way that can cause older target-branch
state to win over already-mergeable fresh code.

### 2. Cadence and sleep are policy, but cycle boundaries are real

- Before phase 1 begins, watch MAY evaluate supervisor-owned system preconditions required
  to start task work for that cycle.
- When `use_docker` is false, watch MUST treat Docker as not required for that precondition
  check and MUST NOT probe Docker readiness before continuing.
- When Docker is required and unavailable after waiting up to the configured startup/wake
  budget, watch MUST emit a visible `HOLD` signal, start no tasks, fail no tasks, skip
  the rest of the cycle entirely, and sleep interruptibly until the next poll boundary.
- Independently of Docker readiness, before dispatching or executing task work for a
  cycle, watch MUST run a host-side git worktree health probe against the project
  checkout. If that probe fails, watch MUST halt dispatch/execution for the cycle, MUST
  NOT call into lifecycle planning or worker start paths for that pass, MUST NOT mark
  runnable tasks failed, MUST NOT create recovery children, and MUST NOT spend worker
  slots while the halt is active.
- A git-health halt MUST surface exactly one visible durable attention row with reason
  `git-worktree-health-red` describing the probe failure in compact operator-facing form.
  The durable state for that alert MUST retain both the compact alert text and the full
  raw probe failure text so the current red condition can be surfaced without rerunning
  the failing path solely for display.
- Watch MUST rerun that git-health probe on later passes and MUST resume automatically
  once the probe succeeds again. When health is restored after one or more halted passes,
  watch MUST emit a visible `RESUME` signal before proceeding with the next normal cycle.
- While held for this system precondition, watch MUST NOT mutate failure-backoff or
  failure-halt state, idle accounting, transition snapshots, or any per-task recovery
  state derived from a normal cycle. A held pass is not a partial cycle.
- When the required precondition becomes available again after one or more held passes,
  watch MUST emit a visible `RESUME` signal and then proceed with the next normal cycle.
- `watch.poll` / `--poll` define the steady-state delay between completed cycles.
- The supervisor MUST sleep only *between* cycles, never in the middle of a partially
  evaluated cycle.
- `watch.max_idle` / `--max-idle` bound consecutive idle supervisor time. When reached,
  watch MUST exit cleanly rather than spin forever doing no work.
- `watch.max_iterations` / `--max-iterations` are **not** a supervisor loop bound. They
  bound iterate workers launched for detached iterate chains. Watch MUST pass that budget
  to those workers, whether the chain is driving implementation review/improve work or
  plan-review/plan-improve work, but MUST NOT treat it as "run only N watch cycles."

### 2A. Per-cycle human-required parity belongs to phase 5

- During phase 5 ("Observe outcomes"), the supervisor MUST recompute the in-scope
  human-required failed-task set on **every** cycle from the same shared failed-task
  recovery policy that powers `gza incomplete`, including the already-landed suppression
  rule in [recovery.md](recovery.md) R5 and the owner/merge-unit visibility rules in
  [lineage.md](lineage.md) P1 and P4.
- When that shared recovery policy returns a failed-task decision that parks the owner for
  human intervention, phase 5 MUST emit `Needs attention` for that owner even when the
  decision is represented internally as a `skip`.
- `--restart-failed` and `--show-skipped` MAY affect which non-attention recovery
  diagnostics are printed, but they MUST NOT gate the visibility of the human-required
  owner set defined by S6.
- Human-required parity is owner-based: the compared set is the set of lineage-owner /
  merge-unit IDs, and the failed leaf ID MUST appear only as detail within the owner's
  signal.
- Non-human skips and hidden recovery decisions MAY remain silent or appear only in
  ordinary skipped diagnostics, as the shared recovery policy requires.

### 3. Concurrency uses live-slot accounting

The batch limit means "maintain at most N concurrent detached worker processes," not
"spawn N workers per cycle."

- `running` MUST count only task-executing live detached workers, including
  detached-session workers that outlive the current watch process after the owned task
  has reached `in_progress` or another equivalent confirmed execution state.
- Live-worker accounting MUST consider both the worker registry and persisted in-progress
  task state. Either source alone is insufficient after crashes or restarts.
- Stale or dead worker state MUST be reconciled before capacity is computed.
- Reconciliation MUST cover both `in_progress` tasks and `pending` tasks that are
  explicitly associated with a registered running-status worker entry. A plain pending
  queue item with no registered worker remains runnable and MUST NOT be reaped just
  because it has no process metadata.
- When reconciliation classifies a task as `WORKER_DIED`, it MUST persist a structured
  worker-death breadcrumb on the task's ops stream before or with terminalization. That
  breadcrumb MUST capture the best available exit evidence (`exit_code`, terminating
  signal when derivable, worker stage, and a short stdout/stderr tail), and MAY add a
  clearly-labelled platform hint (for example, Darwin sleep/jetsam context) when
  available. This capture is best-effort and MUST NOT itself crash reconciliation.
- Watch capacity and sleep-slot accounting MUST exclude a `pending` task whose worker is
  merely registered/alive but has not yet reached task-executing state. That worker is
  still live evidence and MUST surface separately as startup/starting capacity detail
  rather than inflating `running`, `running_task_ids`, or unavailable-slot math.
- Query and triage surfaces that render runtime state from that reconciliation (including
  `gza ps`) MUST still treat a `pending` task with a registered `running` worker as live
  in-flight startup work even when the task row has not yet stamped `running_pid` for the
  main iterate loop. They MUST derive `stale` from reconciled worker liveness, not from
  the task row's empty `running_pid` alone.
- The effective watch worker target for a pass MUST be `min(batch, max_concurrent)`.
  When `max_concurrent` is unset, `gza watch` MUST derive the runtime cap from the
  effective watch batch for that run, including any CLI `--batch` override.
- `slots` MUST equal `max(0, min(batch, max_concurrent) - running)`.
- If the requested `batch` exceeds an explicit `max_concurrent`, watch MUST emit one
  startup warning that the requested batch was capped by the global ceiling.
- `watch.recovery_slots` (default `1`) MUST reserve that many worker slots per cycle for
  actionable failed-task recovery before pending pickup, capped by available slots and
  actionable in-scope worker-consuming recovery count, when pending pickup is enabled.
- The rule is uniform for worker-consuming recovery. There is no separate batch-1 policy:
  with batch 1 and the default `watch.recovery_slots = 1`, plain watch gives the single
  slot to worker-consuming recovery until that lane drains. `--pending-only` is the
  operator escape hatch for single-slot pending-only behavior, and `--recovery-only` is
  the `recovery_slots = batch` extreme that also suppresses pending pickup while direct
  actionable recovery remains.
- Explicit merge-unit scope disables pending pickup entirely, so worker-consuming
  in-scope recovery MUST be able to use all available slots in that scoped pass unless
  the operator explicitly selected `--pending-only`; config/default
  `watch.recovery_slots: 0` MUST NOT suppress scoped recovery on its own.
- Eligibility remains owned by the shared recovery engine. The supervisor MUST use the
  same `decide_failed_task_recovery(...)` policy regardless of watch mode and MUST NOT
  invent a separate recovery-only eligibility predicate. Recovery-only lane gating may
  still depend on the presence of any actionable recovery, including direct reconcile
  actions that do not consume worker slots.
- Only worker-consuming actions spend a slot. Direct actions such as merge,
  merge-with-followups, scope evaluation, re-exec decisions, and attention emission MUST
  NOT consume slots.
- One detached iterate chain occupies one slot for as long as its worker process remains
  live, even though that worker may drive several engine steps internally.

### 4. In-progress work causes wait, not respawn

This is the process-level expression of overview invariant 1.

- If the needed work for a lineage already exists as `pending` or `in_progress`, watch
  MUST wait/adopt that work rather than create another child for the same step.
- While such a `pending` task is backed by a reconciled live registered worker, operator
  runtime surfaces MUST present it as waiting/live startup work rather than `stale`.
  For watch's slot accounting and SLEEP/WAKE summaries, that startup work is a separate
  live `starting` bucket, not a `running` task slot.
- A `pending` task with a registered worker that is dead/stale and also carries concrete
  exit/startup-abort evidence (for example a detached-exit lifecycle event or nonzero
  exit code) is not live existing work. Watch MUST reconcile it to `WORKER_DIED`,
  persist the task-visible worker-death breadcrumb, and surface the captured startup/exit
  evidence before treating the lineage as something to wait on or adopt.
- A `pending` task with a registered worker that is dead/stale but has no worker-death
  exit evidence and is silent past `watch.no_activity_timeout` is not live existing work.
  Watch MUST reconcile that residue to a terminal failure (`NO_ACTIVITY`) before
  treating the lineage as something to wait on or adopt.
- A worker that dies after provider preflight but before the normal `worker_lifecycle/start`
  registration breadcrumb MUST still leave a `worker_lifecycle` abort/death event on the
  task-visible ops stream identifying that earlier stage.
- If a worker is already live for the lineage an iterate start would own, watch MUST NOT
  start a second iterate worker for that same lineage, whether the detached chain is an
  implementation chain or a plan chain.
- Re-running watch after a crash, operator restart, or code re-exec MUST NOT treat
  detached workers as lost merely because the old parent process exited.

### 5. Restarted watch adopts live workers

Watch workers are detached on purpose. A restarted supervisor MUST adopt them.

- On startup and on each cycle, watch MUST reconcile in-progress state and collect live
  running state from detached-worker metadata plus persisted task state.
- If a live worker is still driving a task or lineage that remains in scope, the
  supervisor MUST treat that work as already running and reduce available slots
  accordingly.
- Adoption MUST happen before any new worker selection for the cycle.
- Watch MUST NOT require a "drain everything, then restart" gate to stay correct.

### 6. Installed-code drift triggers re-exec at the next cycle boundary

When the installed `gza` package fingerprint changes while watch is running:

- Watch MUST detect the drift and mark a pending self-restart.
- With automatic drift restart enabled, watch MUST re-exec at the **next cycle
  boundary**, regardless of current running-worker count, pending-work count, or whether
  the queue is idle.
- The contract MUST NOT require a drain-first or "only when no workers are active" gate.
- Detached workers survive supervisor process re-exec; the restarted watch MUST adopt
  them under invariant 5.
- When automatic drift restart is disabled, watch MUST still surface the drift to the
  operator and MUST NOT pretend the old process loaded the new code.

### 7. Failure backoff is bounded and visible

- This lane is distinct from the system-precondition hold/resume path in section 2.
  Required-Docker unavailability is a supervisor hold condition, not a task failure.
- Newly observed failures that the shared recovery policy does not auto-resume/retry MUST
  increment the failure streak for that failing lineage owner / merge unit, not a single
  process-global streak.
- The configured exponential backoff policy (`watch.failure_backoff_initial`,
  `watch.failure_backoff_max`) MUST quarantine only the failing owner. Backoff on owner A
  MUST NOT block dispatch of runnable work from owners B, C, and so on in the same or a
  later cycle.
- `watch.failure_halt_after` MUST be keyed to fleet-wide failure, not repeated failures on
  one owner alone. A single poisoned owner MAY keep its own escalating streak/backoff
  without halting the whole watch process.
- A nonzero per-owner failure streak and each quarantine/halt decision MUST be
  operator-visible and MUST name the affected owner.
- Watch MUST reuse the shared bounded recovery policy; it MUST NOT invent a different
  resume/retry/manual boundary from `advance` or `iterate`.

### 8. Restart-safe no-progress loops must park instead of respawn forever

- Watch MUST persist a no-progress observation for each repeated worker-launch or
  recovery-launch candidate, keyed by the subject merge unit when one exists or otherwise
  by the subject lineage, plus the selected action type and reason. This MUST cover **every**
  path that launches work for a subject, including the **pending-queue worker dispatch** —
  not only `advance`-derived actions and failed-task recovery launches.
- The persisted observation MUST include enough evidence to distinguish durable progress
  from a true no-op repeat: merge-unit identity/state/head, selected action type/reason,
  action task ID, relevant failed/recovery task ID, and current task status.
- Watch MUST increment the no-progress streak only after it actually **executes** the
  selected action and the resulting post-execution evidence is unchanged for the same
  subject/action. Merely evaluating a candidate, failing to reserve capacity, being denied
  launch, skipping the action, or finding an already-running/in-flight child MUST NOT
  increment the streak. Restarting watch MUST NOT reset a streak created by actual executed
  no-progress outcomes.
- After selecting a worker-consuming action, watch MUST wait only a small bounded
  `watch.slot_settle_seconds` window for the chosen task to prove execution. A live
  running state counts, and a live registered worker counts, including the legitimate
  preloop case where the task row is still `pending`. A task that already reaches an
  observable terminal outcome inside that same bounded window also counts as executed
  work rather than an undispatched launch no-show. Only when neither live-running proof
  nor an observable post-launch terminal outcome appears in the window may watch log the
  action as undispatched, skip no-progress accounting for that attempted launch, and
  continue scanning the same cycle for another runnable candidate instead of leaving the
  slot idle.
- When the latest relevant failed recovery or improve attempt for that selected
  subject/action is a **transient terminal** (for example provider-capacity,
  infrastructure/setup failure before durable work such as `WORKSPACE_NOT_POPULATED`,
  infrastructure/worker death before durable work, or timeout before meaningful execution),
  watch MUST NOT increment the no-progress streak for that cycle. Instead it MUST preserve
  the last real no-progress streak unchanged, clear any deferred launch marker for the
  observed attempt, and persist or update per-subject transient recovery cooldown state for
  that same subject/action pair.
- Progress MUST be measured by **outcome**, not by the act of launching. Starting (or
  re-starting) a worker for a subject is NOT, by itself, durable progress. A worker launch
  that leaves the subject in the same state — no task status transition, no branch-head
  change, no merge-unit state change, no recovery-edge creation — is a no-op repeat and MUST
  advance the streak, not reset it.
- A completed no-op attempt is **not** transient for this rule. If a completed improve,
  rebase, or other launched action reaches a durable terminal with unchanged evidence
  (for example completed improve with `changed_diff = false`), it MUST still count toward
  the normal no-progress parking threshold.
- Dead prepared recovery workers are a distinct primary failure signal, not merely
  "no-progress" evidence. When watch can prove a detached worker for a pending recovery row
  died before claiming it, reconciliation MUST terminalize that row as a failed recovery
  descendant before the next recovery decision is computed. Watch MUST NOT keep reusing the
  same dead pending recovery row forever and rely on the no-progress backstop as the
  primary stop condition.
- Watch MUST reset the streak only when durable progress actually occurs: a newly created
  task, a task status transition, a recovery edge creation, a review/improve/rebase
  completion, a branch-head change, a merge-unit state change, or a different selected
  action/reason. Merely launching a worker for the same task in the same state MUST NOT reset
  it.
- Re-invoking the **same command on the same task is permitted when the task's state has
  changed** since the prior cycle — e.g. a prior `iterate` worker was killed, leaving the
  task reclaimable, so the next cycle legitimately re-invokes `iterate` (possibly with resume
  or retry). The backstop suppresses only repeats where the task is in the **exact same
  state** as the prior observed cycle.
- Persisted no-progress parks MUST be lifted once their basis no longer holds. In
  particular, watch MUST clear parks for never-started pending launches and for stale
  residue rows whose parked merge-unit subject is no longer in an active unresolved state.
- When the streak reaches `watch.no_progress_cycles`, watch MUST park the subject with a
  shared needs-attention reason of `watch-no-progress-backstop` and MUST stop respawning
  that unchanged no-op automatically.

### 8a. Human-needed parked lineages surface once, then stay parked

- When the shared lifecycle engine returns a human-needed parked action for a lineage,
  watch MUST emit an operator-visible `ATTENTION` event for that parked state once, then
  treat that lineage as parked on later cycles until persisted lineage state changes.
- Watch MUST consume the engine's shared parked-reason taxonomy rather than maintain a
  separate hand-curated allowlist of parked slugs.
- Watch MUST NOT start a new iterate worker for a lineage whose latest shared action is
  already one of those parked human-needed states.
- `uv run gza unstick` is the manual operator escape hatch for parked owner states such as
  `watch-no-progress-backstop`, `retry-limit-reached`, and
  `reconcile-needs-manual-resolution`.
- Plain `uv run gza unstick` MUST clear only the watch-owned exclusion state for the
  selected owner/subject and MUST NOT itself start work.
- `uv run gza unstick --run` MAY immediately dispatch only the owners it just cleared, but
  it MUST do so by reusing the same scoped watch dispatch path, slot accounting, and
  launch-permit rules instead of inventing a second lifecycle engine or bypassing shared
  capacity. If no slots are available, it MUST still clear state and report zero starts.
- After the clear-only or clear-plus-run pass, the next shared owner-row evaluation by
  `watch` or `advance` decides whether the owner is actionable again or still parked for
  the same underlying reason.
- `uv run gza unstick` MUST require an explicit selector (`task-id`, `--tag`, `--reason`,
  or `--all`) so an operator cannot accidentally clear every parked owner in the project.
- For the no-progress backstop, the command MAY discover parked owners either from the
  current shared owner-row action or from persisted parked watch-progress observations so
  stale operator-visible residue can still be selected and cleared after the current row
  shape changed. Stale persisted backstop rows whose basis no longer holds remain subject
  to the existing stale-reconciliation rule before manual selection.

### 9. Tag scope is a hard boundary

- `watch --tag ...` MUST only act on work that matches the requested scope.
- Out-of-scope work MUST NOT consume watch slots, be merged, be resumed/retried, or be
  selected from the pending queue by that watch process.
- When a scoped watch can detect that an in-scope lineage owner is blocked by a pending,
  runnable, or already-running derived child that does not match the active tag filter,
  watch MUST surface that blocker in operator-facing attention output without starting,
  resuming, retrying, merging, or reordering the out-of-scope child.
- That scoped blocker signal exists to surface **real in-scope stalls**, not intentional
  future-scope planning. If the owner's own deliverable is already terminal and the
  out-of-scope child has at least one explicit scope tag of its own, watch MUST suppress
  that attention unless some in-scope unfinished member is still genuinely blocked on
  that child. An untagged/scope-less child remains a surfaced orphan until it is given a
  scope.
- Scope banners, wake summaries, and attention output SHOULD make the active scope
  explicit so operators can tell when watch is intentionally ignoring other work.
- `watch <task-id>...` MUST normalize each supplied ID to the canonical lineage /
  merge-unit owner before the loop starts, then use those owner IDs as the scope for all
  cycle planning.
- Explicit merge-unit scope is mutually exclusive with tag scope. The supervisor MUST
  fail closed rather than AND-combine named owners with `--tag` / `--all-tags`.
- Explicit merge-unit scope MUST disable global pending pickup and the global failed-task
  recovery lane. In-scope failed members may still be recovered through their scoped owner
  rows; unrelated failed and pending tasks MUST NOT be selected, reported as actionable
  work, or counted as keeping the scoped watch alive.
- `--restart-failed` is incompatible with explicit merge-unit scope because scoped mode
  has no global recovery-priority lane. The supervisor MUST reject that combination.
- A scoped watch MUST exit once every named owner unit is terminal or parked with no
  automatic advance path. Ambiguous idle states may still use `--max-idle` as a backstop,
  but unrelated global pending or failed work MUST NOT prevent scoped exit.

### 10. Stop signals stop the supervisor, not the detached workers

- On `SIGINT` or `SIGTERM`, watch MUST stop the supervisor loop cleanly at the next safe
  boundary and return a signal-derived exit status.
- Watch MUST NOT kill detached child workers merely because the supervisor is stopping.
- A second interrupt MAY short-circuit a long sleep or long pass so the operator can
  regain control promptly, but it MUST NOT convert normal shutdown into "kill every
  worker."

## What watch does not do

These are exclusions in the contract, not omissions in the current implementation.

- Watch MUST NOT define its own lifecycle transition rules; that belongs to
  [lifecycle-engine.md](lifecycle-engine.md).
- Watch MUST NOT create task goals or budget policy beyond the queued and lineage-derived
  work already in scope.
- Watch MUST NOT require daemonization, PID files, or an internal multi-threaded worker
  pool to satisfy this contract. Detached external workers are sufficient.
- Watch MUST NOT rely on an internal parallel executor pool; its concurrency model is
  detached worker processes plus supervisor polling.
- Watch MUST NOT kill, reset, or discard code work solely to make the loop progress.
- Watch MUST NOT widen scope past explicit tag filters.

## Policy knobs this layer owns

The existence of these knobs is contract; their values are operator policy.

| Knob | Governs |
|------|---------|
| `watch.batch` | Maximum concurrent detached worker processes the supervisor maintains |
| `max_concurrent` | Global hard ceiling applied to detached worker launches across commands; watch clamps batch to this when explicit |
| `watch.poll` | Delay between completed cycles |
| `watch.max_idle` | Consecutive idle loop time before clean exit |
| `watch.max_iterations` | Iterate-worker loop cap for implementation chains launched by watch |
| `watch.recovery_slots` | Slots per cycle reserved for worker-consuming failed-task recovery before pending pickup |
| `watch.failure_backoff_initial` / `watch.failure_backoff_max` | Exponential cooldown after non-auto-resumable failures |
| `watch.failure_halt_after` | Failure streak threshold that stops watch for human intervention |
| `watch.transient_recovery_backoff_max` | Maximum persisted cooldown for transient failed recovery/improve retries |
| `watch.no_progress_cycles` | Repeated unchanged watch-action cycles before the supervisor parks the subject with `watch-no-progress-backstop` |
| `watch.slot_settle_seconds` | Bounded wait for selected work to prove live execution, either by a running worker or by an observable post-launch terminal outcome, before watch treats the dispatch as undispatched and moves on |
| `watch.no_activity_timeout` | Reconciliation threshold for deciding a registered worker for a pending or in-progress task has gone silent and must be failed/reconciled |
| `--tag` / `--all-tags` | Supervisor execution scope (`--tag` matches any requested tag by default; `--all-tags` requires all of them) |
| `--[no-]auto-restart-on-drift` | Whether installed-code drift triggers automatic re-exec at the next cycle boundary |

Deprecated compatibility aliases remain accepted for now: `--restart-failed` maps to
`--recovery-only`, `--restart-failed-batch` maps to `--recovery-slots`, and
`watch.restart_failed_batch` maps to `watch.recovery_slots`. Unchanged existing recovery
descendants may still surface an already-persisted parked state, but merely re-evaluating
`recovery_already_pending` or `recovery_already_running` without re-executing a recovery
action MUST NOT create new no-progress ticks.

## Boundary with the engine

- The engine spec owns **what next action a work unit needs**.
- This supervisor spec owns **when that action runs, whether it consumes a slot, and
  whether the current watch process waits, restarts, or exits**.

Any rule that depends on cycle order, slot accounting, detached-process adoption, or
watch-process restart belongs here even if it influences lifecycle outcomes. Any rule that
depends only on the state of one work unit belongs in
[lifecycle-engine.md](lifecycle-engine.md).
