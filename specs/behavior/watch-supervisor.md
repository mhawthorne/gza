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
   the current implementation also includes the resolved automation timeout settings. If
   the current default-branch checkout cannot produce an exact tree fingerprint before or
   after that verify, watch MUST treat freshness as unproven instead of reusing `HEAD`
   equality alone; it MUST halt further merges for the current cycle and surface one
   visible durable attention row explaining that exact-tree freshness could not be proven.
   More generally, if that verify is not `passed`, watch MUST halt further merges for the
   current cycle and emit one visible durable attention row with reason
   `main-integration-verify-red` naming the failing target SHA and, when structured phase
   output exists, the failing phase. If no `verify_command` is configured for the project,
   that is an explicit no-gate
   exception: watch MAY record an `unavailable` checkpoint with
   `exit_status="not configured"` but MUST NOT halt merges or emit red-main attention for it.
4. **Spend slots on worker-consuming actions.** Use remaining capacity for recovery and
   lifecycle worker starts selected by the shared engine. Recovery allocation is not a
   pending leftover: the supervisor MUST reserve worker-consuming recovery capacity before
   pending pickup, and `--recovery-only` MUST gate pending pickup entirely while any
   actionable in-scope recovery remains, even if that recovery action is direct and does
   not consume a worker slot.
5. **Observe outcomes.** Emit operator-visible events for starts, merges, waits, skips,
   parked states, recovery decisions, and failures. Snapshot-based transition detection
   remains responsible for repaired or otherwise out-of-band merge transitions, but it
   MUST emit at most one `MERGE` line per merge unit per cycle and MUST NOT duplicate a
   `MERGE` line that was already emitted inline for the same merge unit owner when the
   direct merge action landed.
6. **Decide the next boundary.** Stop, back off, re-exec, idle-exit, or sleep until the
   next poll interval.

The supervisor MUST NOT reorder these phases in a way that can cause older target-branch
state to win over already-mergeable fresh code.

### 2. Cadence and sleep are policy, but cycle boundaries are real

- `watch.poll` / `--poll` define the steady-state delay between completed cycles.
- The supervisor MUST sleep only *between* cycles, never in the middle of a partially
  evaluated cycle.
- `watch.max_idle` / `--max-idle` bound consecutive idle supervisor time. When reached,
  watch MUST exit cleanly rather than spin forever doing no work.
- `watch.max_iterations` / `--max-iterations` are **not** a supervisor loop bound. They
  bound iterate workers launched for implementation chains. Watch MUST pass that budget to
  those workers, but MUST NOT treat it as "run only N watch cycles."

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

- `running` MUST count live detached workers, including detached-session workers that
  outlive the current watch process.
- Live-worker accounting MUST consider both the worker registry and persisted in-progress
  task state. Either source alone is insufficient after crashes or restarts.
- Stale or dead worker state MUST be reconciled before capacity is computed.
- Reconciliation MUST cover both `in_progress` tasks and `pending` tasks that are
  explicitly associated with a registered running-status worker entry. A plain pending
  queue item with no registered worker remains runnable and MUST NOT be reaped just
  because it has no process metadata.
- `slots` MUST equal `max(0, batch - running)`.
- `watch.recovery_slots` (default `1`) MUST reserve that many worker slots per cycle for
  actionable failed-task recovery before pending pickup, capped by available slots and
  actionable in-scope worker-consuming recovery count.
- The rule is uniform for worker-consuming recovery. There is no separate batch-1 policy:
  with batch 1 and the default `watch.recovery_slots = 1`, plain watch gives the single
  slot to worker-consuming recovery until that lane drains. `--pending-only` is the
  operator escape hatch for single-slot pending-only behavior, and `--recovery-only` is
  the `recovery_slots = batch` extreme that also suppresses pending pickup while direct
  actionable recovery remains.
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
- A `pending` task with a registered worker that is dead/stale and silent past
  `watch.no_activity_timeout` is not live existing work. Watch MUST reconcile it to a
  terminal failure (`NO_ACTIVITY`) before treating the lineage as something to wait on
  or adopt.
- If a worker is already live for the implementation lineage an iterate start would own,
  watch MUST NOT start a second iterate worker for that lineage.
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

- Newly observed failures that the shared recovery policy does not auto-resume/retry MUST
  increment the watch failure streak.
- The sleep before starting more work MUST use the configured exponential backoff policy
  (`watch.failure_backoff_initial`, `watch.failure_backoff_max`).
- When `watch.failure_halt_after` is reached, watch MUST stop for human intervention
  instead of continuing to launch more work.
- A nonzero failure streak and each backoff/halt decision MUST be operator-visible.
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
- Watch MUST increment the no-progress streak only when the next cycle selects the same
  subject/action with unchanged evidence. Restarting watch MUST NOT reset that streak.
- Progress MUST be measured by **outcome**, not by the act of launching. Starting (or
  re-starting) a worker for a subject is NOT, by itself, durable progress. A worker launch
  that leaves the subject in the same state — no task status transition, no branch-head
  change, no merge-unit state change, no recovery-edge creation — is a no-op repeat and MUST
  advance the streak, not reset it.
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
- When the streak reaches `watch.no_progress_cycles`, watch MUST park the subject with a
  shared needs-attention reason of `watch-no-progress-backstop` and MUST stop respawning
  that unchanged no-op automatically.

### 9. Tag scope is a hard boundary

- `watch --tag ...` MUST only act on work that matches the requested scope.
- Out-of-scope work MUST NOT consume watch slots, be merged, be resumed/retried, or be
  selected from the pending queue by that watch process.
- When a scoped watch can detect that an in-scope lineage owner is blocked by a pending,
  runnable, or already-running derived child that does not match the active tag filter,
  watch MUST surface that blocker in operator-facing attention output without starting,
  resuming, retrying, merging, or reordering the out-of-scope child.
- Scope banners, wake summaries, and attention output SHOULD make the active scope
  explicit so operators can tell when watch is intentionally ignoring other work.

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
| `watch.poll` | Delay between completed cycles |
| `watch.max_idle` | Consecutive idle loop time before clean exit |
| `watch.max_iterations` | Iterate-worker loop cap for implementation chains launched by watch |
| `watch.recovery_slots` | Slots per cycle reserved for worker-consuming failed-task recovery before pending pickup |
| `watch.failure_backoff_initial` / `watch.failure_backoff_max` | Exponential cooldown after non-auto-resumable failures |
| `watch.failure_halt_after` | Failure streak threshold that stops watch for human intervention |
| `watch.no_progress_cycles` | Repeated unchanged watch-action cycles before the supervisor parks the subject with `watch-no-progress-backstop` |
| `watch.no_activity_timeout` | Reconciliation threshold for deciding a registered worker for a pending or in-progress task has gone silent and must be failed/reconciled |
| `--tag` / `--all-tags` | Supervisor execution scope (`--tag` matches any requested tag by default; `--all-tags` requires all of them) |
| `--[no-]auto-restart-on-drift` | Whether installed-code drift triggers automatic re-exec at the next cycle boundary |

Deprecated compatibility aliases remain accepted for now: `--restart-failed` maps to
`--recovery-only`, `--restart-failed-batch` maps to `--recovery-slots`, and
`watch.restart_failed_batch` maps to `watch.recovery_slots`. Recovery-lane no-progress
parking applies to unchanged existing recovery descendants too: when watch repeatedly
selects the same `recovery_already_pending` or `recovery_already_running` descendant with
unchanged descendant liveness evidence, the shared `watch-no-progress-backstop` MUST park
that failed subject after `watch.no_progress_cycles` so later actionable recovery
candidates can continue.

## Boundary with the engine

- The engine spec owns **what next action a work unit needs**.
- This supervisor spec owns **when that action runs, whether it consumes a slot, and
  whether the current watch process waits, restarts, or exits**.

Any rule that depends on cycle order, slot accounting, detached-process adoption, or
watch-process restart belongs here even if it influences lifecycle outcomes. Any rule that
depends only on the state of one work unit belongs in
[lifecycle-engine.md](lifecycle-engine.md).
