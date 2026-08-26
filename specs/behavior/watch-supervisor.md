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
  In multi-project mode, scope is the ordered set of project selections plus each
  selection's own tag filter. Unkeyed tag flags are allowed only when the invocation
  resolves exactly one execution project.
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
- **S8 — Dry-run logs stay separate.** In single-project mode, each watch invocation MUST
  rotate the selected non-empty log before writing a fresh file: dry-run watch output uses
  `.gza/watch.dry-run.log`; non-dry watch output uses `.gza/watch.log`. A dry run MUST NOT
  mutate the live operational watch log or its archives. Rotation MUST NOT overwrite an
  existing archive; when the current whole-second archive name is already taken, watch
  waits for a distinct rotation timestamp.

## Multi-project supervisor contract

This contract permits a single `gza watch` process to supervise more than one project.
The multi-project shape is a two-level scheduler:

- The **fleet supervisor** owns the ordered selected project set, one aggregate worker
  budget, the poll and idle boundary, cross-project selection strategy, aggregate
  confirmation and dry-run output, installed-code drift re-exec, and the watch lease set.
- A **project runtime** owns exactly one loaded project config, task store, Git context,
  worker registry, detailed project logs, tag filter, lifecycle/recovery state, local
  pickup order, and project-local health. Every mutation or worker launch MUST pass
  through the runtime for the task's owning project.

Multi-project mode MUST remain explicit. The selected set is an ordered list of command
or manifest records keyed by a command-local selector name. Each record resolves to one
execution project plus that project's own tag filter and tag matching mode. The selector
name is part of supervisor identity for logs, summaries, attention keys, backoff maps, and
strategy state, because task IDs and project IDs can collide across unrelated databases.
Project declaration order is the stable tie-breaker and the priority order for strategies
that use priority semantics.

### Scope, tags, and selectors

- A selected project with no keyed tags means all tasks in that project.
- Repeated keyed tags for one selector use the current any-tag matching semantics unless
  that selector is explicitly placed in all-tags mode.
- Unkeyed `--tag` / `--all-tags` semantics are valid whenever the invocation resolves
  exactly one execution project, including an explicit single `--watch-project`
  selection. An invocation that resolves more than one execution project MUST reject an
  unkeyed global tag or global all-tags flag because ownership is ambiguous.
- For a single selected execution project, explicit unkeyed CLI tag policy overrides
  manifest or keyed selector defaults. In particular, a global `--all-tags` flag MUST
  preserve all-tag intersection mode even when the selected manifest or keyed selector
  supplies tags whose default mode is any-tag matching.
- Positional raw task/merge-unit selectors are valid only when the invocation resolves
  exactly one execution project. A multi-project invocation MUST reject positional raw task
  IDs or merge-unit selectors before runtime construction because raw IDs can collide
  across databases. Keyed multi-project task selectors are out of scope until a later
  contract specifies their syntax and validation.
- Duplicate selector names and duplicate resolved `(db_path, project_id)` selections MUST
  fail closed before supervision starts.

### Global phase barrier

A fleet cycle MUST preserve S4 across the entire selected set, not merely within each
project. One multi-project cycle MUST execute these barriers in order:

1. Resolve or revalidate selected projects, refresh changed configs at the cycle boundary,
   and acquire a watch lease before enabling any newly healthy project.
2. Reconcile or observe runtime state independently for every selected project according
   to that runtime state's capabilities. States with validated config and an owned lease
   MAY perform mutating reconciliation as specified below. States without validated
   config or without an owned lease MUST be limited to read-only liveness and occupancy
   observations.
3. Compute aggregate live worker, starting-worker, and unsettled reservation state across
   all selected runtimes, deduplicating by PID where the same process is visible through
   more than one source.
4. Analyze every selected runtime that is enabled for planning with its own config, store,
   Git context, and tag filter.
5. Execute all project-local direct lifecycle work in declared project order before any
   new worker dispatch in any project. A direct action that changes one project, such as a
   merge or main-verify result, invalidates only that project's analysis; the supervisor
   MUST re-plan that project before exposing a worker-dispatch head.
6. Spend aggregate worker slots through the common fleet dispatch loop. The loop asks
   the cross-project strategy to choose between currently eligible per-project head
   candidates, dispatches through the selected runtime, waits for that launch to settle
   under the existing bounded settlement rules, refreshes aggregate occupancy, and then
   exposes the selected runtime's next locally ordered head candidate before invoking the
   strategy again. It MUST repeat until either `dispatch_slots` is zero or no selected
   runtime can expose an eligible head.
7. Observe transitions per project, emit aggregate and project-prefixed summaries, decide
   re-exec/idle/stop once for the fleet, and sleep once using the global poll interval.

The supervisor MUST NOT run a complete single-project cycle for project A, start workers
there, and only then evaluate project B's direct landing work. That ordering violates the
fleet-wide direct-action-before-dispatch barrier.

### Aggregate slot accounting

- The multi-project supervisor batch is one global watch budget. `aggregate_running`
  MUST count only task-executing live workers from every selected runtime after
  reconciliation. Starting workers and unsettled reservations MUST NOT inflate
  `aggregate_running`.
- Dispatch capacity is governed by aggregate occupancy, not by `aggregate_running`
  alone. The supervisor MUST compute `aggregate_occupied` as the count of deduplicated
  slot claims across selected runtimes, then compute
  `dispatch_slots = max(0, supervisor_batch - aggregate_occupied)`.
- A slot claim is one task-executing live worker, one starting worker, or one unsettled
  global reservation. The same launch MUST produce at most one claim: deduplicate by
  worker PID when available, then by a launch token proven globally unique across the
  supervised fleet, or by a project-qualified fallback identity that includes supervisor
  selector/runtime identity, resolved `(db_path, project_id)`, and task ID. A bare task ID
  MUST NOT deduplicate slot claims across project runtimes. Stronger evidence replaces
  weaker evidence for the same launch. A starting worker launched by this supervisor is
  covered by its unsettled reservation until settlement; an adopted starting worker
  without a known reservation MUST create an equivalent occupied claim until it proves
  task-executing or terminal.
- Starting workers and unsettled provisional reservations remain distinct aggregate
  buckets for reporting. The supervisor MUST include every known live task-executing
  worker, known starting worker, and known unsettled reservation from selected runtimes
  in aggregate occupancy, even when the runtime state currently allows only read-only
  observation. Provisional global reservations MUST cover the plan-to-live settlement
  window so same-process launches cannot oversubscribe the fleet budget.
- Runtime state controls dispatch eligibility separately from occupancy. A runtime
  consumes zero aggregate capacity only when it has no known live task-executing worker,
  starting worker, or unsettled reservation. Healthy projects MAY use all currently free
  global slots, subject to their own local launch permit and explicit local cap.
- Aggregate occupancy MUST be refreshed after each settled launch. After a settled
  successful dispatch, the selected runtime MUST expose its next locally ordered head, if
  any, and the supervisor MUST continue the same fleet dispatch loop while both
  `dispatch_slots > 0` and an eligible head remain. If a launch reaches a terminal result
  before running or never proves live within the bounded settlement window, the
  provisional global reservation MUST be released, aggregate occupancy MUST be refreshed,
  and the same cycle MAY continue to another eligible candidate.
- Existing project-local launch permits remain required. They protect races with manual
  commands and local `max_concurrent`; the aggregate budget protects this supervisor's
  selected watch-managed work. A true hard ceiling across unrelated DBs and all commands
  requires a machine-global registry or permit and is outside this contract.
- Worker-consuming recovery capacity is allocated from the same fleet budget. The
  supervisor MUST NOT multiply `watch.recovery_slots` by the number of projects.
  In multi-project mode, the effective recovery reservation is one supervisor-global value
  resolved before per-project runtime analysis: explicit CLI `--recovery-slots` wins over
  a supervisor manifest value, which wins over the anchor/supervisor config's
  `watch.recovery_slots`, which wins over the default. Selected project configs do not
  provide competing recovery-slot reservations. `--recovery-only` is a global run-mode
  override: after `dispatch_slots = max(0, supervisor_batch - aggregate_occupied)` is
  computed for the current fleet pass, `effective_recovery_slots` MUST equal
  `dispatch_slots`. This is the fleet equivalent of the legacy `recovery_slots = batch`
  preset and continues to suppress pending pickup. Otherwise, the resolved reservation
  value is `effective_recovery_slots` for the fleet dispatch lanes below.

### Local pickup order and cross-project strategies

Each project runtime MUST produce recovery and pending candidates in that project's
existing local order. For pending pickup, local order is non-null `queue_position` first,
then `queue_position ASC`, `urgent DESC`, `urgent_bumped_at DESC`, and `created_at ASC`,
with dependency-runnable filtering applied locally. The cross-project strategy chooses
only between eligible project heads. It MUST NOT reorder candidates within one project.

The fleet dispatch loop has two worker-consuming lanes:

1. **Recovery lane.** When recovery pickup is enabled, each runtime exposes at most its
   current locally ordered recovery head. The supervisor arbitrates those heads with the
   selected cross-project strategy for at most
   `min(dispatch_slots, effective_recovery_slots)` worker-consuming starts. Recovery
   heads beyond that supervisor-global allocation wait for a later cycle or for scoped
   recovery rules that explicitly bypass pending pickup. Under `--recovery-only`,
   `effective_recovery_slots` equals `dispatch_slots`, so enough eligible recovery heads
   MUST be allowed to fill every free fleet slot.
2. **Pending lane.** When pending pickup is enabled, each runtime exposes at most its
   current locally ordered pending head. The supervisor offers this lane all remaining
   dispatch capacity after the recovery lane, including any unused recovery allocation
   when fewer recovery heads are available than `effective_recovery_slots`.

Pending pickup MUST NOT bypass an available worker-consuming recovery head while the
recovery lane still has unused supervisor-global recovery allocation. Unused recovery
allocation is donated immediately to pending in the same cycle; it is not held idle for a
possible later recovery candidate discovered after pending starts. `--recovery-only`
sets the fleet recovery allocation to all current dispatch capacity and disables the
pending lane. `--pending-only` disables the global failed-task recovery lane.
In a scoped single-project watch, existing scoped recovery and pending suppression rules
remain the lane gates.

Strategy state MUST be deterministic across lanes. Round-robin and weighted-round-robin
MUST maintain separate persistent cursor/quota state for the recovery lane and the
pending lane so draining one lane does not advance the other lane's cursor. Project
priority has no mutable cursor; it applies the same declared project order in both lanes.

The supervisor MUST NOT create, assign, compare, or infer a global cross-project
`queue_position`. In particular, it MUST NOT arbitrate all runnable rows from all projects
by one global `queue_position` or timestamp query. Cross-project scheduling is strategy
arbitration over project-local heads.

The initial named strategies are:

- **`round-robin` (default):** Visit one eligible head per project in declared project
  order and persist the cursor across cycles. When capacity remains after one pass, the
  strategy MUST begin another round within the same fleet cycle, using each selected
  runtime's refreshed local head after any successful dispatch. Empty projects and
  runtimes whose current capability state exposes no eligible head are skipped without
  spending a turn. With
  continued eligibility and eventual slot turnover, no selected project is starved;
  restarting at the first project every cycle is not valid because it can starve later
  projects when slots are scarce.
- **`weighted-round-robin`:** Give each project its positive configured number of turns per
  round while retaining a persistent cursor. When capacity remains after every eligible
  project's current weighted turns are exhausted, the strategy MUST begin another
  weighted round within the same fleet cycle. If a fleet cycle ends in the middle of a
  weighted round because aggregate capacity is exhausted, the strategy MUST preserve both
  cursor position and any unfinished in-round quota for the next cycle. Weight zero MUST
  be rejected rather than used as an implicit disable switch. Positive-weight projects are
  not starved, though lower weights receive fewer starts and longer waits.
- **`project-priority`:** Always choose the first eligible project's head, falling through
  only when earlier projects are empty or their current capability state exposes no
  eligible head. This strategy intentionally permits starvation: a continuously runnable
  earlier project can indefinitely prevent later projects from starting. Startup output
  and operator documentation MUST state that behavior plainly.

`oldest-first` is not part of the initial named set. A later strategy MAY add a global-age
policy, but it must do so explicitly through the strategy interface and without adding a
global queue-position ordering.

When the scheduler implementation lands, targeted strategy tests MUST prove that a batch
larger than the eligible project count starts multiple locally ordered candidates from a
project in one fleet cycle, weighted turns repeat across same-cycle rounds, a cycle ending
mid-round resumes from preserved cursor and quota state, recovery receives no more than
the configured supervisor-global reserved starts before pending receives remaining
capacity, pending cannot bypass available reserved recovery, unused recovery allocation
is donated according to the lane rule above, lane cursor behavior remains deterministic,
and dispatch stops only when aggregate capacity or eligible heads are exhausted.

### Leases, holds, invalid config, and isolation

- Watch supervision uses one lease name, `watch-supervisor`, for both legacy
  single-project and multi-project watch.
- A multi-project startup MUST acquire `(project_id, watch-supervisor)` for every resolved
  healthy selected project in deterministic selector order before the first mutation or
  preview that assumes exclusivity. A startup lease conflict for any otherwise valid
  selected project MUST abort the requested healthy set and release leases already
  acquired; the supervisor MUST NOT silently run a surprising subset. When the same
  startup selection also contains preflight-disabled projects, the supervisor MUST surface
  each disabled project's typed diagnostic together with the lease-conflict error instead
  of suppressing either class of startup failure.
- Invalid or unhealthy projects are represented explicitly as runtime states with
  explicit capabilities:
  - `enabled`: config is valid and the lease is owned. The runtime MAY perform normal
    mutating reconciliation, direct lifecycle work, candidate planning, and worker
    dispatch.
  - `config-invalid`: the current config failed validation or identity checks. The
    runtime MUST NOT perform task persistence, permit reservation, queue mutation,
    lifecycle repair, worker launch, or any other write derived from stale last-known-good
    configuration. It MAY contribute read-only observations about already-known live
    workers, starting workers, and unsettled launches to aggregate occupancy when those
    observations can be collected without loading or trusting the invalid config.
  - `lease-conflict`: config is valid but this supervisor does not own the project's
    watch lease. At startup this state aborts the requested healthy set as specified
    above. If a later renew/adoption attempt loses ownership, the runtime MUST NOT
    perform mutating reconciliation, lifecycle repair, queue mutation, permit reservation,
    or worker launch until ownership is recovered. It MAY contribute read-only liveness
    and occupancy observations for already-known workers/reservations so capacity output
    remains truthful.
  - `docker-held` and `git-health-held`: config is valid and the lease remains owned, but
    the local execution precondition is red. The runtime MAY retain the lease and perform
    only the mutating reconciliation needed to keep already-live worker state accurate and
    the durable hold/attention state truthful. It MUST suppress new launches and ordinary
    direct lifecycle mutations that require the failed precondition until the hold clears.
  - `main-red/merge-held`: config is valid and the lease remains owned. The runtime MUST
    hold only ordinary merge actions for the affected local target/merge lane while still
    performing observation, required mutating reconciliation, bounded rerun bookkeeping,
    and the `system-main-verify` remediation creation/reuse/queue-bump/dispatch path
    defined by this spec and
    [main-verify-self-heal.md](main-verify-self-heal.md). It MUST NOT suppress worker
    dispatch for that remediation lane merely because ordinary merges are held.
  - `locally-capped` or launch-capacity blocked: config is valid and the lease remains
    owned, but the project-local launch permit or local cap has no launch capacity. The
    runtime MAY observe and reconcile existing live/starting work, but MUST expose no new
    worker-consuming dispatch candidate until local capacity is available.
  - temporary launch holds/failures that are not capacity exhaustion MAY suppress only
    the affected launch attempt while preserving read-only observation and other
    eligible direct or worker work that is safe for that runtime state.
  Expected project operational failures are caught at the project boundary, logged with
  selector name, project ID, and root, and retried at later global poll boundaries.
  Programming or invariant errors MUST NOT be swallowed as ordinary project holds.
- A project whose config fails validation MUST fail closed before task persistence, permit
  reservation, queue mutation, lifecycle repair, or worker launch for that project. Watch
  MUST NOT execute with a stale last-known-good config after the current config fails
  validation. If the project later validates, the supervisor MUST acquire or renew that
  project's lease before enabling it.
- Docker unavailability, Git-health red, deterministic main-red/merge holds, local caps,
  and launch holds isolate to the owning project. They MUST NOT block healthy projects
  from direct work or worker dispatch. A runtime state that retains an owned lease MUST
  keep that lease so a separate single-project watch cannot take over during a temporary
  hold; a runtime without an owned lease MUST remain read-only until the lease is
  acquired or renewed.
- Backoff, no-progress, parked-attention, and failure maps MUST be keyed by selector name
  plus project identity and owner/merge-unit identity. Identical task IDs from different
  databases MUST NOT collide.
- Owned leases MUST be released in reverse acquisition order on normal exit, startup
  rollback, handled signals, and confirmation refusal. Installed-code drift re-exec MUST
  preserve the ordered selector set and the run token so the new process can adopt its
  live rows and leases idempotently instead of conflicting with itself.

When the runtime implementation lands, targeted regressions MUST prove both safety
boundaries in this state model:

- A runtime with an invalid reload or later lease conflict performs no writes while its
  known live/startup occupancy is still counted read-only and healthy projects continue.
- A deterministic main-red state blocks ordinary merges for the affected lane while still
  creating or reusing, queue-bumping, and dispatching the matching
  `system-main-verify` remediation, without blocking healthy projects.
- Two unrelated databases that contain the same task ID and no usable PID evidence remain
  two occupied claims unless a launch token proves they are the same globally unique
  launch, so the supervisor does not expose a phantom dispatch slot.

### Global idle, re-exec, and observability

- Global idle means there is no direct work, runnable candidate, live task-executing
  worker, starting worker, or unsettled launch across selected runtimes. Disabled or held
  runtimes with no known live or reserved work remain visible in summaries but MUST NOT
  keep the process falsely active forever; `max_idle` MAY exit with an unhealthy-project
  summary.
- Drift detection is process-global. Re-exec happens only at a fleet cycle boundary and
  MUST preserve the full ordered selectors, keyed tags, tag modes, strategy state,
  manifest identity when used, and lease token. The restarted supervisor MUST adopt all
  detached workers and owned leases before new dispatch.
- Detailed project-owned logs belong in each project's configured `.gza` area. Aggregate
  supervisor state and the aggregate supervisor log MUST resolve to one supervisor-owned
  state directory in this order: the explicit CLI `--watch-state-dir` override wins;
  otherwise manifest `aggregate_state_dir` wins; otherwise a manifest invocation defaults
  to `<manifest directory>/.gza/watch-supervisor`; otherwise the fallback is
  `<anchor project root>/.gza/watch-supervisor`. The aggregate supervisor log is appended
  as `watch-supervisor.log` under that resolved directory.
  Project detail logs rotate per invocation through the normal live/dry-run watch log
  lifecycle; the aggregate supervisor log is pass history and is not rotated with each
  project detail log. Console and aggregate events MUST be prefixed by selector name. A
  log-path failure in one project degrades that sink and surfaces a warning without
  losing other projects' events.

### Supervisor policy versus project-local execution settings

The multi-project supervisor owns global policy: supervisor batch, poll interval,
`max_idle`, strategy, project order, keyed tag selection, tag modes, weights, the
worker-consuming recovery-slot reservation, confirmation, dry-run behavior, drift re-exec,
aggregate logging, and aggregate lease coordination. CLI values override a reusable
supervisor manifest. In legacy single-project mode, absent global values continue to come
from that project's `watch` config.

Each project remains the source of its own execution and lifecycle settings, including
provider/model routing, `verify_command`, `unit_verify_command`,
`autonomous_verify_timeout_seconds`, Docker and setup settings, `advance_mode`, worktree
paths, target branch, quiet-period policy, recovery eligibility and recovery attempt
policy, local worker registry, local logs, and explicit local `max_concurrent`. The
project runtime decides which local failed work is actionable through the shared recovery
policy; the supervisor decides how many worker-consuming recovery starts may reserve the
aggregate fleet budget. A CLI setting documented as an all-project override, such as
`--max-iterations`, MAY override the corresponding project setting for every selected
runtime; otherwise settings MUST NOT bleed between project configs. Loading one project's
execution config MUST NOT silently replace supervisor presentation/theme state chosen by
the anchor or supervisor config.

### Settled compatibility choices before CLI integration

These choices are part of the initial multi-project contract and MUST be enforced before
the multi-project CLI is enabled:

- Positional task-ID or merge-unit scoped watch is rejected whenever more than one project
  is selected. Legacy single-project positional scope remains compatible.
- Round-robin cursor state must survive continuous-process drift re-exec; durable
  persistence across a full stop/start is optional unless a later contract requires it.
- Aggregate supervisor state and logging MUST use the same deterministic directory
  resolution rule as the observability contract: explicit override, then
  `<manifest directory>/.gza/watch-supervisor` for manifest invocations, then
  `<anchor project root>/.gza/watch-supervisor` for no-manifest invocations. Fleet state
  MUST NOT be written into every selected project's task DB.
- Explicit per-project `max_concurrent` remains a local sub-cap in multi-project mode,
  while supervisor batch controls aggregate watch appetite.

## Core invariants

Unless the multi-project supervisor contract above explicitly says otherwise, these
invariants describe both legacy single-project watch and each project runtime inside a
multi-project fleet cycle.

### 1. One cycle has a fixed order

Each watch cycle MUST execute these phases in order:

1. **Reconcile runtime state.** Reap or reconcile stale in-progress state, then discover
   live detached workers and live in-progress tasks. In a multi-project fleet, this
   phase is mutating reconciliation only for runtime states with validated config and an
   owned lease; `config-invalid` and `lease-conflict` runtimes are limited to read-only
   liveness and occupancy observation until validation and lease ownership are restored.
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
   keyed at least by the normalized `verify_command`, gate-enabled/no-gate state, and the
   verify environment identity that produced the checkpoint; the current implementation
   also includes the resolved automation timeout settings. For configured gates, an older
   checkpoint that lacks the required environment identity MUST be treated as stale rather
   than reused optimistically.
   Watch MUST keep local-target verify progress truthful and concise. It MAY first emit a
   checkpoint-status line that says the verify will run if the checkpoint is stale or if a
   red result needs bounded confirmation, but that line MUST NOT describe an active suite
   execution. When a configured-gate checkpoint is stale or a current red checkpoint needs
   bounded confirmation and watch is about to execute the suite, watch MUST emit a
   distinct actual-run start line immediately before attempt 1 starts; that line MUST
   include attempt numbering as `1/<bounded-total>`, the target branch and resolved target
   SHA when available, and a note that the suite may take a while. If the target SHA
   cannot be resolved because of an expected Git lookup failure, watch MUST emit a concise
   `WARN` explaining why the SHA is unavailable before displaying `unknown`; programming
   errors in target lookup MUST surface instead of being rendered as `unknown`. Bounded
   merge-halting reruns MUST use the same total-attempt count and concise rerun lines,
   and those rerun lines MUST describe the preceding evidence truthfully: `red` only for
   an actual failed gate verdict, `unavailable` for unavailable freshness or environment
   evidence, and `unknown` for malformed evidence. Completion
   output MUST include the verdict, the target/SHA, elapsed time, and the performed
   attempt count when a suite ran;
   red completions MUST also name the failing phase when structured phase evidence exists.
   Cached checkpoint completions MUST be visibly marked as cached and MUST NOT include the
   actual-run start line. Under `--quiet` and `--yes`, raw verify command stdout MUST be
   suppressed; under `--yes`, watch MUST still keep structured checkpoint, start, rerun,
   warning, completion, and attention stdout visible.
   Independently of tree change, a configured-gate checkpoint that is not `passed` MUST
   expire after a bounded configured TTL and be rerun on that cadence so red/unavailable
   results cannot persist indefinitely on an unchanged target tree. If the current
   default-branch checkout cannot produce an exact tree fingerprint before or after that
   verify, watch MUST treat freshness as unproven instead of reusing `HEAD` equality
   alone; it MUST halt further merges for the current cycle and surface one visible
   durable attention row explaining that exact-tree freshness could not be proven. That
   persisted row MUST NOT embed a target SHA; watch may render a SHA-bearing
   `merges halted` freshness message only after proving the recorded SHA still matches
   the current local target HEAD. More generally, if that verify is not `passed`, watch
   MUST first perform the bounded
   rerun-before-halt sequence owned by
   [main-verify-self-heal.md](main-verify-self-heal.md). A flaky red that turns green in
   that sequence MUST clear the halt for the current cycle and MUST create or reuse
   exactly one active de-flake remediation attempt for that failure identity. A deterministic
   red that stays red across the full bounded sequence MUST halt further merges for the
   current cycle, MUST create or reuse exactly one active fix-remediation attempt for that
   failure identity, and MUST emit one visible durable attention row with reason
   `main-integration-verify-red` naming the failing target SHA only while that SHA still
   matches the current local target HEAD and, when structured phase output exists, the
   failing phase. If the local target HEAD advances before the attention summary is
   rendered, watch MUST NOT assert that the recorded SHA is still red or that merges are
   currently halted based only on that stale red evidence. The convergence requirements
   for how that red state self-heals or escalates over time are owned by
   [main-verify-self-heal.md](main-verify-self-heal.md). If no `verify_command` is
   configured for the project, that is an explicit no-gate
   exception: watch MAY record an `unavailable` checkpoint with
   `exit_status="not configured"` but MUST NOT halt merges or emit red-main attention for it.
   Watch MAY persist a completed failed-recovery scan marker for the current target branch,
   but that marker is authoritative only when it proves the target head and every active
   branch/unit classification at branch granularity. The proof MUST be invalidated by new
   units, source-head changes, and target movement; target movement MUST revalidate
   branch content equivalence rather than relying only on stable merge-base ancestry.
   Incomplete reconciliation, missing head/base proof, or classifier exceptions MUST keep
   the previous marker and force the ordinary live fail-closed recovery classification path.
   Only a current complete marker MAY let failed-task discovery reuse DB-backed
   classifications instead of per-lineage Git probes.
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
   The bounded rerun evidence's observed verify environment identity, or an explicit
   `unknown/unavailable` marker when that identity could not be captured, MUST travel
   into the remediation metadata and prompt so the remediation worker can see where the
   failure was observed, but that metadata MUST NOT veto filing or requeueing the
   bounded remediation task.
   While that deterministic-red freeze is active, watch MUST skip unrelated merge
   actions for the cycle but MUST allow one narrow exemption: the merge subject that is
   the active deterministic `system-main-verify` fix remediation for the currently red
   failure identity. That exemption MUST be watch-owned, MUST match the actual merge
   subject rather than a display owner row, and MUST require the remediation trigger
   source, prompt kind `fix`, exact failure signature, and exact tree fingerprint when
   one is available from the active evidence. If the active evidence has no tree
   fingerprint, the exemption MUST stay conservative and only match a remediation prompt
   whose parsed fingerprint is likewise unavailable. Deflake remediations and mismatched
   signatures or fingerprints MUST NOT bypass the freeze. After an exempt remediation
   merge succeeds, watch MUST immediately rerun the same local-target verify gate
   against the post-merge tree before allowing any later merge in that cycle. Only a
   green rerun clears the cycle-local freeze; if the rerun stays red, watch MUST keep
   merges halted, emit the durable `main-integration-verify-red` attention, and create
   or reuse the remediation task for the new active failure identity through the same
   dedup path.
   `advance` MAY surface the red-main condition from the shared state, but it MUST NOT
   create these remediation tasks itself.
   When watch stages merges in the isolated host merge checkout, it MUST batch the
   selected direct-merge candidates for that phase into the detached checkout in
   lifecycle order and run one combined pre-promotion candidate verify for the staged
   result before the canonical default-branch ref is updated. If that combined staged
   verify passes, watch MUST promote once, adopt the candidate checkpoint only when the
   promoted tree exactly matches the verified candidate tree, and emit one `MERGE` line
   per landed merge unit without paying a duplicate full post-merge verify for the same
   exact tree.
   If target promotion fails after candidate verification succeeds and rollback restores
   the previous target tip, watch MUST report an isolated merge promotion failure with
   the preserved failure reason, MUST NOT route the result through conflict/rebase
   handling, and MUST leave the affected merge units unmerged with unchanged provenance.
   If rollback fails after the target ref moved, watch MUST re-read the target ref. An
   exact candidate tip MUST continue through post-promotion checkpoint/finalization replay;
   an exact previous tip remains a promotion failure; any other observed tip MUST emit
   explicit operator attention without claiming the target is untouched or routing through
   conflict/rebase handling.
   If a post-promotion merge finalization proof was durably stored but merge-state
   finalization fails, watch MUST stop selecting later merges for that target in the
   current pass so the pending finalization can replay against the promoted target before
   another promotion advances it.
   If a member conflicts while being staged into the detached checkout, watch MUST reset
   the detached checkout to the pre-member staged tip, emit a `SKIP` line identifying the
   member conflict or conflict-routed rebase, drop only that member from the staged batch,
   and continue staging later members in lifecycle order. The conflicting member MUST run
   the same isolated merge failure assessment and durable rebase-task routing used by
   the non-batch isolated merge path: a real conflict creates or reuses exactly one
   active rebase child for the member's branch against the target branch before any
   worker-dispatch capacity is required, while duplicate active rebase children are
   reused/deduped instead of accumulating another rebase task on later watch cycles.
   If that durable child cannot be created or reused, watch MUST fail closed for the
   batch before staging later members, before running candidate verify, and before
   promoting or finalizing any staged prefix.
   Non-batch conflict routing MAY immediately dispatch the child because the target
   branch is already authoritative. Batch conflict routing MUST NOT start that child
   until after the canonical target branch has been promoted to include the accumulated
   staged prefix against which the conflict was classified. If the batch is not
   promoted, or if launch capacity is unavailable after promotion, the child MUST remain
   queued for a later cycle and MUST be excluded from generic pending-worker pickup in
   the same unpromoted cycle. After reserving launch capacity and immediately before
   startup preparation, both batch and non-batch conflict routing MUST re-read the
   durable child row and dispatch only a currently pending child. A child that refreshed
   to `in_progress` MUST be treated as already owned work and MUST NOT be relaunched;
   a missing or terminal refreshed row MUST emit a visible stale-launch diagnostic
   without preparing or spawning the stale snapshot. If post-promotion startup
   preparation for that child fails, watch MUST keep or restore the same child as
   pending, emit the preparation failure, and allow a later cycle to reuse it instead of
   creating a duplicate.
   Only an explicitly classified merge conflict may use that per-member skip path;
   non-conflict staging failures MUST remain visible failures and MUST NOT promote the
   remaining batch under conflict semantics. A branch that is already merged into the
   pre-batch live target during staging MUST be reconciled as successful merge-state
   work, not reported as a conflict. A branch that is contained only by the accumulated
   detached candidate MUST remain a staged batch member: its mandatory debt, merge
   provenance, and merge-state finalization MUST wait until the combined candidate passes
   verification and the live target is promoted.
   Pre-promotion source-proof refusals, including unavailable authorized source refs,
   pre-merge proof failures, and source refs that change after mandatory child
   materialization, MUST report that the target is unchanged and MUST NOT route through
   conflict/rebase handling.
   With default `on_max_cycles=merge_and_defer`, the shared
   lifecycle engine projects an eligible capped review as an annotated direct `merge`
   action. Watch MUST execute that action in this direct phase, render lifecycle summaries
   using merge-and-defer wording, create or reuse all mandatory deferred-blocker tasks
   before promotion, already-merged mutation, or merge-unit finalization, persist
   `max_cycles_deferred` merge provenance on success, and MUST NOT spawn an iterate
   worker for that capped review. Under rollback `on_max_cycles=park`, a
   `review-max-cycles-reached` action remains a human-attention signal. Missing or stale source verify evidence MUST run the
   normal pre-merge verify path before eligibility is reconsidered; red or unavailable
   source verify evidence, spec-coherence blockers, no-op/adjudication lanes,
   duplicate-blocker stops, and plan-review caps stay on their existing non-deferred
   paths and cannot merge-and-defer.
   If the isolated checkout is unavailable, or combined candidate verify is red or
   freshness is unproven, watch MUST leave the canonical target untouched. A red staged
   batch MUST trigger bounded replay from the canonical target until the first
   red-producing merge unit is identified; watch MUST route only that merge unit to
   blocked-candidate attention plus one queued rework task, keep later candidates
   eligible for future cycles, and MUST NOT convert that candidate-red state into a
   global canonical-main freeze. When one or more earlier staged prefixes verified green
   before that first red was isolated, the queued rework prompt or equivalent queued
   metadata MUST include compact staged-prefix context sufficient to reproduce the exact
   interaction tree, at minimum the prior green-prefix owner identity or identities plus
   the failing unit's position within the staged batch. Before watch considers any later
   merge candidate in that same cycle, it MUST refresh or rebuild the isolated checkout
   back to the canonical target, or stop the merge lane for the cycle.
4. **Blind parked auto-rearm phase.** After the direct non-worker lifecycle phase has
   reconciled the freshest target state for this cycle, watch MAY run one conservative
   parked-owner auto-rearm pass before any worker dispatch. This phase MUST stay
   supervisor-owned and MUST reuse the shared parked clear service; it MUST NOT invoke
   operator-triggered guarded landing, create a landing judge task, inspect per-merge
   relevance, defer blockers outside the shared capped-review merge action, or fork a
   second lifecycle policy. For each
   currently parked subject/reason candidate, watch MUST first exclude
   `verify-fix-failed`, which remains a manual-only fresh-verify escape hatch, and then
   apply these gates in order: feature enabled, budget remaining, cooldown elapsed, and
   target branch advanced when `watch.parked_auto_rearm.require_target_advanced` is true.
   An excluded reason or failed gate MUST leave the
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
5. **Spend slots on worker-consuming actions.** Use remaining `dispatch_slots` for
   recovery and lifecycle worker starts selected by the shared engine. In multi-project
   mode, the supervisor MUST use the recovery lane and pending lane defined in
   [Local pickup order and cross-project strategies](#local-pickup-order-and-cross-project-strategies).
   Recovery allocation is not a pending leftover: the supervisor MUST offer
   worker-consuming recovery heads the configured supervisor-global recovery allocation
   before pending pickup. Under `--recovery-only`, that allocation MUST be all current
   fleet dispatch capacity, and pending pickup MUST be gated entirely while any
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
   After stale-candidate and current-head validation, but before dry-run `START`
   reporting, active-recovery-backoff checks, no-progress bookkeeping, launch-permit
   acquisition, startup preparation, or worker spawn, watch MUST validate the refreshed
   pending task's concrete provider/model execution route against the owning runtime's
   current config. An invalid route MUST fail closed as an explicit non-dispatchable
   result with an operator-visible reason and MUST NOT mutate the task row or persisted
   watch-progress/backoff state. This ordering applies identically to `implement` and
   non-`implement` pending candidates through the same pending-dispatch validation path.
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
- During watch-supervised phases that can exceed `watch.long_phase_threshold_seconds`,
  watch MUST emit operator-visible long-phase progress. The phase MUST emit a `START`
  line before silence begins and then `BUSY` heartbeats every
  `watch.heartbeat_interval_seconds` while still in flight. Each heartbeat MUST identify
  the phase and task or merge-unit subject, report elapsed wall time, and include real
  liveness evidence such as CPU delta or captured output advancement. Child-process
  phases SHOULD report captured output deltas. In-process cycle-level phases have no
  subprocess output stream and MUST render that field distinctly, such as `out
  unavailable`, instead of reporting a misleading zero-byte output delta. If supported
  liveness evidence was sampled and has not advanced since the previous heartbeat, the
  heartbeat MUST say `NO PROGRESS` explicitly. Unsupported evidence MUST be rendered
  distinctly, such as `cpu unavailable`, and MUST NOT be treated as observed flat
  progress. This heartbeat stream is separate from the restart-safe watch-progress
  backstop state.
- In-process cycle-level phases MUST use the non-task subject `cycle`. Each emitted
  `START <phase> cycle` line for a cycle phase MUST be paired with
  `DONE <phase> cycle elapsed <duration>` when that phase boundary exits, including
  exceptional exits that propagate to the caller. This cycle-level start/done pairing is
  the completion event for the in-process phase boundary; it does not change or replace
  the background worker heartbeat ticker described below.
- Scoped completion checks MUST analyze scoped activity in a standalone
  `scoped-completion-analysis` cycle-level phase before `cycle-finalize`. That phase
  includes scoped plan construction, selector recovery closure, and active owner counting,
  and MUST NOT overlap another cycle-level phase.
- Background workers are long phases even when watch is sleeping between cycles. Watch
  MUST track each live registered worker independently across sleep intervals, including
  workers that were already running when watch started. Worker heartbeat subjects MUST
  preserve the task-to-phase mapping, using `rebase <task-id>` for rebase workers and
  `agent:execution <task-id>` for other task workers, with elapsed time derived from the
  durable task or worker start timestamp. Short `watch.poll` values MUST NOT suppress a
  due worker heartbeat.
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
- A shared recovery decision that selects `resume`, `retry`, or another actionable
  recovery step MUST either enter dispatch planning or emit an explicit undispatched
  supervisor log explaining why that decided action was not dispatchable. Watch MUST NOT
  silently discard a decided recovery action before dispatch.

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
- In legacy single-project mode, the effective watch worker target for a pass MUST be
  `min(batch, max_concurrent)`. When `max_concurrent` is unset, `gza watch` MUST derive
  the runtime cap from the effective watch batch for that run, including any CLI `--batch`
  override. In multi-project mode, supervisor batch is the aggregate cap and each
  project's explicit `max_concurrent` remains a local sub-cap.
- In legacy single-project mode, `slots` MUST equal
  `max(0, min(batch, max_concurrent) - running)`.
- In legacy single-project mode, if the requested `batch` exceeds an explicit
  `max_concurrent`, watch MUST emit one startup warning that the requested batch was
  capped by the configured ceiling.
- In legacy single-project mode, `watch.recovery_slots` (default `1`) MUST reserve that
  many worker slots per cycle for actionable failed-task recovery before pending pickup,
  capped by available slots and actionable in-scope worker-consuming recovery count, when
  pending pickup is enabled.
- The legacy single-project rule is uniform for worker-consuming recovery. There is no
  separate batch-1 policy: with batch 1 and the default `watch.recovery_slots = 1`, plain
  watch gives the single slot to worker-consuming recovery until that lane drains.
  `--pending-only` is the operator escape hatch for single-slot pending-only behavior,
  and `--recovery-only` is the `recovery_slots = batch` extreme that also suppresses
  pending pickup while direct actionable recovery remains. Multi-project
  `--recovery-only` preserves the same command contract by making the effective recovery
  allocation equal every currently free fleet dispatch slot, not the configured
  reservation.
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
  merge-with-followups, annotated max-cycle merge-and-defer, scope evaluation, re-exec
  decisions, and attention emission MUST NOT consume slots. Annotated max-cycle
  merge-and-defer remains an executable `merge` action and MUST NOT route through an
  iterate worker.
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
  increment the failure streak for that failing lineage owner / merge unit and MUST retain
  that owner in the owning project runtime's distinct failing-owner set, not a single
  process-global set shared across unrelated projects.
- The project-runtime halt metric is the **distinct failing-owner count**: the number of
  owner units in that runtime with retained failure-backoff state. Repeated failures from
  one owner MUST only advance that owner's streak/backoff and MUST NOT increase this
  project-runtime halt count. An owner leaves the count only when its retained
  failure-backoff state is reset after owner-scoped work completes; cooldown expiry alone
  removes the temporary dispatch quarantine but does not clear the owner from the halt
  count.
- The configured exponential backoff policy (`watch.failure_backoff_initial`,
  `watch.failure_backoff_max`) MUST quarantine only the failing owner. Backoff on owner A
  MUST NOT block dispatch of runnable work from owners B, C, and so on in the same or a
  later cycle.
- `watch.failure_halt_after` MUST be keyed to the owning project runtime's distinct
  failing-owner count, not repeated failures on one owner alone and not a fleet-wide count
  shared across projects. In multi-project mode, reaching the threshold holds only that
  project runtime for human intervention; healthy project runtimes continue to reconcile,
  run direct work, and dispatch. In legacy single-project mode, the single project runtime
  is the whole invocation, so reaching the threshold retains the existing one-runtime stop
  behavior.
- A nonzero per-owner failure streak, a nonzero project-runtime distinct failing-owner
  count, and each quarantine/halt/hold decision MUST be operator-visible and MUST name the
  affected owner or project runtime.
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
- Watch MUST increment the no-progress streak when watch re-selects the same
  subject/action and the subject evidence remains unchanged across cycles, including both:
  actually **executed** no-op repeats and selected actions that never dispatch in that
  cycle. Merely evaluating a candidate before selection, remaining on a blocked merge lane,
  or switching to a different selected action/reason MUST NOT increment the streak.
  Restarting watch MUST NOT reset a streak created by those unchanged repeats.
- There is one narrow compatibility exception for legacy unmaterialized `create_review`
  evidence: if the selected action is `create_review` and the observed action task is the
  same row as the subject implementation, watch MUST treat that subject-as-action-task pair
  as invalid stale progress evidence. It MUST clear the subject's persisted watch-progress
  and recovery-backoff state, emit the ordinary dispatch/routing diagnostic for the current
  cycle, and retry on a later cycle instead of parking from that invalid evidence. This
  exception applies only before a review row matching the selected create-review action
  has been materialized. Resolution-review actions MUST match the expected
  implementation, rebase task, resolved head SHA, and resolved target SHA; historical
  reviews from an older action epoch MUST NOT count as materialization. Once the action
  task is an actual matching review row, unchanged executed outcomes and unchanged
  undispatched selections MUST follow the normal no-progress streak and parking policy.
- After selecting a worker-consuming action, watch MUST wait only a small bounded
  `watch.slot_settle_seconds` window for the chosen task to prove execution. A live
  running state counts, and a live registered worker counts, including the legitimate
  preloop case where the task row is still `pending`. A task that reaches an observable
  terminal outcome inside that same bounded window is a settled launch outcome, but it
  does NOT occupy a worker slot or count as a confirmed running start. Watch MUST emit a
  visible diagnostic for that terminal-before-running outcome, release the provisional
  launch budget for the same cycle, skip no-progress accounting for that attempted
  launch, and continue scanning the same cycle for another runnable candidate instead of
  leaving the slot idle. Only when neither live-running proof nor an observable
  post-launch terminal outcome appears in the window may watch log the action as
  undispatched, count that unchanged selected action toward the same subject/action
  no-progress streak, and continue scanning the same cycle for another runnable
  candidate instead of leaving the slot idle. This bounded settle window does NOT change
  the fixed sleep cadence: after the cycle completes, watch still waits on the ordinary
  `watch.poll` / `--poll` boundary before the next steady-state pass.
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
- Alternating skip reasons for the same selected subject/action (for example capacity-block
  one cycle and routing-skip the next) MUST NOT reset the streak by themselves.
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
  `reconcile-needs-manual-resolution`, and `verify-fix-failed`.
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
- `watch <task-id>...` MUST keep one explicit selector record per supplied raw task ID.
  Each selector records the raw ID, its startup canonical lineage / merge-unit owner, and
  the current effective live owner/leaf established by scoped owner-row analysis.
- Ordinary explicit owner scope still drives the whole owner when the operator names the
  canonical owner itself. The exception is a raw descendant selector whose scoped
  analysis re-roots to a selector-matching failed leaf under a landed or otherwise
  terminal canonical owner; that selector's effective identity is the matching leaf, not
  the canonical owner or any sibling.
- Selector closure MUST be established before transition and failure-boundary processing
  for the first analyzed cycle, including after an initial preview, and then carried into
  subsequent preview, dispatch, reanalysis, and completion checks.
- Recovery rows in explicit selector scope MUST come only from failed leaves matching one
  of the raw selectors. An actionable sibling that sorts earlier MUST NOT become the
  recovery row, recovery action, launch target, or no-progress/rearm subject for the
  selected leaf.
- Multiple raw selectors MAY share one startup canonical owner. Watch MUST retain one
  effective identity per selector and exclude any unselected sibling under that same owner.
- If an effective selector leaf becomes terminal and its owner row disappears while a
  sibling remains unresolved, watch MUST retain the leaf-specific terminal state long
  enough to complete that selector and render the leaf identity in the scope banner. It
  MUST NOT broaden missing or ambiguous effective rows back to the canonical owner.
- Active counting, transition filtering, failure halt/backoff boundaries, scope rendering,
  scoped recovery, and state-mutating parked auto-rearm paths MUST consume the effective
  selector identities. They MUST NOT count, report, clear, rearm, retry, resume, or launch
  an unselected sibling merely because it shares the startup owner.
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
- Watch MUST NOT invoke `gza land` semantics, create or reuse landing judgments, defer
  review blockers outside the shared capped-review merge action, or bypass parked
  lifecycle gates. Guarded landing is an explicit operator command for one selected merge
  unit; ordinary watch remains strict except for executing the annotated
  `on_max_cycles=merge_and_defer` action selected by the shared lifecycle engine.

## Policy knobs this layer owns

The existence of these knobs is contract; their values are operator policy.

| Knob | Governs |
|------|---------|
| `watch.batch` / supervisor batch | Maximum concurrent detached worker processes the supervisor maintains; in multi-project mode this is one supervisor-global aggregate watch budget enforced by `dispatch_slots = max(0, supervisor_batch - aggregate_occupied)` |
| `max_concurrent` | Project-local launch ceiling; legacy single-project watch clamps batch to this when explicit, and multi-project watch treats each selected project's value as a local sub-cap |
| `watch.poll` / supervisor poll | Delay between completed cycles; in multi-project mode this is one supervisor-global fleet-level sleep boundary |
| `watch.max_idle` / supervisor max-idle | Consecutive idle loop time before clean exit; in multi-project mode idle is aggregate across selected runtimes |
| `watch.max_iterations` | Iterate-worker loop cap for implementation chains launched by watch; an explicit CLI override may apply to every selected project, otherwise each project runtime uses its own value |
| `watch.recovery_slots` / supervisor recovery slots | Worker-consuming failed-task recovery reservation before pending pickup; in legacy single-project mode this comes from that project's watch config when no CLI value is supplied, while in multi-project mode it is one supervisor-global recovery-lane allocation resolved by CLI, then manifest, then anchor/supervisor config, then default, not multiplied per project, and donated to pending when unused in the same cycle; `--recovery-only` overrides the effective recovery allocation to all current dispatch capacity and suppresses pending pickup |
| `watch.failure_backoff_initial` / `watch.failure_backoff_max` | Project-local exponential cooldown after non-auto-resumable failures |
| `watch.failure_halt_after` | Project-local distinct failing-owner threshold that stops or holds that runtime for human intervention; repeated failures from one owner advance only that owner's backoff streak |
| `watch.transient_recovery_backoff_max` | Project-local maximum persisted cooldown for transient failed recovery/improve retries |
| `watch.no_progress_cycles` | Project-local repeated unchanged watch-action cycles before the supervisor parks the subject with `watch-no-progress-backstop` |
| `watch.slot_settle_seconds` | Project-local bounded wait for selected work to prove live execution; only live proof occupies a slot, while terminal-before-running outcomes release provisional budget and no-live-proof outcomes remain undispatched |
| `watch.no_activity_timeout` | Project-local reconciliation threshold for deciding a registered worker for a pending or in-progress task has gone silent and must be failed/reconciled |
| `--tag` / `--all-tags`; keyed selector tags | Unkeyed `--tag` / `--all-tags` are accepted whenever the invocation resolves exactly one execution project (`--tag` matches any requested tag by default; `--all-tags` requires all of them). Invocations that resolve more than one execution project MUST use keyed selector or manifest tags and MUST reject unkeyed global tag flags |
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
