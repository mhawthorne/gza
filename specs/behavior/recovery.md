# Recovery semantics — resume, retry, and manual escalation

> **Status: Draft.** This document is the prescriptive contract for failed-task recovery:
> when gza MUST resume an existing provider session, when it MUST start a fresh retry,
> when an empty/redundant merge unit is moot versus still recoverable, and when automation MUST
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
- **R2 — Terminal no-work state is not enough.** An `empty` or `redundant` merge unit
  alone MUST NOT decide whether a failed task is moot or recoverable. The policy MUST
  inspect recorded provider execution evidence first.
- **R3 — Fail closed for lost-work risk.** When evidence is incomplete, the policy MUST
  prefer "attempt recovery once" over "declare moot and silently drop work".
- **R4 — Recovery is bounded.** Automatic recovery MUST stop at a named budget and then
  park for a human; it MUST NOT loop forever.
- **R5 — Already-landed work is not recoverable.** Once a failed task has a valid landed
  or completed representative, recovery MUST suppress the older failed row instead of
  re-queueing it. **Branch reachability from the target is not, by itself, a valid landed
  representative**: a branch counts as landed only if it contributed at least one commit now
  contained in the target. An `empty` or `redundant` branch (no unique commits ahead of
  the target) never satisfies R5 — it is governed by the no-work recovery predicate (§1),
  not by landed suppression. This rule and `lifecycle-engine.md` §7 ("already landed")
  MUST stay in lockstep.
- **R7 — Tombstoned merge units are intentionally inactive, not recoverable winners.**
  A merge unit in state `dropped` or `superseded`, or one hidden behind
  `superseded_by_unit_id != NULL`, is an operator-declared losing unit. Shared active-unit
  reads MUST stop resolving it as active work, and failed-task recovery queues/dry-run
  surfaces MUST omit its attached failed members. This suppression is abandonment, not
  dependency satisfaction: the tombstoned unit still does not count as landed/no-work for
  `lineage.md` L1.
- **R6 — A recovery row carries its action.** A task created to carry recovery
  (`recovery_origin = resume` or `recovery_origin = retry`) MUST be executed with that
  action whenever it runs, by **any** launch path (pending-queue worker pickup, `iterate`,
  `advance`, recovery lane) — not only by the recovery lane. Its `pending` status MUST NOT
  cause it to be dispatched as ordinary fresh work that ignores the stored action.

## Decision model

### 1. Terminal no-work merge units split into moot vs recoverable

The operative condition is **"the branch has no commits ahead of the merge target"**. This surfaces
either as an active merge-unit state of `empty`/`redundant`, **or** as a failed task with no merge unit at all
(`merge_status` absent / `None`) whose branch is reachable-but-empty vs the target. In all cases
the policy MUST evaluate the single shared predicate below — it MUST NOT gate solely on the literal
merge-unit enum, since a task that died before its first commit may never have acquired a merge unit:

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

- `empty`/`redundant` + predicate **false** = **moot**. The task MUST stay out of
  failed-task recovery queues and dry-run recovery surfaces.
- `empty`/`redundant` + predicate **true** = **recoverable**. The task MUST stay eligible
  for the normal recovery policy and MUST NOT be suppressed merely because the branch
  currently has no commits to land.
- A failed recoverable `empty`/`redundant` task MUST continue to block downstream
  merge-required dependencies until recovery resolves it through a valid completed
  representative. Terminal no-work merge evidence alone is not enough while the failed
  task remains recoverable.
- A no-change / no-commit failed run whose branch resolves to `empty` or `redundant`
  against the target MUST persist a canonical non-`UNKNOWN` no-work failure reason
  (currently `TERMINAL_NO_WORK`) and MUST be classified through this shared no-work
  policy rather than falling back to `UNKNOWN` / manual-only parking.
- Conversely, a successful no-op code path with trustworthy completion evidence and green
  verification is terminal moot work, not failed recovery: it MUST persist as
  `status == "completed"` plus authoritative `empty`/`redundant` merge evidence, and it
  therefore satisfies downstream merge-required dependencies under `lineage.md` L1.

### 2. Resume vs retry vs manual

- Timeout-style or otherwise resumable failures with a recoverable preserved session MUST
  choose `resume` on the first automatic recovery attempt.
- Retryable provider/infrastructure failures that should not reuse the same execution
  thread MUST choose `retry`.
- A code-task run that cannot read post-run worktree git status (for example
  `git status --porcelain` fails because the worktree admin linkage was detached or
  pruned) MUST classify as the existing retryable infrastructure failure bucket,
  not as "no changes made" / `UNKNOWN`. Git inspection failure is not proof of a
  clean tree.
- Git/worktree-admin failures that indicate the task hit container-only or otherwise
  unavailable worktree metadata (for example `git worktree list --porcelain` or similar
  commands failing on `/gza-git/...`) MUST classify as retryable infrastructure failure,
  not as generic `GIT_ERROR` / manual-only parking.
- Provider-side transient availability failures surfaced as stream/log errors (for
  example a Codex `turn.failed` reporting that the selected model is at capacity)
  MUST classify as a retryable provider-availability failure, not `UNKNOWN`.
- A rebase that still has unresolved conflict state after the automated/provider-assisted
  path completes MUST persist a distinct manual failure reason for "rebase conflict
  requires manual resolution". It MUST NOT collapse into the same bucket as infrastructure
  or worktree-admin git failures.
- Manual-only failures MUST park for a human and MUST NOT be auto-resumed or auto-retried.
- A fresh retry MUST create a new execution attempt. A resume MUST preserve the provider
  session/thread being continued.
- A failure meaning the completed work **could not be published to `origin`** (push
  rejected / local branch diverged from `origin/<branch>`, reason `BRANCH_UNPUSHABLE`) is
  **recoverable**, not manual. Its recovery action is neither session-resume nor fresh
  retry: it MUST route to the `lifecycle-engine.md` §4 reconcile/rebase gate (reconcile or
  mechanically rebase, then re-publish and continue to PR creation and the §8 merge gate).
  Only a genuine host-side conflict surfaced by that gate parks for a human. This reason
  MUST NOT be classified manual-only.
- Conversely, failing to **open a PR after a successful push** is not a recoverable failure
  because it is not a failure at all: the unit completes and the missing PR is recorded and
  surfaced ([lifecycle-engine.md](lifecycle-engine.md) §9).

### 3. Recovery suppression after valid resolution

`empty_task_requires_recovery(task)` MUST evaluate false once the failed task is already
resolved by a valid representative, including:

- a completed recovery descendant for the same failed chain
- a completed automatic sibling recovery that already resolved the replaced attempt
- another valid landed representative for the same code by an independent path

The same failed task being `empty` or `redundant` on its own MUST NOT count as proof of
resolution. A branch that is merely reachable from the target but has **no unique commits**
is terminal no-work, not a landed representative, and MUST NOT count as proof of
resolution either (see R5 and `lifecycle-engine.md` §7).

Failed members of a dropped/superseded merge unit are a separate suppression case: they
MUST disappear from shared recovery queues because the unit is intentionally inactive, even
though that abandonment does not satisfy dependencies or create a landed representative.

### 4. Executing a pending recovery row

Sections 1–3 decide whether to **create** a resume/retry for a failed task. This section
governs what happens when the resulting recovery row (a `pending` task with
`recovery_origin` set) is later **executed**. These are distinct: creation policy may
legitimately decline to auto-create recovery (e.g. a manual-only failure), but once a
recovery row exists — whether auto-created or operator-created — executing it MUST honor
its stored action.

- A `pending` task with `recovery_origin = resume` and a stored `session_id` MUST, when
  run, **continue that provider session**, including when its branch currently has **no
  commits**. An empty branch MUST NOT downgrade a resume row to a no-op.
- Any launch path that executes a prepared pending recovery row in detached `iterate`
  mode MUST target that recovery row itself as the concrete iterate task. Passing the
  failed parent only as display context while relying on `prepared_task_id` to bypass
  failed-task validation is not authoritative and MUST NOT be required for correctness.
- Such a row MUST NOT be dispatched as a fresh `iterate` that terminates with an `empty` /
  "no remaining commits to land" message. That terminal applies only to work that has no
  remaining action — a resume row with a continuable session always has a remaining action.
- A `pending` task with `recovery_origin = resume` but **no** stored `session_id` is not
  resumable; it MUST be treated as a retry (fresh attempt) or, if nothing is left to do,
  parked — never silently no-op'd.
- A `pending` task with `recovery_origin = retry` MUST start a fresh execution attempt.
- Retry prompts MAY mention the immediate prior attempt id (`based_on`) and offer an
  opt-in log/transcript lookup handle for that attempt, but this context is advisory
  only; the retry must remain executable without consulting prior logs.
- The empty-recovery mootness logic in section 1 (which governs *failed* tasks) MUST NOT be
  used to suppress an explicit pending resume/retry row to a no-op.
- If a detached worker for a prepared pending recovery row dies before claiming the task,
  reconciliation MUST terminalize that recovery row as a failed recovery descendant rather
  than leaving it `pending` forever. That startup abort MUST consume the same bounded
  automatic recovery budget as any other failed recovery attempt.
- A same-action failed recovery descendant that merely consumed one bounded automatic
  attempt MUST count toward that budget first. Automation MAY spend the remaining bounded
  attempt(s) before escalating to shared needs-attention; it MUST NOT park immediately on
  the first same-action startup-abort descendant alone.
- Those transient failed recovery descendants still remain distinct from **real**
  no-progress outcomes. When watch later reevaluates the same selected recovery or improve
  action, an explicit transient terminal such as `PROVIDER_UNAVAILABLE`,
  `RETRYABLE_PROVIDER_ERROR`, `INFRASTRUCTURE_ERROR`, `WORKER_DIED`,
  `WORKSPACE_NOT_POPULATED`, `NO_ACTIVITY`, or a timeout before meaningful execution MUST
  NOT increment the shared
  `watch-no-progress-backstop` streak by itself. Watch may persist retry cooldown for that
  subject/action, but the preserved real no-progress streak remains unchanged until a
  durable no-op or non-transient repeat occurs.
- A bounded automatic recovery attempt starts when automation creates a recovery edge/child
  or adopts an existing explicit recovery descendant for execution.
- A non-terminal recovery descendant MUST NOT leave the original failed subject recoverable
  forever. When the descendant stays non-terminal, watch MUST either reconcile proven
  dead/silent descendant work to terminal failure or park the original failed subject via
  the shared no-progress attention backstop after the configured unchanged-repeat limit.

- Operator wording MUST distinguish **moot terminal no-work** from **terminal no-work but
  resumable failed work**, and MUST distinguish `empty` from `redundant` labels.
- `iterate` MUST check the shared recovery predicate before printing an `empty` /
  `redundant` terminal message, and MUST NOT print that terminal for a pending resume row
  with a continuable session (see §4).
- Failed-task recovery queues and dry-run reports MUST omit moot empty/redundant tasks and
  retain recoverable empty/redundant tasks.
- Failed-task recovery queues and dry-run reports MUST also omit failed tasks whose only
  attached merge unit is inactive via `superseded_by_unit_id != NULL` or
  `state in {dropped, superseded}`.
- `watch`, `advance`, `iterate`, and query/recovery-lane surfaces MUST agree on the same
  recovery decision for the same task.

## Policy knobs

| Knob | Default | Governs |
|------|---------|---------|
| `recovery attempts` | bounded | Automatic resume/retry budget before escalation |
| provider resumability classification | provider-owned | Which failure reasons are resume-capable vs retry-only vs manual |

## Open questions

- **OQ1 — Provider evidence fidelity.** The fail-closed rule assumes step/token capture
  can be incomplete. If providers become fully reliable here, the predicate may be
  tightened later at this single policy point.
