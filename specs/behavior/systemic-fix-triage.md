# Systemic-fix triage — turning stuck tasks into auto-merge improvements

> **Status: Draft.** This document is the prescriptive contract for how a stuck-task pile
> is converted into systemic fixes: how failure classes are named, how candidate fixes are
> ranked, when a class is already covered, and when automation MUST stop patching a symptom
> and re-diagnose deeper.
>
> Read [00-overview.md](00-overview.md) for the shared model and the human-escalation table,
> [recovery.md](recovery.md) for the failure reasons this policy buckets on, and
> [watch-supervisor.md](watch-supervisor.md) for the cycle-level gate that environmental
> cascade-preventers attach to.

## What this owns

This spec owns the **policy for systemic improvement driven by stuck tasks** — the layer
*above* per-task recovery. It does not decide whether a single task resumes, retries, or
parks (that is [recovery.md](recovery.md)); it decides what *systemic change* would stop a
whole class of tasks from getting stuck in the first place.

- `recovery.md` owns per-task resume/retry/manual decisions and the failure reasons.
- `lifecycle-engine.md` owns the parked reason codes a class is named after.
- This document owns: the **failure-class taxonomy**, the **blast-radius ranking** of
  candidate fixes, the **already-tracked / escalation** rules, and the **`system` tag**
  convention that links a class to the work fixing it.

## Principles

- **S1 — A stuck task is a failed auto-merge.** Systemic triage MUST evaluate "what change
  would let this *class* auto-merge unattended next time", not "how do I hand-merge this
  instance". A manual hand-merge is a stopgap that resolves one instance and teaches the
  system nothing; it MUST NOT be treated as the resolution of a class.

- **S2 — Fixes are ranked by blast radius.** Candidate systemic fixes MUST be ordered by
  leverage: **(1) cascade-preventer** (gates execution on a broken precondition; one fix
  neutralizes a whole batch and its retries) **> (2) auto-merge enabler** (a general rule
  that lets a stuck class clear the gate on its own) **> (3) loop-exit / recovery** (a
  re-entry path so a parked class can make progress). Within a tier, order by the number of
  stuck units the fix would clear.

- **S3 — Environmental cascades are gates, not tasks.** When a precondition outage (e.g. the
  container runtime is down) fails many tasks and their retries together, the fix MUST be a
  cycle-level precondition gate that pauses the whole cycle — recovery lane included — so no
  attempt is spent while the precondition is broken. It MUST NOT be modeled as a scheduled
  task: under scarce slots such a task deadlocks behind a recovery lane that cannot drain.
  Such a cascade MUST be reported as a single root cause, never as one finding per failed
  task.

- **S4 — A class is named by a stable signal, not free text.** Each failure class MUST be
  keyed to a system-emitted reason (the recovery `failure_reason` / planner
  `next_action_reason`), so the same root cause is named identically across triage sessions
  and its recurrence is countable. Ad-hoc per-session labels MUST NOT be used as class
  identity.

- **S5 — Recurrence after a fix means re-diagnose deeper.** A class that recurs *after* a
  fix for it has landed MUST be escalated to re-diagnosis at the cause layer. Shipping
  another narrow guard for the same class is a defect, not a fix: the prior fix addressed a
  symptom one level below the cause. This is the rule that distinguishes systemic
  improvement from an endless micro-patch treadmill.

- **S6 — Already-tracked classes MUST collapse, not re-derive.** Before proposing a fix for
  a class, triage MUST check whether open systemic work already covers it. A class with an
  open covering task MUST collapse to "tracked by &lt;task&gt; — skip" rather than being
  re-analyzed. Re-deriving a known, already-tracked class is wasted effort and the primary
  failure mode this policy removes.

- **S7 — Systemic-fix work is tagged and discoverable.** Every task created to carry a
  systemic fix MUST carry the shared **`system`** tag. The tag is the link between a failure
  class and the work fixing it: the S6 "already tracked?" check and the S5 recurrence check
  both read it. A systemic-fix task MUST also satisfy the active supervisor scope
  (`watch --tag ...`, with the active any-tag vs all-tags matching mode), so it is actually
  executed rather than silently orphaned.

- **S8 — Minimize the human.** The only human action this policy should require is deciding
  *whether* to file proposed fixes (and only in manual mode). Every other state — bucketing,
  dedup, ranking, recurrence detection — MUST be automatable from recorded task state.
  Per-instance hand-merging is explicitly a cost to be designed out, not a supported step.

## Failure-class taxonomy

The class set is keyed to emitted reasons. It is a **single swappable policy point** — new
reasons extend the table; they do not fork the policy.

| Class | Keyed on | Default tier |
|---|---|---|
| environmental-cascade | infra failure reasons (timeout / worker-died / infrastructure-error) clustered with retries in a short window | cascade-preventer |
| verify-unreproducible | no-op-improve where the verify result cannot be reproduced | auto-merge enabler |
| review-no-exit | review-max-cycles / no-progress-backstop / reviewer-wrong with no loop exit | loop-exit / recovery |
| rebase-reconcile-manual | rebase-failed / merge-source needs manual resolution | auto-merge enabler |
| retry-limit-reached | retry budget exhausted | attribute to the upstream cause class |
| manual-unknown | manual-failure-reason with no machine-actionable cause | per-task diagnosis; cluster only when common |
| moot | merge-unit merged / empty / redundant | not systemic — route to per-lineage drop |

## Policy knobs

| Knob | Default | Governs |
|------|---------|---------|
| systemic-fix tag | `system` | The durable cross-skill tag marking system-improvement work (S6, S7). |
| supervisor scope | operator-set (for example `--tag 202606-recovery --tag system`, optionally `--all-tags`) | The active watch scope a filed fix must satisfy so it is executed (S7). |
| recency window | last 24h | How far back stuck rows are gathered before bucketing. |
| triage mode | manual | Whether proposed fixes are presented for confirmation (manual) or filed automatically (auto). |

## Human escalation

| State | Trigger | How a human clears it | What would remove the human |
|---|---|---|---|
| Fix proposed, manual mode | A ranked systemic fix is ready | Confirm to file, or skip | Switch to auto mode once trust in the ranking is established |
| Class escalated (S5) | A class recurred after its fix landed | Re-diagnose at the cause layer and author a deeper fix | Cause-layer diagnosis the system can derive from recurrence evidence (Open question OQ1) |
| Cascade fix needed before retries can pass | An environmental gate is not yet in place | Land the gate (or run it) so retries stop being wasted | The cycle-level gate itself, once it exists (S3) |

## Open questions

- **OQ1 — Recurrence attribution.** Measuring "recurred after the fix landed" is confounded
  by a moving codebase and by new work that manufactures its own stuck-ness. The intended
  signal is a sustained class rate that does not fall after a fix lands; the precise metric
  and window are not yet settled.
- **OQ2 — Scope-tag derivation.** Whether a systemic-fix task should auto-derive the active
  supervisor's scope tag, or always require it to be stated, is unsettled. The conservative
  default (state it explicitly) avoids silently orphaning a fix, at the cost of one input.
