# Behavior specs

This file is the index for `specs/behavior/`. It lists what each file owns and the writing
convention for the set. It does not define behavior.

## Reading order

Read [00-overview.md](00-overview.md) first. Then read the subsystem file that owns the
question you are answering.

## Writing convention

Every normative statement uses [RFC 2119](https://www.rfc-editor.org/rfc/rfc2119)
keywords:

- **MUST / MUST NOT** — invariant. A violation is a bug, always.
- **SHOULD / SHOULD NOT** — strong default. Deviation requires a stated reason.
- **MAY** — permitted, not required.

Additional rules for authors:

- **Intent first, mechanism second.** Describe *what must be true* and *why*. Refer to
  implementation (function names, file paths, column names) only in clearly marked
  *Implementation note* asides, never in the normative text. The normative text must
  survive a rewrite in another language unchanged.
- **Name policy knobs explicitly.** Where a threshold or a "hold for a human" default
  was chosen conservatively, say so, give it a name, and state that it is a single
  swappable policy point. Conservative defaults are a starting position, not the goal.
- **Minimizing human involvement is the goal.** Every state that requires a human is a
  cost. Each such state MUST name the trigger, how a human clears it, and — where
  known — what automation would let us remove it.
- **Mark genuine open questions.** When the intended behavior is not yet settled, write
  an **Open question** rather than inventing a contract or copying current behavior.
- **Status banner.** Each doc carries a status line distinguishing *agreed contract*
  from *draft / under discussion*.

## Contents

| File | Owns |
|------|------|
| [00-overview.md](00-overview.md) | Shared model for the whole set: vocabulary, system-wide invariants, lifecycle diagram, and the consolidated human-escalation table. |
| [lifecycle-engine.md](lifecycle-engine.md) | Engine-only decision rules: ordered rule set, policy knobs, parked reason codes, and ratified decisions. |
| [main-verify-self-heal.md](main-verify-self-heal.md) | North-star convergence contract for red local-target integration verify: rerun-before-reuse, bounded red lifetime, deterministic-red repair, and no merge-stall-to-launch-stall cascade. |
| [lineage.md](lineage.md) | Task-graph operations: dependency satisfaction, owner/merge-unit resolution, latest-node resolution, and recovery-target attachment. |
| [recovery.md](recovery.md) | Failed-task recovery policy: moot vs recoverable terminal no-work (`empty`/`redundant`) states, resume vs retry vs manual stop. |
| [watch-supervisor.md](watch-supervisor.md) | Runtime loop contract for `gza watch`: cycle order, slot accounting, adoption, restart, and stop/backoff behavior. |
| [worktree-reclaim.md](worktree-reclaim.md) | Worktree acquisition and reclaim rules at task start. |
| [systemic-fix-triage.md](systemic-fix-triage.md) | Turning the stuck-task pile into systemic auto-merge fixes: failure-class taxonomy, blast-radius ranking, already-tracked/escalation rules, and the `system` tag convention. |
