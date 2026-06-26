# Worktree reclaim — workspace acquisition contract

> **Status: Draft.** This document is the prescriptive contract for how a code work unit
> acquires its isolated workspace (git worktree) when it starts, and the single condition
> under which an existing worktree may be reclaimed to make room. The core model (reclaim a
> clean worktree, never a dirty one) is settled; it is governed by one named policy knob.
>
> Read [00-overview.md](00-overview.md) for the lifecycle invariants this spec applies —
> in particular invariant **"Never destroy work to make progress."** This document does
> not restate that invariant; it specifies the worktree-acquisition mechanics that honor
> it.

## What this models

Every code work unit (implement, improve, rebase, fix, and any other task that produces
commits) runs in an isolated git worktree checked out to its branch. Acquiring that
workspace is a step of **task start**, performed by the task runner — it is *not* owned by
the watch supervisor. Watch is only one of several callers that drive a task into the
runner; manual runs, inline runs, and recovery all reach the same acquisition path. A
contract that lived in the supervisor would therefore miss most of the ways a worktree gets
created or reclaimed.

This document answers:

- When a task needs a workspace for its branch and a worktree already exists, when may that
  existing worktree be removed, and when MUST the task fail and surface the conflict to a
  human instead?
- What happens to a human (or another agent) whose uncommitted work would otherwise be
  discarded?

## Governing invariant

This contract is the local application of overview invariant
**"Never destroy work to make progress"**: the engine MUST NOT discard uncommitted or
untracked work to make room for a starting task. Workspace acquisition is the most common
place that invariant is at risk, because making room for a new task is exactly a "make
progress" pressure that can motivate destroying an existing worktree.

This spec exists because a real incident violated that invariant: a tag-scheduled rebase
claimed a branch whose worktree held verified-but-uncommitted rescue edits, force-removed
the worktree, and lost the work (see [the incident
report](../../docs/incidents/2026-06-02-rescue-worktree-clobbered.md)). The contract below
is the backstop that makes that outcome impossible regardless of *which* scheduler trigger
comes to claim a branch.

## Cross-task worktree-registration isolation

The canonical repository's worktree registration is shared state. It records which
worktrees exist for which branches and therefore determines whether another task can start,
reclaim, or fail closed on a branch conflict. Because that registry directly controls task
ownership and reclaim decisions, git operations performed inside a provider or agent context
MUST NOT be able to mutate the registration of some other in-progress task's worktree.

This invariant is narrower than the reclaim gate:

- The host task context MAY still reclaim or fail on the **same branch** according to the
  gate below, because that is the governed worktree-acquisition path.
- Provider or agent git activity MUST be isolated so that pruning, checkout cleanup, or
  other git housekeeping done for one task cannot unregister, prune, or otherwise mutate a
  sibling task's live worktree registration.
- If a task needs agent-side git, it MUST run in a git context whose registry is private to
  that task rather than against the canonical shared worktree registry.
- Provider containers MUST NOT expose the canonical checkout's shared `.git` as an ambient
  gitdir path. Mutating provider-side git commands MUST be scoped to the prepared task
  workspace and fail closed when launched from unrelated provider directories or when
  gitdir/worktree-targeting environment overrides resolve outside the prepared task pair.
  From a non-workspace cwd, `--work-tree /workspace` alone MUST NOT be treated as sufficient
  pinning; the command MUST either run via `git -C /workspace ...` or provide the prepared
  gitdir/worktree pair explicitly so git cannot infer a mounted metadata directory from
  `PWD`. Any temporary host metadata rewrite used to prepare that task pair MUST be
  restored immediately after provider execution leaves the runner/provider boundary, before
  host-side completion, WIP capture, timeout checkpointing, or other host-owned git
  bookkeeping runs.
- Automation MUST check the configured canonical checkout after provider execution and
  during watch passes. If it is no longer on the expected default branch, it MAY restore the
  expected branch only when there are no tracked changes; with tracked changes present it
  MUST leave the checkout untouched and surface `canonical-main-checkout-hijacked`.

The goal is structural, not advisory: "another task's worktree disappeared from git's
registry" MUST be impossible as a side effect of provider-side git for a different task.

## The reclaim gate

When a code work unit starts and a worktree already exists for its branch (at the intended
path or any other path), the runner MUST classify that worktree before touching it. There
are exactly two outcomes:

1. **Dirty — has uncommitted or untracked changes.**
   The worktree MUST NOT be removed. The starting task MUST **fail** and surface the
   conflict for a human (see *Failure semantics*). A dirty worktree is presumed to hold
   unsaved work whose value the system cannot judge; destroying it is never an acceptable
   cost of starting a task. This holds regardless of *what* is in the worktree, *who* put it
   there, or how the policy knob below is set. **Dirty → fail is an invariant, not a tunable
   — there is no configuration that permits reclaiming a dirty worktree.**

2. **Clean — no uncommitted or untracked changes.**
   The worktree holds no unsaved work; everything of value is committed on the branch and
   safe. Whether the runner reclaims it automatically or fails for a human is governed by
   the **`worktree_auto_reclaim_clean`** policy knob (below):
   - knob **on** (default): the runner MUST reclaim the clean worktree automatically and
     proceed. This is the leftover-from-a-finished-or-crashed-run case; escalating on every
     such harmless leftover would force needless manual cleanup and violate the goal of
     minimizing human involvement.
   - knob **off**: the runner MUST fail and surface the conflict, exactly as for a dirty
     worktree. An operator who wants full control over every existing worktree selects this.

The asymmetry is deliberate and narrow: the *only* thing that can be auto-reclaimed is a
**clean** worktree, and only when the knob permits it. Anything with unsaved work is off
limits, always.

> **Note — foreign worktrees (outside managed roots).** A worktree that lives outside the
> directories gza manages for task workspaces is never reclaimed by this gate, clean or not.
> A worktree a human created by hand in some other location is theirs; the runner MUST refuse
> to remove it and fail instead. (The implementation already enforces this via a
> permitted-roots check; see the implementation note.)

## Failure semantics

When the gate decides a starting task MUST fail (case 1, or case 2 with the knob off), the
failure MUST be legible and actionable, not a silent stall:

- The task MUST be marked **failed** with a dedicated reason that names the conflict (e.g.
  a worktree-conflict reason code) and the offending worktree path — *not* left stuck
  `in_progress` to be reconciled later by dead-process or stuck-timeout detection.
- The failure MUST surface to the operator as a **needs-attention** item through the normal
  watch attention path, so the operator sees "this task needs a worktree that already exists
  — investigate" rather than an opaque crash.
- The failure MUST NOT be auto-retried as if transient. An existing-worktree conflict only
  clears when a human acts (commits, discards, relocates, or removes the other worktree);
  retrying before then just fails again and burns the retry budget. It is terminal until the
  human clears it.

## Auditability

A reclaim is a destructive operation and MUST leave a record. Whenever the runner removes an
existing worktree (case 2, knob on) or refuses to (case 1, or knob off), it MUST log enough
to reconstruct the decision after the fact: the branch, the worktree path, whether it was
clean or dirty, and the action taken (reclaimed / failed). The motivating incident was hard
to diagnose precisely because the destructive removal emitted nothing; a reclaim that cannot
be traced is itself a defect.

Workspace acquisition failures that happen before provider execution starts MUST also leave
a canonical task ops-log outcome before the task is marked failed. This includes branch
resolution failures, code-task worktree creation failures, and detached non-code worktree
setup failures. The outcome MUST include the exact setup/git failure text, the canonical
failure reason, and setup-phase metadata so operators can distinguish "claimed but never
acquired a workspace" from "provider ran and failed." A direct manual run and a supervisor
run MUST use the same logging path.

## Policy knob

- **`worktree_auto_reclaim_clean`** (default: **on**).
  Governs the **clean** case only (case 2). When **on**, a clean existing worktree blocking a
  task start is reclaimed automatically. When **off**, any existing worktree — even a clean
  one — makes the task fail for a human. This is a single, named, swappable policy point.
  The default is **on** because a clean worktree has no unsaved work to lose, so auto-clearing
  it is safe and avoids forcing manual cleanup of harmless leftovers.

  Note that this knob does **not** govern the dirty case: a dirty worktree fails regardless.
  There is deliberately no knob that flips dirty-reclaim on, because making "destroy unsaved
  work" a one-line configuration is exactly the footgun the motivating incident exposed.

## Human-escalation

| Trigger | How a human clears it | Automation that would remove the human |
|---------|----------------------|----------------------------------------|
| A starting task needs a worktree whose existing worktree is **dirty** (case 1). | Commit, discard, or relocate the changes, then re-run; or remove the worktree. | None desired — by invariant this MUST involve the human; the value of unsaved work is theirs to judge. The automation already in place is the default-on auto-reclaim of *clean* worktrees, which keeps harmless leftovers from ever reaching this row. |
| A starting task needs a worktree that already exists and is **clean**, with `worktree_auto_reclaim_clean` **off** (case 2, strict mode). | Remove the worktree, then re-run; or set the knob on. | Setting `worktree_auto_reclaim_clean` on (the default) removes this row entirely — clean leftovers are reclaimed automatically. |

## Open questions

- **Scope of "code work unit"** — whether detached/ephemeral worktrees (e.g. a review
  checkout that produces no branch commits) participate in the same gate or are exempt
  because they hold no reclaimable work.
- **Self-resume on the task's own worktree** — when a task resumes and the existing worktree
  is its *own*, a dirty tree is the task's own in-progress work. Case 1 currently fails it to
  a human like any other dirty conflict; whether a resuming task should instead *adopt* its
  own dirty worktree (rather than escalate) is not yet settled. Until it is, the conservative
  reading applies: dirty → fail, even for self-resume.

## Deliberately not required (yet)

An earlier draft of this contract proposed a **claim primitive** (an active session marks a
worktree "in use" so a concurrent task start defers rather than reclaims) and a **staleness
signal** (positive proof that a clean, unclaimed worktree is abandoned before auto-reclaim).
Both are intentionally **dropped** from the core contract in favor of the simpler clean/dirty
gate.

The accepted tradeoff: a worktree that is clean *at the instant of classification* but held
by a live session (e.g. an operator editing a branch between commits while a scheduled task
starts on it) MAY be reclaimed out from under that session. This is a recoverable
interruption, **not** data loss — a clean tree has nothing uncommitted to lose, and committed
work remains safe on the branch. The claim/staleness machinery existed only to close that
non-destructive gap, at the cost of a whole lockfile/staleness subsystem. If that margin is
ever wanted, it is an additive feature layered on top of this gate, not a change to it.

---

*Implementation note (non-normative): at the time of writing, workspace acquisition happens
in the task runner's code-task worktree setup (`_setup_code_task_worktree` in `runner.py`),
which calls `cleanup_worktree_for_branch(..., force=True)` and bare
`git.worktree_remove(..., force=True)` to clear any existing worktree before re-adding. The
`force=True` path bypasses the runner's own uncommitted-changes guard
(`cleanup_worktree_for_branch` raises on a dirty worktree only when `force=False`), and three
of the teardown call sites invoke `git.worktree_remove` directly, bypassing even the
foreign-worktree permitted-roots check. There is more than one teardown site — at the time of
writing, `runner.py` lines 2554, 5112, 5119, 6458, and 6816 — so the gate is NOT a single
chokepoint today. Closing the conformance gap means routing **every** worktree teardown
through one guarded reclaim helper that applies this gate (dirty → fail with a legible reason;
clean → reclaim or fail per `worktree_auto_reclaim_clean`; foreign → refuse), rather than
hardening any one call site. The runner is the right home because every caller — watch,
manual `gza work`, inline runs, recovery — reaches worktree acquisition through it.*
