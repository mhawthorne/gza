# Worktree reclaim — workspace acquisition contract

> **Status: Draft — under discussion.** This document is the prescriptive contract for how
> a code work unit acquires its isolated workspace (git worktree) when it starts, and the
> conditions under which an existing worktree may be reclaimed to make room. Parts of it
> carry **Open questions** that are not yet settled; those are marked inline and MUST NOT
> be treated as ratified contract.
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
  existing worktree be removed, and when MUST the task defer instead?
- How does a work unit that is actively using a worktree protect it from concurrent
  reclaim?
- What happens to a human (or another agent) whose work would otherwise be discarded?

## Governing invariant

This contract is the local application of overview invariant
**"Never destroy work to make progress"**: the engine MUST NOT delete branches and MUST NOT
discard uncommitted work; cleanup is an operator concern. Workspace acquisition is the most
common place that invariant is at risk, because making room for a new task is exactly a
"make progress" pressure that can motivate deleting someone else's worktree.

## The reclaim gate

When a code work unit starts and a worktree already exists for its branch (at the intended
path or any other path), the runner MUST classify that worktree before touching it. The
classification has three outcomes, evaluated in order:

1. **Dirty — has uncommitted or untracked changes.**
   The worktree MUST NOT be removed to make room for the starting task. The starting task
   MUST defer and surface the conflict for a human, rather than reclaiming. A dirty
   worktree is presumed to hold unsaved work whose value the system cannot judge; destroying
   it is never an acceptable cost of starting a task. This holds regardless of *what* is in
   the worktree or *who* put it there.

2. **Clean but claimed — a live work unit is using it.**
   The worktree MUST NOT be reclaimed. The starting task MUST defer until the claim is
   released. A worktree is "claimed" when a work unit has marked it in use (see *The claim
   primitive*). Reclaiming a claimed-but-clean worktree would interrupt active work and is a
   different shape of the same harm as discarding dirty work.

3. **Clean, unclaimed, and stale — genuinely abandoned.**
   The worktree MAY be reclaimed automatically. This is the leftover-from-a-finished-or-
   crashed-run case: no unsaved work, no live owner. Auto-reclaiming it is required so the
   system does not accumulate dead worktrees or escalate to a human for every stale
   leftover — escalating on abandoned workspaces would violate the goal of minimizing human
   involvement.

The asymmetry is deliberate: the default answer to "may I remove this worktree?" is **no**,
and it flips to **yes** only on positive evidence of abandonment (clean *and* unclaimed
*and* stale). Absence of evidence is treated as "in use," not "free."

## The claim primitive

A work unit actively using a worktree MUST be able to mark it claimed so that a concurrent
task start classifies it as case 2 (defer) rather than case 3 (reclaim). Without such a
mark, a clean worktree held by an active-but-idle session is indistinguishable from an
abandoned one, and the reclaim gate cannot protect it.

A claim MUST be releasable — on normal completion, on failure, and on a human's explicit
request — so that a crashed owner does not lock a worktree forever. A claim whose owner is
no longer live MUST NOT keep a worktree out of the reclaimable set indefinitely; staleness
detection (below) is what reconciles a claim left behind by a dead owner.

> **Open question — claim mechanism.** Whether a claim is recorded as a lockfile inside the
> worktree, as a reserved tag / state on the owning task row, or as a live-process
> registration is not yet settled. The requirement is the *behavior* (an active owner can
> reserve a worktree; reservations are releasable and cannot outlive a dead owner), not the
> storage.

## Staleness — distinguishing abandoned from active

The hard case is a **clean** worktree with no obvious live owner: is it abandoned (case 3)
or held by an active session that simply has not written to disk recently (case 2)? The
contract requires positive evidence of abandonment before auto-reclaim, but does not yet
fix the exact signal.

> **Open question — staleness signal.** What positively proves a clean, unclaimed worktree
> is abandoned (e.g. no live owning process, no recent git or filesystem activity within a
> bound, owning task in a terminal state) is not yet settled. Until it is, the conservative
> reading applies: when abandonment cannot be positively established, treat the worktree as
> in use and defer.

## Policy knob

- **`worktree_reclaim_requires_human_when_dirty`** (default: **on** — hold for a human).
  When on, a dirty worktree blocking a task start is escalated to a human rather than
  reclaimed (case 1). This is a single, named, swappable policy point. The default is
  conservative — never trade unsaved work for task progress — but it is a starting position,
  not a goal: a future signal that proves dirty content is disposable could flip it.

## Human-escalation

| Trigger | How a human clears it | Automation that would remove the human |
|---------|----------------------|----------------------------------------|
| A starting task needs a worktree whose existing worktree is **dirty** (case 1). | Commit, discard, or relocate the changes, then re-run; or release the worktree. | A claim primitive plus a reliable staleness signal lets the system reclaim genuinely-abandoned clean worktrees automatically, leaving only true unsaved-work conflicts for a human. |
| A starting task needs a worktree that is **claimed** by a live work unit (case 2). | None usually required — the starting task defers and proceeds once the claim releases. A human intervenes only if the claim is stuck (owner died without releasing). | Releasable claims that cannot outlive a dead owner remove the stuck-claim case entirely. |

## Open questions (summary)

- **Claim mechanism** — lockfile vs. reserved task state vs. process registration.
- **Staleness signal** — what positively proves a clean, unclaimed worktree is abandoned.
- **Scope of "code work unit"** — whether detached/ephemeral worktrees (e.g. a review
  checkout that produces no branch commits) participate in the same gate or are exempt
  because they hold no reclaimable work.

---

*Implementation note (non-normative): at the time of writing, workspace acquisition happens
in the task runner's code-task worktree setup, which force-removes any existing worktree for
the branch before re-adding it. That force-removal bypasses the runner's own
uncommitted-changes guard and is the conformance gap this spec exists to close. The runner
is the single home for the reclaim gate because every caller — watch, manual `gza work`,
inline runs, recovery — reaches worktree acquisition through it.*
