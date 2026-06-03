# Incident: rescue worktree clobbered by a tag-scheduled rebase

- **Date:** 2026-06-02
- **Detected by:** operator (Claude), immediately, when the inline rescue's verified
  edits vanished mid-session
- **Severity:** data loss — verified-but-uncommitted work destroyed; recovered only by
  redo, not by git
- **Status:** diagnosed; fixes filed (pending operator decision); rescue redo pending

## One-line summary

During an inline rescue of **gza-4073**, the rescue worktree held verified but
**uncommitted** edits. Applying a `v0.5.0` tag enrolled the lineage into a live
`gza watch --tag v0.5.0` scope; watch scheduled a rebase (gza-4272) that claimed the
branch via `cleanup_worktree_for_branch(force=True)`, which **bypasses the dirty-changes
guard** and tore down the worktree. The edits were never committed, so git had nothing to
restore.

## Impact

- Verified B1/B3/B4 edits (merge-state classification, lifecycle completion, lineage
  query) destroyed. Branch reverted to the last-good commit `b35aefa5`.
- No bad merge; no other lineage corrupted. The cost was redo + the discovery that a
  force-prune can silently eat uncommitted work.

## Timeline

1. Reset gza-4073's branch to last-good `b35aefa5` in its rescue worktree.
2. Made the B1/B3/B4 edits. They **passed verification** but were only dirty working-tree
   changes — not committed, not stashed.
3. Tagged "this session's work" with `v0.5.0`, including the gza-4073 lineage. A
   `gza watch --tag v0.5.0` was running.
4. The tag enrolled the lineage into the live watch scope. Watch judged the branch needed
   reconciling and scheduled rebase **gza-4272**.
5. The rebase ran `cleanup_worktree_for_branch(force=True)` (`runner.py:5112`) to claim
   the branch. `force=True` skipped the dirty guard and removed the rescue worktree.
6. The uncommitted edits were lost. The branch sat back at `b35aefa5` as if the rescue
   never happened.

## Root cause

Two independent defects that stacked:

### 1. Tag-as-scheduler side effect
Applying a watch-scoped tag does not merely *label* work — it enrolls that work into a
live scheduler's execution scope. "Label this" and "enqueue this for execution" were the
same action, which was not anticipated when tagging an in-flight rescue lineage.

### 2. Force-prune ignores dirty state (data loss)
`cleanup_worktree_for_branch(force=True)` (`runner.py:5112`) bypasses the dirty-changes
guard, violating invariant #5 — "Never destroy work to make progress"
(`specs/behavior/00-overview.md:137`). A force-prune that does not check for uncommitted
work is a data-loss bug regardless of what triggers it; the tagging race in defect 1 just
exposed it. (Other `force=True` teardown sites: `runner.py` 2554 / 5119 / 6458 / 6816.)

## Why it was unrecoverable

The work was never committed — no WIP commit, no stash, no reflog entry for the dirty
tree. git had nothing to restore. The only recovery path is to redo the edits.

## Detection

Immediate: the next operation in the rescue found the verified edits gone and the branch
back at `b35aefa5`. Tracing the watch log showed gza-4272 (rebase) had run and pruned the
worktree.

## Resolution / actions taken

Fixes filed (note: authored by a background agent **without authorization** — pending
operator decision on whether to keep, drop, or untag):

- **gza-4273** (pending, v0.5.0) — implement a worktree **reclaim gate**: refuse on dirty,
  defer on clean+claimed, reclaim only on clean+unclaimed+stale.
- **`specs/behavior/worktree-reclaim.md`** (draft, untracked) — the reclaim-gate behavior
  spec, with open questions on the claim mechanism and the staleness threshold. Wired into
  README / lifecycle-engine / watch-supervisor by uncommitted edits (also unauthorized).

Operational containment:
- Verified no live rescue process; nothing committed except the pending gza-4273 row; the
  branch is stable at `b35aefa5`; watch is off.

## Pending

- **Decide the fate of the unauthorized artifacts** (gza-4273 + the 4 spec edits): keep,
  drop, or untag-out-of-watch-scope.
- **Redo the gza-4073 rescue** (B1 must thread `source_has_commits` so genuinely-landed
  branches stay `merged`), in a clean worktree, with a WIP checkpoint committed before the
  long verify.

## Lessons / operational notes

1. **Commit before you verify.** Any long-running verify in a worktree is a window in
   which a scheduler can reclaim the branch. A WIP checkpoint commit makes the work
   recoverable; a dirty tree is not.
2. **Labeling should not silently schedule execution.** Enrolling work into a live watch
   scope via a tag is a footgun — make the scheduling consequence explicit or separable
   from labeling.
3. **`force=True` is not a license to destroy uncommitted work.** A reclaim/teardown that
   can run unattended must distinguish "branch is free to take" from "branch has live work
   in its worktree," and refuse the latter (invariant #5).
4. **Don't tag in-flight rescue lineages into a running watch.** Until the work is
   committed and the branch is safe, keep it out of any tag-scoped scheduler.
