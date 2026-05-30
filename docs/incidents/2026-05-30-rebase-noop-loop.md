# Incident: overnight no-op-rebase loop (95 GIT_ERROR tasks)

- **Date:** 2026-05-30 (loop ran ~06:17–14:56 UTC, ~8.5h)
- **Detected by:** operator running `gza incomplete` the next morning
- **Severity:** low user impact (no data loss, no bad merge), but ~8.5h of wasted
  automated work, queue/DB pollution, and a measurable latency regression
- **Status:** diagnosed; fixes filed; cleanup pending

## One-line summary

A timed-out implementation task left a branch that already contained `main`. The
`gza watch` advance engine repeatedly prescribed a rebase for that
already-rebased branch; each rebase was a no-op, which the rebase code
mislabeled as `GIT_ERROR`, and nothing stopped the cycle — so ~95 identical dead
rebase tasks accumulated overnight, one every ~5 minutes.

## Impact

- 95 dead `rebase` tasks (`gza-3852`–`gza-3946`), all failed `GIT_ERROR`.
- Queue/DB pollution and a wide dead lineage that materially slowed
  `gza incomplete` / `gza watch` startup (see Performance side effect).
- No code was lost, nothing incorrect was merged. The underlying implementation
  task still needs to be resumed.

## The lineage

```
gza-3850  implement  failed (TIMEOUT)   spec: specs/monorepo-project-boundary.md
                                        branch: 20260530-implement-the-design-in-specs-monorepo-project
└── gza-3851  implement [resume]  pending   (recovery_origin=resume, trigger=auto-recovery; never ran)
    ├── gza-3852  rebase  failed (GIT_ERROR)   based_on gza-3851, trigger=manual, recovery_origin=NULL
    ├── …  (one new rebase ~every 5 min)
    └── gza-3946  rebase  failed (GIT_ERROR)
```

Git state at time of incident:
- branch tip `2e5f3f96` = "WIP: gza task interrupted"
- `main` tip `9596356b` = "Merge branch 'agent-sessions'"
- `merge-base(main, branch) == main` → **main is an ancestor of the branch**; the
  branch is `main` + 1 WIP commit. There was nothing to rebase.

## Root cause

A chain of three independent defects, the first two of which interlock into an
infinite loop:

### 1. A no-op rebase is mislabeled `GIT_ERROR`
`src/gza/rebase_publish.py:78-85` raises `GitError` whenever the branch tip does
not move after a rebase (`previous_sha == local_sha`). For a branch that already
contains the target, the rebase is correctly a no-op ("already up to date") — a
*success* in plain git — but it was scored as a hard error.

### 2. The advance engine's loop stop-condition is suppressed exactly when the branch is already rebased
In `src/gza/advance_engine.py`:
- `resolve_post_merge_rebase_state` sees `is_ancestor(main, branch) == True` and
  returns `rebase_resolution_proved=True` (reason `"branch-contains-target-tip"`,
  ~line 392-402).
- `_failed_rebase_still_blocks_advance` short-circuits to `False` on that flag
  (~line 1001-1005), so the rule that would raise a manual "needs_discussion" and
  STOP the loop (`conflict_rebase_failed`, ~line 1671) never fires.
- Meanwhile `can_merge` is independently `False` (the lineage is not mergeable:
  the implement timed out, the resume never ran, no approved review).
- With the stop-condition suppressed and no *completed* rebase, evaluation falls
  through to the catch-all `conflict_needs_rebase` (~line 1687,
  `matches=lambda ctx: not ctx.can_merge`), which spawns another rebase. Every
  watch cycle. Forever.

Net: the branch was blocked from merging for a reason that rebasing cannot fix,
but the engine kept prescribing a rebase — the one action guaranteed to be a
no-op — and the no-op was mislabeled an error (defect 1).

### 3. No circuit breaker
Nothing capped repeated identical rebase failures. 95 instant failures carrying
zero new information, and the loop never tripped a breaker.

## Why it wasn't auto-recovery

`GIT_ERROR` is in `recovery_engine._MANUAL_ONLY_REASONS`, so the recovery engine
did **not** retry these (correctly). Confirmed by the data: the 95 tasks have
`recovery_origin = NULL` and `trigger_source = manual`. The loop came from the
**advance/watch path** (`conflict_needs_rebase`), not from recovery retries.

## Cadence / scale

One rebase ~every 5 minutes (06:17:52 → 14:56:07 UTC), matching the `gza watch`
poll interval. Each task was created/started/completed within the same second
(instant no-op failure).

## Performance side effect (discovered during triage)

`gza incomplete` runs ~60–66s warm on the current DB (vs an operator memory of
10–20s). Profiling (`cProfile`):
- `sqlite3 execute`: ~77,982 calls / ~48s — an **N+1** in lineage-tree
  construction (`query.build_lineage_tree` → `store.get_lineage_children` ~9,547
  calls; `recovery_engine._is_resolved_by_landed_lineage` rebuilds trees ~155×).
- `sqlite3 connect`+`close`: ~25,130 each / ~5s — `SqliteTaskStore` opens a fresh
  connection per method (72 `with self._connect()` sites; no reuse).
- git subprocess: 287 spawns / ~7s — secondary; **not** the bottleneck.

The 95-wide dead lineage under `gza-3851` amplifies the repeated tree traversal,
so the incident also degraded `incomplete`/`watch` latency. There is currently
**no latency instrumentation** in the data/query layer — the cause was only found
by attaching a profiler manually.

## Detection

Operator ran `gza incomplete` and saw the `gza-3850` lineage row with a long
`unresolved:` list of `GIT_ERROR` rebase IDs.

## Resolution / actions taken

Fixes filed as gza tasks:
- **gza-3948** (implement, v0.5.0, urgent) — Fix defect 1: treat an
  already-up-to-date no-op rebase as success, not `GIT_ERROR`
  (`rebase_publish.py`).
- **gza-3947** (implement, v0.5.0, urgent) — Fix defects 2 & 3: guard
  `conflict_needs_rebase` so it does not fire when the branch already contains the
  target, route to the real blocker, and add a per-branch rebase-attempt circuit
  breaker (`advance_engine.py` + watch path).
- **gza-3949** (plan, v0.6.0, depends_on gza-3947) — design an explicit 3-tier
  watch scheduler (urgent → recovery → normal) with a per-cycle recovery cap;
  arose from discussing how the resume should be ordered vs new work.
- **gza-3954** (plan, v0.6.0) — latency observability for the data/query layer
  (decorator auto-instrumentation, metrics facade, Prometheus/OTel `/metrics`
  endpoint for the always-on server, env-gated CLI summary; no unbounded logs).
- **gza-3955** (explore, v0.6.0) — confirm the lineage-traversal amplification and
  scope the durable fix (connection reuse, batched/CTE traversal, memoization,
  exclude terminal/dropped leaves).

Operational:
- `gza-3851` (the pending resume) was **held** by removing its `v0.5.0` tag so the
  tag-scoped watch cannot pick it up prematurely. It must only be resumed AFTER
  gza-3948/gza-3947 are merged AND watch is restarted on the fixed code (defect 2
  lives in the watch daemon, which loads code at startup — merging alone does not
  activate the fix). If the resume runs before `main` advances, its branch is
  still a no-op rebase and (under the unfixed daemon) the loop recurs.

## Pending cleanup

Drop the 95 dead rebase tasks `gza-3852`–`gza-3946`. This is safe and does NOT
lose the "must resume" signal: a failed task is only considered resolved when its
recovery chain ends in a *completed* task (`is_chain_resolved_by_recovery`), and
`gza-3851` is only *pending* — so `gza-3850` (TIMEOUT) remains unresolved on its
own and `gza incomplete` still surfaces "Resume failed task (TIMEOUT)". Keeping a
leaf would be counterproductive: a lingering `GIT_ERROR` row signals "rebase needs
manual resolution" (the wrong action) rather than "resume."

## Lessons / operational notes

1. **A no-op is not a failure.** Verification proxies ("did the tip advance?")
   must distinguish "already in the desired state" from "failed to reach it."
2. **Self-healing loops need a stop-condition that can actually fire**, plus a
   circuit breaker independent of the specific bug. Here the stop-condition was
   suppressed by the very state (already-rebased) that triggered the loop.
3. **Prescribe the action that addresses the real blocker.** The branch couldn't
   merge because the implementation wasn't done — rebasing could never fix that.
4. **Merging a daemon fix ≠ activating it.** Long-running processes (`watch`) load
   code at startup; restart after merge.
5. **Instrument the hot path.** With no latency metrics, a 4× regression and an
   N+1 went unnoticed until a manual profile. (Tracked: gza-3954.)
6. **Dead lineages aren't free.** Runaway task creation also degrades read paths
   that traverse lineages.
