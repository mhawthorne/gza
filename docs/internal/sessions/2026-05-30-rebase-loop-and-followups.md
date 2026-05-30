# Session — 2026-05-30: overnight rebase loop, fixes, and follow-ups

Working session triggered by `gza incomplete` showing ~95 `GIT_ERROR` tasks
overnight. Started as incident triage, branched into perf/observability, branch
divergence handling, and a duplicate-resume discovery. This is the session log;
the focused postmortem is `docs/incidents/2026-05-30-rebase-noop-loop.md`.

## TL;DR

- Diagnosed an overnight loop: ~95 no-op rebase tasks, root cause = a no-op
  rebase mislabeled `GIT_ERROR` + an advance-engine loop with no stop condition.
- Filed fixes; gza-3947 & gza-3948 are **merged**. Cleaned up the 95 dead tasks
  (`gza incomplete` went 64s → 18s).
- Surfaced and filed several follow-ups (observability, scheduler tiers, lineage
  perf, Ctrl-C, divergence auto-reconcile, a lineage display bug).
- Hit a NEW, still-open problem: gza-3850 **double-resumed** into two competing
  lineages. Root cause narrowed to a structural recovery-dedup bug (see Handoff).

## Tasks filed this session

| ID | Type | Tag | Status | What |
|----|------|-----|--------|------|
| gza-3948 | implement | v0.5.0 | **merged** | Bug 1: treat already-up-to-date no-op rebase as success, not `GIT_ERROR` (`rebase_publish.py`) |
| gza-3947 | implement | v0.5.0 | **merged** | Bug 2+3: guard `conflict_needs_rebase` when branch already contains target + per-branch rebase circuit breaker (`advance_engine.py`) |
| gza-3960 | implement | v0.5.0 | **merged** | Suppress Ctrl-C traceback in watch; top-level `KeyboardInterrupt` guard → clean exit 130 |
| gza-3969 | implement | v0.5.0 | done, **unmerged** (improve gza-3975 in flight) | Auto-reconcile local/origin divergence at advance time (force-with-lease → rebase-onto-origin → existing conflict flow); state-based, not task-type-based. No `rebase`→`reconcile` rename. |
| gza-3949 | plan | v0.6.0 | queued (deps gza-3947) | Explicit 3-tier watch scheduler: urgent → recovery → normal, with per-cycle recovery cap |
| gza-3954 | plan | v0.6.0 | queued | Latency observability: decorator auto-instrumentation of store/query layer; metrics facade; Prometheus/OTel `/metrics` for the server; env-gated CLI summary; no unbounded logs |
| gza-3955 | explore | v0.6.0 | queued | Confirm + fix lineage-traversal N+1 / connection churn (the ~18s structural floor) |
| gza-3973 | implement | v0.6.0 | queued | `gza lineage <id>` omits the queried task when it's a recovery-superseded failed task; always render the target |
| gza-3977 | implement | v0.5.0 | queued | **Duplicate-resume bug (handoff #4, confirmed).** Recovery dedup walk drops an explicitly-recorded `recovery_origin="resume"` edge when the resume forks session/branch, because `_classify_recovery_edge` re-validates via `_is_resume_recovery_edge` instead of trusting stored provenance → false-negative `is_chain_resolved_by_recovery` → second resume spawned. Fix: trust stored `recovery_origin`; keep session/branch invariants on the legacy path only. |
| gza-3989 | implement | v0.5.0 | queued | **Auto-reconcile fails on the dead-WIP divergence it was built for.** `_is_benign_gza_rewrite_divergence` (git_ops.py:305) only accepts *symmetric* patch-equivalence, so the real savepoint-finalize case (origin = partial `WIP: gza task interrupted`, local = finalized superset) is rejected → mechanical rebase onto dead WIP → conflict → doomed sandboxed rebase (gza-3987) → GIT_ERROR. Fix: recognize dead-WIP savepoints (shared marker w/ runner.py:2561/2594) as benign → force-with-lease. Secondary: conflict fallback spawns `origin/*`-targeted rebase into a sandbox with no origin ref. Host-side regression tests required. |
| gza-3990 | implement | v0.5.0 | queued | **Watch self-restart on code drift.** `_warn_if_installed_gza_changed` (watch.py:405) only WARNS that the running daemon's code is stale; operator must manually kill+restart. Make it safely re-exec (drain batch → `os.execv`, preserving argv + gza-3960 shutdown contract). Removes a forced babysitting step. |
| gza-3982 | plan | v0.6.0 | queued | **Merge-unit-level supersede/drop.** Corrective actions are task-shaped but the unit of reasoning is the merge unit; abandoning a moot lineage today = N `gza delete`s + re-trigger hazard if the failed owner is missed. Data model already fits: `MergeUnit` is branch-keyed (winner/loser are distinct units, e.g. gza-mu-142 vs gza-mu-140) and `superseded_by_unit_id` exists unused. Design a branch-preserving tombstone op that cascades resolution to recovery + `incomplete`. Complementary to gza-3977. |

## Key diagnoses

- **No-op rebase loop:** `rebase_publish.py:78-85` raised `GitError` when the tip
  didn't move. The branch already contained `main`, so every rebase was a no-op →
  `GIT_ERROR`. In `advance_engine.py`, `rebase_resolution_proved=True`
  (branch-contains-target) suppressed the failed-rebase stop condition
  (`_failed_rebase_still_blocks_advance`), while `can_merge=False` (timed-out
  implement) routed to the catch-all `conflict_needs_rebase` → new rebase every
  ~5 min. `GIT_ERROR` is recovery-manual-only, so this was the advance/watch
  path, not recovery retries.
- **`gza incomplete` latency (~64s warm):** ~78k SQL `execute` (N+1 lineage
  traversal) + ~25k connection open/close (no reuse). Git was minor (~7s).
  Deleting the 95-wide dead lineage dropped it to ~18s.
- **Branch divergence dead-end:** `git.resolve_fresh_merge_source` fails closed on
  ANY local/origin divergence → `merge_source_needs_manual_resolution`. Recurs
  because history-rewriting completion paths (incl. resume finalizing a WIP
  savepoint via amend — same parent, new hash) don't re-publish, so origin goes
  stale. Fix = reconcile on tip divergence (gza-3969).

## Operational actions taken

- Force-with-lease pushed `…-project-2` to reconcile origin (NOTE: this was done
  on a misheard "push it"; see Lessons).
- Deleted 95 dead rebase tasks gza-3852–gza-3946 (kept gza-3850 owner + gza-3851).
- Held then re-released gza-3851 (removed/re-added `v0.5.0`) around the fix merges.
- Committed the incident postmortem.
- **(2nd session, same day) Dropped the loser unit gza-mu-140** — `set-status
  dropped` on gza-3851 + gza-3961 + gza-3962 (whole unit, owner included; branches
  preserved, reversible). Confirmed gza-3967 is now the sole remaining
  monorepo-project row in `gza incomplete`.

## Open loose ends / Handoff

1. **Pick the winner between two competing lineages for gza-3850 — DONE.**
   - Winner kept: **gza-3967** (merge unit gza-mu-142, branch `…-project`).
   - Loser dropped as a unit: gza-3851 → gza-3961 → gza-3962 (gza-mu-140, branch
     `…-project-2`). Verified distinct merge units (branch-keyed), so the drop did
     not touch the winner or the shared root gza-3850. Triage classification +
     drops recorded under Operational actions.
2. **Land the winner** gza-3967: **gza-3969 merged AND watch restarted (20:00 UTC,
   has the fix) — but auto-reconcile STILL failed** on this branch. It spawned
   rebase gza-3987 → `GIT_ERROR`. Root-caused (see below); fix is gza-3989. The
   winner stays blocked until gza-3989 lands + watch reloads. Do NOT hand-push.
3. **gza-3969 reconcile is BROKEN for the dead-WIP case → gza-3989 (v0.5.0).**
   Correction to an earlier wrong read in this doc: the reconcile does NOT run in
   the sandbox — `_reconcile_diverged_branch_with_origin` (git_ops.py:129) does
   force-with-lease and even the mechanical rebase host-side. The actual bug is
   `_is_benign_gza_rewrite_divergence` (git_ops.py:305) requiring *symmetric*
   patch-equivalence; the savepoint-finalize divergence (origin = partial
   `WIP: gza task interrupted` @505284a0, local = finalized @6ca79b68, merge-base
   62c36e48) is asymmetric → rejected → mechanical rebase onto the dead WIP →
   conflict → sandboxed rebase gza-3987 → GIT_ERROR (sandbox also can't reach
   origin = secondary defect). Verified by reproducing the divergence + reading the
   code locally.
3b. **Watch only warns on code drift → gza-3990 (v0.5.0).** `watch.py:405` detects
   its own stale code but only logs "restart watch to pick up new code"; the manual
   kill+restart is a babysitting step to eliminate via self-re-exec.
4. **Duplicate-resume root cause — CONFIRMED, filed as gza-3977 (v0.5.0).**
   Verified against the live DB and `recovery_engine.py`. The walk
   (`_build_recovery_chain_snapshot`, lines 259-331) drops the gza-3850→gza-3851
   edge at line 292 because `_classify_recovery_edge` (229-238) re-validates a
   stored `recovery_origin="resume"` edge through `_is_resume_recovery_edge`
   (179-195), which demands same session AND branch. **Correction to the original
   guess:** the resume forked **both** session (`…460f7`→`…e96b3a`) and branch
   (`…project`→`…project-2`); the **session check (189-190) rejects the edge first**
   — the branch check (193-194) is never reached, so a branch-only relaxation would
   NOT fix it. gza-3851 dropped → gza-3961 unreachable →
   `completed_terminal_descendant is None` → `is_chain_resolved_by_recovery(gza-3850)`
   False → `list_failed_tasks_for_recovery` (646) keeps gza-3850 → second resume
   gza-3967 spawned. Fix direction (in gza-3977): trust stored `recovery_origin` as
   authoritative; reserve the session/branch invariants for the legacy
   (NULL-provenance) path (`_classify_legacy_recovery_edge`). Upstream question of
   *why* a resume forked session+branch is noted out-of-scope for that task.

## Lessons / process notes

- **Don't act on terse imperatives for outward/irreversible actions.** The user
  dictates via speech-to-text (words get cut); "push it" was a rhetorical point
  about system behavior, not a command — but a force-push to GitHub was done.
  Confirm first. (Saved to memory.)
- **Verify lineage state before asserting.** I claimed gza-3961 was progressing to
  review cleanly; it wasn't (rebase didn't unblock; a second lineage existed).
  Two wrong reads before checking the DB/git directly.
- **System fixes over manual fixes** remains the preference: the right output of
  "why is X manual?" is a fix to make X automatic, not Claude hand-fixing it.
