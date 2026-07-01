# gza advance

> **Status: Implemented** — This spec describes the current behavior of `gza advance` as of 2026-04-12.

## Overview

`gza advance` is the main orchestration command. It now collects one owner-keyed lineage row set, determines the next action for each owner row, and executes those actions (spawning workers, merging, etc.). It is designed to be idempotent and safe to run repeatedly.

## Scope note (gza-956)

The shared rule engine introduced for `advance` is also the decision source for `iterate` (`determine_next_action` in `src/gza/cli/advance_engine.py` wraps the same `evaluate_advance_rules()` chain). Keeping both commands on one rule evaluator is intentional to preserve the project learning: avoid diverging procedural forks between lifecycle commands.

As a result, this change set includes iterate-facing contract alignment where needed (status wording, help text, and regressions) in the same patch as the engine migration, rather than splitting into a separate task with duplicated decision logic changes.

`uv run gza unstick --run` follows the same principle. It does not own a second
executor. After clearing parked state, it reuses watch's scoped one-shot dispatch helper
for only the selected owners, so slot ceilings, launch permits, recovery routing, and
worker-consuming-vs-direct action accounting stay identical across `watch` and
`unstick --run`.

Manual `gza merge` has one deliberate operator-only escape hatch that `advance`/`watch`
do not share: it can defer review blockers into urgent PR-required follow-up tasks when a
human explicitly invokes merge (or when the latest blocked review is verify-only). The
automated lifecycle remains stricter and does not merge `CHANGES_REQUESTED` reviews by
deferring blockers.

## Usage

```bash
uv run gza advance                        # Advance all eligible tasks
uv run gza advance <task-id>              # Advance a specific task
uv run gza advance --batch N              # Limit to N concurrent worker spawns
uv run gza advance --batch N --new        # Fill remaining batch slots with pending tasks
uv run gza advance --type plan            # Only advance plan tasks
uv run gza advance --type implement       # Only advance implement tasks
uv run gza advance --dry-run              # Show plan without executing
uv run gza advance --no-resume-failed     # Skip recovery-only failed work; keep lifecycle merges/reviews
uv run gza advance --max-resume-attempts N
uv run gza advance --max-review-cycles N
uv run gza advance --squash-threshold N
```

## Task Collection

Advance collects owner rows from one shared source:

1. **Lineage owner rows**: `src/gza/lineage_query.py::query_lineage_owner_rows(...)` materializes one in-memory lineage snapshot from `store.get_all()`, groups tasks by branch/merge ownership, evaluates one lineage-resolved predicate, and returns one canonical row per unresolved owner. The same owner-row query now feeds `gza incomplete`, `gza advance`, and the `gza watch --restart-failed` recovery queue. Same-branch orphan rebase descendants stay available for the explicit `no-descendant-on-the-impl-branch` attention signal, but they are excluded before lifecycle action selection so they cannot be planned for merge/rebase/review work.

2. **Completed lifecycle work inside the owner row**: Completed `merge_status='unmerged'` compatibility rows, merge-unit-backed unmerged work, and completed plan/explore sources all surface through the row's `lifecycle_action_task` and `next_action`. Merge lifecycle collection is merge-unit scoped: each active unit contributes at most one lifecycle owner candidate, and merge execution attributes provenance back to the unit owner even when a descendant task triggered the action. In the no-explicit-task path, `cmd_advance()` passes `git.current_branch()` as the target branch, so completed work only participates when its merge unit targets the currently checked-out branch. For explicit `gza advance <task-id>` planning, `cmd_advance()` instead resolves the lineage's canonical merge target (the task's merge-unit target when present, otherwise the project's strict default merge target) so the next action is deterministic across worktrees. Non-dry explicit merge execution must still run on that exact target branch; if the active checkout differs, advance fails closed and tells the operator to switch branches instead of merging on the wrong checkout. Legacy rows with no resolvable merge unit remain compatibility-oriented fallback candidates instead of being branch-target-filtered away.
   Advance also keeps one shared local-target integration verify checkpoint. Before rendering or confirming any merge plan on the local target, before merge execution, and again after each successful merge, it compares the current local target tree fingerprint with the last verified fingerprint. A changed fingerprint reruns `verify_command` on the local target checkout, and the checkpoint is also invalidated on the same tree when the configured verify-gate identity changes. Freshness is keyed by the normalized `verify_command`, gate-enabled/no-gate state, and the resolved automation timeout settings. Configured-gate red or unavailable checkpoints also expire after `main_integration_verify_red_ttl_minutes` even when the fingerprint is unchanged, so watch/advance re-verify on a bounded cadence instead of pinning merges behind one flaky red forever. For configured gates, checkpoint reuse is fail-closed: if the current checkout or the persisted checkpoint cannot produce an exact tree fingerprint, advance must rerun the gate instead of falling back to `HEAD` equality, and a still-unavailable fingerprint persists an operator-visible unavailable proof state that halts merges. Any configured-gate result other than `passed` parks further merges for that command run behind one `main-integration-verify-red` attention row instead of silently continuing to merge onto a red `main`, and the same deduped attention row is reused for both preview and execution output. The only exception is an explicit no-gate project with no configured `verify_command`: that path may persist `status="unavailable"` / `exit_status="not configured"` for visibility, but it does not halt merges or emit red-main attention.

3. **Failed-task recovery inside the owner row**: Failed leaves are filtered through the same bounded recovery policy used by `decide_failed_task_recovery(...)`. That policy can classify candidates as `resume`, `retry`, or manual review required, but the command no longer performs a second standalone failed-task sweep to build the plan. Rows whose authoritative action is recovery expose `row.recovery_action_task` / `row.recovery_leaf_task`; rows whose failed leaf has already handed off to newer completed lifecycle work keep the owner row and its `lifecycle_action_task` so merge/review/rebase planning remains eligible independently. Failed ancestors are omitted silently once the same automatic recovery intent has completed, whether that completion sits on the failed task's own recovery-only `based_on` chain or on a sibling resume/retry of the same failed parent. The completed recovery task is then handled through the ordinary completed-task rules (merge, rebase, review, or dependency wait) instead of re-printing a permanent `SKIP: recovery child/descendant already completed` row. Failed `review` and `rebase` tasks whose structured target implementation is already `merged` are omitted silently because no advance/watch/iterate action can move them forward. Completed same-branch `improve` tasks still remain visible because they can represent real post-merge follow-up work, but failed same-branch improves fall back to the landed-lineage suppression check because a failed attempt on an already merged branch did not land any additional work. Failed resumable timeout `implement` tasks are narrower: branch reachability alone is not enough to hide them, because lifecycle now requires a valid completed merge representative before merge or mark-merged bookkeeping can proceed. They only drop out of failed-task recovery when another valid merged lineage member or completed recovery descendant proves the work already landed. If branch reachability probes fail after the default branch is known, `advance --dry-run` surfaces one warning that only git branch reachability suppression is unavailable for this run; metadata-based same-lineage merged-task suppression may still apply, so failed-row visibility remains conservative only for the git-reachability decision. If a project-backed store cannot resolve the real default merge target at all, failed-task recovery now raises `MergeTargetResolutionError` instead of silently assuming `main`.

`--no-resume-failed` only suppresses rows whose actionable work is failed-task recovery. Owner rows that also carry a non-failed `lifecycle_action_task` remain eligible for merge/review/rebase planning even when they surface a failed recovery descendant.

Optional filters: `--type plan|implement`, `--max N`, or a specific task ID.

## Configuration

| Field | Default | Description |
|-------|---------|-------------|
| `require_review_before_merge` | `true` | Implement tasks must have a valid current review before merge |
| `advance_create_reviews` | `true` | Auto-create review tasks for implements when review gating still requires them; otherwise lifecycle parks for manual attention instead of creating reviews. |
| `max_resume_attempts` | `1` | Shared automatic failed-task recovery toggle (`0` disables; any positive value enables the fixed bounded resume/retry policy) |
| `max_review_cycles` | `3` | Max review→improve cycles within one durable-progress epoch before flagging for manual intervention |
| `max_noop_improve_cycles` | `1` | Max consecutive no-op improves before lifecycle automation stops for discussion |
| `advance_off_topic_verify_unblock` | `false` | Whether verify-only no-op red reverify outcomes MAY clear through the audited off-topic verify-failure path instead of parking |
| `autonomous_verify_timeout_seconds` | `120` | Timeout for lifecycle/automation-initiated `verify_command` runs |
| `review_verify_timeout_grace_seconds` | `5` | Grace period after SIGTERM before autonomous review verification escalates to SIGKILL; accepts float values >= 1 second |
| `recommend_rebase_behind_commits` | `1` | Deprecated compatibility key; accepted but ignored by lifecycle planning |
| `merge_squash_threshold` | `0` | Auto-squash branches with >= N commits (0 = disabled) |

## Decision Tree

For each task, `evaluate_advance_rules()` returns an action from `src/gza/advance_engine.py`. The decision tree is evaluated by an ordered rule list; first match wins.

### 1. Plan tasks

| Condition | Action |
|-----------|--------|
| Completed held plan whose latest completed `plan_review` is `APPROVED` with a valid manifest and no materialized slices yet | `release_approved_plan_review` — persist `auto_implement=true` only, then let the next evaluation reuse the existing `materialize_plan_slices` path |
| Completed held plan with no implement child (`auto_implement = false`) | `awaiting_human` — review the plan, then run `uv run gza implement <id>` or re-enable automatic follow-up (`reason=awaiting-human-review`) |

Manual `implement` follow-up for a held plan is intentionally explicit. `uv run gza add --type implement --depends-on <plan-id>` and `uv run gza add --type implement --based-on <plan-id>` are not valid substitutes while the plan is still held; the CLI refuses them and tells the operator to release the hold first with `uv run gza implement <plan-id>` or `uv run gza edit <plan-id> --no-hold-for-review`.
| Completed non-held plan with no plan review and `require_plan_review_before_implement=true` | `create_plan_review` — create and run plan-review task |
| Completed non-held plan with pending or in-progress plan review | `run_plan_review` / `wait_plan_review` — reuse the current review attempt, never duplicate it |
| Completed non-held plan whose latest approved plan-review manifest has an unambiguous integer-like `schema_version` such as string `"1"` or float `1.0` | `materialize_plan_slices` — normalize through the shared validator and materialize the approved slice set |
| Completed non-held plan whose latest approved plan-review manifest only fails because `schema_version` is missing or not an unambiguous integer representation | `create_plan_review` — rerun plan review to re-derive the manifest instead of parking `plan-review-invalid-slices` |
| Completed non-held plan whose failed `plan_review` attempts hit the configured cap | `needs_discussion` — stop auto-respawning and require a human decision (`reason=plan-review-repeatedly-failed`) |
| Completed non-held plan with approved valid plan review manifest | `materialize_plan_slices` — create sliced implement tasks |
| Completed non-held plan with `CHANGES_REQUESTED` plan review | `create_plan_improve` / `run_plan_improve` / `wait_plan_improve` — revise the plan until approval or the configured iteration bound |
| Completed non-held plan with `NEEDS_DISCUSSION` or unknown plan-review verdict | `needs_discussion` — stop for a human (`reason=plan-review-needs-discussion` or `plan-review-unknown-verdict`) |
| Completed non-held plan with auto plan-review creation disabled | `needs_discussion` — require manual plan-review creation (`reason=plan-review-needs-manual-creation`) |
| Completed non-held plan whose plan-review loop hit `max_plan_review_cycles` | `needs_discussion` — stop repeated plan churn (`reason=plan-review-max-cycles-reached`) |
| Completed non-held plan with approved plan review slices partially present, the current partial slice set is a proven safe pending subset of the validated manifest, and the durable materialization record is either missing/incomplete or already complete while stale extra pending duplicate slice descendants remain outside the recorded set | `repair_plan_slice_materialization` — revalidate the partial slice set, drop the safe pending partial rows, and rematerialize the full validated slice set through the shared guarded executor path using the same matched slice `trigger_source` that proved the repair candidate |
| Completed non-held plan with approved plan review slices partially present, but the materialization state is ambiguous or unsafe | `needs_discussion` — stop for manual repair or drop of the partial slice set (`reason=plan-review-materialization-repair-needed`) |
| Completed non-held plan with `require_plan_review_before_implement=false` | `create_implement` — legacy compatibility path |
| Plan with existing implement child | `skip` — either approved slices are already materialized, or a legacy/direct implement child already exists |

Foreground `gza iterate <plan>` now drives this same action table directly for `plan`
and `plan_improve` sources. In that mode, iterate may run/recover the plan source
itself, then create/run `plan_review` or `plan_improve`, and finally materialize the
approved slice set. It stops at materialization; it does not continue into the new
implement children.

### 2. Explore source follow-up

| Condition | Action |
|-----------|--------|
| Completed `explore` with no non-dropped plan/implement descendant | `needs_discussion` — decide whether to drop it or spawn follow-up work |

### 3. No branch

| Condition | Action |
|-----------|--------|
| Completed task has no branch | `skip` — completed `<type>` task has no branch; no mergeable commits found |
| Non-completed task has no branch | `skip` — `<status>` `<type>` task has no branch; no merge action available |

### Strict project scope

Before advance queues rebase, review, improve, or merge work for a code-changing branch, it checks the branch diff against the task's strict project scope. This uses the existing project-boundary machinery, but for this verdict only the configured project subdirectory is writable unless the task is explicitly tagged `cross-project`. Cross-project tasks still fail closed if the branch touches paths outside all discovered project roots or any new roots declared by changed branch-local `gza.yaml` files.

| Condition | Action |
|-----------|--------|
| Branch diff includes any path outside the strict project scope AND task is not tagged `cross-project` | `needs_discussion` — park for human review immediately, list the offending paths, and tell the operator to tag `cross-project` and re-advance if intended or fix the branch |
| Branch diff for a tagged `cross-project` task includes any path outside all discovered project roots and branch-declared `gza.yaml` roots | `needs_discussion` — park for human review immediately, list the offending paths, and tell the operator to fix the branch or add missing project configs so the affected roots are discoverable |
| Branch diff cannot be inspected reliably for the strict-scope check | `needs_discussion` — fail closed, say that strict project scope could not be verified, and stop all automation until the operator fixes the ref/diff problem or tags `cross-project` if the wider scope is intended |

### 4. Merge conflicts

Conflict detection uses the same target-branch resolution as task collection:

- Default `gza advance` uses the currently checked-out branch as the merge target (`target_branch = git.current_branch()`).
- Explicit `gza advance <task-id>` uses the lineage's canonical merge target (`_resolve_advance_target_branch()`): the task's merge-unit target when present, otherwise the project's strict default merge target. If that target cannot be resolved, the command errors instead of silently assuming `main`.

| Condition | Action |
|-----------|--------|
| Branch cannot merge into the resolved target branch during ordinary queue-wide projection (`selected_for_merge=false`) | Continue through the normal review / improve / merge rules; do not surface `needs_rebase` yet |
| Branch cannot merge into the resolved target branch AND rebase child is `pending`/`in_progress` | `skip` — rebase already running |
| Branch cannot merge into the resolved target branch AND rebase child is `failed` | `needs_discussion` — manual intervention required unless later local post-resolution proof exists |
| Branch cannot merge into the resolved target branch AND a same-branch rebase child already completed AND the branch already contains the current target tip | `needs_discussion` — reason `rebase-did-not-unblock-merge`; stop repeated no-op rebases only when the completed rebase already includes the current target tip |
| Local branch and `origin/<branch>` diverged | `reconcile_branch_divergence` — treat publication reconcile and merge proof as separate steps: first inspect or fetch any needed remote publication state and publish directly with `--force-with-lease` when the local branch is strictly ahead, when the divergence is a symmetric gza rewrite of equivalent patch content, or when the remote-only commits are all stale gza `WIP: gza task interrupted` savepoints; if publication still needs branch-content reconciliation, mechanically rebase onto the resolved local target branch, then publish, and park real host-side conflicts as explicit needs-attention instead of spawning a sandboxed `rebase` against an unreachable remote-tracking ref |
| Branch cannot merge into the resolved target branch AND the lineage has already been selected for merge in this pass AND no active rebase child AND the branch does not already contain the target tip | `needs_rebase` — create rebase task, including stale completed rebases whose branch no longer contains the current target tip |
| Branch cannot merge into the resolved target branch AND the branch already contains the target tip AND the lineage task is still incomplete | `needs_discussion` — rebase is already proved unnecessary; surface the incomplete lineage instead of looping |

A failed rebase is not cleared just because the latest implementation tip becomes mergeable again. If an implementation lineage still has no later approved or cleared review after that failed rebase, advance continues to surface `rebase-failed-needs-manual-resolution` instead of creating a first review from the now-clean tip, unless a later local post-resolution proof exists. The local proofs are intentionally narrow: a merged merge unit, exact branch-tip equality with the current target branch, or proof that the implementation branch already contains the current target tip. That proof now suppresses fresh `needs_rebase` planning as well: when the branch already contains the target, advance either continues with the ordinary review/merge flow or raises one shared `needs_attention` row for the real non-rebase blocker.

Repeated failed rebases are bounded independently of the ordinary failed-rebase rule. Once the same branch accumulates 3 failed rebase attempts with no intervening successful rebase, completed review, review clear, or completed code change, advance/watch stop creating more rebases and emit `needs_discussion` with reason `rebase-failure-circuit-breaker`.

### 5. Post-rebase review invalidation

| Condition | Action |
|-----------|--------|
| Review requirement for the implementation-owned lineage is disabled (`require_review_before_merge=false`) | Fall through to the normal no-review merge path; do not create, run, or wait on a stale refresh review |
| A completed rebase on the implementation branch exists that is newer than the latest review and changed the tracked diff AND `advance_create_reviews=true` | `create_review` — rebase may have introduced changes |
| Current implementation branch / merge-unit head differs from the latest completed review's recorded reviewed head SHA AND both SHAs are known AND `advance_create_reviews=true` | `create_review` — durable branch progress made the latest review stale |
| Either stale-review condition above AND `advance_create_reviews=false` | `needs_discussion` — park and require a manual review refresh before merge |

When a required post-rebase refresh review already exists but its persisted resolution metadata is blank, ordinary, or otherwise inconsistent with the authoritative rebase context, lifecycle first re-derives the canonical resolved head/target SHAs from the completed rebase and live refs, rewrites the review row with structured resolution metadata, and then continues through the normal review/merge path. Rows that still claim malformed resolution metadata after that deterministic repair attempt remain fail-closed and park with `resolution-review-metadata-invalid`.

### 6. Review state (when reviews exist)

#### 6a. Review was cleared (improve task ran after review)

| Condition | Action |
|-----------|--------|
| Review requirement for the implementation-owned lineage is disabled (`require_review_before_merge=false`) | Fall through to the normal no-review merge path; do not create, run, or wait on a stale refresh review, and do not enforce the closing-review gate |
| Active review is `pending` | `run_review` — spawn worker for it |
| Active review is `in_progress` | `wait_review` — skip |
| Completed improve exists after latest review and changed the tracked reviewable diff | `create_review` — code changed, need fresh review |
| Completed no-op improve exists after latest review and current passing in-improve verify evidence already cleared a verify-only review at the same branch/head | Normal approved/merge paths apply; do not spawn a separate detached re-verify action. This same-head clear path covers ordinary verify failures and verify timeouts when the review-side runner evidence failed and the later no-op improve-side runner evidence passed. If a different completed sibling review on the same implementation still carries unresolved non-verify CODE blockers, lifecycle must park on that sibling review instead of merging, even when that sibling already has a pending, in-progress, or completed-but-non-clearing improve. But a sibling CODE review that already received a completed code-changing improve and was then covered by the newer current same-head review is treated as superseded rather than still owning the parked attention |
| Completed no-op improve exists after latest review, the latest no-op improve captured a fresh red verify result, and `advance_off_topic_verify_unblock=true` with full same-head/same-fingerprint off-topic proof plus target-side baseline evidence | Treat the review as cleared and continue through the normal approved/merge paths. Lifecycle also persists a `review_clearance` audit artifact on the implementation row next to `clear_review_state(...)` and must create or reuse the required non-blocking `REPRODUCE-OR-RECORD` investigation record before clearing. Only still-actionable investigation tasks may be reused; terminal matches require a fresh actionable record. Investigation creation, investigation artifacts, and review-clearance audit persistence are one fail-closed transaction from lifecycle's perspective, and any persistence failure must surface an operator-visible warning while leaving the review uncleared. The resulting `merge` action carries the created/reused investigation task IDs so `advance` and `watch` can print them alongside other follow-up-style merge messages |

#### 6b. Review is active (not cleared)

Severity semantics for `BLOCKER`, `FOLLOWUP`, and `NIT` live in [docs/merge-policy.md](../merge-policy.md). Use that rubric when interpreting review output or adjusting the review contract.

| Condition | Action |
|-----------|--------|
| Latest review is `pending` | `run_review` — spawn worker |
| Latest review is `in_progress` | `wait_review` — skip |
| Task type is `implement`, verdict is `APPROVED`/`APPROVED_WITH_FOLLOWUPS` (or review is cleared), and unresolved comments are newer than the latest completed review | Prefer improve flow (`wait_improve`/`run_improve`/`improve`) before any merge |
| Verdict = `APPROVED` and the review is still valid for the current mergeable diff | `merge` |
| Verdict = `APPROVED_WITH_FOLLOWUPS` with at least one parsed `FOLLOWUP` finding and the review is still valid for the current mergeable diff | `merge_with_followups` — create/reuse follow-up implement tasks, then merge |
| Verdict = `APPROVED_WITH_FOLLOWUPS` with zero parsed `FOLLOWUP` findings | `needs_discussion` — fail closed; review output is inconsistent |
| Verdict = `CHANGES_REQUESTED` AND last 2 completed reviews are verify-timeout-only AND no improve is `in_progress`/`pending` | `needs_discussion` — reason=`verify-blocked-no-code-issues` once current passing no-op-improve evidence has not already cleared the verify-only review at the same branch head; do not keep spawning improves that cannot help once timeout-only reviews hit the threshold |
| Verdict = `CHANGES_REQUESTED` AND improve is `in_progress` | `wait_improve` — skip |
| Verdict = `CHANGES_REQUESTED` AND improve is `pending` | `run_improve` — spawn worker |
| Verdict = `CHANGES_REQUESTED` AND consecutive completed no-op improves for the latest `(impl, review)` pair >= `max_noop_improve_cycles` AND the latest blocker set includes an adjudication-eligible disputed non-verify CODE blocker | `create_review_adjudication` / `run_review_adjudication` / `wait_review_adjudication` before the generic no-op park; lifecycle persists and consumes the strict `VALID | INVALID | NEEDS_HUMAN` outcome for the matching disputed blocker |
| Verdict = `CHANGES_REQUESTED`, the latest blocker set is verify-only, current trusted green verify evidence already exists for the exact reviewed head/tree fingerprint, `advance_off_topic_verify_unblock=true`, and a fresh red reverify classifies as off-topic with full failing-node enumeration | `clear_off_topic_verify_blocker` — clear the review blocker, create or reuse exactly one non-blocking investigation task per normalized failing-node signature, and surface the created/reused investigation IDs in operator output. If the investigation record cannot be durably created or reused, lifecycle fails closed and keeps the review blocking |
| Consecutive completed no-op improves for the latest `(impl, review)` pair >= `max_noop_improve_cycles` | `needs_discussion` — reason=`improve-no-op`; stop repeated no-op improve loops unless runner-owned current passing verify evidence already cleared the review before lifecycle evaluation, or the opt-in `advance_off_topic_verify_unblock` path proved the later red verify result off-topic with same-head/same-fingerprint evidence. This row applies only when the lineage is not tagged `allow-noop-improve`. If the evidence is absent, stale, missing the required SHA-bound `review_clearance`, branch/head mismatched, the fresh in-improve verify still fails, the classifier is unavailable, or the blocker is not verify-only, lifecycle parks instead of auto-clearing. If parallel sibling reviews exist, the parked text must name the sibling review whose blockers still remain unresolved instead of blaming a zero-diff verify-only improve on a different review, and that same sibling-review park still wins even after current same-head green evidence clears the latest verify-only review. Older sibling CODE reviews that already received a completed code-changing improve and were then covered by the newer current same-head review do not own this parked attention. If the branch-head freshness probe itself fails, lifecycle still parks but the action description carries that probe failure instead of a generic no-op message |
| Verdict = `CHANGES_REQUESTED` AND the same primary blocker repeats for 3 consecutive completed review cycles with no completed rebase boundary between them | `create_review_adjudication` / `run_review_adjudication` / `wait_review_adjudication` using synthesized repeated-review dispute metadata before the generic duplicate-blocker or review-max-cycles park |
| Live branch-head probe fails while checking whether the latest completed review is still current | `needs_discussion` — reason=`review-freshness-unverified`; fail closed, surface the probe warning, and do not treat cached merge-unit head metadata as proof the review is current |
| Verdict = `CHANGES_REQUESTED` AND durable-progress-epoch cycles >= `max_review_cycles` with no stale-review refresh path | `max_cycles_reached` — manual intervention |
| Verdict = `CHANGES_REQUESTED` AND no improve exists | `improve` — create improve task |
| Verdict = unknown | `needs_discussion` — manual intervention |

When a review blocker is one instance of a repeated same-module pattern, reviewers should consolidate the affected-file gaps plus any analogous gaps in diff-touched same-module siblings into one blocker so improve can close the whole class in one pass. `BLOCKER` remains merge-blocking; `FOLLOWUP` remains non-gating but task-worthy.

Tracked review/improve report contracts are stricter than the current lifecycle action
table alone. `specs/behavior/lifecycle-engine.md` is the behavior owner for this
observable contract; this document mirrors that operator-facing rule and must stay in
sync with it rather than redefining it elsewhere:

- Every `BLOCKER` must be falsifiable. The review report must carry current-state
  `Evidence:`, at least one current-source `Open-state citation:`, and a concrete
  `Required fix:` that would close the blocker if implemented. Prior review prose or task
  history is not enough on its own.
- A `CHANGES_REQUESTED` improve pass owns the full current blocker/comment set
  atomically. The worker must re-read all current feedback before editing, build one
  inventory covering every current blocker and unresolved comment, treat grouped blocker
  classes as grouped work, re-check that full initial inventory after meaningful edit
  batches and again after the last edit, and run the configured full final verify gate
  before reporting completion.
- Improve completion reporting must be atomic too: the report must include the machine-
  readable ledger plus an explicit closure matrix for every current blocker/comment and a
  short anti-regression statement covering the full initial inventory.
- If an improve completes as a no-op because a non-verify CODE blocker is stale,
  unreproducible, already satisfied, out of scope, or otherwise invalid, the improve
  report may include a structured `## Disputed Blockers` section instead of fabricating a
  code change. Each disputed item should identify the blocker (`Finding:`), the dispute
  reason (`Reason:`), current-state evidence (`Evidence:`), and a current-source
  `Current-state citation:`; `Scope citation:` and `Downstream task:` are optional.
- Once `max_noop_improve_cycles` is reached for a disputed non-verify CODE blocker, or
  once the same non-verify CODE blocker repeats across the duplicate-blocker review
  bound, the required lifecycle contract is adjudication before the generic `improve-no-op`,
  `duplicate-blocker-no-progress`, and `review-max-cycles` parks. The current runtime
  plumbing now creates/runs one dedicated adjudication worker and persists its strict
  `VALID | INVALID | NEEDS_HUMAN` outcome as a `review_blocker_resolution` artifact,
  and lifecycle consumes those persisted outcomes immediately: `INVALID` clears the
  current blocker for lifecycle purposes, `VALID` re-opens the normal improve lane, and
  `NEEDS_HUMAN` parks with `review-blocker-adjudication-needed`. Verify-only blockers
  remain governed by runner-owned same-branch, same-head verify provenance.

When the engine emits `improve`, the caller (iterate) delegates to `resolve_improve_action(store, impl_id, review_id, max_resume_attempts)` to pick one of:

| Condition | Sub-action |
|-----------|-----------|
| No prior failed improve for this (impl, review) | `new` — create a fresh improve |
| Shared failed-task recovery policy returns `resume` | `resume` — continue from the latest failed improve |
| Shared failed-task recovery policy returns `retry` | `retry` — create a new improve attempt on the same shared branch |
| `max_resume_attempts == 0` (automatic recovery disabled) | `give_up` — stop iterating; surface `automatic_recovery_disabled` as the stop reason |
| Shared failed-task recovery policy returns `retry_limit_reached` / `recovery_ambiguous` or another terminal manual-attention stop (for example, failed resume descendants or a dropped recovery terminal) | `manual_review` — stop iterating and require operator intervention |

The improve flow now defers recovery edge selection to the shared recovery engine (`decide_failed_task_recovery`), and iterate also resolves fully recovered failed implement IDs through the same completed-descendant planner handoff used by advance/watch. That keeps iterate/advance/watch on one consistent resume/retry/manual-review boundary and avoids stale completed-recovery skip output on recovered ancestors.

### 7. No reviews / all cleared

| Condition | Action |
|-----------|--------|
| Reviews exist but all cleared, and no newer rebase or closing-review requirement invalidates that state | `merge` — previous review addressed |
| Standalone non-implement task type (plan, explore, etc.), or a merge-unit lineage whose owner does not require review | `merge` — no review required |

Merge-unit members inherit the review state and review requirement of the actionable implementation lineage member on that shared branch. Merge planning and merge-state mutation now also require that representative to have execution status `completed` or legacy-compatible `unmerged`; failed owners cannot satisfy merge eligibility on behalf of the unit. When the compatibility owner row is a failed historical implement and the current code lives on a completed resume descendant, closing-review state, post-rebase invalidation, and merge eligibility all resolve against that completed descendant. A completed `rebase` or other same-branch member of such an implement-owned merge unit must create or wait on that lineage review before merge when no review evidence exists yet.

### 8. Implementation-owned lineage with no review

| Condition | Action |
|-----------|--------|
| `require_review_before_merge=true` and `advance_create_reviews=true` | `create_review` |
| `require_review_before_merge=true` and `advance_create_reviews=false` | `needs_discussion` with `reason=review-needs-manual-creation` |
| `require_review_before_merge=false` | `merge` |

### 9. Failed task recovery

Failed task recovery rules run in the same ordered rule engine.

| Condition | Action |
|-----------|--------|
| Failure is outside the fixed bounded shared policy (for example failed resume descendants or dropped recovery terminals) | `skip` |
| Shared failed-task recovery policy returns `resume` | `resume` — create resume task and spawn worker |
| Shared failed-task recovery policy returns `retry` | `retry` — create retry task and spawn worker |

## Improve chain semantics

A single (impl, review) pair can produce a **chain** of improve tasks — the original improve plus any retries or resumes of it. The chain's shape:

- **depends_on** is stable across the chain. Every improve in the chain sets `depends_on = review.id`. This is the canonical link between an improve and the review that prompted it.
- **based_on** points to the *previous* task in the chain:
  - The original improve: `based_on = impl.id`
  - A retry of an improve: `based_on = failed_improve.id` (the improve being retried, *not* the impl)
  - A resume of an improve: `based_on = failed_improve.id` (same)

Implication for queries: **to find all improves for an (impl, review) pair, filter by `depends_on = review.id`, not by `based_on = impl.id`.** Filtering by `based_on = impl.id` only finds first-generation improves and misses every retry/resume. This has been the root cause of multiple bugs where iterate or the engine couldn't "see" chained work (e.g. keeping the review state dirty because a completed retry wasn't counted as addressing the review).

Likewise, post-completion side effects that logically target "the impl this improve belongs to" must walk up the `based_on` chain until a non-improve ancestor is found, because `task.based_on` on a retry/resume points at the previous improve, not the impl. The helper `runner._resolve_impl_ancestor()` encapsulates this walk.

Completed improve tasks persist `changed_diff` to record whether the task changed the tracked aggregate review diff compared with the branch state captured immediately before the improve started. `changed_diff = 0` means the improve completed but made no tracked reviewable change, so the runner does not clear review state, resolve comments, or create a closing review. The only exception is a verify-only review blocker backed by runner-owned verify evidence on both rows. On the no-op improve path, `runner._capture_noop_improve_review_verify_result()` reruns `verify_command` in the improve's own worktree and persists `review_verify_*` evidence on the improve row. The clear remains fail-closed: lifecycle must first conservatively classify the latest review as verify-only, then require the blocking review row to already persist runner-owned review-time `review_verify_status="failed"` for the current branch/head, and finally require either (a) a completed no-op improve row that later persisted `review_verify_status="passed"` for that same branch/head, captured after the review completed, or (b) when `advance_off_topic_verify_unblock=true`, a later failed no-op improve verify result whose full failing-node set was classified off-topic with same-head and same-tree-fingerprint provenance plus bounded target-side baseline evidence against the caller-selected local target branch. Same-head green verify evidence is only merge-authorizing after runner or lifecycle persists the SHA-bound `review_clearance` artifact for that `(implementation, review, head)` tuple; raw verify evidence alone is an input to that persistence decision, not a merge gate. When no current same-head green verify evidence is already recorded, lifecycle MAY run one bounded fresh verify in an isolated worktree for the current evaluated head; that detached recovery path must fail closed on setup drift, head drift, and cleanup failure, must persist the verify evidence it captured on the no-op improve row, and must record SHA-bound clearance metadata only after cleanup succeeds. If the detached recovery path captures a passing verify result but clearance persistence fails, lifecycle must leave the review uncleared and park with explicit persistence-failure messaging instead of creating another review. The audited off-topic branch also persists a durable `review_clearance` artifact beside `clear_review_state(...)`, creates or reuses the required investigation record before clearing, never runs target-side baseline work from read-only preview/query paths, and parks with an operator-visible probe warning when detached baseline planning or execution raises. Reviewer prose can corroborate the classification, but prose alone does not decide the stale/non-stale provenance gate. Advance counts consecutive no-op improves for the latest `(implementation, review)` pair. When a matching structured `review_clearance` artifact from runner-owned passing verify evidence, the bounded isolated reverify path, or the audited off-topic path has already cleared the review at the same head, the lineage follows the normal approved/merge paths instead of parking. If that evidence is missing, stale, fingerprint-mismatched, still failing, classifier-unavailable, or the blocker is not verify-only, the lineage parks once `max_noop_improve_cycles` is reached; repeated genuine timeout-only reviews still use `verify-blocked-no-code-issues`. If advance cannot resolve the current branch head while validating the same-head provenance check, it still fails closed but surfaces the probe failure in the parked action so operators can distinguish a git freshness problem from an ordinary no-op loop. `NULL` is legacy/unknown and is treated as changed.

When the same-head clear path does not apply because the fresh verify remains red, lifecycle may optionally consult the off-topic verify contract instead of parking immediately. That lane is controlled by `advance_off_topic_verify_unblock` and stays fail-closed by default: lifecycle must prove the latest review is verify-only blocked, the current trusted green and later red evidence are both bound to the exact same reviewed head SHA and tree fingerprint, and the red failing-node set was fully enumerated without fail-fast. On a successful off-topic classification, lifecycle clears the review only after it durably creates or reuses exactly one non-blocking investigation task per normalized failing-node signature, with structured evidence metadata for the failing node, provenance, and verify command. Those investigation prompts now carry an explicit `REPRODUCE-OR-RECORD` contract: reproduce the same signature under a bounded targeted harness before fixing, rerun that same harness green after fixing, or record a structured inconclusive result instead of making a speculative patch. The operator/agent entrypoint for that harness is `uv run gza flaky reproduce <investigation-task-id>`, which preserves the recorded project cwd, prefixes `PYTHONFAULTHANDLER=1`, reuses the targeted failing-node command, and adds xdist/randomization stress flags only when the relevant tooling is actually available. Each harness run persists `flaky_verify_attempt` artifacts, and a budget-exhausted no-repro outcome persists one `flaky_verify_inconclusive` artifact carrying the attempt IDs, environment details, and operator-supplied hypotheses. Operator-facing advance/watch output includes the created or reused investigation task IDs. If classification is unavailable, the failure scopes into the branch diff, the target-side proof is inconclusive, or the investigation record cannot be persisted, lifecycle keeps the review blocker in place.

No-op improves may still carry structured dispute evidence for non-verify CODE blockers
in a `## Disputed Blockers` section. At the no-op bound, that evidence is meant to feed
the adjudication-first contract above rather than falling straight through to the generic
no-op, duplicate-blocker, or bounded-review-loop parks; if runtime behavior still differs,
that mismatch is an implementation gap against the spec, not operator-facing no-op-only
guidance.

More generally, the improve worker contract is atomic over the full current blocker set,
not one finding at a time. Improve prompts must require the worker to re-read all current
feedback, inventory every current review blocker and unresolved feedback comment before
editing, plan one shared fix set, treat grouped blocker classes as grouped work, re-check
that same full initial inventory after meaningful edit batches and again after the last
edit, and run the configured final full verify gate after any targeted inner-loop checks.
The report must also include a machine-readable `## Blocker Closure Ledger (Machine
Readable)` section plus an explicit closure matrix and anti-regression statement so
humans and later tasks can audit exactly which blockers/comments were addressed,
disputed, or left unresolved.

Advance also computes a duplicate-blocker streak from completed review reports only. When the latest completed `CHANGES_REQUESTED` review and the two immediately preceding completed review cycles all carry the same primary blocker fingerprint (normalized blocker title plus the first open-state citation, or the normalized required-fix text when no citation exists), the engine first routes that repeated blocker through review-blocker adjudication using synthesized dispute metadata bound to the current reviewed branch state. Only if adjudication later returns `NEEDS_HUMAN` (or the adjudication path is otherwise exhausted) does lifecycle surface `needs_discussion` with reason `duplicate-blocker-no-progress`. The streak resets across any completed same-lineage rebase between the compared reviews, on any non-`CHANGES_REQUESTED` review, on missing blocker fingerprints, or when the primary blocker changes.

## Action Types

### Worker-spawning actions

These actions create background workers and count toward the batch limit. The source of truth is `WORKER_CONSUMING_ACTIONS` in `src/gza/advance_engine.py`.

| Action | What it does |
|--------|-------------|
| `needs_rebase` | Creates rebase task via `_create_rebase_task()`, spawns worker |
| `create_review` | `gza advance`: creates review task, spawns worker. `gza watch`: for unmerged implementation chains, launches `gza iterate <impl>` and lets iterate create/reuse the review work internally. |
| `run_review` | `gza advance`: spawns worker for existing pending review. `gza watch`: for unmerged implementation chains, launches `gza iterate <impl>` instead of the child review directly. |
| `improve` | `gza advance`: creates/resumes/retries improve work directly. `gza watch`: for unmerged implementation chains, launches `gza iterate <impl>` and lets iterate choose the improve action. |
| `run_improve` | `gza advance`: spawns worker for existing pending improve. `gza watch`: for unmerged implementation chains, launches `gza iterate <impl>` instead of the child improve directly. |
| `create_plan_review` | Creates `plan_review` task for a completed plan source, spawns worker |
| `run_plan_review` | Starts an existing pending `plan_review` task |
| `create_plan_improve` | Creates `plan_improve` task after `CHANGES_REQUESTED` plan review, spawns worker |
| `run_plan_improve` | Starts an existing pending `plan_improve` task |
| `materialize_plan_slices` | Creates sliced implement tasks from an approved plan-review manifest; each slice becomes its own branch/merge unit and ordering stays on `depends_on` |
| `create_implement` | Creates implement task for a plan, spawns worker (legacy compatibility path when plan-review gate is disabled) |
| `resume` | Creates resume task, spawns worker |
| `retry` | Creates retry task, spawns worker |

### Direct actions

| Action | What it does |
|--------|-------------|
| `merge` | Merges the task's branch synchronously. Respects `merge_squash_threshold`. |
| `merge_with_followups` | Creates/reuses follow-up `implement` tasks from parsed `## Follow-Ups` findings, then merges synchronously. |
| `release_approved_plan_review` | Releases a held approved plan source by persisting `auto_implement=true`; slice materialization remains a later pass through the normal approved-manifest action. |
| `repair_plan_slice_materialization` | Re-reads the current approved plan-review/source rows, revalidates the current partial descendant slice set against the validated manifest with the same matched slice `trigger_source` that selected the repair action, and only then drops the safe pending partial rows and rematerializes the full slice set. `advance`, `watch`, and foreground `iterate` all route through this same guarded executor path. |
| `clear_off_topic_verify_blocker` | Clears a verify-only review blocker after audited off-topic classification, then durably creates or reuses one non-blocking investigation task per normalized failure signature before the lineage can continue toward merge. |

### Skip actions

| Action | Meaning |
|--------|---------|
| `skip` | No action needed or possible |
| `wait_review` | Review in progress, wait for it |
| `wait_improve` | Improve in progress, wait for it |
| `awaiting_human` | Plan is intentionally held for manual review before implementation follow-up (`reason=awaiting-human-review`) |
| `needs_discussion` | Requires manual intervention (shown in attention summary) |
| `max_cycles_reached` | Review iteration limit exceeded (shown in attention summary) |

## Execution Order

1. **Direct lifecycle actions execute before worker-consuming actions.** `merge` and `merge_with_followups` still lead that direct lane, but other non-worker lifecycle actions such as approved-plan materialization or branch-divergence reconciliation must also run before watch spends worker slots on review/improve/rebase work.
2. Within the same lane, existing lifecycle ordering stays deterministic and keeps plan/explore rows behind implementation rows at the same action rank.

## Batch Limits

When `--batch N` is specified:
- Worker-spawning actions are skipped once `workers_started >= N`
- Merge actions are not subject to the batch limit
- `--new` mode fills remaining batch slots with pending tasks from the queue
- In `gza watch`, a routed iterate launch holds one slot for the whole implementation review/improve chain. This preserves the existing one-slot-per-process accounting but can reduce interleaving fairness at higher batch sizes because iterate may execute multiple inner review/improve steps before releasing that slot.

## Rebase Flow

When advance detects merge conflicts:

1. Creates a rebase task (`task_type='rebase'`, `same_branch=True`, `based_on=<parent>`)
2. Spawns a background worker
3. Worker runs through the standard runner as a code task (with `skip_commit=True`)
4. The agent invokes `/gza-rebase --auto` which:
   - Stashes any uncommitted changes
   - Rebases onto the already-present local target branch without fetching or other remote operations
   - Resolves conflicts autonomously
   - Restores stashed changes before final verification
   - Runs the configured project `verify_command` on the final checkout before reporting success
5. On completion, the host runner force-pushes the rebased branch (`git push --force-with-lease`)
6. Advance sees no more conflicts on next run
7. If a completed rebase is newer than the latest review and merge is now possible → advance creates a fresh review before merging
8. If a completed same-branch rebase still leaves `can_merge=False` and the branch already contains the current target tip → advance reports `needs_discussion` with reason `rebase-did-not-unblock-merge` instead of queueing another identical rebase
9. If the latest rebase task fails and there is no later successful same-branch rebase/recovery or later approved/cleared review → advance reports `needs_discussion` (no automatic retry)

### Docker considerations

Rebase tasks need git identity for `git rebase --continue`. The Docker container receives `GIT_AUTHOR_NAME`, `GIT_AUTHOR_EMAIL`, `GIT_COMMITTER_NAME`, and `GIT_COMMITTER_EMAIL` env vars from the host's git config.

## Output

For worker-spawning actions that first create or reuse a child task (`create_plan_review`, `create_plan_improve`, `create_review`, `create_implement`, `resume`, `retry`, `needs_rebase`), operator output must distinguish task selection/creation success from worker-launch failure. If task creation succeeds, or if the executor reuses an existing eligible recovery task, but the background worker fails to start, `gza advance` should print the relevant created/reused task ID and a separate `Failed to start ... worker` line rather than collapsing that state into `✗ Created ...`.

For `clear_off_topic_verify_blocker`, operator output should include the created and/or reused investigation task IDs in the success line so the off-topic clearance remains auditable without opening the DB or artifact store. A persistence failure while creating or reusing those investigation records must surface as an error and leave the review uncleared. For the downstream investigation itself, `gza flaky reproduce` is the only supported reproduce helper: it keeps the run bounded, records every attempt, and automatically writes a structured inconclusive artifact when the exact signature does not reproduce within budget. The workflow intentionally forbids blanket sleeps, blanket retries, `@flaky`, or broad timeout inflation as the default remedy.

```
Will advance N task(s):

  gza-2a Implement feature X
      → Merge (review APPROVED)

  gza-2d Fix caching bug
      → Create review (required before merge)

  gza-26 Refactor API
      → SKIP: rebase gza-33 already in progress

Advanced: 1 merged, 1 review started, 1 skipped

Needs attention:
  gza-27 implement "Update deps" reason=review-max-cycles-reached max review cycles (3) reached, needs manual intervention
```

In interactive mode, the same `Needs attention` section is part of the plan preview before the `Proceed?` prompt, even when actionable rows are also present.

## Idempotency

`gza advance` is safe to run repeatedly:
- Already-merged tasks don't appear in `get_unmerged()`
- Running workers cause `wait_review`/`wait_improve` skips
- Pending rebase/review/improve tasks are detected and reused (not duplicated)
- Batch limits prevent runaway worker spawning

## Relationship to other commands

| Command | Relationship |
|---------|-------------|
| `gza work` | Advance spawns workers that run `gza work --worker-mode` |
| `gza review` | Advance creates review tasks equivalent to bare `gza review` (queue-by-default) |
| `gza improve` | Advance creates improve tasks equivalent to bare `gza improve` (queue-by-default) |
| `gza rebase` | Advance creates rebase tasks equivalent to `gza rebase --background` |
| `gza merge` | Advance merges directly, same as `gza merge <id>` |
| `gza watch` | Runs advance in a loop with sleep intervals; `watch.recovery_slots` reserves failed-task recovery capacity before pending pickup |

## Watch integration

`gza watch` reuses the same advance executor and improve-resolution helpers described above; it does not maintain a separate improve retry policy. For `create_review`, `run_review`, `improve`, and `run_improve` on unmerged implementation chains, watch resolves the root implementation first and launches `gza iterate <impl>` before any child review/improve side effects occur. Iterate then owns child creation, reuse, recovery, and immediate follow-on steps inside its loop. Because stale-review refresh resolves on the shared advance path before `review_max_cycles`, a capped lineage whose latest review is stale due to rebase-changed diff or branch-head advancement stays on the auto-refresh path instead of surfacing as needs-attention. When both stale sources apply, the shared operator wording stays rebase-specific so the refresh reason still explains that the rebase changed code (or that the change proof is unknown), rather than collapsing to generic branch-head advancement. If the live branch-head probe itself fails, the same shared path now fails closed with `review-freshness-unverified` instead of trusting cached merge-unit head metadata to keep the review mergeable or to park as ordinary `review-max-cycles-reached`.

Watch renders human-needed advance outcomes (`needs_discussion`, `max_cycles_reached`, failed-task recovery states that now require an operator decision, and improve-recovery stop reasons) as `ATTENTION` log lines instead of deduped `SKIP` lines. The reminder reuses the same formatted task line as the `advance` needs-attention section, including the stable `reason=...` policy slug and the shared single-line shortened prompt. Inline `ATTENTION` appears only when an attention key is newly visible or when its message changes from the previous watch pass; unchanged inline reminders are suppressed until the next change. Attention identity comes from the action's declared `subject_task_id`, not from owner-row rollup heuristics. Legacy or malformed attention actions still fall back defensively, but the shared resolver logs a warning before doing so. Each watch pass that emits visible attention also prints a counted `Needs attention (...)` section with the same formatted task rows for the full current visible set, so operators still see unchanged rows in the roundup. For `review-max-cycles-reached`, the CLI attention surfaces pair that row with `Recommended next step: uv run gza fix <task-id>`. For failed-recovery reasons such as `automatic-recovery-disabled`, `retry-limit-reached`, and `retryable-provider-error`, the shared CLI recommendation is category-aware: never-completed implementations still tell the operator to retry or re-implement instead; completed implementations with retryable terminal failures now point at `uv run gza unstick <owner-id> --reason retry-limit --run`; completed implementations with non-retryable/manual terminal failures keep the `gza fix` handoff. Guarded-pending routing skips are promoted through the same centralized attention path on the first observed guarded skip, using the pending task as the named subject so parked child work does not stay hidden behind deduped `SKIP` lines or the counted needs-attention summary. After that first emission, unchanged guarded-pending inline reminders are suppressed by the same dedupe rule while the roundup continues to list them. For owner rows already parked on the failed-task-recovery stop reasons `retry-limit-reached` or `retryable-provider-error`, watch reuses that parked action instead of recomputing a fresh lifecycle step, so no background iterate worker is re-spawned until a human changes the lineage state. Treat `manual-review-required` as a legacy alias rather than a current parked recovery reason. Ordinary watch skip/wait lines remain deduped across passes.

`gza watch <task-id>...` is an explicit merge-unit scope over the same owner-row planning model. The command resolves each supplied ID to the canonical owner once, then passes those owner IDs into `LineageOwnerQuery.owner_task_ids` for every watch pass. It keeps normal watch merge semantics and worker-slot accounting, but disables unrelated pending pickup and the global failed-task recovery lane; only recovery surfaced through the scoped owner rows is eligible. Because there is no pending lane in that mode, worker-consuming scoped recovery may use all currently available slots in the pass instead of staying capped by the global `watch.recovery_slots` reserve. Only an explicit `--pending-only` selection suppresses that scoped recovery path; config/default `watch.recovery_slots: 0` does not.
If a merge action reaches the default checkout and finds tracked local changes, watch now treats that as a structured merge blocker instead of a generic merge failure. It emits one `ATTENTION` line per pass with `merges blocked: main checkout has uncommitted changes - commit or stash them first`, stops the rest of that merge pass, and leaves `work_done` false so the operator-facing state stays loud. `gza incomplete` renders the same warning whenever mergeable rows exist and the non-isolated default checkout is dirty.

At the start of each watch pass, watch also fingerprints the installed `gza` Python package on disk. If the package contents drift from what the process started with, watch emits one loud `WARNING` line for that newly observed fingerprint. In the default mode, the warning explicitly says watch will re-exec itself at the next cycle boundary without waiting for running or pending work to drain; detached workers stay alive and the replacement process reconciles them after it auto-resumes, skipping the first-pass confirmation gate because the session was already approved. `--no-auto-restart-on-drift` switches back to the warn-only manual-restart message.

When `main_checkout_isolate: true`, `gza watch` still plans against the repo default branch but executes merge attempts inside a dedicated detached integration checkout reset to the default-branch tip (`config.main_checkout_integration_path`). Successful isolated merges are then promoted onto the real default-branch ref before `merge_status` flips to `merged`; if the default branch is attached in another checkout, watch stashes tracked edits first, hard-resets that checkout to the new tip, and restores the stash when it applies cleanly. Watch emits a `WARN` line naming that stash whether it was replayed or had to stay parked for manual recovery. Rebase/conflict-resolution ownership is unchanged: conflicts create rebase tasks on the task branch, and those tasks run through the normal rebase workflow.

Default `gza watch` uses the same bounded shared recovery policy as the explicit failed-task recovery queue, but it now exposes that policy through a two-lane split. `watch.recovery_slots` (default `1`) reserves that many worker slots per watch pass for worker-consuming failed-task recovery before pending pickup, and leaves the remaining `batch - recovery_slots` worker slots for pending work. The rule is uniform for worker-consuming recovery: batch-1 plain watch gives the single slot to worker-consuming recovery first; `--pending-only` or `watch.recovery_slots: 0` are the escape hatch for operators who intentionally want single-slot pending-only behavior. `--recovery-only` is the other extreme (`recovery_slots = batch`) and suppresses pending pickup until actionable recovery drains, even for direct reconcile actions that do not consume a worker slot.

`gza watch` now shares the same lifecycle execution gate as `gza advance`: every actionable non-worker lifecycle action runs regardless of free worker slots, while worker-consuming actions remain slot-gated. Watch still owns scheduling order and live slot accounting; only the action-type gate is shared.
Separately from the failed-task recovery lane, each watch pass now emits one concise `Lifecycle actions (...)` summary line for the actionable review/rebase/merge/materialization work already queued in that pass's lifecycle plan. The summary reuses the shared lifecycle action types rather than inventing watch-only wording, and it appears once per pass before execution so operators can compare watch behavior with `advance --dry-run`.
`gza queue` is now the lighter operator preview: by default it renders only the git-free pending lane, and `gza queue --full` opts into the recovery lane, lifecycle lane, and scope-gap diagnostics that depend on the shared git-backed planning context.

When the recovery lane is active:

- Watch evaluates failed tasks through the shared recovery engine before spending the reserved pending slots
- Actionable failed tasks are selected oldest-created first, but they only consume the configured recovery slots for that watch pass
- Implement recovery launches through iterate-aware execution; non-implement recovery launches through plain worker execution
- `gza watch --recovery-only --dry-run` prints the failed-task recovery decision report, including shared `Needs attention` rows by default, and exits
- Fully recovered failed ancestors are omitted from that report and from live watch recovery logs; only unresolved failed tasks and their completed recovery descendants remain visible through the normal advance plan
- Failed `review` / `improve` / `rebase` rows whose structured target implementation is already merged are omitted from live recovery-lane failure transition output and do not contribute to backoff or halt streaks
- `--max-resume-attempts` applies to all unattended watch-managed resume/retry decisions for that run, including plain watch, failed-task recovery, and advance-driven improve recovery

Deprecated compatibility aliases remain accepted for now: `--restart-failed` maps to `--recovery-only`, `--restart-failed-batch` maps to `--recovery-slots`, and `watch.restart_failed_batch` maps to `watch.recovery_slots`.
