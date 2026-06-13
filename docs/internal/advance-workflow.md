# gza advance

> **Status: Implemented** — This spec describes the current behavior of `gza advance` as of 2026-04-12.

## Overview

`gza advance` is the main orchestration command. It now collects one owner-keyed lineage row set, determines the next action for each owner row, and executes those actions (spawning workers, merging, etc.). It is designed to be idempotent and safe to run repeatedly.

## Scope note (gza-956)

The shared rule engine introduced for `advance` is also the decision source for `iterate` (`determine_next_action` in `src/gza/cli/advance_engine.py` wraps the same `evaluate_advance_rules()` chain). Keeping both commands on one rule evaluator is intentional to preserve the project learning: avoid diverging procedural forks between lifecycle commands.

As a result, this change set includes iterate-facing contract alignment where needed (status wording, help text, and regressions) in the same patch as the engine migration, rather than splitting into a separate task with duplicated decision logic changes.

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

3. **Failed-task recovery inside the owner row**: Failed leaves are filtered through the same bounded recovery policy used by `decide_failed_task_recovery(...)`. That policy can classify candidates as `resume`, `retry`, or manual review required, but the command no longer performs a second standalone failed-task sweep to build the plan. Rows whose authoritative action is recovery expose `row.recovery_action_task` / `row.recovery_leaf_task`; rows whose failed leaf has already handed off to newer completed lifecycle work keep the owner row and its `lifecycle_action_task` so merge/review/rebase planning remains eligible independently. Failed ancestors are omitted silently once the same automatic recovery intent has completed, whether that completion sits on the failed task's own recovery-only `based_on` chain or on a sibling resume/retry of the same failed parent. The completed recovery task is then handled through the ordinary completed-task rules (merge, rebase, review, or dependency wait) instead of re-printing a permanent `SKIP: recovery child/descendant already completed` row. Failed `review` and `rebase` tasks whose structured target implementation is already `merged` are omitted silently because no advance/watch/iterate action can move them forward. Completed same-branch `improve` tasks still remain visible because they can represent real post-merge follow-up work, but failed same-branch improves fall back to the landed-lineage suppression check because a failed attempt on an already merged branch did not land any additional work. Failed resumable timeout `implement` tasks are narrower: branch reachability alone is not enough to hide them, because lifecycle now requires a valid completed merge representative before merge or mark-merged bookkeeping can proceed. They only drop out of failed-task recovery when another valid merged lineage member or completed recovery descendant proves the work already landed. If branch reachability probes fail after the default branch is known, `advance --dry-run` surfaces one warning that only git branch reachability suppression is unavailable for this run; metadata-based same-lineage merged-task suppression may still apply, so failed-row visibility remains conservative only for the git-reachability decision. If a project-backed store cannot resolve the real default merge target at all, failed-task recovery now raises `MergeTargetResolutionError` instead of silently assuming `main`.

`--no-resume-failed` only suppresses rows whose actionable work is failed-task recovery. Owner rows that also carry a non-failed `lifecycle_action_task` remain eligible for merge/review/rebase planning even when they surface a failed recovery descendant.

Optional filters: `--type plan|implement`, `--max N`, or a specific task ID.

## Configuration

| Field | Default | Description |
|-------|---------|-------------|
| `require_review_before_merge` | `true` | Implement tasks must have a valid current review before merge |
| `advance_create_reviews` | `true` | Auto-create review tasks for implements when review gating still requires them; otherwise lifecycle parks for manual attention instead of creating reviews. |
| `max_resume_attempts` | `1` | Shared automatic failed-task recovery toggle (`0` disables; any positive value enables the fixed bounded resume/retry policy) |
| `max_review_cycles` | `3` | Max review→improve cycles before flagging for manual intervention |
| `max_noop_improve_cycles` | `1` | Max consecutive no-op improves before lifecycle automation stops for discussion |
| `autonomous_verify_timeout_seconds` | `120` | Timeout for lifecycle/automation-initiated `verify_command` runs |
| `recommend_rebase_behind_commits` | `1` | Deprecated compatibility key; accepted but ignored by lifecycle planning |
| `merge_squash_threshold` | `0` | Auto-squash branches with >= N commits (0 = disabled) |

## Decision Tree

For each task, `evaluate_advance_rules()` returns an action from `src/gza/advance_engine.py`. The decision tree is evaluated by an ordered rule list; first match wins.

### 1. Plan tasks

| Condition | Action |
|-----------|--------|
| Completed held plan with no implement child (`auto_implement = false`) | `awaiting_human` — review the plan, then run `uv run gza implement <id>` or re-enable automatic follow-up (`reason=awaiting-human-review`) |
| Completed non-held plan with no plan review and `require_plan_review_before_implement=true` | `create_plan_review` — create and run plan-review task |
| Completed non-held plan with pending or in-progress plan review | `run_plan_review` / `wait_plan_review` — reuse the current review attempt, never duplicate it |
| Completed non-held plan with approved valid plan review manifest | `materialize_plan_slices` — create sliced implement tasks |
| Completed non-held plan with `CHANGES_REQUESTED` plan review | `create_plan_improve` / `run_plan_improve` / `wait_plan_improve` — revise the plan until approval or the configured iteration bound |
| Completed non-held plan with `NEEDS_DISCUSSION` or unknown plan-review verdict | `needs_discussion` — stop for a human (`reason=plan-review-needs-discussion` or `plan-review-unknown-verdict`) |
| Completed non-held plan with auto plan-review creation disabled | `needs_discussion` — require manual plan-review creation (`reason=plan-review-needs-manual-creation`) |
| Completed non-held plan whose plan-review loop hit `max_plan_review_cycles` | `needs_discussion` — stop repeated plan churn (`reason=plan-review-max-cycles-reached`) |
| Completed non-held plan with approved plan review slices partially present but no durable materialization record | `needs_discussion` — repair or drop the partial slice set before retrying (`reason=plan-review-materialization-repair-needed`) |
| Completed non-held plan with `require_plan_review_before_implement=false` | `create_implement` — legacy compatibility path |
| Plan with existing implement child | `skip` |

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
| Branch cannot merge into the resolved target branch AND rebase child is `pending`/`in_progress` | `skip` — rebase already running |
| Branch cannot merge into the resolved target branch AND rebase child is `failed` | `needs_discussion` — manual intervention required unless later local post-resolution proof exists |
| Branch cannot merge into the resolved target branch AND a same-branch rebase child already completed AND the branch already contains the current target tip | `needs_discussion` — reason `rebase-did-not-unblock-merge`; stop repeated no-op rebases only when the completed rebase already includes the current target tip |
| Local branch and `origin/<branch>` diverged | `reconcile_branch_divergence` — publish directly with `--force-with-lease` when the local branch is strictly ahead, when the divergence is a symmetric gza rewrite of equivalent patch content, or when the remote-only commits are all stale gza `WIP: gza task interrupted` savepoints; if `origin/<branch>` is already ahead or the remote side has genuinely distinct commits, fetch + mechanically rebase onto `origin/<branch>` before publishing, and park real host-side conflicts as explicit needs-attention instead of spawning a sandboxed `rebase` against an unreachable remote-tracking ref |
| Branch cannot merge into the resolved target branch AND no active rebase child AND the branch does not already contain the target tip | `needs_rebase` — create rebase task, including stale completed rebases whose branch no longer contains the current target tip |
| Branch cannot merge into the resolved target branch AND the branch already contains the target tip AND the lineage task is still incomplete | `needs_discussion` — rebase is already proved unnecessary; surface the incomplete lineage instead of looping |

A failed rebase is not cleared just because the latest implementation tip becomes mergeable again. If an implementation lineage still has no later approved or cleared review after that failed rebase, advance continues to surface `rebase-failed-needs-manual-resolution` instead of creating a first review from the now-clean tip, unless a later local post-resolution proof exists. The local proofs are intentionally narrow: a merged merge unit, exact branch-tip equality with the current target branch, or proof that the implementation branch already contains the current target tip. That proof now suppresses fresh `needs_rebase` planning as well: when the branch already contains the target, advance either continues with the ordinary review/merge flow or raises one shared `needs_attention` row for the real non-rebase blocker.

Repeated failed rebases are bounded independently of the ordinary failed-rebase rule. Once the same branch accumulates 3 failed rebase attempts with no intervening successful rebase, completed review, review clear, or completed code change, advance/watch stop creating more rebases and emit `needs_discussion` with reason `rebase-failure-circuit-breaker`.

### 5. Post-rebase review invalidation

| Condition | Action |
|-----------|--------|
| Review requirement for the implementation-owned lineage is disabled (`require_review_before_merge=false`) | Fall through to the normal no-review merge path; do not create, run, or wait on a stale refresh review |
| A completed rebase on the implementation branch exists that is newer than the latest review AND `advance_create_reviews=true` | `create_review` — rebase may have introduced changes |
| A completed rebase on the implementation branch exists that is newer than the latest review AND `advance_create_reviews=false` | `needs_discussion` — park and require a manual review refresh before merge |

### 6. Review state (when reviews exist)

#### 6a. Review was cleared (improve task ran after review)

| Condition | Action |
|-----------|--------|
| Review requirement for the implementation-owned lineage is disabled (`require_review_before_merge=false`) | Fall through to the normal no-review merge path; do not create, run, or wait on a stale refresh review, and do not enforce the closing-review gate |
| Active review is `pending` | `run_review` — spawn worker for it |
| Active review is `in_progress` | `wait_review` — skip |
| Completed improve exists after latest review | `create_review` — code changed, need fresh review |

#### 6b. Review is active (not cleared)

| Condition | Action |
|-----------|--------|
| Latest review is `pending` | `run_review` — spawn worker |
| Latest review is `in_progress` | `wait_review` — skip |
| Task type is `implement`, verdict is `APPROVED`/`APPROVED_WITH_FOLLOWUPS` (or review is cleared), and unresolved comments are newer than the latest completed review | Prefer improve flow (`wait_improve`/`run_improve`/`improve`) before any merge |
| Verdict = `APPROVED` and the review is still valid for the current mergeable diff | `merge` |
| Verdict = `APPROVED_WITH_FOLLOWUPS` with at least one parsed `FOLLOWUP` finding and the review is still valid for the current mergeable diff | `merge_with_followups` — create/reuse follow-up implement tasks, then merge |
| Verdict = `APPROVED_WITH_FOLLOWUPS` with zero parsed `FOLLOWUP` findings | `needs_discussion` — fail closed; review output is inconsistent |
| Verdict = `CHANGES_REQUESTED` AND last 2 completed reviews are verify-timeout-only AND no improve is `in_progress`/`pending` | `needs_discussion` — reason=`verify-blocked-no-code-issues`, unless the latest `(impl, review)` pair has already hit the no-op improve limit and lifecycle can still prove the current branch tip for a safe reverify |
| Verdict = `CHANGES_REQUESTED` AND improve is `in_progress` | `wait_improve` — skip |
| Verdict = `CHANGES_REQUESTED` AND improve is `pending` | `run_improve` — spawn worker |
| Consecutive completed no-op improves for the latest `(impl, review)` pair >= `max_noop_improve_cycles`, lineage is not tagged `allow-noop-improve`, and the latest review is blocked only by `verify_command` with auto-review still enabled | `verify_noop_improve_then_review` — re-run `verify_command` on the current implementation tip, then create a fresh review if it passes |
| Consecutive completed no-op improves for the latest `(impl, review)` pair >= `max_noop_improve_cycles` and lineage is not tagged `allow-noop-improve` | `needs_discussion` — stop repeated no-op improve loops |
| Verdict = `CHANGES_REQUESTED` AND the same primary blocker repeats for 3 consecutive completed review cycles with no completed rebase boundary between them | `needs_discussion` — reason `duplicate-blocker-no-progress`; stop the generic review/improve loop and require manual intervention |
| Verdict = `CHANGES_REQUESTED` AND cycles >= `max_review_cycles` | `max_cycles_reached` — manual intervention |
| Verdict = `CHANGES_REQUESTED` AND no improve exists | `improve` — create improve task |
| Verdict = unknown | `needs_discussion` — manual intervention |

When a review blocker is one instance of a repeated same-module pattern, reviewers should consolidate the affected-file gaps plus any analogous gaps in diff-touched same-module siblings into one blocker so improve can close the whole class in one pass.

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

Completed improve tasks persist `changed_diff` to record whether the task changed the tracked aggregate review diff compared with the branch state captured immediately before the improve started. `changed_diff = 0` means the improve completed but made no tracked reviewable change, so the runner does not clear review state, resolve comments, or create a closing review. The only exception is a verify-only review blocker backed by durable positive verify evidence on the task row that cleared it: the completed improve row for runner-side no-op completion, or the implementation row for fresh advance/watch/iterate reverify. That persisted review-verify provenance must say `passed`, must be newer than the blocking review, and must still match the current branch tip before lifecycle clears that blocker. Advance counts consecutive no-op improves for the latest `(implementation, review)` pair. When the latest review is blocked only by `verify_command`, auto-review is enabled, and lifecycle can still prove the current branch tip, the engine emits `verify_noop_improve_then_review`: advance/watch/iterate re-run review verification on the current tip using the same execution path as autonomous review itself, persist that fresh verify provenance on the implementation row, and only then either clear the verify-only blocker or create a fresh review. This no-op reverify path wins before the generic timeout-only parking rule, so stale timeout evidence is refreshed before the lineage is parked. For `cross-project` branches that means fan-out to each affected project root discovered from the detached review worktree diff, aggregate the worst outcome, create a fresh review only when that aggregate passes, and otherwise park with fresh failure or unavailable evidence. If lifecycle cannot prove the current branch tip at all, it now parks with explicit branch-tip proof failure evidence instead of the generic `improve-no-op` wording. If lifecycle cannot even inspect the cross-project branch diff needed to prove whether re-verification is available, it parks with an explicit diff-probe-unavailable reason that includes the failed revision range and compact git error. Worktree creation, HEAD resolution, verify-launch, execution-directory, and cleanup failures are part of that same fail-closed path: they return a visible parked outcome instead of raising through the lifecycle command. In all other cases the engine returns `needs_discussion` with reason `improve-no-op` once `max_noop_improve_cycles` is reached, unless the implementation/review/improve lineage is tagged `allow-noop-improve`. `NULL` is legacy/unknown and is treated as changed.

Advance also computes a generic duplicate-blocker backstop from completed review reports only. When the latest completed `CHANGES_REQUESTED` review and the two immediately preceding completed review cycles all carry the same primary blocker fingerprint (normalized blocker title plus the first open-state citation, or the normalized required-fix text when no citation exists), the engine returns `needs_discussion` with reason `duplicate-blocker-no-progress`. The streak resets across any completed same-lineage rebase between the compared reviews, on any non-`CHANGES_REQUESTED` review, on missing blocker fingerprints, or when the primary blocker changes.

## Action Types

### Worker-spawning actions

These actions create background workers and count toward the batch limit. The source of truth is `WORKER_CONSUMING_ACTIONS` in `src/gza/advance_engine.py`.

| Action | What it does |
|--------|-------------|
| `needs_rebase` | Creates rebase task via `_create_rebase_task()`, spawns worker |
| `create_review` | `gza advance`: creates review task, spawns worker. `gza watch`: for unmerged implementation chains, launches `gza iterate <impl>` and lets iterate create/reuse the review work internally. |
| `run_review` | `gza advance`: spawns worker for existing pending review. `gza watch`: for unmerged implementation chains, launches `gza iterate <impl>` instead of the child review directly. |
| `verify_noop_improve_then_review` | `gza advance`: re-runs `verify_command` on the current implementation tip, persists that verify provenance on the implementation row, then clears a verify-only blocker or creates/spawns a fresh review; failures still park with `improve-no-op`. `gza watch`: routes the same decision through iterate for unmerged implementation chains. |
| `improve` | `gza advance`: creates/resumes/retries improve work directly. `gza watch`: for unmerged implementation chains, launches `gza iterate <impl>` and lets iterate choose the improve action. |
| `run_improve` | `gza advance`: spawns worker for existing pending improve. `gza watch`: for unmerged implementation chains, launches `gza iterate <impl>` instead of the child improve directly. |
| `create_plan_review` | Creates `plan_review` task for a completed plan source, spawns worker |
| `run_plan_review` | Starts an existing pending `plan_review` task |
| `create_plan_improve` | Creates `plan_improve` task after `CHANGES_REQUESTED` plan review, spawns worker |
| `run_plan_improve` | Starts an existing pending `plan_improve` task |
| `materialize_plan_slices` | Creates sliced implement tasks from an approved plan-review manifest |
| `create_implement` | Creates implement task for a plan, spawns worker (legacy compatibility path when plan-review gate is disabled) |
| `resume` | Creates resume task, spawns worker |
| `retry` | Creates retry task, spawns worker |

### Direct actions

| Action | What it does |
|--------|-------------|
| `merge` | Merges the task's branch synchronously. Respects `merge_squash_threshold`. |
| `merge_with_followups` | Creates/reuses follow-up `implement` tasks from parsed `## Follow-Ups` findings, then merges synchronously. |

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

1. **Merges execute first** (priority 0) before worker spawns (priority 1). This ensures fresh code lands on the current branch before review/improve workers start, reducing rebase conflicts.
2. Within the same priority, tasks are processed in DB order.

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
| `gza review` | Advance creates review tasks equivalent to `gza review --queue` |
| `gza improve` | Advance creates improve tasks equivalent to `gza improve --queue` |
| `gza rebase` | Advance creates rebase tasks equivalent to `gza rebase --background` |
| `gza merge` | Advance merges directly, same as `gza merge <id>` |
| `gza watch` | Runs advance in a loop with sleep intervals; `watch.recovery_slots` reserves failed-task recovery capacity before pending pickup |

## Watch integration

`gza watch` reuses the same advance executor and improve-resolution helpers described above; it does not maintain a separate improve retry policy. For `create_review`, `run_review`, `improve`, and `run_improve` on unmerged implementation chains, watch resolves the root implementation first and launches `gza iterate <impl>` before any child review/improve side effects occur. Iterate then owns child creation, reuse, recovery, and immediate follow-on steps inside its loop.

Watch renders human-needed advance outcomes (`needs_discussion`, `max_cycles_reached`, failed-task recovery states that now require an operator decision, and improve-recovery stop reasons) as `ATTENTION` log lines instead of deduped `SKIP` lines. The reminder reuses the same formatted task line as the `advance` needs-attention section, including the stable `reason=...` policy slug and the shared single-line shortened prompt. Inline `ATTENTION` appears only when an attention key is newly visible or when its message changes from the previous watch pass; unchanged inline reminders are suppressed until the next change. Attention identity comes from the action's declared `subject_task_id`, not from owner-row rollup heuristics. Legacy or malformed attention actions still fall back defensively, but the shared resolver logs a warning before doing so. Each watch pass that emits visible attention also prints a counted `Needs attention (...)` section with the same formatted task rows for the full current visible set, so operators still see unchanged rows in the roundup. For the fix-handoff reasons `review-max-cycles-reached`, `automatic-recovery-disabled`, `retry-limit-reached`, and `retryable-provider-error`, the CLI attention surfaces pair that row with `Recommended next step: uv run gza fix <task-id>`. Guarded-pending routing skips are promoted through the same centralized attention path on the first observed guarded skip, using the pending task as the named subject so parked child work does not stay hidden behind deduped `SKIP` lines or the counted needs-attention summary. After that first emission, unchanged guarded-pending inline reminders are suppressed by the same dedupe rule while the roundup continues to list them. For owner rows already parked on the failed-task-recovery stop reasons `retry-limit-reached` or `retryable-provider-error`, watch reuses that parked action instead of recomputing a fresh lifecycle step, so no background iterate worker is re-spawned until a human changes the lineage state. Treat `manual-review-required` as a legacy alias rather than a current parked recovery reason. Ordinary watch skip/wait lines remain deduped across passes.

If a merge action reaches the default checkout and finds tracked local changes, watch now treats that as a structured merge blocker instead of a generic merge failure. It emits one `ATTENTION` line per pass with `merges blocked: main checkout has uncommitted changes - commit or stash them first`, stops the rest of that merge pass, and leaves `work_done` false so the operator-facing state stays loud. `gza incomplete` renders the same warning whenever mergeable rows exist and the non-isolated default checkout is dirty.

At the start of each watch pass, watch also fingerprints the installed `gza` Python package on disk. If the package contents drift from what the process started with, watch emits one loud `WARNING` line for that newly observed fingerprint. In the default mode, the warning explicitly says watch will re-exec itself on the next watch-pass boundary without waiting for running or pending work to drain; detached workers stay alive and the replacement process reconciles them after it auto-resumes, skipping the first-pass confirmation gate because the session was already approved. `--no-auto-restart-on-drift` switches back to the warn-only manual-restart message.

When `main_checkout_isolate: true`, `gza watch` still plans against the repo default branch but executes merge attempts inside a dedicated detached integration checkout reset to the default-branch tip (`config.main_checkout_integration_path`). Successful isolated merges are then promoted onto the real default-branch ref before `merge_status` flips to `merged`; if the default branch is attached in another checkout, watch hard-resets that checkout to the new tip so it stays clean. Rebase/conflict-resolution ownership is unchanged: conflicts create rebase tasks on the task branch, and those tasks run through the normal rebase workflow.

Default `gza watch` uses the same bounded shared recovery policy as the explicit failed-task recovery queue, but it now exposes that policy through a two-lane split. `watch.recovery_slots` (default `1`) reserves that many worker slots per watch pass for worker-consuming failed-task recovery before pending pickup, and leaves the remaining `batch - recovery_slots` worker slots for pending work. The rule is uniform for worker-consuming recovery: batch-1 plain watch gives the single slot to worker-consuming recovery first; `--pending-only` or `watch.recovery_slots: 0` are the escape hatch for operators who intentionally want single-slot pending-only behavior. `--recovery-only` is the other extreme (`recovery_slots = batch`) and suppresses pending pickup until actionable recovery drains, even for direct reconcile actions that do not consume a worker slot.

When the recovery lane is active:

- Watch evaluates failed tasks through the shared recovery engine before spending the reserved pending slots
- Actionable failed tasks are selected oldest-created first, but they only consume the configured recovery slots for that watch pass
- Implement recovery launches through iterate-aware execution; non-implement recovery launches through plain worker execution
- `gza watch --recovery-only --dry-run` prints the failed-task recovery decision report, including shared `Needs attention` rows by default, and exits
- Fully recovered failed ancestors are omitted from that report and from live watch recovery logs; only unresolved failed tasks and their completed recovery descendants remain visible through the normal advance plan
- Failed `review` / `improve` / `rebase` rows whose structured target implementation is already merged are omitted from live recovery-lane failure transition output and do not contribute to backoff or halt streaks
- `--max-resume-attempts` applies to all unattended watch-managed resume/retry decisions for that run, including plain watch, failed-task recovery, and advance-driven improve recovery

Deprecated compatibility aliases remain accepted for now: `--restart-failed` maps to `--recovery-only`, `--restart-failed-batch` maps to `--recovery-slots`, and `watch.restart_failed_batch` maps to `watch.recovery_slots`.
