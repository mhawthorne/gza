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

Manual `gza merge` has two deliberate operator-only escape hatches that `advance`/`watch`
do not share: a human can defer latest plain-full or resolution review blockers into
urgent PR-required follow-up tasks, and can pass `--force` to override parked
needs-attention lifecycle gates such as malformed resolution-review metadata. It still
refuses ordinary actionable gates such as rebase, verify, or improve work, and
behavior-spec coherence review blockers are not deferable with `--defer-blockers`. The
automated lifecycle remains stricter for ordinary review handling and does not share
manual `--defer-blockers` or `--force`; its only automated `CHANGES_REQUESTED` deferral
exception is `on_max_cycles=merge_and_defer`, where an ordinary plain-full or resolution
review at the review-cycle cap may merge only with current-head review proof, fresh green
verify evidence, a resolvable live merge source, and validated persisted blocker content
that must be materialized as deferred work before merge finalization.

## Usage

```bash
uv run gza advance                        # Advance all eligible tasks
uv run gza advance <task-id>              # Advance a specific task
uv run gza advance --batch N              # Limit to N concurrent worker spawns
uv run gza advance --batch N --new        # Fill remaining batch slots with pending tasks
uv run gza advance --type plan            # Only advance plan tasks
uv run gza advance --type implement       # Only advance implement tasks
uv run gza advance --dry-run              # Show plan without executing
uv run gza advance <task-id> --repeat     # Foreground-drain one task lifecycle
uv run gza advance --no-resume-failed     # Skip recovery-only failed work; keep lifecycle merges/reviews
uv run gza advance --max-resume-attempts N
uv run gza advance --max-review-cycles N
uv run gza advance --squash-threshold N
```

## Task Collection

Advance collects owner rows from one shared source:

1. **Lineage owner rows**: `src/gza/lineage_query.py::query_lineage_owner_rows(...)` materializes one in-memory lineage snapshot from `store.get_all()`, groups tasks by branch/merge ownership, evaluates one lineage-resolved predicate, and returns one canonical row per unresolved owner. The same owner-row query now feeds `gza incomplete`, `gza advance`, and the `gza watch --restart-failed` recovery queue. Explicit `gza advance <task-id>` queries still build the shared snapshot, but failed-task recovery classification is scoped to the selected task's canonical owner/active merge-unit lineage so unrelated failed leaves are not enumerated before the selected row is evaluated. When the selected task is a skipped same-branch member that resolves the visible row through its implementation root, the recovery scope maps back to that root owner and includes the root owner's ordinary and skipped members. Ordinary `depends_on` parents or dependents are not pulled into that recovery scope unless existing ownership metadata has already attached them to the same owner lineage. Same-branch orphan rebase descendants stay available for the explicit `no-descendant-on-the-impl-branch` attention signal, but they are excluded before lifecycle action selection so they cannot be planned for merge/rebase/review work.

2. **Completed lifecycle work inside the owner row**: Completed `merge_status='unmerged'` compatibility rows, merge-unit-backed unmerged work, and completed plan/explore sources all surface through the row's `lifecycle_action_task` and `next_action`. Merge lifecycle collection is merge-unit scoped: each active unit contributes at most one lifecycle owner candidate, and merge execution attributes provenance back to the unit owner even when a descendant task triggered the action. In the no-explicit-task path, `cmd_advance()` passes `git.current_branch()` as the target branch, so completed work only participates when its merge unit targets the currently checked-out branch. For explicit `gza advance <task-id>` planning, `cmd_advance()` instead resolves the lineage's canonical merge target (the task's merge-unit target when present, otherwise the project's strict default merge target) so the next action is deterministic across worktrees. Non-dry explicit merge execution must still run on that exact target branch; if the active checkout differs, advance fails closed and tells the operator to switch branches instead of merging on the wrong checkout. Legacy rows with no resolvable merge unit remain compatibility-oriented fallback candidates instead of being branch-target-filtered away.
   Advance also keeps one shared local-target integration verify checkpoint. Before rendering or confirming any merge plan on the local target, before merge execution, and again after each successful merge, it compares the current local target tree fingerprint with the last verified fingerprint. A changed fingerprint reruns `verify_command` on the local target checkout, and the checkpoint is also invalidated on the same tree when the configured verify-gate identity changes. Freshness is keyed by the normalized `verify_command`, gate-enabled/no-gate state, the verify environment identity recorded with the checkpoint, and the resolved automation timeout settings. That environment identity uses stable semantic runtime fields such as runner class, platform system/machine, and Python implementation/version, rather than exact interpreter paths. Configured-gate checkpoints that lack the required environment identity also fail closed as stale. Configured-gate red or unavailable checkpoints also expire after `main_integration_verify_red_ttl_minutes` even when the fingerprint is unchanged, so watch/advance re-verify on a bounded cadence instead of pinning merges behind one flaky red forever. For configured gates, checkpoint reuse is fail-closed: if the current checkout or the persisted checkpoint cannot produce an exact tree fingerprint, advance must rerun the gate instead of falling back to `HEAD` equality, and a still-unavailable fingerprint persists an operator-visible unavailable proof state that halts merges. Any configured-gate result other than `passed` parks further merges for that command run behind one `main-integration-verify-red` attention row instead of silently continuing to merge onto a red `main`, and the same deduped attention row is reused for both preview and execution output. When that red lane hands off to automatic remediation, the observed verify environment identity still travels into the remediation metadata and prompt, but it no longer vetoes filing or requeueing the bounded remediation task. The only exception is an explicit no-gate project with no configured `verify_command`: that path may persist `status="unavailable"` / `exit_status="not configured"` for visibility, but it does not halt merges or emit red-main attention.

3. **Failed-task recovery inside the owner row**: Failed leaves are filtered through the same bounded recovery policy used by `decide_failed_task_recovery(...)`. That policy can classify candidates as `resume`, `retry`, or manual review required, but the command no longer performs a second standalone failed-task sweep to build the plan. Rows whose authoritative action is recovery expose `row.recovery_action_task` / `row.recovery_leaf_task`; rows whose failed leaf has already handed off to newer completed lifecycle work keep the owner row and its `lifecycle_action_task` so merge/review/rebase planning remains eligible independently. For tag-scoped mixed rows that have both lifecycle work and a distinct failed recovery leaf, `advance --dry-run` and `incomplete` now surface the shared `recovery_leaf_task` ID so queue/advance/incomplete agree on the recovery subset identity without hiding the lifecycle owner row from advance. Failed ancestors are omitted silently once the same automatic recovery intent has completed, whether that completion sits on the failed task's own recovery-only `based_on` chain or on a sibling resume/retry of the same failed parent. The completed recovery task is then handled through the ordinary completed-task rules (merge, rebase, review, or dependency wait) instead of re-printing a permanent `SKIP: recovery child/descendant already completed` row. Failed `review`, `improve`, and `rebase` tasks whose structured target implementation is already landed are omitted only when they are historical same-unit side quests. A descendant with its own distinct failed work, or one whose uniqueness cannot be disproved because legacy branch or merge-unit metadata is incomplete, is re-rooted as its own unresolved unit or legacy leaf so recovery selection can act on that work without reopening the landed owner. Failed resumable timeout `implement` tasks are narrower: branch reachability alone is not enough to hide them, because lifecycle now requires a valid completed merge representative before merge or mark-merged bookkeeping can proceed. They only drop out of failed-task recovery when another valid merged lineage member or completed recovery descendant proves the work already landed. If branch reachability probes fail after the default branch is known, `advance --dry-run` surfaces one warning that only git branch reachability suppression is unavailable for this run; metadata-based same-lineage merged-task suppression may still apply, so failed-row visibility remains conservative only for the git-reachability decision. If a project-backed store cannot resolve the real default merge target at all, failed-task recovery now raises `MergeTargetResolutionError` instead of silently assuming `main`.

`--no-resume-failed` only suppresses rows whose actionable work is failed-task recovery. Owner rows that also carry a non-failed `lifecycle_action_task` remain eligible for merge/review/rebase planning even when they surface a failed recovery descendant.

Optional filters: `--type plan|implement`, `--max N`, or a specific task ID.

Task-scoped `advance <task-id> --repeat` runs the same lifecycle engine in a foreground drain loop. A live repeat session acquires one shared launch permit before any cycle executes and publishes one same-process worker registration for the duration of the drain, so direct actions such as merge and `verify_gate` are still counted against `max_concurrent`. Worker-style foreground children reuse that session slot instead of acquiring a second concurrent slot. Dry-run repeat stays preview-only: merge previews inspect only already-persisted main-integration checkpoints and stop at the execution boundary when freshness cannot be proven without running the gate.

## Configuration

| Field | Default | Description |
|-------|---------|-------------|
| `require_review_before_merge` | `true` | Implement tasks must have a valid current review before merge |
| `advance_create_reviews` | `true` | Auto-create review tasks for implements when review gating still requires them; otherwise lifecycle parks for manual attention instead of creating reviews. |
| `max_resume_attempts` | `1` | Shared automatic failed-task recovery toggle (`0` disables; any positive value enables the fixed bounded resume/retry policy) |
| `max_review_cycles` | `3` | Max review→improve cycles for a merge unit since the current deliberate scope/base boundary before flagging for manual intervention |
| `max_noop_improve_cycles` | `1` | Max consecutive no-op improves before lifecycle automation stops for discussion |
| `advance_off_topic_verify_unblock` | `false` | Whether the narrow legacy compatibility lane for verify-only blocked reviews MAY clear through the audited off-topic verify-failure path instead of parking |
| `autonomous_verify_timeout_seconds` | `120` | Configured floor for lifecycle/automation-initiated `verify_command` runs; recent full-suite observations can derive a larger effective timeout |
| `autonomous_verify_min_margin_seconds` | `60` | Minimum margin required between recent successful `./bin/tests` runtime and the autonomous verify timeout cap |
| `autonomous_verify_observation_max_age_hours` | `168` | Maximum age for successful full-suite runtime evidence used by the lifecycle verify budget preflight |
| `autonomous_verify_bootstrap_timeout_seconds` | `120` | Conservative bootstrap floor; when recent full-suite runtime evidence is missing or stale, the autonomous verify cap must be at least this floor |
| `review_verify_timeout_grace_seconds` | `5` | Grace period after SIGTERM before autonomous lifecycle verification escalates to SIGKILL; accepts float values >= 1 second |
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
| Completed non-held plan whose plan-review loop hit `max_plan_review_cycles` | `create_implement` — accept the latest plan revision for lifecycle purposes and continue on the shared direct-implement path |
| Completed non-held plan with approved plan review slices partially present, the current partial slice set is a proven safe pending subset of the validated manifest, and the durable materialization record is either missing/incomplete or already complete while stale extra pending duplicate slice descendants remain outside the recorded set | `repair_plan_slice_materialization` — revalidate the partial slice set, drop the safe pending partial rows, and rematerialize the full validated slice set through the shared guarded executor path using the same matched slice `trigger_source` that proved the repair candidate |
| Completed non-held plan with approved plan review slices partially present, but the materialization state is ambiguous or unsafe | `needs_discussion` — stop for manual repair or drop of the partial slice set (`reason=plan-review-materialization-repair-needed`) |
| Completed non-held plan with `require_plan_review_before_implement=false` | `create_implement` — legacy compatibility path |
| Plan with existing implement child | `skip` — either approved slices are already materialized, or a legacy/direct implement child already exists |

Foreground `gza iterate <plan>` now drives this same action table directly for `plan`
and `plan_improve` sources. In that mode, iterate may run/recover the plan source
itself, then create/run `plan_review` or `plan_improve`, and finally either materialize
the approved slice set or continue through the shared direct-implement path when capped
plan-review churn is accepted for lifecycle purposes. It stops at slice materialization;
only the direct-implement fallback continues into the created implement child.

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

Before advance queues rebase, review, improve, or merge work for a code-changing branch, it checks the branch diff against the task's strict project scope. This uses the existing project-boundary machinery, but for this verdict only the configured project subdirectory is writable unless the task is explicitly tagged `cross-project` or the project sets `default_cross_project: true`. Cross-project tasks still fail closed if the branch touches paths outside all discovered project roots or any new roots declared by changed branch-local `gza.yaml` files. Nested project discovery includes ancestor project configs for parent-owned path attribution, while most-specific path matching prevents ancestor projects from being selected merely because a descendant project changed.

| Condition | Action |
|-----------|--------|
| Branch diff includes any path outside the strict project scope AND task is not explicitly or implicitly cross-project | `needs_discussion` — park for human review immediately, list the offending paths, and tell the operator to tag `cross-project` and re-advance if intended or fix the branch |
| Branch diff for an explicitly or implicitly cross-project task includes any path outside all discovered project roots and branch-declared `gza.yaml` roots | `needs_discussion` — park for human review immediately, list the offending paths, and tell the operator to fix the branch or add missing project configs so the affected roots are discoverable |
| Branch diff cannot be inspected reliably for the strict-scope check AND task is not explicitly or implicitly cross-project | `needs_discussion` — fail closed, say that strict project scope could not be verified, and stop all automation until the operator fixes the ref/diff problem or tags `cross-project` if the wider scope is intended |
| Branch diff cannot be inspected reliably for the strict-scope check AND task is already explicitly or implicitly cross-project | `needs_discussion` — fail closed, say that strict project scope could not be verified, and stop all automation until the operator fixes the ref/diff problem |

### 4. Merge conflicts

Conflict detection uses the same target-branch resolution as task collection:

- Default `gza advance` uses the currently checked-out branch as the merge target (`target_branch = git.current_branch()`).
- Explicit `gza advance <task-id>` uses the lineage's canonical merge target (`_resolve_advance_target_branch()`): the task's merge-unit target when present, otherwise the project's strict default merge target. If that target cannot be resolved, the command errors instead of silently assuming `main`.

| Condition | Action |
|-----------|--------|
| A review / improve / verify-fix / verify-gate / merge / branch-verification recovery action is selected for a merge-unit branch, and the branch cannot merge into the resolved target or is positively known to be behind it | Run the shared pre-dispatch rebase gate before returning the selected action |
| The selected branch action needs the pre-dispatch rebase AND target freshness cannot be verified because the behind-count probe failed, returned no result, or returned malformed data | `needs_discussion` — reason `pre-dispatch-target-freshness-unverified`; fail closed instead of dispatching against an unproven-current branch |
| The selected branch action needs the pre-dispatch rebase AND a rebase child is `pending`/`in_progress` | `skip` — rebase already running |
| The selected branch action needs the pre-dispatch rebase AND the lineage has three qualifying failed same-branch rebases with no later reset proof | `needs_discussion` — reason `rebase-failure-circuit-breaker`; stop repeated rebase creation |
| The selected branch action needs the pre-dispatch rebase AND a rebase child is `failed`, and shared recovery classification says that failure is manual/conflict | `needs_discussion` — manual intervention required unless later local post-resolution proof exists |
| Branch cannot merge into the resolved target branch AND a same-branch rebase child already completed AND the branch already contains the current target tip | `needs_discussion` — reason `rebase-did-not-unblock-merge`; stop repeated no-op rebases only when the completed rebase already includes the current target tip |
| Local branch and `origin/<branch>` diverged during explicit host-side publication reconcile | `reconcile_branch_divergence` — treat publication reconcile and lifecycle merge proof as separate steps: advance/watch merge proof still uses confirmed local refs only, so remote-only or divergent `origin/<branch>` is not merge-source proof; explicit reconcile may inspect or fetch remote publication state and publish directly with `--force-with-lease` when the local branch is strictly ahead, when the divergence is a symmetric gza rewrite of equivalent patch content, or when the remote-only commits are all stale gza `WIP: gza task interrupted` savepoints; if publication still needs branch-content reconciliation, mechanically rebase onto the resolved local target branch, then publish, and park real host-side conflicts as explicit needs-attention instead of spawning a sandboxed `rebase` against an unreachable remote-tracking ref |
| The selected branch action needs the pre-dispatch rebase AND no failed/active rebase policy blocks it AND the branch does not already contain the target tip | `needs_rebase` — create rebase task, including stale completed rebases whose branch no longer contains the current target tip |
| Branch cannot merge into the resolved target branch AND the branch already contains the target tip AND the lineage task is still incomplete | `needs_discussion` — rebase is already proved unnecessary; surface the incomplete lineage instead of looping |

Manual/conflict failed rebases are not cleared just because the latest implementation tip becomes mergeable again. If an implementation lineage still has no later approved or cleared review after that failed rebase, advance continues to surface `rebase-failed-needs-manual-resolution` instead of creating a first review from the now-clean tip, unless a later local post-resolution proof exists. Retryable/transient failed rebases instead stay on the shared recovery path first; if recovery picks `resume` or `retry`, lifecycle follows that shared decision and only inserts the local-target `recovery-preflight-rebase` when the branch does not yet contain the current target tip. The local proofs are intentionally narrow: a merged merge unit, exact branch-tip equality with the current target branch, or proof that the implementation branch already contains the current target tip. That proof now suppresses fresh `needs_rebase` planning as well: when the branch already contains the target, advance either continues with the ordinary review/merge flow or raises one shared `needs_attention` row for the real non-rebase blocker.

Repeated failed rebases are bounded independently of `can_merge`. Once the same branch accumulates 3 failed rebase attempts with no intervening successful rebase, completed review, review clear, or completed code change, advance/watch stop creating more rebases and emit `needs_discussion` with reason `rebase-failure-circuit-breaker`, including cleanly mergeable branches that are still behind the current target. A behind-count probe failure similarly fails closed with `pre-dispatch-target-freshness-unverified`; only a successful zero count lets a mergeable branch proceed without pre-dispatch rebase.

### 5. Post-rebase review invalidation

| Condition | Action |
|-----------|--------|
| Review requirement for the implementation-owned lineage is disabled (`require_review_before_merge=false`) | Fall through to the normal no-review merge path; do not create, run, or wait on a stale refresh review |
| Implementation-owned lineage has no resolvable local merge source, no explicit merge-source warning, and persisted merge state is not terminal | `needs_discussion` — reason=`merge-source-needs-manual-resolution`; fail closed before any pre-review `verify_gate`, `create_review`, or `run_review` automation |
| Current verify gate for the implementation owner is missing or stale before any review can be created/refreshed | `verify_gate` — rerun the current owner verify gate first |
| Current verify gate for the implementation owner timed out with `failure_origin=timeout` and complete evidence from current metadata, recoverable legacy output, or cross-project aggregate summaries proving no structured failed phase | `needs_discussion` — reason=`verify-budget-exceeded`; do not create `verify_fix`; surface completed and never-started phase details so the operator can increase `autonomous_verify_timeout_seconds`, split the suite, or refresh green runtime evidence before rerunning verify |
| Current verify gate for the implementation owner timed out with missing, unrecoverable, malformed, or contradictory phase summary evidence | `needs_discussion` — reason=`verify-phase-evidence-invalid`; do not create `verify_fix`; rerun lifecycle verify because the timeout evidence cannot prove whether a phase failed |
| Current verify gate for the implementation owner is red before any review can be created/refreshed, and no same-epoch `verify_fix` exists yet | `create_verify_fix` — create one same-branch verify-fix lane keyed by the reviewed branch and reviewed head SHA; the normalized verify command and timeout settings are recorded as run provenance, not freshness identity |
| A same-merge-unit contributor has newer current same-head verify evidence than the canonical owner before review/merge gating | `reconcile_verify_gate_evidence` — recredit that evidence to the canonical owner without rerunning verify, then reevaluate lifecycle before choosing review, merge, verify rerun, `verify_fix`, or park routing |
| Current verify gate for the implementation owner is red before any review can be created/refreshed, and same-epoch `verify_fix` is `pending` / `in_progress` | `run_verify_fix` / `wait_verify_fix` — reuse the existing lane instead of creating another |
| Completed same-epoch `verify_fix` made no source changes, the current red verify result is structured timeout-origin, the completion outcome has an exact head and no consumed recovery rerun, and the managed worktree is clean for that exact head | `rerun_completed_verify_fix` — rerun the same-head verify once and persist the owner gate result and consumed-rerun outcome atomically before continuing; legacy completed rows may be upgraded only when the canonical outcome field is absent and the live branch still equals the verify epoch head with a clean tree. Present-but-invalid canonical outcome metadata parks with `verify-fix-proof-unavailable` instead of falling back to legacy proof |
| Current verify gate for the implementation owner is still red after one completed same-epoch `verify_fix` | `needs_discussion` — reason=`verify-fix-failed`; stop after one completed verify-fix attempt for that epoch; deterministic test failures and unknown structured failure origins remain blocking. Manual `iterate --force` or `unstick --reason verify-fix-failed` rearm reruns the verify gate at the current head before this park can be bypassed. If that fresh rerun cannot execute or persist, scoped watch/unstick and iterate render a direct blocked diagnostic and do not continue to review or merge |
| Same-epoch `verify_fix` failed before completing | Shared failed-task recovery first; retryable failures dispatch/reuse a bounded retry of the `verify_fix`, while exhausted or non-retryable decisions park with `verify-failed-needs-fix` and name the recovery stop |
| A completed rebase on the implementation branch exists that is newer than the latest review and changed the tracked diff AND `advance_create_reviews=true` | `create_review` — rebase may have introduced changes; use a narrow resolution review only when complete rebase/resolution provenance is available |
| Current live implementation branch head differs from the latest completed review's recorded reviewed head SHA AND both SHAs are known AND `advance_create_reviews=true` | `create_review` — durable branch progress made the latest review stale |
| Either stale-review condition above AND `advance_create_reviews=false` | `needs_discussion` — park and require a manual review refresh before merge |

Manual `gza merge` can bypass current red verify-gate actions only with the two-key gesture
`--force --ignore-verify-gate`. The covered actions are `create_verify_fix`,
`rerun_completed_verify_fix`, and same-epoch `needs_discussion` with
reason=`verify-fix-failed`. Successful bypasses print the failing epoch head and verify
command and persist `manual_force` merge provenance. The red-gate bypass does not cover
pending/in-progress verify-fix task recovery (`run_verify_fix` / `wait_verify_fix`),
unavailable verify evidence, invalid verify-fix proof, failed/stopped verify-fix task
recovery, git conflicts, or open review `BLOCKER` findings. Manual `gza merge` is not a
rebase orchestration surface: the removed merge flags `--rebase`, `--remote`, and
`--resolve` are rejected by the parser with no compatibility aliases. Use
`gza rebase <task-id> --run` for standalone task-backed rebase execution, or `gza land
<task-id>` for full operator landing orchestration.

When a completed changed-diff rebase row has lost or partially lost its persisted provenance block, a writable maintenance/lifecycle path first re-derives that provenance from local reflogs plus the surviving branch/target refs and writes the repaired `review_scope` back to the rebase row. Complete recovered provenance is usable for the narrow resolution-review optimization when all pre-rebase SHAs plus resolved post-rebase head/target SHAs are available. Changed-diff review-cycle reset authority is narrower: it requires durable affirmative boundary proof tied to the exact immutable target SHA consumed by the rebase operation and used for the post-rebase comparison, so target movement cannot be laundered by a later return to the same ref value. Review-cycle membership after such a boundary is attributed by review attempt creation time, not completion time, so a stale in-flight review cannot consume a fresh post-boundary round. When a required post-rebase refresh review already exists but its persisted resolution metadata is blank or otherwise inconsistent with that authoritative rebase context, writable lifecycle re-derives the canonical resolved head/target SHAs, rewrites the review row with structured resolution metadata, and continues through the normal narrow review/merge path. Plain reviews remain plain; they are not rewritten into resolution reviews merely because they were completed after a changed-diff rebase.

If rebase or resolution metadata cannot be validated or deterministically repaired, lifecycle degrades to the coarser full-review rule instead of permanently parking on provenance absence: only a plain full review whose stored reviewed head SHA equals the proven current live implementation branch head can satisfy the review gate, and stale full-review approvals are rejected. Until that review exists, lifecycle runs the normal pre-review verify gate and creates a plain full review at the live head. A pending plain full-review row is reused only when its stored reviewed head already matches the proven live head, and the executor checks that selected head before launch; otherwise the row is superseded or lifecycle fails closed if no live proof exists. Pending malformed resolution-review refresh rows can be dropped before that fallback, but valid spec-coherence reviews remain owned by the spec-coherence gate and an `in_progress` malformed refresh row is waited on until terminalization so its worker cannot complete concurrently with a replacement review. If the live branch head itself cannot be established, lifecycle fails closed with `review-freshness-unverified`; cached merge-unit head metadata is not equivalent proof. Operator wording distinguishes unavailable resolution metadata from true branch-head advancement. Query-only surfaces such as `gza show`, incomplete-lineage rendering, dispatch preview, and watch scope-gap analysis may apply proven repairs in memory, but they must not write.

### 6. Review state (when reviews exist)

#### 6a. Review was cleared (improve task ran after review)

| Condition | Action |
|-----------|--------|
| Review requirement for the implementation-owned lineage is disabled (`require_review_before_merge=false`) | Fall through to the normal no-review merge path; do not create, run, or wait on a stale refresh review, and do not enforce the closing-review gate |
| Active review is `pending` | `run_review` — spawn worker for it |
| Active review is `in_progress` | `wait_review` — skip |
| Completed improve exists after latest review and changed the tracked reviewable diff | Route back through the two-gate flow: rerun verify first, then create/run a fresh review for the improved head. Concretely, lifecycle must first satisfy the current pre-review verify gate (`verify_gate`, `create_verify_fix`, `run_verify_fix`, or `wait_verify_fix` as applicable) before it creates that fresh review |
| Completed no-op improve exists after latest review | The no-op improve does not bypass the two-gate model by itself. Ordinary lifecycle work still needs a current green verify gate and a current merge-permitting review for the current head. Verify-only review rows may still use the narrow legacy compatibility lane, but that is not the default merge path |

#### 6b. Review is active (not cleared)

Severity semantics for `BLOCKER`, `FOLLOWUP`, and `NIT` live in [docs/merge-policy.md](../merge-policy.md). Use that rubric when interpreting review output or adjusting the review contract.

| Condition | Action |
|-----------|--------|
| Latest review is `pending` | `run_review` — spawn worker |
| Latest review is `in_progress` | `wait_review` — skip |
| Task type is `implement`, verdict is `APPROVED`/`APPROVED_WITH_FOLLOWUPS` (or review is cleared), and unresolved comments are newer than the latest completed review | Prefer improve flow (`wait_improve`/`run_improve`/`improve`) before any merge |
| Verdict = `APPROVED` and the review is still valid for the current mergeable diff | If the current pre-merge verify gate is green, `merge`; otherwise lifecycle must route through `verify_gate` or the same-epoch `verify_fix` lane before merge |
| Verdict = `APPROVED_WITH_FOLLOWUPS` with at least one parsed `FOLLOWUP` finding and the review is still valid for the current mergeable diff | If the current pre-merge verify gate is green, `merge_with_followups`; otherwise lifecycle must route through `verify_gate` or the same-epoch `verify_fix` lane before creating follow-ups and merging |
| Verdict = `APPROVED_WITH_FOLLOWUPS` with zero parsed `FOLLOWUP` findings | `needs_discussion` — fail closed; review output is inconsistent |
| Verdict = `CHANGES_REQUESTED` AND improve is `in_progress` | `wait_improve` — skip |
| Verdict = `CHANGES_REQUESTED` AND improve is `pending` | `run_improve` — spawn worker |
| Verdict = `CHANGES_REQUESTED` AND consecutive completed no-op improves for the latest `(impl, review)` pair >= `max_noop_improve_cycles` AND the latest blocker set includes an adjudication-eligible disputed non-verify CODE blocker | `create_review_adjudication` / `run_review_adjudication` / `wait_review_adjudication` before the generic no-op park; lifecycle persists and consumes the strict `VALID | INVALID | NEEDS_HUMAN` outcome for the matching disputed blocker |
| Verdict = `CHANGES_REQUESTED`, the latest blocker set is a verify-only compatibility case, current trusted green verify evidence already exists for the exact reviewed head/tree fingerprint, `advance_off_topic_verify_unblock=true`, and a fresh red reverify classifies as off-topic with full failing-node enumeration | `clear_off_topic_verify_blocker` — clear the review blocker, create or reuse exactly one non-blocking investigation task per normalized failing-node signature, and surface the created/reused investigation IDs in operator output. If the investigation record cannot be durably created or reused, lifecycle fails closed and keeps the review blocking |
| Consecutive completed no-op improves for the latest `(impl, review)` pair >= `max_noop_improve_cycles` | `needs_discussion` — reason=`improve-no-op`; stop repeated no-op improve loops after adjudication and any narrow historical compatibility handling are exhausted |
| Verdict = `CHANGES_REQUESTED` AND the same primary blocker repeats for 3 consecutive completed review cycles with no completed rebase boundary between them | `create_review_adjudication` / `run_review_adjudication` / `wait_review_adjudication` using synthesized repeated-review dispute metadata before the generic duplicate-blocker or review-max-cycles park |
| Live branch-head probe fails while checking whether the latest completed review is still current | `needs_discussion` — reason=`review-freshness-unverified`; fail closed, surface the probe warning, and do not treat cached merge-unit head metadata as proof the review is current |
| Verdict = `CHANGES_REQUESTED` AND merge-unit cycles since the current deliberate scope/base boundary >= `max_review_cycles` with no stale-review refresh path | `max_cycles_reached` — manual intervention under `on_max_cycles=park`; spec-coherence reviews use the same count after pending/in-progress improves are allowed to run or finish. With `on_max_cycles=merge_and_defer`, eligible ordinary current-head candidates first prove local merge source and readable deterministic blocker content, then traverse the existing pre-merge verify gate, so missing/stale verify runs and red/unavailable verify stays on the verify-fix or attention path, while fresh green verify plus validated persisted blocker metadata emits the existing `merge` action annotated for max-cycle deferral. Missing merge source surfaces `merge-source-needs-manual-resolution`; otherwise missing, blank, unreadable, or invalid review content surfaces dedicated max-cycle review-content reasons before verify gating. |
| Verdict = `CHANGES_REQUESTED` AND no improve exists | `improve` — create improve task |
| Verdict = unknown | `needs_discussion` — manual intervention |

Review-cycle accounting includes strict exact legacy unlinked review rows independently
from canonical lifecycle review state. Canonical lifecycle state uses linked or
merge-unit-attached evidence first and consults legacy unlinked rows only as fallback
when that evidence is absent. Accounting can still count eligible legacy rounds toward
`max_review_cycles` without changing the authoritative latest review used for merge,
finalization, or improve decisions. Overlapping implementation slugs such as `foo` and
`foo-bar` stay separate. When separate implementation epochs reuse the same semantic
slug, dated legacy review slugs are matched against the implementation's dated identity;
prompt-only `review <slug>` rows are bounded by attempt creation time to one
implementation epoch so they cannot be charged to multiple merge units.

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
  `NEEDS_HUMAN` parks with `review-blocker-adjudication-needed`. Verify-only
  review rows remain a separate narrow compatibility case and do not redefine the normal
  two-gate merge flow.

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
| Implementation-owned lineage reviews all exist but are cleared/addressed, and no newer rebase or closing-review requirement invalidates that state | If the current pre-merge verify gate is green, `merge`; otherwise lifecycle must route through `verify_gate` or the same-epoch `verify_fix` lane before merge |
| Standalone non-implement task type (plan, explore, etc.), or a merge-unit lineage whose owner does not require review | `merge` — no review required |

Merge-unit members inherit the review state and review requirement of the actionable implementation lineage member on that shared branch. Merge planning and merge-state mutation now also require that representative to have execution status `completed` or legacy-compatible `unmerged`; failed owners cannot satisfy merge eligibility on behalf of the unit. When the compatibility owner row is a failed historical implement and the current code lives on a completed resume descendant, closing-review state, post-rebase invalidation, and merge eligibility all resolve against that completed descendant. Review evidence for that implementation includes direct implementation-linked reviews, merge-unit-attached reviews, and review recovery descendants whose `based_on` chain stays on automatic review recovery edges; manual same-type review follow-ups do not count. A completed `rebase` or other same-branch member of such an implement-owned merge unit must create or wait on that lineage review before merge when no review evidence exists yet.

For the closing-review invariant after a newer completed code change, any eligible follow-on completed review recovery descendant counts as the required review evidence. If the closing review fails, bounded retry accounting follows that same recovery chain, so a failed review plus its failed retry/resume descendants exhaust the configured closing-review retry budget together instead of re-triggering unbounded fresh `create_review` selection on a stable head.

### 8. Implementation-owned lineage with no review

| Condition | Action |
|-----------|--------|
| `require_review_before_merge=true`, `advance_create_reviews=true`, and the implementation lineage has no resolvable local merge source while persisted merge state is not terminal | `needs_discussion` with `reason=merge-source-needs-manual-resolution` |
| `require_review_before_merge=true` and `advance_create_reviews=true` | If the current pre-review verify gate is green, `create_review`; otherwise lifecycle must route through `verify_gate` or the same-epoch `verify_fix` lane first |
| `require_review_before_merge=true` and `advance_create_reviews=false` | `needs_discussion` with `reason=review-needs-manual-creation` |
| `require_review_before_merge=false` | If the current pre-merge verify gate is green, `merge`; otherwise lifecycle must route through `verify_gate` or the same-epoch `verify_fix` lane first |

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

Completed improve tasks persist `changed_diff` to record whether the task changed the tracked aggregate review diff compared with the branch state captured immediately before the improve started. `changed_diff = 0` means the improve completed but made no tracked reviewable change, so the runner does not clear review state, resolve comments, or create a closing review. In the ordinary two-gate model, that means lifecycle still needs current green review and verify evidence for the current head; a no-op improve does not create those gates on its own. Verify-only review rows may still flow through a separate compatibility path, including persisted `review_clearance` artifacts and the audited off-topic classifier, but that path is residual compatibility behavior rather than the default lifecycle model for current work.

Historical verify-only `CHANGES_REQUESTED` reviews may still flow through a narrow compatibility lane when the same-head clear path does not apply because the fresh verify remains red. That legacy lane is controlled by `advance_off_topic_verify_unblock` and stays fail-closed by default: lifecycle must prove the latest review is verify-only blocked, the current trusted green and later red evidence are both bound to the exact same reviewed head SHA and tree fingerprint, and the red failing-node set was fully enumerated without fail-fast. On a successful off-topic classification, lifecycle may clear only that persisted legacy review state after it durably creates or reuses exactly one non-blocking investigation task per normalized failing-node signature, with structured evidence metadata for the failing node, provenance, and verify command. This compatibility path does not authorize ordinary merge for current two-gate work, does not replace the pre-review or pre-merge lifecycle verify gates, and does not bypass the requirement for current green verify evidence plus a merge-permitting current review on the current head. Those investigation prompts now carry an explicit `REPRODUCE-OR-RECORD` contract: reproduce the same signature under a bounded targeted harness before fixing, rerun that same harness green after fixing, or record a structured inconclusive result instead of making a speculative patch. The operator/agent entrypoint for that harness is `uv run gza flaky reproduce <investigation-task-id>`, which preserves the recorded project cwd, prefixes `PYTHONFAULTHANDLER=1`, reuses the targeted failing-node command, and adds xdist/randomization stress flags only when the relevant tooling is actually available. Each harness run persists `flaky_verify_attempt` artifacts, and a budget-exhausted no-repro outcome persists one `flaky_verify_inconclusive` artifact carrying the attempt IDs, environment details, and operator-supplied hypotheses. Operator-facing advance/watch output includes the created or reused investigation task IDs. If classification is unavailable, the failure scopes into the branch diff, the target-side proof is inconclusive, or the investigation record cannot be persisted, lifecycle keeps the legacy review blocker in place.

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
| `create_verify_fix` | Creates one same-branch `verify_fix` task for the exact red verify epoch, then starts the remediation worker |
| `run_verify_fix` | Starts an existing pending same-epoch `verify_fix` task; manual red-gate force bypass must refuse while this live task exists |
| `create_review` | `gza advance`: creates review task, spawns worker. `gza watch`: for unmerged implementation chains, launches `gza iterate <impl>` and lets iterate create/reuse the review work internally. Foreground iterate and background iterate startup both use the same selected-action review factory, so stale-review refreshes after changed rebases create resolution-scoped review rows instead of falling back to ordinary reviews. |
| `run_review` | `gza advance`: spawns worker for existing pending review. `gza watch`: for unmerged implementation chains, launches `gza iterate <impl>` instead of the child review directly. |
| `improve` | `gza advance`: creates/resumes/retries improve work directly. `gza watch`: for unmerged implementation chains, launches `gza iterate <impl>` and lets iterate choose the improve action. |
| `run_improve` | `gza advance`: spawns worker for existing pending improve. `gza watch`: for unmerged implementation chains, launches `gza iterate <impl>` instead of the child improve directly. |
| `create_plan_review` | Creates `plan_review` task for a completed plan source, spawns worker |
| `run_plan_review` | Starts an existing pending `plan_review` task |
| `create_plan_improve` | Creates `plan_improve` task after `CHANGES_REQUESTED` plan review, spawns worker |
| `run_plan_improve` | Starts an existing pending `plan_improve` task |
| `materialize_plan_slices` | Creates sliced implement tasks from an approved plan-review manifest; each slice becomes its own branch/merge unit and ordering stays on `depends_on` |
| `create_implement` | Creates implement task for a plan, spawns worker (used both for the legacy compatibility path when the plan-review gate is disabled and for accepting the latest plan revision after capped plan-review churn) |
| `resume` | Creates resume task, spawns worker |
| `retry` | Creates retry task, spawns worker |

### Direct actions

| Action | What it does |
|--------|-------------|
| `merge` | Merges the task's branch synchronously. Respects `merge_squash_threshold`. |
| `merge_with_followups` | Creates/reuses follow-up `implement` tasks from parsed `## Follow-Ups` findings, then merges synchronously. |
| `verify_gate` | Runs the lifecycle-owned verify gate for the current canonical implementation owner/head before review creation or merge. In `watch`/`iterate`, this routes through the shared advance executor and does not consume a worker slot. |
| `reconcile_verify_gate_evidence` | Recredits the newest current same-head merge-unit verify evidence to the canonical owner without rerunning verify, then lets lifecycle reevaluate the next review, merge, verify-fix, or park action. |
| `release_approved_plan_review` | Releases a held approved plan source by persisting `auto_implement=true`; slice materialization remains a later pass through the normal approved-manifest action. |
| `repair_plan_slice_materialization` | Re-reads the current approved plan-review/source rows, revalidates the current partial descendant slice set against the validated manifest with the same matched slice `trigger_source` that selected the repair action, and only then drops the safe pending partial rows and rematerializes the full slice set. `advance`, `watch`, and foreground `iterate` all route through this same guarded executor path. |
| `rerun_completed_verify_fix` | Runs verification synchronously for a completed same-epoch no-source `verify_fix` and does not consume a worker batch slot. It proves the managed worktree's checked-out `HEAD`, branch ref, verify epoch, and persisted completion SHA all match before and after the rerun; the owner verify-gate result and consumed-rerun outcome are committed atomically, and any persisted non-green result consumes the one recovery rerun so the next advance pass parks instead of rerunning. |
| `clear_off_topic_verify_blocker` | Clears a verify-only review blocker after audited off-topic classification, then durably creates or reuses one non-blocking investigation task per normalized failure signature before the lineage can continue toward merge. |

### Skip actions

| Action | Meaning |
|--------|---------|
| `skip` | No action needed or possible |
| `wait_review` | Review in progress, wait for it |
| `wait_verify_fix` | Verify-fix remediation is already in progress for the current verify epoch; manual red-gate force bypass must refuse while this live task exists |
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

For worker-spawning actions that first create or reuse a child task (`create_plan_review`, `create_plan_improve`, `create_verify_fix`, `create_review`, `create_implement`, `resume`, `retry`, `needs_rebase`), operator output must distinguish task selection/creation success from worker-launch failure. If task creation succeeds, or if the executor reuses an existing eligible recovery task, but the background worker fails to start, `gza advance` should print the relevant created/reused task ID and a separate `Failed to start ... worker` line rather than collapsing that state into `✗ Created ...`.

For `clear_off_topic_verify_blocker`, operator output should include the created and/or reused investigation task IDs in the success line so the off-topic clearance remains auditable without opening the DB or artifact store. A persistence failure while creating or reusing those investigation records must surface as an error and leave the review uncleared. For the downstream investigation itself, `gza flaky reproduce` is the only supported reproduce helper: it keeps the run bounded, records every attempt, and automatically writes a structured inconclusive artifact when the exact signature does not reproduce within budget. The workflow intentionally forbids blanket sleeps, blanket retries, `@flaky`, or broad timeout inflation as the default remedy.

With `on_max_cycles: park`, capped implementation reviews park for attention:

```
Will advance N task(s):

  gza-2a Implement feature X
      → Run verify gate before merge (review APPROVED; current merge gate missing)

  gza-2d Fix caching bug
      → Run verify gate before review (required before merge)

  gza-26 Refactor API
      → SKIP: rebase gza-33 already in progress

Advanced: 2 verify gates run, 1 skipped

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

`gza advance <task-id> --repeat` is the task-scoped foreground drain form. It
re-resolves the named task after each cycle, executes the selected action through the
shared advance executor, and continues until the task merges, parks for human attention,
hits a skip/no-progress backstop, or reaches `--max-iterations` (defaulting to
`iterate_max_iterations`). Worker-style lifecycle actions run foreground so the next
cycle observes their completed state; merge actions use the same advance merge executor
and main-integration/candidate verification gates as the single-pass command. Each
foreground worker-style action acquires and transfers a shared launch permit through the
existing worker startup path, then releases the launch lock once the action is visible;
the complete repeat drain does not hold the launch flock for its whole lifetime. Use
`--dry-run` to print engine-selected cycle actions without side effects. If a later
cycle cannot be established without executing the current action, dry-run reports that
boundary instead of synthesizing follow-on actions. Use `--auto`/`-y` to skip the
initial prompt.

## Relationship to other commands

| Command | Relationship |
|---------|-------------|
| `gza work` | Advance spawns workers that run `gza work --worker-mode` |
| `gza review` | Advance only creates review tasks after the relevant pre-review verify gate is green; once that gate is satisfied, the created review task is equivalent to bare `gza review` (queue-by-default) |
| `gza improve` | Advance creates improve tasks equivalent to bare `gza improve` (queue-by-default) |
| `gza rebase` | Advance creates rebase tasks equivalent to `gza rebase --background` |
| `gza merge` | Advance normally merges directly only after the current pre-merge verify gate and current review gate are both green. The narrow exception is `on_max_cycles=merge_and_defer`: an ordinary capped `CHANGES_REQUESTED` review can emit an annotated merge only after current-head proof, live merge-source proof, readable deterministic blocker content, fresh green verify evidence, and mandatory deferred-blocker payload validation. Manual `gza merge <id>` uses the shared direct verify prerequisite executor for verify reruns and verify-evidence reconciliation before it reevaluates whether merge is allowed |
| `gza watch` | Runs advance in a loop with sleep intervals; `watch.recovery_slots` reserves failed-task recovery capacity before pending pickup |

## Watch integration

`gza watch` reuses the same advance executor and improve-resolution helpers described above; it does not maintain a separate improve retry policy. For `verify_gate`, `create_verify_fix`, `run_verify_fix`, `create_review`, `run_review`, `improve`, and `run_improve` on unmerged implementation chains, watch resolves the root implementation first and launches `gza iterate <impl>` before any child review/improve side effects occur. Iterate then owns child creation, reuse, recovery, and immediate follow-on steps inside its loop. Because stale-review refresh resolves on the shared advance path before `review_max_cycles`, a capped lineage whose latest review is stale due to rebase-changed diff or branch-head advancement stays on the auto-refresh path instead of surfacing as needs-attention. With `on_max_cycles=merge_and_defer`, only ordinary current-head capped review candidates with live merge-source proof and readable deterministic blocker content are allowed into the pre-merge verify gate before the cap action; missing merge sources and unavailable or invalid review content surface their own attention reasons before verify handling, with missing merge source taking precedence when both states are present. No-op, adjudication, duplicate-blocker, and spec-coherence lanes continue to preempt that candidate. When both stale sources apply, the shared operator wording stays rebase-specific so the refresh reason still explains that the rebase changed code (or that the change proof is unknown), rather than collapsing to generic branch-head advancement. If the live branch-head probe itself fails, the same shared path now fails closed with `review-freshness-unverified` instead of trusting cached merge-unit head metadata to keep the review mergeable or to park as ordinary `review-max-cycles-reached`.

Watch no-progress accounting has one compatibility exception for old or malformed `create_review` observations where the implementation subject itself was recorded as the action task. That state means the selected create-review action never materialized a matching review row, so watch clears the subject's persisted watch-progress and recovery-backoff rows and leaves only the current dispatch/routing diagnostic visible before retrying later. Historical reviews from an older action epoch do not count: ordinary branch-head or closing-review refreshes match the selected implementation and reviewed head when the selected action has one, resolution-review materialization must match the selected implementation, rebase task, resolved head SHA, and resolved target SHA, and spec-coherence materialization must match the selected implementation, reviewed head SHA, and changed behavior-spec paths. The same predicate applies before reusing an active duplicate review for a selected create-review action. The exception ends as soon as a matching review row is recorded as the action task: repeated completed no-op review outcomes, unchanged materialized review launches, and selected materialized review actions that still cannot dispatch remain bounded by `watch.no_progress_cycles` and park with `watch-no-progress-backstop`.

Watch renders human-needed advance outcomes (`needs_discussion`, `max_cycles_reached`, failed-task recovery states that now require an operator decision, and improve-recovery stop reasons) as `ATTENTION` log lines instead of deduped `SKIP` lines. The first inline reminder for a newly visible attention key, or for a key whose message changed since the previous watch pass, reuses the same formatted task line as the `advance` needs-attention section, including the stable `reason=...` policy slug and the shared single-line shortened prompt. Unchanged inline reminders are suppressed until the next change. Attention identity comes from the action's declared `subject_task_id`, not from owner-row rollup heuristics. Legacy or malformed attention actions still fall back defensively, but the shared resolver logs a warning before doing so. Each watch pass that has visible attention also prints a counted `Needs attention (...)` roundup grouped by attention category, for example `advance-attention=2`; when the visible set is unchanged from the previous pass, the roundup instead reports only that the same number of tasks still need attention. The per-cycle roundup deliberately does not repeat formatted task rows, so operators should use the one-time `ATTENTION` detail or `uv run gza incomplete` for task IDs and prompts. For `review-max-cycles-reached`, the CLI attention surfaces pair that row with `Recommended next step: uv run gza fix <task-id>`. For failed-recovery reasons such as `automatic-recovery-disabled`, `retry-limit-reached`, and `retryable-provider-error`, the shared CLI recommendation is category-aware: never-completed implementations still tell the operator to retry or re-implement instead; completed implementations with retryable terminal failures now point at `uv run gza unstick <owner-id> --reason retry-limit --run`; completed implementations with non-retryable/manual terminal failures keep the `gza fix` handoff. Guarded-pending routing skips are promoted through the same centralized attention path on the first observed guarded skip, using the pending task as the named subject so parked child work does not stay hidden behind deduped `SKIP` lines or the counted needs-attention summary. After that first emission, unchanged guarded-pending inline reminders are suppressed by the same dedupe rule while the roundup continues to count them. For owner rows already parked on the failed-task-recovery stop reasons `retry-limit-reached` or `retryable-provider-error`, watch reuses that parked action instead of recomputing a fresh lifecycle step, so no background iterate worker is re-spawned until a human changes the lineage state. Treat `manual-review-required` as a legacy alias rather than a current parked recovery reason. Ordinary watch skip/wait lines remain deduped across passes.

When watch checks the local target integration verify gate, it emits concise `INFO` progress lines before the checkpoint check, before each bounded merge-halting rerun attempt, and after completion. The pre-check line says the suite will run if the checkpoint is stale or if a red result needs bounded confirmation, so a cached checkpoint is not described as an active long-running verify. Rerun progress labels the preceding evidence truthfully: `red` only for an actual failed gate verdict, `unavailable` for unavailable evidence, and `unknown` for malformed evidence. The completion line reports `green` only for a passed configured gate, `red` for a merge-halting failure with failing phase when known, `cached <status>` when no verify ran, and non-green statuses such as `disabled`, `unavailable`, `launch-failed`, or `unknown` for no-gate, environment, or malformed evidence failures. Raw suite stdout is suppressed for the main-verify subprocess when watch is in `--quiet` or `--yes` mode, but the structured watch progress lines remain visible unless `--quiet` suppresses all watch stdout.

`gza watch <task-id>...` is an explicit selector scope over the same owner-row planning model. The command preserves every raw positional ID, pairs it with its startup canonical owner, and carries the current effective live owner/leaf from scoped analysis. Naming the canonical owner itself retains whole-owner semantics. Naming a descendant can re-root the effective scope to a selector-matching failed leaf under a landed canonical owner; that leaf then owns transition filtering, active counting, failure boundaries, recovery actions, blind parked auto-rearm, same-cycle reanalysis, and the visible scope banner. The effective closure is established before the first transition/failure boundary, including after initial preview, and missing or ambiguous rows do not broaden back to the canonical owner while a raw leaf selector is available. Multiple selectors may share one startup owner and remain distinct; unselected siblings under that owner are excluded from recovery rows, launches, mutable rearm paths, and scoped completion. Watch keeps normal merge semantics and worker-slot accounting, but disables unrelated pending pickup and the global failed-task recovery lane; only recovery surfaced through the effective scoped owner rows is eligible. Because there is no pending lane in that mode, worker-consuming scoped recovery may use all currently available slots in the pass instead of staying capped by the global `watch.recovery_slots` reserve. Only an explicit `--pending-only` selection suppresses that scoped recovery path; config/default `watch.recovery_slots: 0` does not.

If a merge action reaches the default checkout and finds tracked local changes, watch now treats that as a structured merge blocker instead of a generic merge failure. It emits one `ATTENTION` line per pass with `merges blocked: main checkout has uncommitted changes - commit or stash them first`, stops the rest of that merge pass, and leaves `work_done` false so the operator-facing state stays loud. `gza incomplete` renders the same warning whenever mergeable rows exist and the non-isolated default checkout is dirty.

At the start of each watch pass, watch also fingerprints the installed `gza` Python package on disk. If the package contents drift from what the process started with, watch emits one loud `WARNING` line for that newly observed fingerprint. In the default mode, the warning explicitly says watch will re-exec itself at the next cycle boundary without waiting for running or pending work to drain; detached workers stay alive and the replacement process reconciles them after it auto-resumes, skipping the first-pass confirmation gate because the session was already approved. `--no-auto-restart-on-drift` switches back to the warn-only manual-restart message.

When `main_checkout_isolate: true`, `gza watch` and `gza advance` still plan against the repo default branch but execute merge attempts inside a dedicated detached integration checkout reset to the default-branch tip (`config.main_checkout_integration_path`) whenever the configured verify gate requires pre-promotion proof. The shared merge executor treats that checkout as the authoritative candidate environment: candidate verify runs there before the canonical target ref is updated, fails closed if the isolated checkout is unavailable, and refuses promotion when candidate verify is red or exact-tree freshness is unproven. Only the exact verified candidate tree may copy its evidence into the shared canonical checkpoint. Watch batches the selected isolated merge actions for the current direct-merge phase, pays one combined candidate verify for the staged batch in the common case, and only then promotes once. If target promotion fails after candidate verification succeeds and rollback restores the previous target tip, watch and advance report an isolated merge promotion failure with the preserved reason, skip conflict/rebase routing, and leave the affected merge units unmerged with unchanged provenance. If rollback fails after the target ref moved, both commands re-read the target ref: an exact candidate tip continues through post-promotion checkpoint/finalization replay, while any other observed tip emits explicit operator attention because canonical target state is uncertain. If a member conflicts while staging into the accumulated candidate tip, watch resets the detached checkout to the pre-member tip and reassesses the failure before applying conflict semantics. Only a confirmed conflict with a created or deduped durable rebase task lets later members continue staging; rebase-task creation failure is emitted as an `ERROR` and stops the batch before later staging, candidate verify, promotion, or finalization. A branch that was already merged in the pre-batch live target is repaired through the shared merge-truth reconciliation before the batch continues; a branch contained only by the accumulated candidate remains staged and waits for candidate verification plus live-target promotion before debt, provenance, or merge state finalize. Missing branches or cleanly merging branches are emitted as `ERROR` lines and stop the batch before verify or promotion. Batch conflict rebase workers are dispatched only after the target branch has been promoted to include the staged prefix used for conflict classification; if promotion is blocked, each queued pending rebase child is left pending and excluded from same-cycle generic pending pickup. Existing in-progress duplicate children are treated as already owned work and are not relaunched; reused pending children are deduped by child task ID so one child can start at most once after promotion. After launch capacity is reserved, the shared batch/non-batch rebase dispatcher re-reads the child row before startup preparation, starts only a still-pending child, treats a refreshed `in_progress` child as already owned, and emits a stale-launch diagnostic for missing or terminal refreshed rows instead of launching the older snapshot. If promotion succeeds but launch capacity is unavailable, or if post-promotion startup preparation fails, the single child remains queued for a later cycle. If the combined staged verify is red, watch replays from the canonical target to isolate the first red-producing merge unit, emits sticky blocked-candidate attention for that subject, queues or refreshes one candidate rework task, and keeps canonical-main freeze state unchanged. Before either command considers a later merge candidate in the same run, it refreshes or rebuilds the isolated checkout back to the canonical target so blocked candidate trees cannot contaminate later promotion attempts. Successful isolated merges are then promoted onto the real default-branch ref before `merge_status` flips to `merged`; if the default branch is attached in another checkout, watch stashes tracked edits first, hard-resets that checkout to the new tip, and restores the stash when it applies cleanly. Watch emits a `WARN` line naming that stash whether it was replayed or had to stay parked for manual recovery. Rebase/conflict-resolution ownership is unchanged: conflicts create rebase tasks on the task branch, and those tasks run through the normal rebase workflow.

Post-promotion automatic finalization replay is advertised only after the merge-finalization proof was durably stored and a later merge-state write failed. Watch stops the current merge pass after such a failure so no later merge advances the same target before replay can finalize the promoted state. If proof persistence itself fails after the target moved, advance and watch emit explicit operator-attention guidance because replay cannot reconstruct the authorized child-task identity from missing proof.

Default `gza watch` uses the same bounded shared recovery policy as the explicit failed-task recovery queue, but it now exposes that policy through a two-lane split. `watch.recovery_slots` (default `1`) reserves that many worker slots per watch pass for worker-consuming failed-task recovery before pending pickup, and leaves the remaining `batch - recovery_slots` worker slots for pending work. The rule is uniform for worker-consuming recovery: batch-1 plain watch gives the single slot to worker-consuming recovery first; `--pending-only` or `watch.recovery_slots: 0` are the escape hatch for operators who intentionally want single-slot pending-only behavior. `--recovery-only` is the other extreme (`recovery_slots = batch` in single-project watch, or every free supervisor dispatch slot in multi-project watch) and suppresses pending pickup until actionable recovery drains, even for direct reconcile actions that do not consume a worker slot.

`gza watch` now shares the same lifecycle execution gate as `gza advance`: every actionable non-worker lifecycle action runs regardless of free worker slots, while worker-consuming actions remain slot-gated. Watch still owns scheduling order and live slot accounting; only the action-type gate is shared.
Separately from the failed-task recovery lane, each watch pass now emits one concise `Lifecycle actions (...)` summary line for the actionable review/rebase/merge/materialization work already queued in that pass's lifecycle plan. The summary reuses the shared lifecycle action types rather than inventing watch-only wording, except that annotated max-cycle `merge` actions project as `merge_and_defer_blockers` so operators can distinguish them from ordinary approved merges while execution still routes through the direct merge path. The summary appears once per pass before execution so operators can compare watch behavior with `advance --dry-run`.
`gza queue` is now the shared dispatch preview: by default it renders runnable recovery plus pending in watch order, with separate needs-human recovery rows and blocked pending rows outside the runnable cap. `gza queue --pending` is the git-free pending-only view; `gza queue --recovery` is the recovery-only view.

When the recovery lane is active:

- Watch evaluates failed tasks through the shared recovery engine before spending the reserved pending slots
- Actionable failed tasks are selected oldest-created first, but they only consume the configured recovery slots for that watch pass
- Implement recovery launches through iterate-aware execution; non-implement recovery launches through plain worker execution
- `gza watch --recovery-only --dry-run` prints the failed-task recovery decision report, including shared `Needs attention` rows by default, and exits
- Fully recovered failed ancestors are omitted from that report and from live watch recovery logs; only unresolved failed tasks and their completed recovery descendants remain visible through the normal advance plan
- Failed `review` / `improve` / `rebase` rows whose structured target implementation is already merged are omitted from live recovery-lane failure transition output only when the shared recovery classifier has affirmative landed proof or explicit historical no-work proof. Same-branch/shared-unit leaves with unknown commit metadata, persisted self-owned active `unmerged` side quests, distinct unmerged side quests, and session-backed recoverable `empty` / `redundant` leaves remain visible to recovery, owner rows, and scoped watch planning, and still contribute to the relevant recovery/backoff decisions.
- `--max-resume-attempts` applies to all unattended watch-managed resume/retry decisions for that run, including plain watch, failed-task recovery, and advance-driven improve recovery

Deprecated compatibility aliases remain accepted for now: `--restart-failed` maps to `--recovery-only`, `--restart-failed-batch` maps to `--recovery-slots`, and `watch.restart_failed_batch` maps to `watch.recovery_slots`.
