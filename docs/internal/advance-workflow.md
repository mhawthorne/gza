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
uv run gza advance --no-resume-failed     # Skip failed task resumption
uv run gza advance --max-resume-attempts N
uv run gza advance --max-review-cycles N
uv run gza advance --squash-threshold N
```

## Task Collection

Advance collects owner rows from one shared source:

1. **Lineage owner rows**: `src/gza/lineage_query.py::query_lineage_owner_rows(...)` materializes one in-memory lineage snapshot from `store.get_all()`, groups tasks by branch/merge ownership, evaluates one lineage-resolved predicate, and returns one canonical row per unresolved owner. The same owner-row query now feeds `gza incomplete`, `gza advance`, and the `gza watch --restart-failed` recovery queue.

2. **Completed lifecycle work inside the owner row**: Completed `merge_status='unmerged'` compatibility rows, merge-unit-backed unmerged work, and completed plan/explore sources all surface through the row's `lifecycle_action_task` and `next_action`. Merge lifecycle collection is merge-unit scoped: each active unit contributes at most one lifecycle owner candidate, and merge execution attributes provenance back to the unit owner even when a descendant task triggered the action. In the no-explicit-task path, `cmd_advance()` passes `git.current_branch()` as the target branch, so completed work only participates when its merge unit targets the currently checked-out branch. Legacy rows with no resolvable merge unit remain compatibility-oriented fallback candidates instead of being branch-target-filtered away.

3. **Failed-task recovery inside the owner row**: Failed leaves are filtered through the same bounded recovery policy used by `decide_failed_task_recovery(...)`. That policy can classify candidates as `resume`, `retry`, or manual review required, but the command no longer performs a second standalone failed-task sweep to build the plan. Rows whose authoritative action is recovery expose `row.recovery_action_task` / `row.recovery_leaf_task`; rows whose failed leaf has already handed off to newer completed lifecycle work keep the owner row and its `lifecycle_action_task` so merge/review/rebase planning remains eligible independently. Failed ancestors whose recovery-only `based_on` chain already terminates in a completed retry/resume descendant are omitted silently; the completed descendant is then handled through the ordinary completed-task rules (merge, rebase, review, or dependency wait) instead of re-printing a permanent `SKIP: recovery child/descendant already completed` row. Failed `review` and `rebase` tasks whose structured target implementation is already `merged` are omitted silently because no advance/watch/iterate action can move them forward. Completed same-branch `improve` tasks still remain visible because they can represent real post-merge follow-up work, but failed same-branch improves fall back to the landed-lineage suppression check because a failed attempt on an already merged branch did not land any additional work. Failed tasks are also omitted when their own branch tip is already reachable from the default branch, or when another merged task in the same canonical lineage owns the same branch/implementation branch, because the code has already landed even if the failed row's `merge_status` is stale. If branch reachability probes fail after the default branch is known, `advance --dry-run` surfaces one warning that only git branch reachability suppression is unavailable for this run; metadata-based same-lineage merged-task suppression may still apply, so failed-row visibility remains conservative only for the git-reachability decision. If a project-backed store cannot resolve the real default merge target at all, failed-task recovery now raises `MergeTargetResolutionError` instead of silently assuming `main`.

Optional filters: `--type plan|implement`, `--max N`, or a specific task ID.

## Configuration

| Field | Default | Description |
|-------|---------|-------------|
| `advance_requires_review` | `true` | Implement tasks must have a passing review before merge |
| `advance_create_reviews` | `true` | Auto-create review tasks for implements. The invariant-closing review after a completed write is always enforced. |
| `max_resume_attempts` | `1` | Shared automatic failed-task recovery toggle (`0` disables; any positive value enables the fixed bounded resume/retry policy) |
| `max_review_cycles` | `3` | Max review→improve cycles before flagging for manual intervention |
| `merge_squash_threshold` | `0` | Auto-squash branches with >= N commits (0 = disabled) |

## Decision Tree

For each task, `evaluate_advance_rules()` returns an action from `src/gza/advance_engine.py`. The decision tree is evaluated by an ordered rule list; first match wins.

### 1. Plan tasks

| Condition | Action |
|-----------|--------|
| Plan with no implement child | `create_implement` — create and run implement task |
| Plan with existing implement child | `skip` |

### 2. No branch

| Condition | Action |
|-----------|--------|
| Task has no branch (no commits) | `skip` |

### 3. Merge conflicts

Conflict detection uses the currently checked-out branch as the merge target (`target_branch = git.current_branch()`).

| Condition | Action |
|-----------|--------|
| Branch cannot merge into current branch AND rebase child is `pending`/`in_progress` | `skip` — rebase already running |
| Branch cannot merge into current branch AND rebase child is `failed` | `needs_discussion` — manual intervention required |
| Branch cannot merge into current branch AND no active rebase child | `needs_rebase` — create rebase task |

### 4. Post-rebase review invalidation

| Condition | Action |
|-----------|--------|
| A completed rebase exists that is newer than the latest review | `create_review` — rebase may have introduced changes |

### 5. Review state (when reviews exist)

#### 5a. Review was cleared (improve task ran after review)

| Condition | Action |
|-----------|--------|
| Active review is `pending` | `run_review` — spawn worker for it |
| Active review is `in_progress` | `wait_review` — skip |
| Completed improve exists after latest review | `create_review` — code changed, need fresh review |

#### 5b. Review is active (not cleared)

| Condition | Action |
|-----------|--------|
| Latest review is `pending` | `run_review` — spawn worker |
| Latest review is `in_progress` | `wait_review` — skip |
| Task type is `implement`, verdict is `APPROVED`/`APPROVED_WITH_FOLLOWUPS` (or review is cleared), and unresolved comments are newer than the latest completed review | Prefer improve flow (`wait_improve`/`run_improve`/`improve`) before any merge |
| Verdict = `APPROVED` | `merge` |
| Verdict = `APPROVED_WITH_FOLLOWUPS` with at least one parsed `FOLLOWUP` finding | `merge_with_followups` — create/reuse follow-up implement tasks, then merge |
| Verdict = `APPROVED_WITH_FOLLOWUPS` with zero parsed `FOLLOWUP` findings | `needs_discussion` — fail closed; review output is inconsistent |
| Verdict = `CHANGES_REQUESTED` AND cycles >= `max_review_cycles` | `max_cycles_reached` — manual intervention |
| Verdict = `CHANGES_REQUESTED` AND improve is `in_progress` | `wait_improve` — skip |
| Verdict = `CHANGES_REQUESTED` AND improve is `pending` | `run_improve` — spawn worker |
| Verdict = `CHANGES_REQUESTED` AND no improve exists | `improve` — create improve task |
| Verdict = unknown | `needs_discussion` — manual intervention |

When the engine emits `improve`, the caller (iterate) delegates to `resolve_improve_action(store, impl_id, review_id, max_resume_attempts)` to pick one of:

| Condition | Sub-action |
|-----------|-----------|
| No prior failed improve for this (impl, review) | `new` — create a fresh improve |
| Shared failed-task recovery policy returns `resume` | `resume` — continue from the latest failed improve |
| Shared failed-task recovery policy returns `retry` | `retry` — fork a fresh improve attempt |
| `max_resume_attempts == 0` (automatic recovery disabled) | `give_up` — stop iterating; surface `automatic_recovery_disabled` as the stop reason |
| Shared failed-task recovery policy returns `manual_review_required` (for example, failed resume descendants or a dropped recovery terminal) | `manual_review` — stop iterating and require operator intervention |

The improve flow now defers recovery edge selection to the shared recovery engine (`decide_failed_task_recovery`), and iterate also resolves fully recovered failed implement IDs through the same completed-descendant planner handoff used by advance/watch. That keeps iterate/advance/watch on one consistent resume/retry/manual-review boundary and avoids stale completed-recovery skip output on recovered ancestors.

### 6. No reviews / all cleared

| Condition | Action |
|-----------|--------|
| Reviews exist but all cleared | `merge` — previous review addressed |
| Non-implement task type (plan, explore, etc.) | `merge` — no review required |

### 7. Implement with no review

| Condition | Action |
|-----------|--------|
| `advance_requires_review=true` | `create_review` |
| `advance_requires_review=false` | `merge` |

### 8. Failed task recovery

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
| `create_implement` | Creates implement task for a plan, spawns worker |
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
| `needs_discussion` | Requires manual intervention (shown in attention summary) |
| `max_cycles_reached` | Review cycle limit exceeded (shown in attention summary) |

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
   - Fetches and rebases onto the target branch
   - Resolves conflicts autonomously
   - Restores stashed changes
5. On completion, the host runner force-pushes the rebased branch (`git push --force-with-lease`)
6. Advance sees no more conflicts on next run
7. If a completed rebase is newer than the latest review → advance creates a fresh review before merging
8. If the rebase task fails → advance reports `needs_discussion` (no automatic retry)

### Docker considerations

Rebase tasks need git identity for `git rebase --continue`. The Docker container receives `GIT_AUTHOR_NAME`, `GIT_AUTHOR_EMAIL`, `GIT_COMMITTER_NAME`, and `GIT_COMMITTER_EMAIL` env vars from the host's git config.

## Output

For worker-spawning actions that first create or reuse a child task (`create_review`, `create_implement`, `resume`, `retry`, `needs_rebase`), operator output must distinguish task selection/creation success from worker-launch failure. If task creation succeeds, or if the executor reuses an existing eligible recovery task, but the background worker fails to start, `gza advance` should print the relevant created/reused task ID and a separate `Failed to start ... worker` line rather than collapsing that state into `✗ Created ...`.

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
| `gza watch` | Runs advance in a loop with sleep intervals; with `--restart-failed`, drains actionable failed-task recovery before pending queue work |

## Watch integration

`gza watch` reuses the same advance executor and improve-resolution helpers described above; it does not maintain a separate improve retry policy. For `create_review`, `run_review`, `improve`, and `run_improve` on unmerged implementation chains, watch resolves the root implementation first and launches `gza iterate <impl>` before any child review/improve side effects occur. Iterate then owns child creation, reuse, recovery, and immediate follow-on steps inside its loop.

Watch renders human-needed advance outcomes (`needs_discussion`, `max_cycles_reached`, failed-task recovery states that now require an operator decision, and improve-recovery stop reasons) as sticky `ATTENTION` log lines instead of deduped `SKIP` lines. The reminder reuses the same formatted task line as the `advance` needs-attention section, including the stable `reason=...` policy slug and the shared single-line shortened prompt. It repeats while the task still resolves to that manual-attention next action, then disappears once the next action changes. Ordinary watch skip/wait lines remain deduped across cycles.

When `main_checkout_isolate: true`, `gza watch` still plans against the repo default branch but executes merge attempts inside a dedicated detached integration checkout reset to the default-branch tip (`config.main_checkout_integration_path`). Successful isolated merges are then promoted onto the real default-branch ref before `merge_status` flips to `merged`; if the default branch is attached in another checkout, watch hard-resets that checkout to the new tip so it stays clean. Rebase/conflict-resolution ownership is unchanged: conflicts create rebase tasks on the task branch, and those tasks run through the normal rebase workflow.

Default `gza watch` uses the same bounded shared recovery policy as the explicit failed-task recovery queue. `gza watch --restart-failed` is opt-in only for recovery-first queue ordering.

When `--restart-failed` is enabled:

- Watch evaluates failed tasks through the shared recovery engine before starting fresh pending work
- Recovery work is recovery-first: actionable failed tasks drain before pending queue pickup, and newly failed actionable tasks continue to outrank pending work for that session
- Implement recovery launches through iterate-aware execution; non-implement recovery launches through plain worker execution
- `gza watch --restart-failed --dry-run` prints the failed-task recovery decision report, including shared `Needs attention` rows by default, and exits
- Fully recovered failed ancestors are omitted from that report and from live watch recovery logs; only unresolved failed tasks and their completed recovery descendants remain visible through the normal advance plan
- Failed `review` / `improve` / `rebase` rows whose structured target implementation is already merged are omitted from live `--restart-failed` failure transition output and do not contribute to backoff or halt streaks
- `--max-resume-attempts` applies to all unattended watch-managed resume/retry decisions for that run, including plain watch, failed-task recovery, and advance-driven improve recovery
