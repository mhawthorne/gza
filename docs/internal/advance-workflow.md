# gza advance

> **Status: Implemented** — This spec describes the current behavior of `gza advance` as of 2026-03-26.

## Overview

`gza advance` is the main orchestration command. It inspects all completed/unmerged tasks, determines the next action for each, and executes those actions (spawning workers, merging, etc.). It is designed to be idempotent and safe to run repeatedly.

## Usage

```bash
gza advance                        # Advance all eligible tasks
gza advance <task-id>              # Advance a specific task
gza advance --batch N              # Limit to N concurrent worker spawns
gza advance --batch N --new        # Fill remaining batch slots with pending tasks
gza advance --type plan            # Only advance plan tasks
gza advance --type implement       # Only advance implement tasks
gza advance --dry-run              # Show plan without executing
gza advance --no-resume-failed     # Skip failed task resumption
gza advance --max-resume-attempts N
gza advance --max-review-cycles N
gza advance --squash-threshold N
```

## Task Collection

Advance collects tasks from two sources:

1. **Unmerged tasks**: `store.get_unmerged()` — completed tasks with `merge_status='unmerged'`. Excludes improve and rebase tasks that have a parent (`based_on IS NOT NULL`) since they operate on the parent's branch.

2. **Resumable failed tasks**: Tasks with `status='failed'`, `failure_reason IN ('MAX_STEPS', 'MAX_TURNS')`, and `session_id IS NOT NULL`. Disabled with `--no-resume-failed`.

3. **Unimplemented plans**: Completed plan tasks with no implement child yet. Excluded when `--type implement`.

Optional filters: `--type plan|implement`, `--max N`, or a specific task ID.

## Configuration

| Field | Default | Description |
|-------|---------|-------------|
| `advance_requires_review` | `true` | Implement tasks must have a passing review before merge |
| `advance_create_reviews` | `true` | Auto-create review tasks for implements (only when `advance_requires_review=true`) |
| `advance_mode` | `work` | `work`: execute one action/task; `iterate`: spawn `iterate --background` for non-merge actions |
| `max_resume_attempts` | `3` | Max times a failed task can be auto-resumed |
| `max_review_cycles` | `3` | Max review→improve cycles before flagging for manual intervention |
| `iterate_max_iterations` | `5` | Default iterate action budget when `--max-iterations` is omitted |
| `merge_squash_threshold` | `0` | Auto-squash branches with >= N commits (0 = disabled) |

## Decision Tree

For each task, `_determine_advance_action()` returns an action. The decision tree is evaluated top-to-bottom; first match wins.

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
| Verdict = `APPROVED` | `merge` |
| Verdict = `CHANGES_REQUESTED` AND cycles >= `max_review_cycles` | `max_cycles_reached` — manual intervention |
| Verdict = `CHANGES_REQUESTED` AND improve is `in_progress` | `wait_improve` — skip |
| Verdict = `CHANGES_REQUESTED` AND improve is `pending` | `run_improve` — spawn worker |
| Verdict = `CHANGES_REQUESTED` AND no improve exists | `improve` — create improve task |
| Verdict = unknown | `needs_discussion` — manual intervention |

### 6. No reviews / all cleared

| Condition | Action |
|-----------|--------|
| Reviews exist but all cleared | `merge` — previous review addressed |
| Non-implement task type (plan, explore, etc.) | `merge` — no review required |

### 7. Implement with no review

| Condition | Action |
|-----------|--------|
| `advance_requires_review=true` AND `advance_create_reviews=true` | `create_review` |
| `advance_requires_review=true` AND `advance_create_reviews=false` | `skip` — user must run `gza review` manually |
| `advance_requires_review=false` | `merge` |

### 8. Failed task resumption

Failed tasks are evaluated separately from the main decision tree.

| Condition | Action |
|-----------|--------|
| Resume chain depth >= `max_resume_attempts` | `skip` |
| Otherwise | `resume` — create resume task and spawn worker |

## Action Types

### Worker-spawning actions

These actions create background workers and count toward the batch limit.

| Action | What it does |
|--------|-------------|
| `needs_rebase` | Creates rebase task via `_create_rebase_task()`, spawns worker |
| `create_review` | Creates review task, spawns worker |
| `run_review` | Spawns worker for existing pending review |
| `improve` | Creates improve task, spawns worker |
| `run_improve` | Spawns worker for existing pending improve |
| `create_implement` | Creates implement task for a plan, spawns worker |
| `resume` | Creates resume task, spawns worker |

### Direct actions

| Action | What it does |
|--------|-------------|
| `merge` | Merges the task's branch synchronously. Respects `merge_squash_threshold`. |

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

```
Will advance N task(s):

  gza-2a Implement feature X
      → Merge (review APPROVED)

  gza-2d Fix caching bug
      → Create review (required before merge)

  gza-26 Refactor API
      → SKIP: rebase gza-33 already in progress

Advanced: 1 merged, 1 review started, 1 skipped

⚠ Tasks needing attention:
  gza-27 Update deps — SKIP: max review cycles (3) reached, needs manual intervention
```

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
| `gza watch` | Runs advance in a loop with sleep intervals |
