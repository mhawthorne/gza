# Handling Failed Tasks

How to recover when tasks fail or hit limits.

## When tasks fail

Tasks can fail for several reasons:

- **Timeout** - Task exceeded `timeout_minutes`
- **Max turns** - Task hit `max_turns` limit without completing
- **Error** - Claude encountered an unrecoverable error
- **Interrupted** - Worker was stopped or crashed

## Check the failure

```bash
$ uv run gza show gza-5
Task gza-5: 20260108-add-user-auth
Status: failed
Branch: feature/20260108-add-user-auth
Worktree: /tmp/gza-worktrees/gza/20260108-add-user-auth
Duration: 10m 00s
Turns: 50/50
Cost: $0.89

Error: max turns of 50 exceeded
```

If git worktree metadata is unavailable, `uv run gza show` prints a warning so lookup failures are distinct from "no active worktree".

View the merged log to understand what happened:

```bash
$ uv run gza log gza-5 --steps
```

## Resume vs Retry

You have two options for recovering:

| Command | Behavior | Use when |
|---------|----------|----------|
| `uv run gza resume` | Continue from where it left off | Task was making progress, just needs more turns |
| `uv run gza retry` | Create a new retry attempt | Task needs another run; implement retries fork fresh, same-branch follow-ups stay on the shared branch |

For bulk unattended recovery after fixing an environment issue, use the watch recovery lane:

| Command | Behavior | Use when |
|---------|----------|----------|
| `uv run gza watch --recovery-only` | Send the full watch batch to failed-task recovery, choosing `resume` or `retry` per task | You want watch to drain the failed queue before resuming normal pending processing |
| `uv run gza watch --recovery-only --dry-run` | Print the recovery decision report and exit | You want to inspect which failed tasks would `resume`, `retry`, or need operator attention before starting recovery |
| `uv run gza watch --recovery-only --dry-run --show-skipped` | Include ordinary skipped failed tasks in the recovery decision report | You want to inspect why some non-attention failed tasks would still be skipped |
| `uv run gza watch --recovery-only --show-skipped` | Include skipped failed tasks in live watch logs | You want recovery-only watch logs to explain why some failed tasks are being skipped |

## Resume a task

Resume continues the existing conversation. The AI picks up where it left off with full context of what it already did.

`uv run gza resume` runs the new task immediately by default. Use `--queue` to add to queue without executing:

```bash
$ uv run gza resume gza-5
=== Resuming Task gza-5: 20260108-add-user-auth ===
...
=== Done ===
Stats: Runtime: 5m 23s | Turns: 15 | Cost: $0.34
```

Increase the turn limit if the original was too low:

```bash
$ uv run gza resume gza-5 --max-turns 100
```

Add to queue without running immediately:

```bash
$ uv run gza resume gza-5 --queue
```

## Retry a task

Retry creates a fresh attempt. Use this when the AI went down a wrong path and you want it to start over.

`uv run gza retry` runs the new task immediately by default. Use `--queue` to add to queue without executing:

```bash
$ uv run gza retry gza-5
Created task gza-6 (retry of gza-5)
=== Task gza-6: 20260108-add-user-auth ===
...
=== Done ===
Stats: Runtime: 8m 12s | Turns: 32 | Cost: $0.67
```

Retry creates a new task that reuses the same branch (if it exists) but starts a new conversation.

## Recover failed tasks with watch

`uv run gza watch` now has a built-in two-lane split. By default, `watch.recovery_slots = 1`, so each watch pass reserves one slot for worker-consuming failed-task recovery before pending pickup and leaves the remaining slots for pending work. Use `uv run gza watch --recovery-only` to dedicate the full batch to failed-task recovery, or `uv run gza watch --pending-only` to disable recovery and keep the watch loop pending-only.

Preview the recovery plan first:

```bash
$ uv run gza watch --recovery-only --dry-run
Failed recovery plan (tags=*, mode=recovery-only)

resume gza-101 implement via iterate reason=MAX_TURNS attempt=1/2
retry  gza-102 plan      via worker  reason=INFRASTRUCTURE_ERROR attempt=1/2

Needs attention (1 task):
  gza-103 improve "Improve feature" reason=manual-failure-reason TEST_FAILURE requires manual intervention

Summary: 2 actionable (1 resume, 1 retry), 1 needs attention, 0 skipped hidden
```

Fully recovered failed ancestors are omitted from this report entirely. Once a retry/resume descendant completes, normal `uv run gza advance`, `uv run gza watch`, and dry-run planning in `uv run gza iterate <failed-id>` treat that completed descendant as the actionable node for merge/review/rebase decisions instead of repeating a permanent recovery skip on the failed ancestor. By contrast, chains that terminate in a failed or dropped recovery descendant stay visible under `Needs attention` until an operator intervenes.

Include ordinary skipped tasks when you need the full picture:

```bash
$ uv run gza watch --recovery-only --dry-run --show-skipped
Failed recovery plan (tags=*, mode=recovery-only)

resume gza-101 implement via iterate reason=MAX_TURNS attempt=1/2
retry  gza-102 plan      via worker  reason=INFRASTRUCTURE_ERROR attempt=1/2
skip   gza-104 implement via none    reason=recovery_already_pending attempt=1/2

Needs attention (1 task):
  gza-103 improve "Improve feature" reason=manual-failure-reason TEST_FAILURE requires manual intervention

Summary: 2 actionable (1 resume, 1 retry), 1 needs attention, 1 skipped
```

The same `--show-skipped` flag also controls live `uv run gza watch --recovery-only` logging. Shared needs-attention recovery rows are shown by default; without `--show-skipped`, only ordinary non-attention skip decisions stay silent in the watch log.

Then run recovery mode for real:

```bash
$ uv run gza watch --recovery-only
```

`--max-resume-attempts` controls that shared policy as a toggle: set it to `0` to disable unattended recovery entirely; any positive value enables the same fixed bounded automatic recovery policy used by plain watch and by the recovery lane. Deprecated compatibility aliases remain accepted for now: `--restart-failed` maps to `--recovery-only` and `--restart-failed-batch` maps to `--recovery-slots`.

## Check history for failed tasks

```bash
$ uv run gza history --status failed
Recent failed tasks:

failed    gza-5 (2026-01-08 14:12) add user auth
    reason: MAX_TURNS
    → resumed as gza-9 ✓
    [implement] ← gza-4

failed    gza-3 (2026-01-07 09:44) refactor api
    reason: MAX_STEPS
    [implement]
```

## Tips for avoiding failures

1. **Increase limits for complex tasks:**
   ```yaml
   # gza.yaml
   task_types:
     implement:
       max_turns: 80
       timeout_minutes: 20
   ```

2. **Break large tasks into smaller ones** - A plan → implement workflow naturally splits work

3. **Use `--max-turns` for one-off increases:**
   ```bash
   $ uv run gza work gza-5 --max-turns 100
   ```

4. **Check progress mid-task:**
   ```bash
   $ uv run gza log gza-5 --steps
   ```
