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
$ gza show gza-5
Task gza-5: 20260108-add-user-auth
Status: failed
Branch: feature/20260108-add-user-auth
Worktree: /tmp/gza-worktrees/gza/20260108-add-user-auth
Duration: 10m 00s
Turns: 50/50
Cost: $0.89

Error: max turns of 50 exceeded
```

If git worktree metadata is unavailable, `gza show` prints a warning so lookup failures are distinct from "no active worktree".

View the conversation to understand what happened:

```bash
$ gza log gza-5 --steps
```

## Resume vs Retry

You have two options for recovering:

| Command | Behavior | Use when |
|---------|----------|----------|
| `gza resume` | Continue from where it left off | Task was making progress, just needs more turns |
| `gza retry` | Start completely fresh | Task went down a wrong path, needs a fresh start |

For bulk unattended recovery after fixing an environment issue, use watch recovery mode:

| Command | Behavior | Use when |
|---------|----------|----------|
| `gza watch --restart-failed` | Drain actionable failed tasks before pending queue work, choosing `resume` or `retry` per task | You want watch to recover the failed queue first, then continue normal processing |
| `gza watch --restart-failed --dry-run` | Print the recovery decision report and exit | You want to inspect which failed tasks would `resume`, `retry`, or `skip` before starting recovery |
| `gza watch --restart-failed --dry-run --show-skipped` | Include skipped failed tasks in the recovery decision report | You want to inspect why some failed tasks would be skipped |
| `gza watch --restart-failed --show-skipped` | Include skipped failed tasks in live watch logs | You want restart-failed watch logs to explain why some failed tasks are being skipped |

## Resume a task

Resume continues the existing conversation. The AI picks up where it left off with full context of what it already did.

`gza resume` runs the new task immediately by default. Use `--queue` to add to queue without executing:

```bash
$ gza resume gza-5
=== Resuming Task gza-5: 20260108-add-user-auth ===
...
=== Done ===
Stats: Runtime: 5m 23s | Turns: 15 | Cost: $0.34
```

Increase the turn limit if the original was too low:

```bash
$ gza resume gza-5 --max-turns 100
```

Add to queue without running immediately:

```bash
$ gza resume gza-5 --queue
```

## Retry a task

Retry creates a fresh attempt. Use this when the AI went down a wrong path and you want it to start over.

`gza retry` runs the new task immediately by default. Use `--queue` to add to queue without executing:

```bash
$ gza retry gza-5
Created task gza-6 (retry of gza-5)
=== Task gza-6: 20260108-add-user-auth ===
...
=== Done ===
Stats: Runtime: 8m 12s | Turns: 32 | Cost: $0.67
```

Retry creates a new task that reuses the same branch (if it exists) but starts a new conversation.

## Recover failed tasks with watch

`gza watch --restart-failed` adds an explicit recovery phase ahead of normal pending work. It is opt-in; plain `gza watch` keeps the narrower legacy auto-resume behavior.

Preview the recovery plan first:

```bash
$ gza watch --restart-failed --dry-run
Failed recovery plan (tags=*, mode=restart-failed)

resume gza-101 implement via iterate reason=MAX_TURNS attempt=1/1
retry  gza-102 plan      via worker  reason=INFRASTRUCTURE_ERROR attempt=1/1

Summary: 2 actionable (1 resume, 1 retry), 1 skipped hidden
```

Include skipped tasks when you need the full picture:

```bash
$ gza watch --restart-failed --dry-run --show-skipped
Failed recovery plan (tags=*, mode=restart-failed)

resume gza-101 implement via iterate reason=MAX_TURNS attempt=1/1
skip   gza-103 review    via none    reason=task_type_out_of_scope attempt=1/1
retry  gza-102 plan      via worker  reason=INFRASTRUCTURE_ERROR attempt=1/1

Summary: 2 actionable (1 resume, 1 retry), 1 skipped
```

The same `--show-skipped` flag also controls live `gza watch --restart-failed` logging. Without it, skipped recovery decisions stay silent in the watch log; with it, skipped items are emitted as `SKIP` events while watch runs.

Then run recovery mode for real:

```bash
$ gza watch --restart-failed
```

`--max-resume-attempts` applies both to plain-watch auto-resume and to `--restart-failed` recovery decisions.

## Check history for failed tasks

```bash
$ gza history --status failed
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
   $ gza work gza-5 --max-turns 100
   ```

4. **Check progress mid-task:**
   ```bash
   $ gza log gza-5 --steps
   ```
