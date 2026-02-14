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
$ gza show 5
Task #5: 20260108-add-user-auth
Status: failed
Duration: 10m 00s
Turns: 50/50
Cost: $0.89

Error: max turns of 50 exceeded
```

View the conversation to understand what happened:

```bash
$ gza log -t 5 --turns
```

## Resume vs Retry

You have two options for recovering:

| Command | Behavior | Use when |
|---------|----------|----------|
| `gza resume` | Continue from where it left off | Task was making progress, just needs more turns |
| `gza retry` | Start completely fresh | Task went down a wrong path, needs a fresh start |

## Resume a task

Resume continues the existing conversation. The AI picks up where it left off with full context of what it already did.

By default, `gza resume` queues a new task. Use `--run` to run it immediately:

```bash
$ gza resume 5 --run
=== Resuming Task #5: 20260108-add-user-auth ===
...
=== Done ===
Stats: Runtime: 5m 23s | Turns: 15 | Cost: $0.34
```

Increase the turn limit if the original was too low:

```bash
$ gza resume 5 --run --max-turns 100
```

## Retry a task

Retry creates a fresh attempt. Use this when the AI went down a wrong path and you want it to start over.

By default, `gza retry` creates a new queued task. Use `--run` to run it immediately:

```bash
$ gza retry 5 --run
Created task #6 (retry of #5)
=== Task #6: 20260108-add-user-auth ===
...
=== Done ===
Stats: Runtime: 8m 12s | Turns: 32 | Cost: $0.67
```

Retry creates a new task that reuses the same branch (if it exists) but starts a new conversation.

## Check history for failed tasks

```bash
$ gza history --status failed
Recent failed tasks:

  #5 20260108-add-user-auth
     failed - max turns exceeded (50/50)
     10m ago

  #3 20260107-refactor-api
     failed - timeout after 10m
     2h ago
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
   $ gza work 5 --max-turns 100
   ```

4. **Check progress mid-task:**
   ```bash
   $ gza log -t 5 --turns
   ```
