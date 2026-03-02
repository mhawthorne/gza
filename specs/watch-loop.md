# gza watch — Continuous Queue Runner with Activity Log

## Overview

A simple long-running loop that drains the task queue by repeatedly calling `gza advance`, sleeping when idle. Coupled with a structured activity log that shows task lifecycle events in real time.

## Motivation

Today, running through a queue requires either:
- Manual invocations: `gza advance`, check, repeat
- `gza work -c N` which runs N tasks sequentially but doesn't advance (review, improve, merge)
- Scripts wrapping gza in a `while true` loop

What's missing is a single command that **continuously advances the entire pipeline** — work, review, improve, merge — and **shows what's happening** in a unified log.

## Command

```bash
gza watch [--batch N] [--sleep S] [--max-idle T] [--dry-run]
```

| Flag | Default | Description |
|------|---------|-------------|
| `--batch N` | 5 | Max tasks to advance per cycle (passed as `--max N` to `advance`) |
| `--sleep S` | 30 | Seconds to sleep when queue is empty |
| `--max-idle T` | (none) | Exit after T seconds of consecutive idle time (no flag = run forever) |
| `--dry-run` | false | Show what each cycle would do without executing |

### Behavior

```
while True:
    pending = count of runnable pending tasks + unmerged tasks needing action
    if pending > 0:
        run `gza advance --max B`
        # no sleep — immediately check for more work
    else:
        log "idle, sleeping {S}s"
        sleep S seconds
        idle_time += S
        if max_idle and idle_time > max_idle:
            log "max idle time reached, exiting"
            break
    # reset idle_time whenever work was done
```

Key points:
- `gza advance` already handles the full lifecycle: running pending tasks, spawning reviews, running improves, merging approved work. `watch` just calls it in a loop.
- Between cycles, `watch` checks both `get_next_pending()` (runnable queue tasks) and `get_unmerged()` (tasks needing advance actions) to decide idle vs active.
- `--batch` limits how many tasks `advance` processes per cycle, preventing runaway parallelism.
- Ctrl+C stops cleanly (let in-flight workers finish, don't kill them).

### Example Session

```bash
# Run until queue is drained, then exit after 5 minutes of idle
gza watch --sleep 60 --max-idle 300

# Tight loop for active development — check every 10s, advance up to 3 at a time
gza watch --batch 3 --sleep 10

# Just watch forever (default)
gza watch
```

## Activity Log

### Requirements

The watch loop prints a real-time activity log to stdout showing task lifecycle events. Each line is timestamped and prefixed with an event type.

### Event Types

| Event | Trigger | Example |
|-------|---------|---------|
| `START` | Task transitions to `in_progress` | `12:03:04 START  #42 implement "Add JWT auth"` |
| `DONE` | Task completes successfully | `12:14:22 DONE   #42 implement (11m18s)` |
| `FAIL` | Task fails | `12:08:44 FAIL   #42 implement: TEST_FAILURE (5m40s)` |
| `REVIEW` | Review task completes | `12:16:01 REVIEW #43 for #42: APPROVED` |
| `MERGE` | Task merged to default branch | `12:16:05 MERGE  #42 → main (branch: 20260301-add-jwt)` |
| `SPAWN` | Background worker spawned | `12:16:02 SPAWN  #44 improve for #42 (worker w-1709312162)` |
| `SKIP` | Advance skipped a task | `12:16:03 SKIP   #45: needs_rebase` |
| `IDLE` | No work, sleeping | `12:16:05 IDLE   sleeping 30s (queue: 0 pending, 0 unmerged)` |
| `WAKE` | Woke from sleep | `12:16:35 WAKE   checking queue...` |

### Format

```
HH:MM:SS EVENT  #ID type "truncated prompt..." [details]
```

- Timestamp is local time, HH:MM:SS
- Event is left-padded to 6 chars for alignment
- Task ID prefixed with `#`
- Task type follows the ID
- Prompt is truncated to ~40 chars on DONE/FAIL/START lines
- Duration shown in parentheses where relevant
- Review verdict shown for REVIEW events
- Failure reason shown for FAIL events (extracted from `extract_failure_reason()`)

### Log Destination

- **stdout**: Always, for interactive use
- **File**: Also written to `.gza/watch.log` (append mode), so you can `tail -f` from another terminal or review after the fact
- Consider: `--quiet` flag to suppress stdout and only write to file (for running in tmux/screen detached)

## Implementation Notes

### What `watch` does NOT do

- **No budget management** — that's the daemon spec's job
- **No goal evaluation** — no autonomous task creation
- **No PID file / daemonization** — it's a foreground process (use tmux/screen/nohup if you want background)
- **No parallel workers internally** — `advance` already spawns background workers; `watch` just calls `advance` in a loop

### Detecting Events

The activity log needs to observe task state changes between cycles. Two approaches:

**Option A: Poll DB between cycles**
- Before each `advance` call, snapshot task statuses
- After `advance` returns, diff against snapshot
- Emit events for any transitions

**Option B: `advance` returns structured results**
- Modify `cmd_advance` to return/print a machine-readable summary of actions taken
- `watch` parses this output to emit log lines

**Recommendation**: Option A is simpler and decoupled. `watch` polls the DB directly via `TaskStore` rather than parsing advance's output. The overhead of a few SQLite queries per cycle is negligible.

### Handling Worker Completion

Background workers (spawned by `advance` for reviews, improves) complete asynchronously. `watch` should detect their completion between cycles by checking:
- `store.get_in_progress()` — tasks that were in_progress last cycle but are now completed/failed
- Worker registry — workers that have exited since last check

### Signal Handling

- `SIGINT` (Ctrl+C): Log "shutting down", stop the loop, let in-flight workers continue (they're detached processes)
- `SIGTERM`: Same as SIGINT
- Do NOT kill child workers on shutdown — they manage their own lifecycle

### Configuration

No new `gza.yaml` fields needed. `watch` is purely a CLI operation that composes existing functionality. If users want to persist their preferred `--batch` and `--sleep` values, that could be a future addition:

```yaml
watch:
  batch: 5
  sleep: 30
  max_idle: 300
```

But start without config — CLI flags are sufficient.

## Prerequisites

- **`gza advance --batch B --new`**: An upcoming change adds `--batch B` (concurrency limit) and `--new` (start pending queue tasks, not just advance unmerged ones) to `advance`. With these flags, `advance` becomes a single command that handles the full pipeline — starting new work, running reviews/improves, and merging. `watch` depends on this and simply calls `gza advance --batch B --new` in a loop.

## Open Questions

1. **Concurrency limit**: `--batch B` on `advance` caps how many tasks it processes per call, but workers are background processes that outlive the call. Should `watch` also cap total concurrent workers (e.g., `--workers W`) by checking the worker registry before each cycle? Or leave that to `advance` to manage?

2. **Should the log be structured (JSON lines)?** Useful for piping to jq or log aggregation. Could offer `--json` flag alongside the human-readable default.
