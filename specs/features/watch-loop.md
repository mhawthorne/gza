# gza watch — Continuous Task Runner

## Overview

A long-running foreground loop that maintains a target number of concurrent workers by polling task state and spawning new work as slots open up. Replaces the manual cycle of `gza advance` → check → repeat.

## Motivation

Today, running a sustained workload requires either:
- Manual invocations: `gza advance --batch 2 --new`, wait, repeat
- Manually running `gza iterate` on individual implement tasks
- Scripts wrapping gza in a `while true` loop

What's missing is a single command that **continuously maintains N concurrent workers**, uses **iterate mode for implement tasks** (full review/improve loop), and **shows what's happening** in a unified activity log.

## Command

```bash
gza watch [--batch N] [--poll S] [--max-idle T] [--max-iterations N] \
  [--recovery-slots N | --recovery-only | --pending-only] [--dry-run]
```

| Flag | Default | Description |
|------|---------|-------------|
| `--batch N` | 2 | Target number of concurrent workers to maintain |
| `--poll S` | 300 | Seconds between polling cycles |
| `--max-idle T` | (none) | Exit after T seconds of consecutive idle time (no flag = run forever) |
| `--max-iterations N` | 10 | Max review/improve iterations for iterate mode on implement tasks |
| `--recovery-slots N` | 1 | Slots per cycle reserved for worker-consuming failed-task recovery before pending pickup |
| `--recovery-only` | false | Preset: dedicate the full batch to failed-task recovery and suppress pending pickup until actionable recovery drains |
| `--pending-only` | false | Preset: disable failed-task recovery and use all slots for pending work |
| `--dry-run` | false | Show what each cycle would do without executing |

Deprecated compatibility aliases remain accepted for now: `--restart-failed` maps to
`--recovery-only`, and `--restart-failed-batch N` maps to `--recovery-slots N`.

### Config

Defaults can be set in `gza.yaml` so the CLI flags are optional:

```yaml
watch:
  batch: 2
  poll: 300
  max_idle: null
  max_iterations: 10
  recovery_slots: 1
```

CLI flags override config values. `watch.restart_failed_batch` remains a deprecated
compatibility alias for `watch.recovery_slots`.

For the prescriptive runtime contract, including the exact recovery-lane gating rules,
see [../behavior/watch-supervisor.md](../behavior/watch-supervisor.md).

## Core Loop

```
while True:
    running = count_live_workers()       # PIDs still alive in worker registry + DB
    slots = batch - running

    if slots > 0:
        # 1. Merge anything that's ready (merges don't consume a slot)
        # 2. Allocate worker slots between recovery and pending lanes
        # 3. Run actionable failed-task recovery in the recovery lane
        # 4. Spawn pending work in the remaining slots
        fill_slots(slots)

    if no_work_done_this_cycle:
        idle_time += poll
        if max_idle and idle_time > max_idle:
            log "max idle time reached, exiting"
            break
    else:
        idle_time = 0

    sleep(poll)
```

### Counting running workers

The batch limit means "maintain N concurrent workers," not "spawn N per cycle." Each cycle:
1. Check worker registry for PIDs that are still alive
2. Check DB for tasks with status `in_progress`
3. Reap dead workers (update stale `in_progress` tasks whose PIDs are gone)
4. `slots = batch - live_worker_count`

### Filling slots

When slots are available, fill them in this priority order:

1. **Merges** — merge completed tasks that are ready. Merges don't consume a slot (they're synchronous and fast). This runs first so that newly freed branches don't cause rebase conflicts for other tasks.

2. **Allocate recovery vs pending lanes** — worker-consuming failed-task recovery is no longer "whatever slots are left after pending pickup." Each cycle reserves `min(slots, recovery_slots, worker_consuming_recovery_count)` worker slots for the recovery lane and gives the remainder to pending work. With the default `recovery_slots: 1`, plain watch always gives worker-consuming recovery first claim on one worker slot per cycle when an in-scope worker-consuming recovery action exists. This rule is uniform: at `--batch 1`, the default plain watch becomes recovery-first until the worker-consuming recovery lane drains. Operators who want a single-slot pending-only loop must opt into `--pending-only`.

3. **Run failed-task recovery** — the recovery lane uses the shared recovery engine, so eligibility is not limited to timeout/resource resumes. Actionable recovery may include `resume`, `retry`, or direct reconcile-style handling for failed tasks such as `WORKER_DIED`, depending on the shared policy. Direct reconcile-style recovery remains actionable for mode gating even when it does not spend a worker slot in plain watch. `--recovery-only` is the `recovery_slots = batch` extreme and suppresses pending pickup while any actionable in-scope recovery remains, including direct actions that do not themselves consume a worker slot.

4. **Start new pending tasks** — pull from the pending queue (ordered by urgent flag, then insertion order) only after the recovery lane has taken its reserved share for the cycle. For implement tasks, spawn in **iterate mode** with `--max-iterations` so the worker does the full review/improve loop autonomously. For plan/explore tasks, spawn as plain workers (no iterate).

### Iterate mode for implement tasks

When watch spawns a worker for an implement task, it runs the equivalent of:

```bash
gza iterate <task-id> --background --max-iterations N
```

This means a single worker handles: run implementation → create review → parse verdict → run improve if needed → re-review → repeat until approved or max iterations exhausted. No manual intervention needed for the happy path.

Plan and explore tasks don't go through review/improve, so they spawn as plain `gza work -b <id>`.

## Task Selection: Queue Ordering

Tasks are selected from the pending queue in this order:

1. **Urgent first** — tasks flagged as urgent are picked before all others
2. **FIFO within each lane** — within urgent and normal, insertion order

### Queue management: `gza queue`

```bash
gza queue                  # list pending tasks in pickup order (urgent first, then FIFO)
gza queue bump <id>        # move task to urgent lane (front of queue)
gza queue unbump <id>      # move task back to normal lane
```

`gza add --next "prompt"` is sugar for add + bump in one step.

Implementation: a boolean `urgent` column on the task table (default false). `get_pending()` sorts by `(urgent DESC, created_at ASC)`.

This avoids numeric priorities (which are a waste of time to manage) while solving the main use case: "I just noticed a bug and want it picked up in the next cycle." If arbitrary reordering is needed later, swap the boolean for an integer position column — the `queue` command is already the interface, just add `move` subcommands.

## Activity Log

### Event Types

| Event | Trigger | Example |
|-------|---------|---------|
| `START` | Task confirmed running (`in_progress` or live worker confirmed) | `12:03:04 START  gza-42 implement "Add JWT auth"` |
| `DONE` | Task completed | `12:14:22 DONE   gza-42 implement (11m18s)` |
| `FAIL` | Task failed | `12:08:44 FAIL   gza-42 implement: TEST_FAILURE (5m40s)` |
| `START` | Recovery task confirmed running after retry/resume | `12:09:00 START  gza-55 implement "Retry flaky test" [retry of gza-42]` |
| `START` | Recovery task confirmed running after resume | `12:09:00 START  gza-55 implement "Finish auth fix" [resume of gza-42]` |
| `REVIEW` | Review completed | `12:16:01 REVIEW gza-43 for gza-42: APPROVED` |
| `MERGE` | Task merged | `12:16:05 MERGE  gza-42 → main` |
| `SKIP` | Task skipped | `12:16:03 SKIP   gza-45: needs_discussion` |
| `RECOVR` | Dry-run preview of a recovery decision only | `12:09:00 RECOVR gza-42 retry via iterate -> (new task) (reason=NO_ACTIVITY, attempt 1/2) [dry-run]` |
| `START_FAILED` | Recovery launch never became confirmed running | `12:09:01 START_FAILED gza-55 [retry of gza-42]: spawned worker never reached in_progress` |
| `SLEEP` | Cycle finished and watch is sleeping | `12:16:05 SLEEP  sleeping 300s (0 pending, 3 running; +1 started)` |
| `WAKE` | Woke from sleep | `12:16:35 WAKE   checking... (3 running, 2 slots)` |

### Format

```
HH:MM:SS EVENT  task-id type "truncated prompt..." [details]
```

### Log Destination

- **stdout**: Always, for interactive use
- **File**: Also appended to `.gza/watch.log` for `tail -f` from another terminal
- Consider `--quiet` flag for headless use (file only)

### Detecting Events

Poll DB between cycles (Option A from original spec). Before each cycle, snapshot task statuses. After the cycle, diff against snapshot and emit events for transitions. The overhead of a few SQLite queries per cycle is negligible at 5-minute intervals.

## Seeing Failures and Orphans

These aren't part of watch itself, but are the complementary commands for monitoring:

- **`gza history --status failed`** — show failed tasks. Already works.
- **`gza advance --unimplemented`** — show completed plans/explores without implement children. Already works.
- **Activity log** — `tail -f .gza/watch.log` shows failures in real time with `FAIL` events.

## Signal Handling

- `SIGINT` (Ctrl+C): Log "shutting down", stop the loop, let in-flight workers continue (they're detached processes)
- `SIGTERM`: Same as SIGINT
- Do NOT kill child workers on shutdown — they manage their own lifecycle

## What Watch Does NOT Do

- **No budget management** — that's the autonomous daemon spec's concern
- **No goal evaluation** — no autonomous task creation beyond what's in the queue
- **No PID file / daemonization** — it's a foreground process (use tmux/screen)
- **No parallel workers internally** — watch spawns detached background workers and monitors them

## Implementation Notes

### Relationship to `advance`

Watch reuses the core logic from advance (merge detection, rebase handling, resume eligibility) but drives it in a loop with concurrency awareness. Whether watch literally calls `cmd_advance` or shares helper functions is an implementation detail — the important thing is a single code path for determining what action a task needs.

Open question: once watch exists, does `advance` remain useful as a standalone command? It's still valuable for one-shot "advance everything right now" without committing to a polling loop. But the manual advance → check → advance cycle is exactly what watch replaces. Revisit after watch ships.

### Prerequisites

- `urgent` column on task table + migration
- `gza queue` command (list, bump, unbump)
- `--next` flag on `gza add`
- `get_pending()` updated to sort by urgent flag
- Ability to spawn iterate-mode background workers (not just plain work)
- Worker registry cleanup / PID liveness checking
