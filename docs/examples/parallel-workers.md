# Running Multiple Workers in Parallel

Maximize throughput by running multiple tasks concurrently.

## Queue up tasks

```bash
$ theo add "Add user avatar upload feature"
$ theo add "Implement email notification preferences"
$ theo add "Add dark mode toggle to settings"
$ theo add "Create API rate limiting middleware"
```

## Spawn multiple background workers

```bash
$ for i in {1..4}; do theo work --background; done
Started worker: w-20260108-160001
Started worker: w-20260108-160002
Started worker: w-20260108-160003
Started worker: w-20260108-160004
```

Each worker atomically claims a pending task—no conflicts or duplicate work.

## Monitor all workers

```bash
$ theo ps
WORKER ID          PID    STATUS   TASK                              DURATION
w-20260108-160001  51234  running  20260108-add-user-avatar-uploa…   1m 23s
w-20260108-160002  51235  running  20260108-implement-email-notif…   1m 22s
w-20260108-160003  51236  running  20260108-add-dark-mode-toggle-…   1m 21s
w-20260108-160004  51237  running  20260108-create-api-rate-limit…   1m 20s
```

Include completed workers:

```bash
$ theo ps --all
```

## Tail logs from a specific worker

```bash
$ theo log -w w-20260108-160001 -f
```

## Stop a single worker

```bash
$ theo stop w-20260108-160001
Stopped: w-20260108-160001
```

## Stop all workers

```bash
$ theo stop --all
Stopped: w-20260108-160001
Stopped: w-20260108-160002
Stopped: w-20260108-160003
Stopped: w-20260108-160004
```

Force kill if workers are unresponsive:

```bash
$ theo stop --all --force
```

## Worker behavior

Background workers:

- Spawn as detached processes (survive terminal close)
- Atomically claim pending tasks (no conflicts with concurrent workers)
- Write logs to `.theo/logs/<task_id>.log`
- Update status in `.theo/workers/<worker_id>.json`
- Clean up automatically on completion

See [specs/concurrent-work.md](../../specs/concurrent-work.md) for full documentation.
