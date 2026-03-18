# Running Multiple Workers in Parallel

Maximize throughput by running multiple tasks concurrently.

## Queue up tasks

```bash
$ gza add "Add user avatar upload feature"
$ gza add "Implement email notification preferences"
$ gza add "Add dark mode toggle to settings"
$ gza add "Create API rate limiting middleware"
```

## Spawn multiple background workers

```bash
$ for i in {1..4}; do gza work --background; done
Started worker: w-20260108-160001
Started worker: w-20260108-160002
Started worker: w-20260108-160003
Started worker: w-20260108-160004
```

Each worker atomically claims a pending task—no conflicts or duplicate work.

## Monitor all workers

```bash
$ gza ps
TASK ID    TYPE       STATUS         STARTED                  STEPS   DURATION   TASK
#101       implement  running        2026-01-08 16:00:01 UTC  12      1m 23s     20260108-add-user-avatar-upload
#102       implement  running        2026-01-08 16:00:02 UTC  11      1m 22s     20260108-implement-email-notifications
#103       implement  failed(startup) 2026-01-08 16:00:03 UTC -       8s         20260108-add-dark-mode-toggle
```

Default output includes running tasks and startup failures. Include all completed/failed rows:

```bash
$ gza ps --all
```

## Tail logs from a specific worker

```bash
$ gza log -w w-20260108-160001 -f
```

## Stop a single worker

```bash
$ gza stop w-20260108-160001
Stopped: w-20260108-160001
```

## Stop all workers

```bash
$ gza stop --all
Stopped: w-20260108-160001
Stopped: w-20260108-160002
Stopped: w-20260108-160003
Stopped: w-20260108-160004
```

Force kill if workers are unresponsive:

```bash
$ gza stop --all --force
```

## Worker behavior

Background workers:

- Spawn as detached processes (survive terminal close)
- Atomically claim pending tasks (no conflicts with concurrent workers)
- Write task logs to `.gza/logs/<task_id>.log`
- Capture startup output in `.gza/workers/<worker_id>-startup.log` until task logging is available
- Update status in `.gza/workers/<worker_id>.json`
- Clean up automatically on completion

See the [Configuration Reference](../configuration.md) for all worker options.
