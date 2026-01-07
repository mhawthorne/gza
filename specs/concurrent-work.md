# Concurrent Work Command Execution

## Overview

This spec describes how to spawn `work` commands in the background, view running commands, and tail their logs—similar to `docker ps` and `docker logs`.

## Current State

- Work commands run **synchronously** in the foreground
- Each task uses an isolated **git worktree**, which already enables concurrent execution at the filesystem level
- All subprocess calls are blocking via `subprocess.run()` or `Popen().wait()`
- Task state is persisted in SQLite (survives restarts)

## Design Goals

1. **Background execution**: Spawn work commands that run independently
2. **Process visibility**: List all running work processes (`theo ps`)
3. **Log streaming**: Attach to a running process to see output (`theo logs`)
4. **Simple implementation**: Leverage existing worktree isolation, minimal new infrastructure

---

## Commands

### 1. `theo work --background` / `theo work -b`

Spawn a work command in the background.

```bash
# Start a background worker for the next pending task
theo work --background

# Start a specific task in background
theo work --background <task_id>

# Start multiple background workers
theo work --background --count 3
```

**Behavior**:
- Forks/daemonizes the work process
- Returns immediately with the worker ID
- Writes PID to `.theo/workers/{worker_id}.pid`
- Logs continue writing to `.theo/logs/{task_id}.log` as normal

**Output**:
```
Started worker w-20260107-001 (PID 12345)
  Task: implement-auth-flow
  Log:  .theo/logs/20260107-implement-auth-flow.log

Use 'theo ps' to view running workers
Use 'theo logs w-20260107-001' to tail output
```

### 2. `theo ps`

List all running work processes.

```bash
theo ps
```

**Output** (table format, similar to `docker ps`):
```
WORKER ID        PID    STATUS     TASK ID                        DURATION
w-20260107-001   12345  running    20260107-implement-auth-flow   5m 23s
w-20260107-002   12350  running    20260107-add-tests             2m 10s
w-20260107-003   12355  completed  20260107-fix-typo              1m 05s
```

**Flags**:
- `--all` / `-a`: Include completed/failed workers (default: only running)
- `--quiet` / `-q`: Only show worker IDs (useful for scripting)
- `--json`: Output as JSON

**Status values**:
- `running`: Process is active
- `completed`: Process finished successfully
- `failed`: Process exited with error
- `stale`: PID file exists but process not running (cleanup needed)

### 3. `theo logs <worker_id>`

Stream/tail logs from a running or completed worker.

```bash
# Tail logs (follow mode, like tail -f)
theo logs w-20260107-001

# Show last N lines only
theo logs w-20260107-001 --tail 50

# Show all logs without following
theo logs w-20260107-001 --no-follow

# Follow logs until worker completes
theo logs w-20260107-001 --follow
```

**Behavior**:
- Reads from `.theo/logs/{task_id}.log` (JSONL format)
- Parses and pretty-prints the log entries (same as current console output)
- In follow mode (`-f`, default for running workers): tails the file
- For completed workers: shows full log then exits

**Output format**:
Same as current foreground execution—tool names, intermediate text, etc.

### 4. `theo stop <worker_id>`

Gracefully stop a running worker.

```bash
theo stop w-20260107-001

# Force kill
theo stop --force w-20260107-001

# Stop all running workers
theo stop --all
```

**Behavior**:
- Sends SIGTERM (or SIGKILL with `--force`)
- Worker cleanup runs (worktree removal, status update)
- Task marked as `failed` with interruption note

---

## Implementation Details

### Worker Registry

Store worker metadata in `.theo/workers/`:

```
.theo/workers/
├── w-20260107-001.json    # Worker metadata
├── w-20260107-001.pid     # PID file (for liveness check)
├── w-20260107-002.json
└── w-20260107-002.pid
```

**Worker metadata** (`w-{id}.json`):
```json
{
  "worker_id": "w-20260107-001",
  "pid": 12345,
  "task_id": "20260107-implement-auth-flow",
  "started_at": "2026-01-07T10:30:00Z",
  "status": "running",
  "log_file": ".theo/logs/20260107-implement-auth-flow.log",
  "worktree": ".theo/worktrees/20260107-implement-auth-flow"
}
```

### Background Process Spawning

Two approaches:

**Option A: Double-fork daemon (Unix)**
```python
def spawn_background_worker(task_id: str | None = None):
    pid = os.fork()
    if pid > 0:
        # Parent returns immediately
        return worker_id

    # Child: detach from terminal
    os.setsid()
    pid = os.fork()
    if pid > 0:
        os._exit(0)

    # Grandchild: run the work
    run_work(task_id)
```

**Option B: subprocess with nohup (simpler, cross-platform)**
```python
def spawn_background_worker(task_id: str | None = None):
    cmd = ["nohup", "theo", "work", "--worker-mode", task_id or ""]
    proc = subprocess.Popen(
        cmd,
        stdout=open(log_path, "a"),
        stderr=subprocess.STDOUT,
        start_new_session=True
    )
    return proc.pid
```

**Recommendation**: Option B is simpler and works on macOS/Linux. Use `--worker-mode` internal flag to indicate the process should write its own PID file and handle cleanup.

### Process Liveness Check

```python
def is_worker_running(worker_id: str) -> bool:
    pid_file = f".theo/workers/{worker_id}.pid"
    if not os.path.exists(pid_file):
        return False

    pid = int(Path(pid_file).read_text().strip())
    try:
        os.kill(pid, 0)  # Check if process exists
        return True
    except OSError:
        return False
```

### Log Streaming

Leverage existing JSONL log format. The `theo logs` command:

1. Opens log file
2. Seeks to end (or last N lines for `--tail`)
3. Parses JSONL entries
4. Pretty-prints using existing output formatting
5. In follow mode: uses `inotify` (Linux) or polling (macOS) to detect new lines

```python
def tail_log(log_path: str, follow: bool = True):
    with open(log_path, "r") as f:
        # Show existing content
        for line in f:
            print_log_entry(json.loads(line))

        if not follow:
            return

        # Follow new content
        while True:
            line = f.readline()
            if line:
                print_log_entry(json.loads(line))
            else:
                time.sleep(0.1)
                if not is_worker_running(worker_id):
                    break
```

### Cleanup

**On worker completion**:
- Update worker JSON status to `completed` or `failed`
- Remove PID file
- Worktree cleanup (existing behavior)

**Stale worker detection** (`theo ps`):
- If PID file exists but process not running → mark as `stale`
- `theo ps --cleanup` removes stale entries

---

## Database Considerations

The existing SQLite store handles concurrent access via transactions. Key points:

- Task status updates are atomic
- Multiple workers selecting "next pending task" need row-level locking or optimistic concurrency
- **Recommendation**: Use `SELECT ... FOR UPDATE` pattern or mark task `in_progress` immediately on selection

```python
def claim_next_task(store: SqliteTaskStore) -> Task | None:
    """Atomically claim the next pending task."""
    with store.transaction():
        task = store.get_next_pending_unblocked()
        if task:
            store.update_status(task.id, "in_progress")
        return task
```

---

## CLI Summary

| Command | Description |
|---------|-------------|
| `theo work -b` | Start background worker |
| `theo ps` | List running workers |
| `theo ps -a` | List all workers (including completed) |
| `theo logs <id>` | Tail worker logs |
| `theo logs <id> --no-follow` | Show full log without following |
| `theo stop <id>` | Stop a worker |
| `theo stop --all` | Stop all workers |

---

## Future Enhancements

1. **Worker pools**: `theo work --pool 3` maintains N concurrent workers
2. **Priority queues**: High-priority tasks get picked up first
3. **Resource limits**: Memory/CPU limits per worker
4. **Web dashboard**: Real-time view of all workers
5. **Notifications**: Slack/email on task completion/failure

---

## Migration Path

1. **Phase 1**: Implement `--background`, `ps`, `logs` commands
2. **Phase 2**: Add `stop` command and graceful shutdown
3. **Phase 3**: Concurrent task claiming with proper locking
4. **Phase 4**: Worker pools and advanced features
