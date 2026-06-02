# Task Resume

## Overview

This spec describes the ability to resume a task that failed due to `max_turns` being exceeded, preserving the full conversation context from the previous attempt.

## Motivation

When a task hits the `max_turns` limit, the work is incomplete but significant progress may have been made. Currently, the only option is `gza retry <id>` which starts completely fresh, losing all context and repeating work already done.

Claude CLI supports session continuation via `--resume <session_id>`, which allows picking up exactly where the conversation left off. By capturing and storing the session ID, gza can offer true resume functionality.

## How Claude CLI Sessions Work

1. Every Claude CLI invocation creates a session with a UUID (e.g., `e9de1481-112a-4937-a06d-087a88a32999`)
2. The session ID appears in every JSONL event, including the final `result` event
3. Sessions are stored locally by Claude CLI and include full message history
4. Using `claude -p "prompt" --resume <session_id>` continues the conversation with all prior context

## Schema Changes

Add to Task model in `db.py`:

```python
@dataclass
class Task:
    # ... existing fields ...
    session_id: str | None = None  # Claude session ID for resume capability
```

Database migration (v4):
```sql
ALTER TABLE tasks ADD COLUMN session_id TEXT;
```

## Implementation

### 1. Capture session_id from result event

In `providers/claude.py`, extract `session_id` from the result event:

```python
# In _run_with_output_parsing, after parsing result event:
if result_data:
    # ... existing stat extraction ...
    if "session_id" in result_data:
        result.session_id = result_data["session_id"]
```

Update `RunResult` in `providers/base.py`:
```python
@dataclass
class RunResult:
    # ... existing fields ...
    session_id: str | None = None
```

### 2. Store session_id on task completion/failure

In `runner.py`, save session_id when marking task complete or failed:

```python
# After provider.run() returns:
if result.session_id:
    task.session_id = result.session_id
    store.update(task)
```

### 3. Add resume command

New CLI command: `gza resume <task_id>`

```python
def cmd_resume(args: argparse.Namespace) -> int:
    """Resume a failed task from where it left off."""
    config = Config.load(args.project_dir)
    store = get_store(config)

    task = store.get(args.task_id)
    if not task:
        print(f"Error: Task #{args.task_id} not found")
        return 1

    if task.status != "failed":
        print(f"Error: Can only resume failed tasks (task is {task.status})")
        return 1

    if not task.session_id:
        print(f"Error: Task #{args.task_id} has no session ID (cannot resume)")
        print("Use 'gza retry' to start fresh instead")
        return 1

    # Resume the task
    return run(config, task_id=args.task_id, resume=True)
```

### 4. Update runner to support resume mode

Modify `run()` in `runner.py`:

```python
def run(config: Config, task_id: int | None = None, resume: bool = False) -> int:
    # ... existing setup ...

    if resume:
        if not task.session_id:
            print(f"Error: Task has no session ID for resume")
            return 1
        # Use existing branch if task had one
        if task.branch:
            branch_name = task.branch
        # Reset task status to in_progress
        store.mark_in_progress(task)
```

### 5. Update provider to support resume

Modify `ClaudeProvider.run()` to accept optional session_id:

```python
def run(
    self,
    config: Config,
    prompt: str,
    log_file: Path,
    work_dir: Path,
    resume_session_id: str | None = None,
) -> RunResult:
```

In `_run_direct()` and `_run_docker()`:

```python
cmd = [
    "timeout", f"{config.timeout_minutes}m",
    "claude", "-p", "-",
    "--output-format", "stream-json", "--verbose",
]

if resume_session_id:
    cmd.extend(["--resume", resume_session_id])

cmd.extend(config.claude_args)
cmd.extend(["--max-turns", str(config.max_turns)])
```

### 6. Resume prompt

When resuming, use a continuation prompt rather than repeating the original:

```python
if resume:
    prompt = "Continue from where you left off. The task was interrupted due to max_turns limit."
else:
    prompt = build_prompt(task, config, store, report_path, git)
```

## CLI Interface

```
gza resume <task_id>     # Resume a failed task
```

Options:
- `--no-docker` - Run Claude directly instead of in Docker

## Example Workflow

```bash
# Task fails due to max_turns
$ gza work
=== Task: Implement feature X ===
...
Task failed: max turns of 50 exceeded
Stats: Runtime: 15m 32s | Turns: 50 | Cost: $1.23

# Check the task
$ gza show 42
Task #42
Status: failed
Session ID: e9de1481-112a-4937-a06d-087a88a32999
...

# Resume where it left off
$ gza resume 42
=== Resuming Task #42 ===
...
```

## Edge Cases

1. **No session_id stored**: Tasks that failed before this feature was implemented won't have session IDs. Show helpful error directing to `gza retry`.

2. **Session expired**: Claude CLI sessions may expire. If resume fails with session-not-found error, suggest using `gza retry`.

3. **Branch already exists**: When resuming, reuse the existing branch from the failed attempt.

4. **Worktree cleanup**: The worktree was cleaned up after failure. Resume should recreate it, checking out the task's branch.

5. **Multiple resume attempts**: Each resume creates a new session. Store the latest session_id for future resumes.

## Future Enhancements

1. **Auto-resume option**: `gza work --auto-resume` could automatically resume any failed max_turns tasks before picking up new pending tasks.

2. **Resume with increased max_turns**: `gza resume 42 --max-turns 100` to give more headroom.

3. **Session info command**: `gza session <task_id>` to show session details and whether resume is possible.
