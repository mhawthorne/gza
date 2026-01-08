# Theo

AI agent task runner. Queue up tasks, let Claude work through them.

## Installation

```bash
uv pip install -e .
```

## Quick Start

```bash
# Initialize a project
theo init

# Add a task
theo add "Refactor the auth module to use JWT tokens"

# Run the next pending task
theo work
```

## Commands

| Command | Description |
|---------|-------------|
| `theo init` | Initialize theo in current directory |
| `theo add <prompt>` | Add a new task |
| `theo next` | List pending tasks |
| `theo work` | Run the next pending task |
| `theo work --background` | Run task in background (detached mode) |
| `theo ps` | List running background workers |
| `theo logs <worker_id>` | Tail logs for a background worker |
| `theo stop <worker_id>` | Stop a running background worker |
| `theo history` | Show completed/failed tasks |
| `theo show <id>` | Show task details |
| `theo log <id>` | Display task execution log |
| `theo stats` | Show cost and usage statistics |
| `theo import <file>` | Import tasks from YAML |

## Background Workers

Run tasks in the background to parallelize work:

```bash
# Start a background worker for the next task
theo work --background

# Start multiple workers (runs 3 tasks concurrently)
for i in {1..3}; do theo work --background; done

# List running workers
theo ps

# Tail logs for a worker
theo logs w-20260107-123456

# Stop a worker
theo stop w-20260107-123456

# Stop all workers
theo stop --all
```

Background workers spawn detached processes that:
- Atomically claim pending tasks (no conflicts with concurrent workers)
- Write logs to `.theo/logs/<task_id>.log`
- Update their status in `.theo/workers/<worker_id>.json`
- Clean up automatically on completion

See [specs/concurrent-work.md](specs/concurrent-work.md) for full documentation.

## Importing Tasks

Import tasks from a YAML file with dependencies:

```bash
theo import tasks.yaml --dry-run  # preview
theo import tasks.yaml            # import
theo import tasks.yaml --force    # skip duplicate detection
```

See [specs/task-import.md](specs/task-import.md) for full documentation on the import file format.

## Configuration

Theo uses `theo.yaml` for project configuration:

```yaml
project_name: my-project

# Optional settings
use_docker: true
timeout_minutes: 30
max_turns: 50
```

Run `theo validate` to check your configuration.
