# Gza

AI agent task runner. Queue up tasks, let Claude work through them.

## Installation

```bash
uv pip install -e .
```

## Quick Start

```bash
# Initialize a project
gza init

# Add a task
gza add "Refactor the auth module to use JWT tokens"

# Run the next pending task
gza work
```

## Commands

| Command | Description |
|---------|-------------|
| `gza init` | Initialize gza in current directory |
| `gza add <prompt>` | Add a new task |
| `gza next` | List pending tasks |
| `gza work` | Run the next pending task |
| `gza work --background` | Run task in background (detached mode) |
| `gza ps` | List running background workers |
| `gza logs <worker_id>` | Tail logs for a background worker |
| `gza stop <worker_id>` | Stop a running background worker |
| `gza history` | Show completed/failed tasks |
| `gza show <id>` | Show task details |
| `gza log <id>` | Display task execution log |
| `gza stats` | Show cost and usage statistics |
| `gza import <file>` | Import tasks from YAML |

## Background Workers

Run tasks in the background to parallelize work:

```bash
# Start a background worker for the next task
gza work --background

# Start multiple workers (runs 3 tasks concurrently)
for i in {1..3}; do gza work --background; done

# List running workers
gza ps

# Tail logs for a worker
gza logs w-20260107-123456

# Stop a worker
gza stop w-20260107-123456

# Stop all workers
gza stop --all
```

Background workers spawn detached processes that:
- Atomically claim pending tasks (no conflicts with concurrent workers)
- Write logs to `.gza/logs/<task_id>.log`
- Update their status in `.gza/workers/<worker_id>.json`
- Clean up automatically on completion

See [specs/concurrent-work.md](specs/concurrent-work.md) for full documentation.

## Importing Tasks

Import tasks from a YAML file with dependencies:

```bash
gza import tasks.yaml --dry-run  # preview
gza import tasks.yaml            # import
gza import tasks.yaml --force    # skip duplicate detection
```

See [specs/task-import.md](specs/task-import.md) for full documentation on the import file format.

## Configuration

Gza uses `gza.yaml` for project configuration:

```yaml
project_name: my-project

# Optional settings
use_docker: true
timeout_minutes: 30
max_turns: 50
```

Run `gza validate` to check your configuration.
