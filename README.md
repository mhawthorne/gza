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
| `theo history` | Show completed/failed tasks |
| `theo show <id>` | Show task details |
| `theo log <id>` | Display task execution log |
| `theo stats` | Show cost and usage statistics |
| `theo import <file>` | Import tasks from YAML |

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
