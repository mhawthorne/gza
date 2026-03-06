# GZA

![Gza - Liquid Swords](https://raw.githubusercontent.com/mhawthorne/gza/main/docs/assets/gza-liquid-swords.jpg)

AI agent task runner. Queue coding tasks, let AI agents work through them autonomously in parallel in isolated Docker containers, get git branches with completed work.

## Why Gza

AI coding agents are powerful but interactive — you describe a task, watch the agent work, review, repeat. That's fine for one task, but not for twenty.

Gza turns agent coding into a batch workflow: queue tasks, run them in parallel on isolated branches, come back to completed work. Each task gets its own Docker container and git worktree. Failed tasks can be resumed. Reviews and improvements can be automated into loops.

## Installation

```bash
pip install gza-agent
```

Or with [uv](https://docs.astral.sh/uv/):

```bash
uv tool install gza-agent
```

## Requirements

- [Docker](https://www.docker.com/) - Tasks run in isolated containers (can be disabled)
- [Claude Code](https://claude.ai/download) - Default AI provider

## Quick Example

```bash
# Initialize your project
gza init

# Queue some tasks
gza add "Add input validation to the registration form"
gza add "Refactor payment module to use the new API"
gza add "Add unit tests for the email service"

# Run them in parallel
gza work --background
gza work --background
gza work --background

# Check progress
gza ps

# Review and merge completed work
gza review 1
gza merge 1 --squash
```

## Documentation

For full documentation, examples, and configuration options, see the [GitHub repository](https://github.com/mhawthorne/gza).

- [Quick Start Guide](https://github.com/mhawthorne/gza/blob/main/docs/quickstart.md)
- [Configuration Reference](https://github.com/mhawthorne/gza/blob/main/docs/configuration.md)
- [Examples](https://github.com/mhawthorne/gza/tree/main/docs/examples)

## License

MIT
