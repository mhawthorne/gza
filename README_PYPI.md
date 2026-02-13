# GZA

![Gza - Liquid Swords](https://raw.githubusercontent.com/mhawthorne/gza/main/docs/assets/gza-liquid-swords.jpg)

AI agent task runner. Queue coding tasks, let Claude work through them autonomously in parallel in isolated Docker containers, get git branches with completed work.

## Installation

```bash
pip install gza-agent
```

Or with [uv](https://docs.astral.sh/uv/):

```bash
uv tool install gza-agent
```

## Requirements

- [Docker](https://www.docker.com/) - Tasks run in isolated containers
- [Claude Code](https://claude.ai/download) - Default AI provider

## Quick Example

```bash
# Add a task
gza task add "Add input validation to the user registration form"

# Start a worker
gza worker start

# Check status
gza task list
```

## Documentation

For full documentation, examples, and configuration options, see the [GitHub repository](https://github.com/mhawthorne/gza).

- [Quick Start Guide](https://github.com/mhawthorne/gza/blob/main/docs/quickstart.md)
- [Configuration Reference](https://github.com/mhawthorne/gza/blob/main/docs/configuration.md)
- [Examples](https://github.com/mhawthorne/gza/tree/main/docs/examples)

## License

MIT
