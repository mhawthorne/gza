# GZA

![Gza - Liquid Swords](/docs/assets/gza-liquid-swords.jpg)

AI agent task runner. Queue coding tasks, let Claude work through them autonomously in parallel in isolated Docker containers, get git branches with completed work.

Gza isn't built for the "run for hours, build a large system, get smarter along the way" approach, like [Ralph](https://github.com/snarktank/ralph)). It's for the "identify 20 tasks, fire them off before bed, merge them when you wake up" workflow.

## Supported Providers

| Provider | Status | Description |
|----------|--------|-------------|
| [Claude Code](https://claude.ai/download) | **Supported** | Default provider |
| [Gemini CLI](https://github.com/google-gemini/gemini-cli) | *Experimental* | Partially implemented, coming soon |

## Dependencies

- [Docker](https://www.docker.com/) - Tasks run in isolated containers
- [uv](https://docs.astral.sh/uv/) - Python package manager (recommended)

## Quick Start

See [Quick Start Guide](docs/quickstart.md) for installation and first steps.

## Usage

See [Configuration Reference](docs/configuration.md) for all commands, options, and settings.

## Examples

See [Examples](docs/examples/) for workflow guides including parallel workers, bulk import, and plan-implement-review patterns.
