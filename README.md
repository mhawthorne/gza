# GZA

![Gza - Liquid Swords](/docs/assets/gza-liquid-swords.jpg)

AI agent task runner. Queue coding tasks, let AI agents work through them autonomously in parallel in isolated Docker containers, get git branches with completed work.

## Why Gza

AI coding agents are powerful but interactive. You describe a task, watch the agent work, review the result, repeat. That's fine for one task — but if you have 20 tasks across a codebase, sitting through each one serially wastes your time.

Gza turns agent coding into a batch workflow:

1. **Queue tasks** — describe what you want done, as many as you need
2. **Fire and forget** — gza runs them in parallel, each in an isolated Docker container on its own git branch
3. **Review and merge** — come back to completed branches, review the work, merge what's good

Each task gets its own worktree, so agents never step on each other. Failed tasks can be resumed or retried. Reviews and improvements can be automated into loops. You define the work; gza handles the execution.

Gza isn't built for the "run for hours, build a large system, get smarter along the way" approach. It's for the "identify 20 tasks, fire them off before bed, merge them when you wake up" workflow.

## Supported Providers

| Provider | Status | Description |
|----------|--------|-------------|
| [Claude Code](https://claude.ai/download) | **Supported** | Default provider |
| [OpenAI Codex](https://github.com/openai/codex) | **Supported** | Alternative provider |
| [Gemini CLI](https://github.com/google-gemini/gemini-cli) | *Experimental* | Partially implemented |

## Dependencies

- [Docker](https://www.docker.com/) - Tasks run in isolated containers (can be disabled)
- [uv](https://docs.astral.sh/uv/) - Python package manager (recommended)

## Quick Start

See [Quick Start Guide](docs/quickstart.md) for installation and your first task through merge.

## Usage

See [Configuration Reference](docs/configuration.md) for all commands, options, and settings.

## Examples

See [Examples](docs/examples/) for workflow guides including parallel workers, bulk import, and plan-implement-review patterns.
