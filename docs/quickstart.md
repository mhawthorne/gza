# Quick Start

Get up and running with Gza in a few steps.

## 1. Install Gza

```bash
pip install gza-agent
```

## 2. Set up authentication

Gza needs credentials for your AI provider. For Claude, either:

- **OAuth (recommended):** Run `claude login` to authenticate interactively
- **API key:** Set `ANTHROPIC_API_KEY` in your shell or in `~/.gza/.env`

Credentials are checked in this order (highest priority first):
1. Project `.env` file
2. Shell environment variables
3. `~/.gza/.env` file
4. OAuth credentials (`~/.claude/`)

See [Configuration](configuration.md#provider-credentials) for Gemini setup.

## 3. Initialize your project

In your project directory, run:

```bash
gza init
```

This creates a `gza.yaml` configuration file with sensible defaults. You can customize it later—see [Configuration](configuration.md) for details.

## 4. Add and run a task

```bash
# Add a task
gza add "Fix the login button not responding on mobile devices"

# Run it
gza work
```

That's it! Gza will execute the task, create a branch, and make the changes.

## Next steps

- See [Simple Task](examples/simple-task.md) for a complete walkthrough
- Learn about [Plan → Implement → Review](examples/plan-implement-review.md) workflows for larger features
- Explore all [Examples](examples/README.md)
