# Quick Start

Get up and running with Gza in a few steps.

## 1. Install Gza

```bash
pip install gza
```

## 2. Initialize your project

In your project directory, run:

```bash
gza init
```

This creates a `gza.yaml` configuration file with sensible defaults. You can customize it later—see [Configuration](configuration.md) for details.

## 3. Add and run a task

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
