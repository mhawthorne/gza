# Quick Start

Get up and running with Gza — from install to your first merged branch.

## Prerequisites

- **Docker** - Tasks run in isolated containers by default. [Install Docker](https://docs.docker.com/get-docker/)
- **Node.js** - Required for Claude Code CLI. [Install Node.js](https://nodejs.org/)

## 1. Install Gza and an AI CLI

```bash
# Install Gza
pip install gza-agent

# Install Claude Code (default provider)
npm install -g @anthropic-ai/claude-code
claude --version
```

<details>
<summary>Other providers</summary>

```bash
# OpenAI Codex
npm install -g @openai/codex
codex --version

# Gemini CLI (experimental)
npm install -g @google/gemini-cli
gemini --version
```

</details>

## 2. Set up authentication

**Claude:**
- **OAuth (recommended):** Run `claude login`
- **API key:** Set `ANTHROPIC_API_KEY` in `~/.gza/.env`

<details>
<summary>Other providers</summary>

**Codex:**
- **OAuth (recommended):** Run `codex login`
- **API key:** Set `CODEX_API_KEY` in `~/.gza/.env`

**Gemini:**
- **OAuth:** Run `gemini login`
- **API key:** Set `GEMINI_API_KEY` in `~/.gza/.env`

</details>

See [Configuration](configuration.md#provider-credentials) for details on credential precedence.

## 3. Initialize your project

In your project directory:

```bash
gza init
```

This creates a `gza.yaml` configuration file. Key settings you may want to change:

```yaml
# gza.yaml
project_name: my-app
use_docker: true          # Set to false to run agents locally (no Docker)
timeout_minutes: 15       # Max time per task
max_steps: 80             # Max conversation steps per task
```

> **Docker vs local:** Docker provides isolation — each task runs in its own container and can't affect your host. Set `use_docker: false` if you don't need isolation or want faster startup. You can also pass `--no-docker` per-run: `gza work --no-docker`.

Add `.gza/` to your `.gitignore` — it contains local state (database, logs) that shouldn't be committed:

```bash
echo ".gza/" >> .gitignore
```

## 4. Your first task

```bash
# Add a task
gza add "Fix the login button not responding on mobile devices"
# Created task gza-1: 20260108-fix-the-login-button (implement)

# Run it
gza work
```

Gza creates a git branch, runs the AI agent in Docker, and commits the changes. When it finishes:

```bash
# See what was done
gza log gza-1

# Check the branch
gza unmerged
#   gza-1 20260108-fix-the-login-button
#      Branch: my-app/20260108-fix-the-login-button
#      +42 -8 across 3 files
```

## 5. Review and merge

For a quick fix, merge directly:

```bash
gza merge gza-1 --squash
```

For anything nontrivial, run a review first:

```bash
# AI reviews the implementation
gza review gza-1
# Created review task gza-2 — runs immediately

# Read the review
cat .gza/reviews/20260108-review-fix-the-login-button.md
```

If the review requests changes, improve and re-review:

```bash
# Address review feedback (continues on the same branch)
# You can pass the implement, improve, or review task ID — gza auto-resolves to the root implementation.
gza improve gza-1

# Review again
gza review gza-1
```

When you're satisfied, merge or create a PR:

```bash
# Merge locally
gza merge gza-1 --squash

# Or create a GitHub PR
gza pr gza-1
```

## 6. Scaling up

The real power of gza is running many tasks in parallel.

### Queue multiple tasks

```bash
gza add "Add input validation to the registration form"
gza add "Refactor the payment module to use the new API"
gza add "Add unit tests for the email service"
```

### Run them in parallel

```bash
# Run 3 tasks simultaneously in background
gza work --background --count 3
gza work --background --count 3
gza work --background --count 3

# Watch progress
gza ps
```

### Let gza manage the lifecycle

The `advance` command handles the full lifecycle automatically — creating reviews, running improvements, and merging approved work:

```bash
# Preview what advance would do
gza advance --dry-run

# Execute: run up to 3 workers, auto-review, auto-merge
gza advance --auto --batch 3
```

### Automate iterate loops

For a single implementation, `--max-iterations` counts code-write iterations. Iteration 1 is the implementation write (existing or newly run), and every iteration ends with a review:

```bash
gza iterate gza-1 --max-iterations 3
```

## Next steps

- [Simple Task](examples/simple-task.md) — complete walkthrough of a single task
- [Plan → Implement → Review](examples/plan-implement-review.md) — multi-phase workflow for larger features
- [Parallel Workers](examples/parallel-workers.md) — running tasks concurrently
- [Bulk Import](examples/bulk-import.md) — importing many tasks from YAML
- [Configuration Reference](configuration.md) — all commands, options, and settings
