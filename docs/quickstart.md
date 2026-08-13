# Quick Start

From install to your first merged branch.

## 1. Install

```bash
uv tool install gza-agent          # the `gza` command
```

Or add it as a project dependency, if you'd rather scope it per-project:

```bash
uv add gza-agent                   # then run via `uv run gza`
```

Plus a provider CLI (at least one), authenticated:

```bash
npm install -g @anthropic-ai/claude-code && claude login   # or:
npm install -g @openai/codex && codex login
```

Docker is optional — tasks run in isolated containers when it's available.

## 2. Set up your project

```bash
cd my-project
gza init                    # writes gza.yaml
echo ".gza/" >> .gitignore  # local state (db, logs) — don't commit
```

Edit `gza.yaml` if needed: `use_docker: false` to run without Docker, `timeout_minutes`, `max_steps`. See [Configuration](configuration.md).

## 3. Add tasks

### Add with prompt as argument

```bash
gza add "Fix login button on mobile"
# ✓ Added task gza-1
```

### Add with prompt from `$EDITOR`

```bash
gza add
```

omitting the prompt opens `$EDITOR`


### Add with tag
```bash
gza add --tag v0.6.0
```

- Tags can be used by `gza queue` to list queued tasks, or in `gza watch` to query for tasks to implement.

The most convenient way to add tasks is to install the skills:

```bash
gza skills-install
```

And then either run the `gza-task-add` skill in your LLM session, or just ask Claude/Codex conversationally to "add a GZA task"


## 4. Run your tasks

### Hands-on — one task at a time

```bash
gza iterate gza-1 --max-iterations 3   # implement → review → improve, up to 3 rounds
gza merge gza-1                         # merge once the review is clean
```

### Async — a background loop

```bash
gza watch --tag v0.6.0 --batch 4 --poll 300
```

- Watch runs matching tasks in parallel, drives each through its lifecycle — implement → review → improve — and **auto-merges what passes review** (no `BLOCKER`).
- Tune `--batch` (how many at once) and `--poll` (seconds between cycles); go wider when things are stable or the backlog is deep.

## 5. Investigate stuck tasks

Not everything completes cleanly — provider outages, stuck tests, and gza bugs all happen.

Watch retries within limits, then flags what stalled. Inspect one with:

```bash
gza log <task_id>
```
