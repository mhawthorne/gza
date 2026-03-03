---
name: gza-worktree-create
description: Create an isolated git worktree for interactive development
allowed-tools: Bash(git:*), Bash(uv run python:*), Bash(cd:*), Read, AskUserQuestion
version: 1.0.0
public: false
---

# Create Interactive Worktree

Create an isolated git worktree for interactive development work, keeping the main checkout clean.

## Arguments

Accepts an optional branch name as argument (e.g., `/gza-worktree-create my-feature`).

## Process

### Step 1: Get the branch name

If a branch name was provided as an argument, use it. Otherwise, ask the user:

Use `AskUserQuestion` to ask: "What branch name should the worktree use?"

### Step 2: Read the interactive worktree directory from config

```bash
uv run python -c "
from pathlib import Path
from gza.config import Config
try:
    cfg = Config.load(Path('.'))
    print(cfg.interactive_worktree_dir)
except Exception as e:
    print(f'ERROR: {e}')
"
```

If the result is empty or an error, ask the user via `AskUserQuestion`:
- "Where should interactive worktrees be created? (absolute path, e.g., /tmp/my-worktrees)"

### Step 3: Create the worktree

Run:

```bash
git worktree add <dir>/<branch> -b <branch>
```

Where `<dir>` is the interactive worktree directory and `<branch>` is the branch name.

If the branch already exists, try without `-b`:

```bash
git worktree add <dir>/<branch> <branch>
```

### Step 4: Report result

Print:
- The full path to the new worktree
- Instruct the user: "Your worktree is ready. To work in it, use `cd <path>` or open it in your editor. Changes made there are isolated from your main checkout."
- Remind: "When done, commit and push your changes, then run `/gza-interactive-review` from the worktree to get a code review."
