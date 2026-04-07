# Worktree Lifecycle

## How tasks use worktrees

Each task type creates a worktree at `config.worktree_path / task.task_id`.

### Code tasks (implement, improve)

- **implement**: Creates a new worktree with a new branch based on `origin/main`.
- **improve**: Uses `same_branch=True`. Creates a worktree at its own path but checks out the *same branch* as the implementation task. The previous worktree for that branch is removed first (`cleanup_worktree_for_branch`).

When a code task is finalized with uncommitted changes, gza writes a single commit with:
- Subject derived from task summary when available, then task prompt, and finally a deterministic fallback (`gza task <slug>` or `Task #<id>`).
- Body metadata lines:
  - `Task #<id>`
  - `Slug: <task_id>`
  - `Gza-Review: #<id>` for improve tasks tied to a review

### Review tasks

Reviews also use `same_branch=True`. They check out the implementation branch in a new worktree directory. The implementation task's worktree directory is removed as a side effect.

### Non-code tasks (explore, plan, review, internal)

These create worktrees in `/tmp` (for Docker compatibility) based on the default branch. See `_run_non_code_task` in `runner.py`.

### `gza rebase` CLI (foreground mode)

`gza rebase` creates a **temporary** worktree at `config.worktree_path / task.id` for the duration of the rebase. Unlike task worktrees, this worktree is always removed after use — a `try/finally` block in `cmd_rebase` calls `git.worktree_remove(worktree_path, force=True)` on all exit paths (mechanical success, provider-resolved success, failure, and exception). If `worktree_remove` leaves the directory behind, `shutil.rmtree` is used as a fallback. This worktree is never left registered with git after `gza rebase` exits.

## Key invariant: one worktree per branch

Git enforces that a branch can only be checked out in one worktree at a time. When a review or improve starts, it calls `cleanup_worktree_for_branch()` which removes any existing worktree for that branch before creating the new one. This means:

- The implementation worktree directory is removed when a review starts
- The review worktree directory is removed when an improve starts
- Only the most recent task in a chain has an active worktree

## Cleanup safety

`gza clean --worktrees` only removes **orphaned** worktree directories — directories in the worktree path that are not tracked by `git worktree list`. These are leftover directories from worktrees that git has already pruned.

Age-based worktree cleanup is intentionally not performed because:
- A completed task's worktree may still be in use by a review or improve running on the same branch
- There is no reliable way to determine from the directory name alone whether the worktree is still needed
- Git's own worktree tracking (`git worktree list`) is the source of truth for what's active

Git itself handles worktree pruning via `git worktree prune` (which removes stale worktree references for directories that no longer exist on disk).
