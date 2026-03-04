# Worktree Lifecycle

## How tasks use worktrees

Each task type creates a worktree at `config.worktree_path / task.task_id`.

### Code tasks (implement, improve)

- **implement**: Creates a new worktree with a new branch based on `origin/main`.
- **improve**: Uses `same_branch=True`. Creates a worktree at its own path but checks out the *same branch* as the implementation task. The previous worktree for that branch is removed first (`cleanup_worktree_for_branch`).

### Review tasks

Reviews also use `same_branch=True`. They check out the implementation branch in a new worktree directory. The implementation task's worktree directory is removed as a side effect.

### Non-code tasks (explore, review, learn)

These create worktrees in `/tmp` (for Docker compatibility) based on the default branch. See `_run_non_code_task` in `runner.py`.

## Key invariant: one worktree per branch

Git enforces that a branch can only be checked out in one worktree at a time. When a review or improve starts, it calls `cleanup_worktree_for_branch()` which removes any existing worktree for that branch before creating the new one. This means:

- The implementation worktree directory is removed when a review starts
- The review worktree directory is removed when an improve starts
- Only the most recent task in a chain has an active worktree

## Cleanup safety

`gza cleanup` only removes **orphaned** worktree directories — directories in the worktree path that are not tracked by `git worktree list`. These are leftover directories from worktrees that git has already pruned.

Age-based worktree cleanup is intentionally not performed because:
- A completed task's worktree may still be in use by a review or improve running on the same branch
- There is no reliable way to determine from the directory name alone whether the worktree is still needed
- Git's own worktree tracking (`git worktree list`) is the source of truth for what's active

Git itself handles worktree pruning via `git worktree prune` (which removes stale worktree references for directories that no longer exist on disk).
