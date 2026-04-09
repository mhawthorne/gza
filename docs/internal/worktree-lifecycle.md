# Worktree Lifecycle

## How tasks use worktrees

Each task type creates a worktree at `config.worktree_path / task.slug`.

### Code tasks (implement, improve)

- **implement**: Creates a new worktree with a new branch based on `origin/main`.
- **improve**: Uses `same_branch=True`. Creates a worktree at its own path but checks out the *same branch* as the implementation task. The previous worktree for that branch is removed first (`cleanup_worktree_for_branch`).

When a code task is finalized with uncommitted changes, gza writes a single commit with:
- Subject derived from task summary when available, then task prompt, and finally a deterministic fallback (`gza task <slug>` or `Task #<id>`).
- Body metadata lines:
  - `Task #<id>`
  - `Slug: <slug>`
  - `Gza-Review: #<id>` for improve tasks tied to a review

### Review tasks

Reviews also use `same_branch=True`. They check out the implementation branch in a new worktree directory. The implementation task's worktree directory is removed as a side effect.

### Non-code tasks (explore, plan, review, internal)

These create worktrees in `/tmp` (for Docker compatibility) based on the default branch. See `_run_non_code_task` in `runner.py`.

Unlike code task worktrees, non-code task worktrees are **removed on success** after the report file is copied back to the project directory. They have no further use once the report is written. On failure, the worktree is preserved for debugging and its path is printed to the console.

**Log-recovery path**: When the expected report artifact is missing but content can be recovered from the provider's JSONL log (a `{"type": "result"}` entry), the task completes successfully and the worktree is cleaned up normally — not preserved. This recovery is logged as a warning and the outcome is recorded as `completed (recovered from provider log)`.

### `gza rebase` CLI (foreground mode)

`gza rebase` creates a **temporary** worktree at `config.worktree_path / task.id` for the duration of the rebase. Unlike task worktrees, this worktree is always removed after use — a `try/finally` block in `cmd_rebase` calls `git.worktree_remove(worktree_path, force=True)` on all exit paths (mechanical success, provider-resolved success, failure, and exception). If `worktree_remove` leaves the directory behind, `shutil.rmtree` is used as a fallback. This worktree is never left registered with git after `gza rebase` exits.

## Local path dependency symlinks

When a project's `pyproject.toml` contains local path dependencies under
`[tool.uv.sources]` (e.g. `path = "../shared-lib"`), those relative paths break
inside worktrees because the worktree lives at a different parent directory than
the original project root.

After creating a worktree, gza automatically resolves each relative path entry in
`[tool.uv.sources]`, finds where that path would resolve *from the worktree*, and
creates a symlink at that location pointing to the real directory on the host. For
example:

- Project at `/Users/me/work/myproject`, dep `path = "../shared-lib"` → real path
  `/Users/me/work/shared-lib`
- Worktree at `/tmp/gza-worktrees/myproject/task-123`
- From worktree, `../shared-lib` → `/tmp/gza-worktrees/myproject/shared-lib`
- Symlink created: `/tmp/gza-worktrees/myproject/shared-lib` →
  `/Users/me/work/shared-lib`

**Rules:**
- Only relative paths are handled (absolute paths work everywhere, no fixup needed).
- Only paths that actually exist on disk are symlinked (no dangling symlinks).
- Paths inside the worktree itself (workspace members) are skipped.
- Symlinks are idempotent: if the correct symlink already exists, it is skipped.
- If something already exists at the target location (wrong symlink or real
  directory), a warning is logged and the existing path is left untouched.
- Symlinks persist in `/tmp` — they are shared across concurrent worktrees for the
  same project and cleaned up by the OS on reboot.
- **Docker mode**: symlinks are not created in Docker mode. Docker users should
  use `docker_volumes` and `docker_setup_command` to mount local dependencies
  inside the container.

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
