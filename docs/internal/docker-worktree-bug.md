# Docker + Git Worktree Path Bug

## Summary

When gza runs tasks in Docker mode with git worktrees, git commands executed by Claude inside the container will fail because the worktree's `.git` file contains host paths that don't exist in the container.

## Status

**Workaround in place**: AGENTS.md instructs Claude not to run git commands. Gza handles all git operations on the host after the task completes.

**Not fixed**: The underlying issue remains. If Claude runs git commands inside Docker, they will fail.

## How It Manifests

1. Claude runs `git status` (or any git command) inside the Docker container
2. Git reads `/workspace/.git` which contains: `gitdir: /Users/username/project/.git/worktrees/task-name`
3. That host path doesn't exist inside the container
4. Git fails with: `fatal: not a git repository: /Users/username/project/.git/worktrees/task-name`
5. Claude may "fix" this by running `rm .git && git init`, creating an orphaned repo
6. Commits made in the orphaned repo are not connected to the main repo

## Root Cause

Git worktrees work via a `.git` **file** (not directory) that points to metadata in the main repo's `.git/worktrees/` directory. The path in this file is an absolute host path.

When Docker mounts the worktree at `/workspace`:
- The `.git` file is mounted with its original content
- The path it contains (`/Users/.../project/.git/worktrees/...`) doesn't exist in the container
- Only the worktree directory itself is mounted, not the main repo's `.git` directory

## Current Architecture

```
Host:
  /Users/m3h/work/project/                    # Main repo
  /Users/m3h/work/project/.git/               # Main git dir
  /Users/m3h/work/project/.git/worktrees/     # Worktree metadata
  /tmp/gza-worktrees/project/task-name/       # Worktree directory
  /tmp/gza-worktrees/project/task-name/.git   # File pointing to main .git

Docker:
  /workspace/                                  # Worktree mounted here
  /workspace/.git                              # File with INVALID host path
```

## Task 90 Incident (2026-02-12)

Task 90 was the first task to trigger this bug because:
1. AGENTS.md was updated to say "Do NOT commit until tests pass"
2. Claude interpreted this as needing to run `git status` before committing
3. Git failed, Claude ran `rm .git && git init`
4. Commits were made to an orphaned repo, not the task branch

The commits still exist in `/private/tmp/gza-worktrees/gza/20260212-add-rich-console-output-coloring-for-gza-work/` but are not connected to the main repo.

## Potential Fixes

### 1. Mount the main `.git` directory (Complex)

Mount both the worktree and the main repo's `.git` at paths the container expects:

```python
# Would need to parse .git file and mount accordingly
"-v", f"{main_repo}/.git:{host_git_path}:ro"
```

Challenges:
- Need to rewrite paths or mount at exact host paths
- Security considerations with exposing full git history

### 2. Rewrite `.git` file in container (Moderate)

After mounting, rewrite the `.git` file to point to a container-valid path, and mount the worktree metadata there.

### 3. Don't use worktrees in Docker mode (Simple but slow)

Use `git clone` or file copy instead of worktrees when Docker mode is enabled.

### 4. Detect git commands and warn/block (Defensive)

Add a hook or wrapper that detects git commands and warns Claude not to use them.

## Recovery: Fetching Orphaned Commits

If commits were made to an orphaned repo, they can be recovered:

```bash
# From the main repo, fetch from the orphaned repo
git fetch /path/to/orphaned-worktree HEAD:refs/heads/recovered-branch

# Or copy objects directly
cp -r /path/to/orphaned-worktree/.git/objects/* .git/objects/
git update-ref refs/heads/branch-name <commit-hash>
```
