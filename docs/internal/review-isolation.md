# Review Task Isolation

Review tasks run in isolated git worktrees that only contain git-tracked files.

## How context flows

1. **Host runner** queries the main database via `store.get()`
2. **Host runner** calls `build_prompt()` which includes:
   - Spec file content (if the implementation task has a `spec` field)
   - Git diff summary (changes from `main...{impl_branch}`)
   - Original plan (walks up the `based_on` chain)
3. **Host runner** passes the complete prompt string to Docker/Claude
4. **Claude** receives all context baked into the prompt

Database lookups happen on the host before the worktree is even created. The worktree isolation doesn't affect prompt building.

See `runner.py:260-291` for the review context building logic.

## What the worktree contains

The worktree is a git checkout of the implementation branch. It contains:
- All git-tracked files from that branch
- Empty `.gza/` directory (since `.gza/` is gitignored)

## If Claude tries to query gza directly

If Claude runs `gza` commands or reads `.gza/gza.db` during the review, it will find an empty database. This is unnecessary - all relevant context is already in the prompt.

If a review complains about "database is empty" or "cannot find task record," the reviewer is going beyond the provided prompt context. The prompt should contain everything needed.
