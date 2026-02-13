# Rebasing Before Merge

How to handle branches that have fallen behind main.

## When you need to rebase

If main has new commits since your task branch was created, you may need to rebase before merging:

```bash
$ gza merge 3
Error: Branch 'feature/add-user-auth' has conflicts with main.
Rebase the branch first:
  gza rebase 3
```

## Check for conflicts

```bash
$ gza unmerged
Unmerged branches:

  #3 20260108-add-user-auth
     Branch: feature/add-user-auth
     Commits: 3 ahead, 5 behind main
     Status: needs rebase
```

## Rebase onto main

```bash
$ gza rebase 3
Rebasing feature/add-user-auth onto main...
Successfully rebased. 3 commits applied.
```

If there are no conflicts, the rebase completes automatically.

## Handling conflicts

If the rebase encounters conflicts:

```bash
$ gza rebase 3
Rebasing feature/add-user-auth onto main...
CONFLICT in src/auth/login.py

Rebase paused. Resolve conflicts and run:
  gza rebase 3 --continue

Or abort with:
  gza rebase 3 --abort
```

You can also use the `/gza-rebase` skill in Claude Code for interactive conflict resolution.

## Rebase against remote

If you're working with a team and main has been updated on the remote:

```bash
$ gza rebase 3 --remote
Fetching origin...
Rebasing feature/add-user-auth onto origin/main...
Successfully rebased.
```

The `--remote` flag fetches the latest from origin before rebasing.

## After rebasing

Once rebased, you can merge normally:

```bash
$ gza merge 3 --squash
Merged: feature/add-user-auth → main (squashed)
```

Or create a PR:

```bash
$ gza pr 3
PR created: https://github.com/myorg/myapp/pull/145
```

## Rebase multiple tasks

If you have several tasks that need rebasing:

```bash
$ gza unmerged
Unmerged branches:

  #3 20260108-add-user-auth (needs rebase)
  #4 20260108-add-dark-mode (needs rebase)
  #5 20260108-fix-typo (up to date)

# Rebase each one
$ gza rebase 3
$ gza rebase 4

# Then merge all at once
$ gza merge 3 4 5 --squash
```

## Tips

1. **Rebase early, rebase often** - Don't let branches fall too far behind
2. **Use `--remote`** - Ensures you're rebasing against the latest remote state
3. **Squash on merge** - `gza merge --squash` creates cleaner history after rebase
