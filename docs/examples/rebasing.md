# Rebasing Before Merge

How to handle branches that have fallen behind main.

## When you need to rebase

If main has new commits since your task branch was created, you may need to rebase before merging:

```bash
$ gza merge gza-3
Error: Branch 'feature/add-user-auth' has conflicts with main.
Rebase the branch first:
  gza rebase gza-3 --run
```

## Check for conflicts

```bash
$ uv run gza unmerged
Unmerged branches:

  gza-3 20260108-add-user-auth
     Branch: feature/add-user-auth
     Commits: 3 ahead, 5 behind main
     Status: needs rebase
```

## Queue or run the rebase

Bare `gza rebase <task_id>` now creates a pending `rebase` child task. Use `--run` for immediate foreground execution or `--background` to detach a worker:

```bash
$ gza rebase gza-3
✓ Created rebase task gza-8
  Parent: gza-3
  Branch: feature/add-user-auth
  Target: main

$ gza rebase gza-3 --run
Rebasing feature/add-user-auth onto main...
Successfully rebased. 3 commits applied.
```

If there are no conflicts, the rebase completes automatically.

## Handling conflicts

If the rebase encounters conflicts:

```bash
$ gza rebase gza-3 --run
Rebasing feature/add-user-auth onto main...
CONFLICT in src/auth/login.py

Rebase paused. Resolve the conflict using the `/gza-rebase` skill,
or run an AI-assisted resolution:
  gza rebase gza-3 --run --resolve
```

`gza rebase` has no `--continue`/`--abort` flags. Resolve conflicts one of two ways:

- **Interactive:** use the `/gza-rebase` skill in your active runtime (Claude, Codex, or Gemini).
- **Non-interactive:** pass `--resolve` to have the agent auto-resolve conflicts.

If `gza rebase --resolve` reports the skill is missing, install it for the active runtime first, for example:

```bash
uv run gza skills-install --target codex gza-rebase --project .
```

## Rebase against remote

If you're working with a team and main has been updated on the remote:

```bash
$ gza rebase gza-3 --run --remote
Fetching origin...
Rebasing feature/add-user-auth onto origin/main...
Successfully rebased.
```

The `--remote` flag fetches the latest from origin before rebasing.

## After rebasing

Once rebased, you can merge normally:

```bash
$ gza merge gza-3 --squash
Merged: feature/add-user-auth → main (squashed)
```

Or create a PR:

```bash
$ gza pr gza-3
PR created: https://github.com/myorg/myapp/pull/145
```

## Rebase multiple tasks

If you have several tasks that need rebasing:

```bash
$ uv run gza unmerged
Unmerged branches:

  gza-3 20260108-add-user-auth (needs rebase)
  gza-4 20260108-add-dark-mode (needs rebase)
  gza-5 20260108-fix-typo (up to date)

# Rebase each one
$ gza rebase gza-3 --run
$ gza rebase gza-4 --run

# Then merge all at once
$ gza merge gza-3 gza-4 gza-5 --squash
```

## Tips

1. **Rebase early, rebase often** - Don't let branches fall too far behind
2. **Use `--remote`** - Ensures you're rebasing against the latest remote state
3. **Squash on merge** - `gza merge --squash` creates cleaner history after rebase
