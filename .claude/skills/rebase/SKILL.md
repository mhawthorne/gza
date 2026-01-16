---
name: rebase
description: Rebase current branch on main, with interactive conflict resolution. Use when rebasing, merging, or resolving git conflicts.
allowed-tools: Read, Edit, Glob, Grep, Bash(git:*), Bash(uv run python -m py_compile:*)
---

# Rebase on Main

Rebase the current branch onto the latest `origin/main`, resolving any merge conflicts interactively.

## Process

### Step 1: Pre-flight checks

1. Check for uncommitted changes - if any exist, stop and ask the user to commit or stash them
2. Show the current branch name

### Step 2: Fetch and attempt rebase

1. Run `git fetch origin main`
2. Run `git rebase origin/main`
3. If rebase succeeds with no conflicts, report success and show the push command

### Step 3: Resolve conflicts (if any)

For each conflicted file:

1. **Show the conflict** - Run `git diff --name-only --diff-filter=U` to list conflicted files
2. **Read and understand** - Read each conflicted file to see the conflict markers (`<<<<<<<`, `=======`, `>>>>>>>`)
3. **Explain the conflict** - Tell the user what both sides are trying to do:
   - "HEAD (your branch) is adding/changing X"
   - "origin/main is adding/changing Y"
4. **Propose a resolution** - Suggest how to combine the changes (usually keeping both)
5. **Ask for approval** - Use AskUserQuestion to confirm the resolution approach before editing
6. **Apply the fix** - Edit the file to resolve the conflict, removing all conflict markers
7. **Verify syntax** - For Python files, run `uv run python -m py_compile <file>`
8. **Stage the file** - Run `git add <file>`

Repeat for each conflicted file.

### Step 4: Continue the rebase

After all conflicts are resolved:

1. Run `git rebase --continue`
2. If more conflicts appear (from subsequent commits), repeat Step 3
3. Continue until rebase completes

### Step 5: Final summary

Show:
- "Rebase completed successfully!"
- Number of conflicts resolved
- The command to push: `git push --force-with-lease`

## Important notes

- **Never force-push automatically** - always let the user do this manually
- **Always ask before resolving ambiguous conflicts** - if the intent isn't clear, ask
- **Preserve both changes when possible** - most conflicts in this project are additive (both sides adding new code)
- **Check Python syntax after each resolution** - catch errors early
