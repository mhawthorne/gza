---
name: gza-rebase
description: Rebase current branch onto the requested target branch, with interactive conflict resolution. Use when rebasing, merging, or resolving git conflicts.
allowed-tools: Read, Edit, Glob, Grep, Bash(git:*), Bash(uv run:*), Bash(./*), Bash(make:*), Bash(npm:*), Bash(pytest:*), Bash(python:*), Bash(awk:*), Bash(sed:*)
version: 1.0.0
public: true
---

# Rebase onto Target Branch

Rebase the current branch onto a local target branch, resolving any merge conflicts interactively.

## Process

### Modes

- Default mode: run the full flow (Steps 1-7).
- `--continue` mode: assume a rebase conflict is already in progress, skip Steps 1-3, and start directly at Step 4.
  - In this mode, do not check for a clean working tree; the tree is expected to be dirty because of conflict markers.
  - If no rebase is in progress, stop and report that `git rebase --continue` cannot proceed.
- `--auto` mode: unattended rebase for background workers. Same as default mode but:
  - Do NOT use AskUserQuestion — resolve all conflicts autonomously using best judgment.
  - If a conflict is truly ambiguous and cannot be resolved confidently, abort the rebase and report failure.
  - Treat edit-vs-delete and two-sided-modification conflicts as ambiguous unless you can preserve every still-referenced symbol with high confidence. Do not silently prefer deletion.
  - Uncommitted changes may be present in the working tree (e.g. leftover from an interrupted run). Stash them before rebasing and restore with `git stash pop` afterwards.
  - Do NOT use remote git operations. Do not run `git fetch`, `git ls-remote`, HTTPS fallback fetches, or modify git remotes/config. Use only local refs already present in the repo. If the required local target is missing, stop and report failure.

### Step 1: Pre-flight checks

1. Check for uncommitted changes (`git status --porcelain`)
   - In default mode: if any exist, stop and ask the user to commit or stash them
   - In `--auto` mode: if any exist, run `git stash` to save them. They will be restored after the rebase completes.
2. Show the current branch name

### Step 2: Choose rebase target

1. Determine whether the caller already named the target branch in the prompt or request.
   - If the caller named a target branch (for example `master`), use that exact branch name. Do not substitute `main` or any other default.
2. If the caller did not name a target branch, resolve the repo's primary branch:
   - First try `git symbolic-ref --quiet --short refs/remotes/origin/HEAD` and strip any leading `origin/`.
   - If that does not produce a branch name, fall back to whichever of `main` or `master` exists locally by checking `git show-ref --verify --quiet refs/heads/<name>`.
   - If no primary branch can be determined, stop and report the failure instead of assuming `main`.
3. In default mode, if the caller did not already fix the target branch, prompt the user to choose between:
   - `<resolved-target>` (local - default) - Use the local branch already present in the repo
   - `origin/<resolved-target>` (remote) - Only use this when the caller explicitly asked for a remote rebase
4. In `--auto` mode, do not choose or synthesize a remote target. Rebase only onto the local target already provided by the caller or resolved locally in this step.

### Step 3: Fetch and attempt rebase

1. If and only if the caller explicitly requested a remote rebase in non-`--auto` mode, run `git fetch origin <resolved-target>`
2. Run `git rebase <chosen-target>`
3. If rebase succeeds with no conflicts, continue to Step 6. Do not report success yet.
4. If the chosen local target does not exist, stop and report the missing ref. Do not try remote probes or alternate transports.

### Step 4: Resolve conflicts (if any)

For each conflicted file:

1. **Show the conflict** - Run `git diff --name-only --diff-filter=U` to list conflicted files
2. **Read and understand** - Read each conflicted file to see the conflict markers (`<<<<<<<`, `=======`, `>>>>>>>`)
3. **Explain the conflict** - Tell the user what both sides are trying to do:
   - "HEAD (your branch) is adding/changing X"
   - "the target branch is adding/changing Y"
4. **Propose a resolution** - Suggest how to combine the changes (usually keeping both)
5. **Ask for approval** - Use AskUserQuestion to confirm the resolution approach before editing
6. **Apply the fix** - Edit the file to resolve the conflict, removing all conflict markers
7. **Stage the file** - Run `git add <file>`

Repeat for each conflicted file.

### Step 5: Continue the rebase

After all conflicts are resolved:

1. Run `git rebase --continue`
2. If more conflicts appear (from subsequent commits), repeat Step 4
3. Continue until rebase completes, then proceed to Step 6

### Step 6: Restore stashed changes

If changes were stashed in Step 1, run `git stash pop` to restore them before final verification. If `git stash pop` introduces conflicts, resolve them before proceeding and do not report success until the current checkout is clean enough to verify.

### Step 7: Final verification

Before declaring success, read `verify_command` directly from `gza.yaml` as the project `verify_command`. Also note `inner_verify_command` if present. If `uv run gza config` is available in this environment, you may use it as an optional confirmation or to inspect merged config, but do not treat `gza config` failure as an error when `gza.yaml` was readable.

- If you need quick feedback while fixing verification fallout, use `inner_verify_command` when configured, or otherwise use targeted tests for the files you touched.
- Run the configured full `verify_command` from the project root or worktree root after the rebase is fully complete, after any stashed changes have been restored, and after the last planned code change.
- Fix any failures you surface in your own context and do not declare success until the full verification passes.
- If `gza.yaml` has no `verify_command`, stop and report that it must be set before this skill can complete successfully.

### Step 8: Final summary

Show:
- "Rebase completed successfully!"
- Number of conflicts resolved
- If not in `--auto` mode, remind the user to push with `git push --force-with-lease`

## Important notes

- **Never force-push automatically** - always let the caller/user do this manually
- **Always ask before resolving ambiguous conflicts** (unless in `--auto` mode) - if the intent isn't clear, ask
- **Preserve both changes when possible** - most conflicts in this project are additive (both sides adding new code)
- **Verification is project-specific** - rely on the configured `verify_command`, not language-specific hardcoded checks
- **No remote creativity** - if remote access is unavailable or the local target ref is missing, stop and report instead of trying SSH workarounds, HTTPS fallbacks, or git-config changes
