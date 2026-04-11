# Simple Task (No Plan, No Review)

A straightforward workflow for quick fixes or small features.

## Add a task

```bash
$ gza add "Fix the login button not responding on mobile devices"
Created task gza-1: 20260108-fix-the-login-button
```

Or open your editor to write a longer prompt:

```bash
$ gza add
# Opens $EDITOR
```

## View pending tasks

```bash
$ gza next
Pending tasks:

  gza-1 20260108-fix-the-login-button
     Fix the login button not responding on mobile devices
```

## Run the task

```bash
$ gza work gza-1
=== Task: Fix the login button... ===
    ID: gza-1 20260108-fix-the-login-button
...
=== Done ===
Stats: Runtime: 3m 42s | Turns: 12 | Cost: $0.08
Branch: feature/fix-the-login-button
```

> **Tip:** If you don't provide a task ID, `gza work` runs the next pending task.

## View the execution log

```bash
$ gza log gza-1

Task gza-1: 20260108-fix-the-login-button
Status: completed
Duration: 3m 42s
Turns: 12
Cost: $0.08

Summary:
  Fixed mobile touch event handling on login button.
  Updated CSS for better tap target size.
  Added test for mobile viewport.
```

For the full conversation:

```bash
$ gza log gza-1 --steps
```

## Check unmerged work

```bash
$ gza unmerged
Unmerged branches:

  gza-1 20260108-fix-the-login-button
     Branch: feature/fix-the-login-button
     Commits: 2 ahead of main
     Files changed: 3
```

## Merge the work

You have two options for getting your changes into main:

### Option A: Merge directly

For quick fixes or solo projects, merge directly:

```bash
$ gza merge gza-1
Merging task gza-1: 20260108-fix-the-login-button
  Branch: feature/fix-the-login-button → main

Merged 2 commits into main.
```

To squash commits into a single commit:

```bash
$ gza merge gza-1 --squash
```

### Option B: Create a PR

For team projects or when you want code review, create a PR instead:

```bash
$ gza pr gza-1
Creating PR for task gza-1: 20260108-fix-the-login-button

PR created: https://github.com/myorg/myapp/pull/142
  Title: Fix the login button not responding on mobile devices
  Branch: feature/fix-the-login-button → main
```

To create a draft PR with a custom title:

```bash
$ gza pr gza-1 --draft --title "fix: mobile login button touch handling"
```

Then merge via GitHub's UI when ready.
