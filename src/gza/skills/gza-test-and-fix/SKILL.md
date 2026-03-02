---
name: gza-test-and-fix
description: Run mypy and pytest, fix any errors found in files changed on the current branch, then commit all fixes.
allowed-tools: Read, Edit, Glob, Grep, Bash(uv run mypy:*), Bash(uv run pytest:*), Bash(git:*)
version: 1.0.0
public: false
---

# Test and Fix

Run `uv run mypy` and `uv run pytest`, fix any errors found in files changed on the current branch (compared to `main`), and commit all fixes at the end.

## Process

### Step 1: Get changed files

Run `git diff --name-only main...HEAD` to get the list of files changed on the current branch compared to `main`. Store this list — it will be used to filter errors throughout.

### Step 2: Run mypy and fix errors (max 2 iterations)

Repeat up to **2 times**:

1. Run `uv run mypy` and capture the output.
2. Filter the output to only include errors for files in the changed-files list.
3. If there are no relevant errors, stop the mypy loop.
4. Fix each relevant error by reading the affected file and editing it to resolve the type error.
5. After fixing, increment the iteration counter and continue to the next iteration.

If errors remain in changed files after 2 iterations, note them but continue to the pytest step.

### Step 3: Run pytest and fix failures (max 2 iterations)

Repeat up to **2 times**:

1. Run `uv run pytest` and capture the output.
2. Filter the failures to only those whose test file path (or the source file under test) is in the changed-files list.
   - A failure is relevant if the test file itself is in the changed files list, **or** if the error traceback references a changed source file.
3. If there are no relevant failures, stop the pytest loop.
4. Fix each relevant failure by reading the affected test and/or source files and editing them.
5. After fixing, increment the iteration counter and continue to the next iteration.

If failures remain in changed files after 2 iterations, note them but continue.

### Step 4: Commit all fixes

If any files were modified during Steps 2–3:

1. Run `git diff --name-only` to see what changed.
2. Stage all modified files with `git add <file>` for each file (do not use `git add -A`).
3. Commit with a descriptive message summarising what was fixed, e.g.:

   ```
   Fix mypy and pytest errors

   - Fixed type errors in src/foo/bar.py
   - Fixed failing test in tests/test_bar.py
   ```

If no files were modified, report that no fixes were needed.

## Important notes

- **Only fix errors in changed files** — do not touch files that are not in the `git diff --name-only main...HEAD` output.
- **Maximum 2 fix iterations per tool** — do not loop indefinitely. If errors remain after 2 rounds, report them to the user.
- **One commit at the end** — accumulate all fixes across both mypy and pytest steps before committing. Do not commit after each fix.
- **Do not run `git push`** — leave pushing to the user.
- After each fix, re-run the tool to verify the fix worked before moving to the next iteration.
