---
name: gza-test-and-fix
description: Run verify_command from gza.yaml, fix any errors found in files changed on the current branch, then commit all fixes.
allowed-tools: Read, Edit, Glob, Grep, Bash(uv run gza:*), Bash(uv run mypy:*), Bash(uv run pytest:*), Bash(git:*)
version: 2.0.0
public: false
---

# Test and Fix

Run the project's `verify_command` (from `gza.yaml`), fix any errors in files changed on the current branch, and commit all fixes.

## Process

### Step 1: Get verify_command

Run `uv run gza config` and extract the `verify_command` value. If it is empty or not set, fall back to `uv run mypy src/ && uv run pytest tests/ -x -q`.

### Step 2: Get changed files

Run `git diff --name-only main...HEAD` to get the list of files changed on the current branch compared to `main`. Store this list — it will be used to scope fixes (but NOT to filter which errors to report).

### Step 3: Run verify_command and fix errors (max 3 iterations)

Repeat up to **3 times**:

1. Run the verify_command and capture the output.
2. If there are no errors or failures, stop.
3. Fix errors by reading the affected files and editing them. **Only edit files that are in the changed-files list** — do not modify files that weren't changed on this branch.
4. After fixing, increment the iteration counter and continue to the next iteration.

**Important**: Run the **full** verify_command each iteration. Do NOT filter or skip test files. All errors and failures are relevant — even if the failing test file itself wasn't changed, the failure may be caused by a change you made to a source file. Fix the root cause in the changed source file, not the test.

If errors remain after 3 iterations, report them to the user.

### Step 4: Commit all fixes

If any files were modified during Step 3:

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

- **Run the full verify_command** — do not skip tests or filter output. All failures matter.
- **Only fix files on the changed-files list** — do not touch files that are not in the `git diff --name-only main...HEAD` output.
- **Maximum 3 fix iterations** — do not loop indefinitely. If errors remain after 3 rounds, report them to the user.
- **One commit at the end** — accumulate all fixes before committing. Do not commit after each fix.
- **Do not run `git push`** — leave pushing to the user.
- After each fix, re-run the verify_command to verify the fix worked before moving to the next iteration.
