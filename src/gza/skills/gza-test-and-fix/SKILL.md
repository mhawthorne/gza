---
name: gza-test-and-fix
description: Run verify_command from gza.yaml, fix any errors found, then commit all fixes.
allowed-tools: Read, Edit, Glob, Grep, Bash(uv run:*), Bash(git:*)
version: 3.0.0
public: true
---

# Test and Fix

Run the project's `verify_command` (from `gza.yaml`), fix any errors, and commit all fixes.

## Process

### Step 1: Get verify_command

Run `uv run gza config` and extract the `verify_command` value. If `gza config` itself fails (e.g. due to a config-loading bug), fall back to reading `verify_command` directly from `gza.yaml`. If it is empty or not set, stop and tell the user to set `verify_command` in `gza.yaml`.

### Step 2: Run verify_command and fix errors (max 3 iterations)

Repeat up to **3 times**:

1. Run the verify_command and capture the output.
2. If there are no errors or failures, stop.
3. Fix errors by reading the affected files and editing them. Fix anything that is broken, regardless of whether it was changed on the current branch — the goal is a green verify.
4. After fixing, increment the iteration counter and continue to the next iteration.

**Important**: Run the **full** verify_command each iteration. Do NOT filter or skip test files. All errors and failures are relevant. Fix the root cause in the source, not the test.

If errors remain after 3 iterations, report them to the user.

### Step 3: Commit all fixes

If any files were modified during Step 2:

1. Run `git status` and `git diff --name-only` to see what changed.
2. Stage the modified files with `git add <file>` for each file (do not use `git add -A`).
3. If there are uncommitted changes already on the current branch, include your fixes in a single new commit on top (do NOT amend someone else's work). If the working tree was clean before you started (no prior uncommitted work), your fixes form a fresh new commit.
4. Commit with a descriptive message summarising what was fixed, e.g.:

   ```
   Fix mypy and pytest errors

   - Fixed type errors in src/foo/bar.py
   - Fixed failing test in tests/test_bar.py
   ```

If no files were modified, report that no fixes were needed.

## Important notes

- **Run the full verify_command** — do not skip tests or filter output. All failures matter.
- **Fix anything broken** — scope is not limited to files changed on the current branch. If verify fails, fix it.
- **Maximum 3 fix iterations** — do not loop indefinitely. If errors remain after 3 rounds, report them to the user.
- **One commit at the end** — accumulate all fixes before committing. Do not commit after each fix. Always create a NEW commit for your fixes; never amend existing commits.
- **Do not run `git push`** — leave pushing to the user.
- After each fix, re-run the verify_command to verify the fix worked before moving to the next iteration.
