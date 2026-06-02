# Rebase with Auto-Resolve

> **Status: Aspirational** - This spec describes a planned feature. The `--resolve` flag for `gza rebase` has not been implemented yet.

## Overview

Add a `--resolve` flag to `gza rebase` that automatically resolves merge conflicts using AI, making rebasing a non-interactive, one-shot operation.

## Motivation

The existing `gza-rebase` skill handles conflict resolution interactively, requiring user approval at each step. This is useful for complex conflicts but overkill for straightforward ones. Most conflicts in this project are additive (both sides adding new code) and can be resolved automatically.

Currently, resolving conflicts requires:
1. Checkout the branch
2. Run the `/gza-rebase` skill interactively
3. Approve each resolution
4. Force-push with lease
5. Checkout main
6. Merge

With `--resolve`, this becomes:
```bash
gza rebase <task_id> --resolve
gza merge <task_id>
```

## CLI Interface

```bash
gza rebase <task_id> --resolve [--onto <branch>] [--remote]
```

Behavior:
1. Attempt rebase onto target (default: local `main`)
2. On conflicts: invoke Claude with `/gza-rebase --auto` to resolve
3. Loop until rebase completes or Claude aborts (can't resolve confidently)
4. Force-push with lease
5. Run tests
6. Checkout main
7. Report result

## Implementation

### 1. Update gza-rebase skill

Add non-interactive `--auto` mode to `.claude/skills/gza-rebase/SKILL.md`:

```markdown
## Non-interactive mode (--auto)

When invoked with `--auto`:
- Always rebase against `main` (local)
- Resolve conflicts using best judgment without asking for confirmation
- If unsure about any resolution, abort immediately with `git rebase --abort`
- Log each resolution decision for transparency
```

Modify the skill process:
- Step 2 (choose rebase target): Skip, use `main`
- Step 4.5 (ask for approval): Skip, resolve directly
- Add confidence check: If conflict is ambiguous, abort rather than guess

### 2. Add --resolve flag to CLI

In `src/gza/cli.py`, update `cmd_rebase`:

```python
def cmd_rebase(args: argparse.Namespace) -> int:
    # ... existing setup ...

    try:
        git.checkout(task.branch)
        git.rebase(rebase_target)
        # ... success path ...
    except GitError as e:
        if not args.resolve:
            # Existing behavior: abort and return error
            git.rebase_abort()
            return 1

        # --resolve: invoke Claude to fix conflicts
        print("Conflicts detected. Invoking Claude to resolve...")
        resolved = invoke_claude_resolve(task.branch, rebase_target)

        if not resolved:
            print("Could not resolve conflicts automatically.")
            git.rebase_abort()
            git.checkout(original_branch)
            return 1

        # Force push the resolved branch
        print(f"Pushing {task.branch}...")
        git.push_force_with_lease(task.branch)

        # Run tests
        print("Running tests...")
        tests_passed = run_tests()

        # Always checkout main at the end
        git.checkout(default_branch)

        if not tests_passed:
            print(f"Tests failed. Branch '{task.branch}' is rebased but needs fixes.")
            return 1

        print(f"Successfully rebased and tested {task.branch}")
        return 0
```

### 3. Implement invoke_claude_resolve

Shell out to Claude CLI with the skill:

```python
def invoke_claude_resolve(branch: str, target: str) -> bool:
    """Invoke Claude to resolve rebase conflicts.

    Returns True if conflicts were resolved, False if Claude aborted.
    """
    cmd = [
        "claude",
        "-p", "/gza-rebase --auto",
        "--allowedTools", "Bash(git add:*)",
        "--allowedTools", "Bash(git rebase --continue:*)",
        "--allowedTools", "Bash(uv run python -m py_compile:*)",
        "--allowedTools", "Edit",
        "--allowedTools", "Read",
        "--allowedTools", "Glob",
        "--allowedTools", "Grep",
    ]

    result = subprocess.run(cmd, capture_output=True, text=True)

    # Check if rebase completed (no longer in rebase state)
    rebase_in_progress = Path(".git/rebase-merge").exists() or Path(".git/rebase-apply").exists()

    return not rebase_in_progress
```

### 4. Add push_force_with_lease to Git class

```python
def push_force_with_lease(self, branch: str, remote: str = "origin") -> None:
    """Force push a branch with lease protection."""
    self._run("push", "--force-with-lease", remote, branch)
```

### 5. Add run_tests helper

```python
def run_tests() -> bool:
    """Run the project's test suite. Returns True if tests pass."""
    result = subprocess.run(
        ["uv", "run", "pytest"],
        capture_output=True,
        text=True,
    )
    return result.returncode == 0
```

### 6. Update argument parser

```python
rebase_parser.add_argument(
    "--resolve",
    action="store_true",
    help="Auto-resolve conflicts using AI (non-interactive)",
)
```

## Failure Modes

| Scenario | Behavior |
|----------|----------|
| Rebase succeeds (no conflicts) | Normal flow, skip Claude invocation |
| Claude resolves all conflicts | Force-push, run tests, checkout main |
| Claude can't resolve a conflict | Abort rebase, checkout original branch, return error |
| Tests fail after resolution | Checkout main, report failure, branch is ready but needs fixes |
| Force-push fails (lease) | Report error, leave branch rebased locally |

## Exit States

After `gza rebase --resolve`:

| Result | Branch State | Working Directory |
|--------|--------------|-------------------|
| Success | Rebased, pushed, tested | On main |
| Conflict unresolvable | Unchanged (aborted) | On original branch |
| Tests failed | Rebased, pushed | On main |
| Push failed | Rebased locally | On task branch |

## Logging

Each resolution should be logged:
```
Resolving conflict in src/gza/cli.py...
  HEAD: Added new argument --foo
  main: Added new argument --bar
  Resolution: Keep both arguments
  ✓ Resolved

Resolving conflict in tests/test_cli.py...
  HEAD: Added test for --foo
  main: Added test for --bar
  Resolution: Keep both tests
  ✓ Resolved

Rebase complete. 2 conflicts resolved.
```

## Future Enhancements

1. **Roll into `gza merge`**: Eventually, `gza merge <task_id>` could auto-rebase if needed, making the two-command workflow a single command.

2. **Confidence threshold**: Add `--force` flag to attempt resolution even when Claude is uncertain.

3. **Dry-run mode**: `--resolve --dry-run` to show what Claude would do without applying changes.
