# Review Task Isolation

Review tasks run in isolated git worktrees that only contain git-tracked files. This is intentional but has implications for what reviews can access.

## What reviews CAN'T see

Since `.gza/` is gitignored, review tasks don't have access to:

- **Database** (`.gza/gza.db`) - Task history, prompts, status
- **Logs** (`.gza/logs/`) - Execution logs from previous tasks
- **Plans** (`.gza/plans/`) - Plan documents from plan tasks

## Why this is okay

Isolated reviews are closer to real-world PR review:

- A human reviewer on GitHub sees the code diff, not internal task logs
- Focuses on what matters: Is the code good? Does it meet requirements? Are there bugs?
- Avoids bias from knowing implementation history (e.g., "this took 3 retries")

## Two different use cases

1. **Code quality review** - "Is this code good?" → Should be isolated, evaluate code on its own merits. This is what `gza review` does.

2. **Implementation audit** - "Did task X accomplish what was planned?" → Needs context (the plan, task prompt, logs). This is a different operation.

If a review tries to verify "did the implementation match the plan" but runs in an isolated environment, it will fail to find task metadata and report "database is empty" or "cannot find task record."

## Design decision

`gza review` is a **code quality review**, not an implementation audit. It evaluates the code as it exists on the branch, independent of task history.

To verify task completion, use:
- `gza history` - Check task status
- `gza info <task-id>` - See task details and logs
- Direct log inspection in `.gza/logs/`
