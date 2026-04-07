# Advance → Rebase Flow

## How `gza advance` handles merge conflicts

When `gza advance` encounters a completed task whose branch has merge conflicts with the currently checked-out branch (the advance target branch), it follows this flow (see `_determine_advance_action` in `git_ops.py`):

1. **Detect conflicts**: `git.can_merge(task.branch, target_branch)` returns `False`, where `target_branch` is determined at runtime via `git.current_branch()`
2. **Check for existing rebase children**: Query `store.get_lineage_children(task.id)` for any child tasks with `task_type="rebase"`
3. **Decide action based on rebase child status**:
   - `pending` or `in_progress` → skip (rebase already running, avoid duplicates)
   - `failed` → `needs_discussion` (manual intervention required)
   - `completed` or no rebase child → create a new rebase task (`needs_rebase` action)

## How rebase tasks are created

`_create_rebase_task()` in `_common.py` creates a rebase task with:
- `task_type="rebase"`
- `based_on=<parent_task_id>` (the implementation task)
- `same_branch=True` (operates on the same branch as the implementation)
- `skip_learnings=True`
- Prompt instructing the agent to use `/gza-rebase --auto`

## How rebase tasks run

Rebase tasks go through the **code task path** in `runner.py:_run_inner` (not the non-code task path). This means they:
- Resolve the branch via `_resolve_code_task_branch_name` (follows `based_on` chain to find parent's branch)
- Set up a worktree on that branch
- Run the provider (Claude) which invokes `/gza-rebase --auto`
- On completion, `skip_commit=True` is set (rebase tasks don't need runner commits)
- After completion, the host runner force-pushes the rebased branch (`git push --force-with-lease`)

## Docker considerations

Rebase tasks need git identity to create commits during `git rebase --continue`. The Docker container gets `GIT_AUTHOR_NAME`, `GIT_AUTHOR_EMAIL`, `GIT_COMMITTER_NAME`, and `GIT_COMMITTER_EMAIL` env vars injected from the host's git config (see `build_docker_cmd` in `providers/base.py`).

The worktree may have uncommitted changes (e.g., from provider initialization). The `/gza-rebase --auto` skill handles this by stashing changes before rebasing and popping them after.

## Failure handling

If a rebase task fails, `gza advance` reports `needs_discussion` — manual intervention is required. Rebases are not automatically retried. The user can manually retry with `gza retry <id>` or rebase interactively with `gza rebase <id>`.

## Relationship to `gza rebase` CLI command

`gza rebase` operates entirely within a fresh worktree — it never modifies the main working tree. When invoked without `--background`:

1. Any stale worktree for the task's branch is force-removed.
2. A fresh worktree is created at `config.worktree_path / task.id`.
3. A mechanical `git rebase` is attempted inside that worktree.
4. If conflicts arise, the rebase is aborted and `invoke_provider_resolve` runs the provider (Claude) inside the same worktree via `/gza-rebase --auto`.
5. On success, the rebased branch is force-pushed from the worktree.
6. The worktree is removed on all exit paths (success, failure, exception) via a `try/finally` block.

The `--resolve` and `--force` flags are accepted for backward compatibility but are no-ops — conflict resolution is always attempted automatically, and existing worktrees are always force-removed before creating a fresh one.

With `--background`, `gza rebase` creates a rebase task via `_create_rebase_task()` and runs it through the standard runner, which already manages its own worktree lifecycle.
