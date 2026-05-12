# Advance → Rebase Flow

## How `gza advance` handles merge conflicts

When `gza advance` encounters a completed task whose branch has merge conflicts with the currently checked-out branch (the advance target branch), it follows this flow (see `evaluate_advance_rules` in `src/gza/advance_engine.py`):

1. **Detect conflicts**: advance first resolves the freshest available source ref for the implementation branch, preferring `origin/<branch>` when that remote-tracking ref exists and otherwise falling back to the local branch name. It then calls `git.can_merge(source_ref, target_branch)`. This keeps planning stable across worktrees whose local feature branches may be missing or stale after an operator pushes corrected commits elsewhere. `target_branch` is still the explicit advance target for the lineage: in the default multi-task flow this is `git.current_branch()`, while explicit `gza advance <task-id>` planning uses the lineage's canonical merge target so the result is stable across worktrees. For non-dry explicit merges, advance also requires the active checkout to already be on that resolved target branch; otherwise it refuses execution instead of mutating a different checkout.
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
- Reject provider `exit_code=0` if git still reports `rebase-merge` or `rebase-apply`, then validate the Python files changed by the provider-backed resolution against the pre-resolve `ruff` baseline before accepting success
- On completion, `skip_commit=True` is set (rebase tasks don't need runner commits)
- After completion, the host runner force-pushes the rebased branch (`git push --force-with-lease`)
- On successful completion, the runner also computes and persists `changed_diff` on the rebase task:
  - `0` means the normalized implementation patch before and after the rebase is identical, so prior review evidence may be preserved
  - `1` means the patch changed or equivalence could not be proven, so prior review evidence must be refreshed
  - legacy `NULL` values are treated conservatively as changed

## Docker considerations

Rebase tasks need git identity to create commits during `git rebase --continue`. The Docker container gets `GIT_AUTHOR_NAME`, `GIT_AUTHOR_EMAIL`, `GIT_COMMITTER_NAME`, and `GIT_COMMITTER_EMAIL` env vars injected from the host's git config (see `build_docker_cmd` in `providers/base.py`).

The worktree may have uncommitted changes (e.g., from provider initialization). The `/gza-rebase --auto` skill handles this by stashing changes before rebasing and popping them after.

## Failure handling

If a rebase task fails for an automatically recoverable reason, the standard failed-task recovery engine may create a follow-up rebase attempt. Those recovery retries must keep `same_branch=True` semantics against the original implementation branch so the completed rebase force-pushes back to the implementation branch instead of creating a sibling `*-rebase-branch-*` orphan. Recovery branch resolution must walk past any failed orphan recovery descendants and re-anchor on the original implementation branch (or the oldest recorded rebase branch if the implementation row no longer has one recorded).

Existing orphan recovery branches created before this behavior was fixed are left in place intentionally. Per project policy, branch cleanup is an operator concern rather than an automatic migration; future automatic recoveries simply stop targeting those orphan branches, and advance planning ignores divergent `same_branch=True` fork owners instead of treating them as merge candidates.

## Relationship to `gza rebase` CLI command

`gza rebase` operates entirely within a fresh worktree — it never modifies the main working tree. When invoked without `--background`:

1. Any stale worktree for the task's branch is force-removed.
2. A fresh worktree is created at `config.worktree_path / task.id`.
3. A mechanical `git rebase` is attempted inside that worktree.
4. If conflicts arise, the rebase is aborted and `invoke_provider_resolve` runs the provider (Claude) inside the same worktree via `/gza-rebase --auto`.
5. Before treating that provider run as success, the host first rejects any still-active `rebase-merge` or `rebase-apply` state, then compares `ruff check --select F401,F821` diagnostics on the provider-touched Python files against the pre-resolve baseline. If unfinished rebase metadata or new undefined-name / unused-import errors appear, the rebase task fails instead of continuing silently.
6. On success, the rebased branch is force-pushed from the worktree.
7. The completed rebase row persists the same `changed_diff` signal used by runner-owned rebase tasks, and review invalidation only happens when that signal is not `False`.
8. The worktree is removed on all exit paths (success, failure, exception) via a `try/finally` block.

## Review invalidation after rebase

`gza advance`, `gza watch`, and `gza show` no longer treat every later completed rebase as invalidating review evidence. They now look at both:

1. whether the rebase completed after the latest completed review
2. whether the rebase task's persisted `changed_diff` signal is not `False`

If the latest completed rebase after the latest review has `changed_diff = 0`, the prior approved review is carried across that rebase. If `changed_diff = 1` or `NULL`, lifecycle behavior stays conservative and requires a fresh review.

Resumed or recovered rebase runs are intentionally fail-closed. This includes direct provider resumes and automatic failed-task recovery descendants such as retry-created rebase children. The runner records those baselines with `recovered=True`, so completion persists `changed_diff = 1` and surfaces a warning instead of claiming the diff was preserved from the original pre-rebase state.

The `--resolve` and `--force` flags are accepted for backward compatibility but are no-ops — conflict resolution is always attempted automatically, and existing worktrees are always force-removed before creating a fresh one.

With `--background`, `gza rebase` creates a rebase task via `_create_rebase_task()` and runs it through the standard runner, which already manages its own worktree lifecycle.

## Auto-resolve guardrails

`/gza-rebase --auto` is allowed to resolve straightforward additive conflicts without operator input, but it must not silently choose deletion when symbol liveness is uncertain. Two guardrails now apply:

1. The skill instructions explicitly treat edit-vs-delete and ambiguous two-sided modifications as stop conditions unless the resolver can preserve all still-referenced symbols confidently.
2. The host-side validation gate rejects any provider result that either leaves git rebase metadata behind or introduces new `F401` or `F821` diagnostics in the Python files changed by the rebase attempt, whether the rebase runs through `invoke_provider_resolve()` or through a standard runner-owned `task_type="rebase"` task.
