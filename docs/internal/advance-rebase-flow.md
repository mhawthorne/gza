# Advance → Rebase Flow

## How `gza advance` handles merge conflicts

When `gza advance` encounters a completed task whose branch has merge conflicts with the currently checked-out branch (the advance target branch), it follows this flow (see `evaluate_advance_rules` in `src/gza/advance_engine.py`):

1. **Detect conflicts**: advance first resolves the freshest available source ref for the implementation branch by comparing the local branch and `origin/<branch>` when both exist. It prefers `origin/<branch>` when the refs are equal or the remote-tracking ref is ahead, prefers the local branch when it is strictly ahead, and classifies true local/remote divergence separately from other merge-source warnings. At execution time, divergence now triggers a direct reconciliation ladder instead of immediate manual attention: the helper may publish directly with `git push --force-with-lease` when the local branch is strictly ahead of the fetched remote-tracking ref, when true local/remote divergence is a symmetric gza rewrite of the same patch content, or when the remote-only side is entirely stale gza `WIP: gza task interrupted` savepoints descending from the shared merge-base. That last case covers the interrupted-savepoint-versus-finalized-work divergence directly instead of replaying the dead WIP commit. When `origin/<branch>` is already ahead or the divergent remote commits are genuinely distinct, advance fetches, mechanically rebases onto `origin/<branch>`, then publishes with `git push --force-with-lease`. If that host-side mechanical rebase hits a real content conflict, advance now parks the lineage in an explicit needs-attention state instead of spawning a sandboxed `rebase` task against `origin/<branch>`, because task sandboxes cannot resolve or fetch remote-tracking refs. Other merge-source warnings still fail closed. The resolved `source_ref` is also carried into merge execution, already-merged detection, and auto-squash commit counting so dry-run and non-dry execution validate and merge the same tip. `target_branch` is still the explicit advance target for the lineage: in the default multi-task flow this is `git.current_branch()`, while explicit `gza advance <task-id>` planning uses the lineage's canonical merge target so the result is stable across worktrees. For non-dry explicit merges, advance also requires the active checkout to already be on that resolved target branch; otherwise it refuses execution instead of mutating a different checkout.
2. **Check for existing rebase children**: Query `store.get_lineage_children(task.id)` for any child tasks with `task_type="rebase"`
3. **Decide action based on rebase child status**:
   - `pending` or `in_progress` → skip (rebase already running, avoid duplicates)
   - `failed` with no later successful same-branch rebase/recovery, no later approved/cleared review, and no local post-resolution proof (`merge unit merged`, branch tip equals target tip, or branch contains the current target tip) → `needs_discussion` (manual intervention required)
   - `failed` 3 times on the same branch with no intervening successful rebase, review, or completed code change → `needs_discussion` with reason `rebase-failure-circuit-breaker` (stop creating fresh rebase tasks and surface manual intervention)
   - rebase planning against a rebase descendant first resolves the canonical rebase target; if that target merge unit is already `merged`, or the descendant no longer attaches to any merge unit at all, advance skips instead of queueing another rebase against an orphan branch
   - `completed`, conflicts still remain, and the branch already contains the current target tip → `needs_discussion` with reason `rebase-did-not-unblock-merge` (a fresh rebase is already proved futile)
   - no rebase child, or only a stale completed rebase, and the branch does not already contain the target tip → create a new rebase task (`needs_rebase` action)
   - no rebase child but the branch already contains the target tip and the lineage is still incomplete → `needs_discussion` (surface the incomplete lineage instead of spawning a guaranteed no-op rebase)
   - mergeable branches continue through the normal review/merge rules even if the branch is behind target or earlier local verify ran slowly

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
- Reject provider `exit_code=0` if git still reports `rebase-merge` or `rebase-apply` before accepting success
- On completion, `skip_commit=True` is set (rebase tasks don't need runner commits)
- Before recording successful completion, the host runner routes rebase publication through the shared post-rebase helper, which verifies the rewritten tip, treats already-up-to-date no-op rebases as success when the branch already contains the target tip, rejects anomalous non-advancing rebases without that containment proof, fails closed on local/remote ref lookup uncertainty, and force-pushes the rebased branch (`git push --force-with-lease`)
- On successful completion, the runner also computes and persists `changed_diff` on the rebase task:
  - `0` means the normalized implementation patch before and after the rebase is identical, so prior review evidence may be preserved
  - `1` means the patch changed or equivalence could not be proven, so prior review evidence must be refreshed
  - legacy `NULL` values are treated conservatively as changed

## Docker considerations

Rebase tasks need git identity to create commits during `git rebase --continue`. The Docker container gets `GIT_AUTHOR_NAME`, `GIT_AUTHOR_EMAIL`, `GIT_COMMITTER_NAME`, and `GIT_COMMITTER_EMAIL` env vars injected from the host's git config (see `build_docker_cmd` in `providers/base.py`).

The worktree may have uncommitted changes (e.g., from provider initialization). The `/gza-rebase --auto` skill handles this by stashing changes before rebasing, restoring them with `git stash pop` after the rebase, and only then running the final project `verify_command`.

## Failure handling

If a rebase task fails and gza creates a follow-up retry attempt, that retry must keep `same_branch=True` semantics against the original implementation branch so the completed rebase force-pushes back to the implementation branch instead of creating a sibling `*-rebase-branch-*` orphan. Recovery branch resolution must walk past any failed orphan recovery descendants and re-anchor on the original implementation branch (or the oldest recorded rebase branch if the implementation row no longer has one recorded).

If a rebase already published successfully and only PR creation/reuse failed (`PR_REQUIRED`), the retry treats that run as a PR-only completion retry: it re-verifies or refresh-publishes the current rebased tip idempotently, but it does not replay rebase-only review invalidation or diff-baseline logic against the already-rebased head.

Existing orphan recovery branches created before this behavior was fixed are left in place intentionally. Per project policy, branch cleanup is an operator concern rather than an automatic migration; future automatic recoveries simply stop targeting those orphan branches, and advance planning ignores divergent `same_branch=True` fork owners instead of treating them as merge candidates.

## Relationship to `gza rebase` CLI command

`gza rebase` operates entirely within a fresh worktree — it never modifies the main working tree. When invoked without `--background`:

1. Any stale worktree for the task's branch is force-removed.
2. A fresh worktree is created at `config.worktree_path / task.id`.
3. A mechanical `git rebase` is attempted inside that worktree.
4. If conflicts arise, the rebase is aborted and `invoke_provider_resolve` runs the provider (Claude) inside the same worktree via `/gza-rebase --auto`.
5. Before treating that provider run as success, the host rejects any still-active `rebase-merge` or `rebase-apply` state. The agent-side `/gza-rebase --auto` skill is responsible for reading and running the configured project `verify_command` only after the rebase is complete and after any stashed changes have been restored, before declaring success.
6. On success, the rebased branch is published through the shared post-rebase helper, which verifies the rewritten tip, treats already-up-to-date no-op rebases as success when the branch already contains the target tip, rejects anomalous non-advancing rebases without that containment proof, fails closed on local/remote ref lookup uncertainty, and force-pushes from the worktree.
7. The completed rebase row persists the same `changed_diff` signal used by runner-owned rebase tasks, and review invalidation only happens when that signal is not `False`.
8. After rebase publication succeeds and the task is ready to be recorded as completed, the host reconciles the parent implementation merge unit through the shared task-scoped sync path using the same merge-proof ref as the rebase itself (`origin/<target>` for `--remote`, otherwise the local target branch). Remote rebase completion does not accept separate local-target reachability as proof, even though it still persists the merge unit against its canonical target branch. This lets empty-net-diff, squash-merged, or cherry-picked rebases flip the implementation back to authoritative `merged` state before the next `advance`, `watch`, or `iterate` pass reads the lineage.
9. The worktree is removed on all exit paths (success, failure, exception) via a `try/finally` block.

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
2. The host rejects any provider result that leaves git rebase metadata behind, whether the rebase runs through `invoke_provider_resolve()` or through a standard runner-owned `task_type="rebase"` task. Project verification is agent-side: `/gza-rebase --auto` must run the configured `verify_command` on the final checkout, after any stash restoration, before it reports success.
