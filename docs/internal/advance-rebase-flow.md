# Advance → Rebase Flow

## How `gza advance` handles merge conflicts

When `gza advance` encounters a completed task whose branch has merge conflicts with the currently checked-out branch (the advance target branch), it follows this flow (see `evaluate_advance_rules` in `src/gza/advance_engine.py`):

1. **Detect conflicts**: advance first resolves lifecycle merge proof from local refs only. If the implementation branch exists locally, that branch is the merge source for mergeability, diff/spec gates, already-merged checks, and merge execution; `origin/<branch>` is not compared or preferred in that lifecycle proof path. Remote-only or divergent `origin/<branch>` state is therefore not merge-source proof for advance/watch. Publication reconcile stays host-side: explicit reconcile/publication helpers may inspect or fetch the remote feature ref only to understand publication state, then publish directly with `git push --force-with-lease` when the local branch is strictly ahead of the fetched remote-tracking ref, when true local/remote divergence is a symmetric gza rewrite of the same patch content, or when the remote-only side is entirely stale gza `WIP: gza task interrupted` savepoints descending from the shared merge-base. That last case covers the interrupted-savepoint-versus-finalized-work divergence directly instead of replaying the dead WIP commit. Merge/rebase correctness remains local-target only: when direct publication is not enough, the mechanical fallback rebases onto the resolved local `target_branch`, then publishes with `git push --force-with-lease`. If that host-side local-target rebase hits a real content conflict, advance parks the lineage in an explicit needs-attention state instead of spawning a sandboxed `rebase` task against any `origin/*` ref, because task sandboxes cannot resolve or fetch remote-tracking refs and worker rebase targets must stay local. Other local-only merge-source warnings still fail closed. The resolved `source_ref` is also carried into merge execution, already-merged detection, and auto-squash commit counting so dry-run and non-dry execution validate and merge the same tip. `target_branch` is still the explicit advance target for the lineage: in the default multi-task flow this is `git.current_branch()`, while explicit `gza advance <task-id>` planning uses the lineage's canonical merge target so the result is stable across worktrees. For non-dry explicit merges, advance also requires the active checkout to already be on that resolved target branch; otherwise it refuses execution instead of mutating a different checkout.
2. **Check for existing rebase children**: Query `store.get_lineage_children(task.id)` for any child tasks with `task_type="rebase"`
3. **Decide action based on rebase child status**:
   - ordinary queue-wide planning keeps `selected_for_merge=false`, so conflicting rows stay on the normal review / improve / merge lane until the execution gate actually selects a merge candidate
   - `pending` or `in_progress` → skip (rebase already running, avoid duplicates)
   - `failed` with no later successful same-branch rebase/recovery, no later approved/cleared review, and no local post-resolution proof (`merge unit merged`, branch tip equals target tip, or branch contains the current target tip) → `needs_discussion` (manual intervention required)
   - `failed` 3 times on the same branch with no intervening successful rebase, review, or completed code change → `needs_discussion` with reason `rebase-failure-circuit-breaker` (stop creating fresh rebase tasks and surface manual intervention)
   - rebase planning against a rebase descendant first resolves the canonical rebase target; if that target merge unit is already `merged`, or the descendant no longer attaches to any merge unit at all, advance skips instead of queueing another rebase against an orphan branch
   - `completed`, conflicts still remain, and the branch already contains the current target tip → `needs_discussion` with reason `rebase-did-not-unblock-merge` (a fresh rebase is already proved futile)
   - once a merge candidate is selected, no rebase child (or only a stale completed rebase), the branch does not already contain the target tip, and a local merge source is still resolvable → create a new rebase task (`needs_rebase` action)
   - if the selected merge candidate no longer has any resolvable local merge source, park it with `needs_discussion` / `merge-source-needs-manual-resolution` instead of creating or continuing rebase automation from a remote-only/deleted source ref
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
- For non-Docker execution, set up the canonical worktree on that branch before running the
  normal code-task flow
- For Docker-backed runner-owned `task_type="rebase"` execution, skip canonical worktree
  setup entirely and instead create a private rebase checkout with its own real `.git/`
  directory; that private checkout is the provider worktree for `/gza-rebase --auto`
- Reject provider `exit_code=0` if git still reports `rebase-merge` or `rebase-apply` before accepting success
- On completion, `skip_commit=True` is set (rebase tasks don't need runner commits)
- Before recording successful completion, the host runner imports the private checkout tip
  back into the canonical branch with an expected-old-SHA guard, then routes publication
  through the shared post-rebase helper, which verifies the rewritten tip, treats
  already-up-to-date no-op rebases as success when the branch already contains the target
  tip, rejects anomalous non-advancing rebases without that containment proof, fails closed
  on local/remote ref lookup uncertainty, and force-pushes the rebased branch (`git push
  --force-with-lease`)
- On successful completion, the runner also computes and persists `changed_diff` on the rebase task:
  - `0` means the normalized implementation patch before and after the rebase is identical, so prior review evidence may be preserved
  - `1` means the patch changed or equivalence could not be proven, so prior review evidence must be refreshed
  - legacy `NULL` values are treated conservatively as changed

## Docker considerations

Rebase tasks need git identity to create commits during `git rebase --continue`. The Docker
container gets `GIT_AUTHOR_NAME`, `GIT_AUTHOR_EMAIL`, `GIT_COMMITTER_NAME`, and
`GIT_COMMITTER_EMAIL` env vars injected from the host's git config (see `build_docker_cmd`
in `providers/base.py`).

Normal Docker-backed tasks no longer receive an implicit bind mount of the canonical
repository's shared `.git` at its host path. When a code-task worktree uses a `.git` file,
the runner rewrites that file and its gitdir `commondir` temporarily to container-only
`/gza-git/...` paths, mounts the matching metadata there for the provider run, and restores
the host metadata before host-side completion, WIP capture, timeout checkpointing, or other
runner-owned git bookkeeping resumes.

When rebase conflict resolution needs agent-side git, the provider runs in the isolated
private checkout instead. Provider containers also install a lightweight `git` shim before
the real binary in `PATH`; mutating git commands are allowed only from `/workspace` (or
with `-C /workspace`). From a non-workspace cwd, `--work-tree /workspace` alone is
rejected; the command must either use `-C /workspace` or provide the prepared
`--git-dir`/`--work-tree` pair so git cannot rediscover mounted metadata from `PWD`.
Explicit or env-based gitdir/worktree targets must match the prepared task metadata, so
accidental commands from provider home/config directories fail closed. The
worktree there may still have uncommitted changes (for example, from provider
initialization). The `/gza-rebase --auto` skill handles this by pinning all git commands to
`GZA_WORKTREE_ROOT`, stashing changes before rebasing, restoring them after the rebase, and
only then running the final project `verify_command`.

As a backstop, Docker-backed provider runs check the configured canonical checkout after
the provider exits. If it moved off the expected default branch and has no tracked changes,
gza checks the expected branch back out and logs a `canonical-main-checkout-hijacked` ops
event. If tracked changes are present, gza leaves the checkout untouched and surfaces the
same reason for operator attention.

## Failure handling

If a rebase task fails and gza creates a follow-up retry attempt, that retry must keep `same_branch=True` semantics against the original implementation branch so the completed rebase force-pushes back to the implementation branch instead of creating a sibling `*-rebase-branch-*` orphan. Recovery branch resolution must walk past any failed orphan recovery descendants and re-anchor on the original implementation branch (or the oldest recorded rebase branch if the implementation row no longer has one recorded).

If a rebase already published successfully, or the runner can still publish the branch after a pre-PR `gh` lookup/availability failure, the task completes and surfaces a non-fatal publication note; the missing PR does not block lifecycle progress. Only branch-publish rejection during that publication step records a runner failure, now `BRANCH_UNPUSHABLE`. From there the lifecycle path is shared and host-side: recovery/advance route the failed row through the §4 reconcile gate, republish the branch once it becomes pushable, retry PR creation if needed, and then continue through the ordinary §8 merge gate with no special `PR_REQUIRED` retry lane. Legacy `PR_REQUIRED` rows are lazily reclassified to `BRANCH_UNPUSHABLE` the first time recovery planning touches them; branchless legacy/corrupted rows still fail closed into needs-attention because there is no branch to reconcile.

Existing orphan recovery branches created before this behavior was fixed are left in place intentionally. Per project policy, branch cleanup is an operator concern rather than an automatic migration; future automatic recoveries simply stop targeting those orphan branches, and advance planning ignores divergent `same_branch=True` fork owners instead of treating them as merge candidates.

## Relationship to `gza rebase` CLI command

`gza rebase` operates entirely within dedicated temporary checkouts — it never modifies the
main working tree. Bare `gza rebase <task-id>` now only creates the pending child task. When invoked with `--run`:

1. Any stale worktree for the task's branch is force-removed.
2. A fresh worktree is created at `config.worktree_path / task.id`.
3. A mechanical `git rebase` is attempted inside that worktree.
4. If conflicts arise, the rebase is aborted and `invoke_provider_resolve` creates a private
   rebase checkout with its own `.git/` directory, then runs the provider (Claude) there via
   `/gza-rebase --auto`.
5. Before treating that provider run as success, the host rejects any still-active `rebase-merge` or `rebase-apply` state. The agent-side `/gza-rebase --auto` skill is responsible for reading and running the configured project `verify_command` only after the rebase is complete and after any stashed changes have been restored, before declaring success.
6. On success, the host imports the rewritten private-checkout tip back into the canonical
   branch with an expected-old-SHA guard, then publishes through the shared post-rebase
   helper, which verifies the rewritten tip, treats already-up-to-date no-op rebases as
   success when the branch already contains the target tip, rejects anomalous non-advancing
   rebases without that containment proof, fails closed on local/remote ref lookup
   uncertainty, and force-pushes host-side.
7. The completed rebase row persists the same `changed_diff` signal used by runner-owned rebase tasks, and review invalidation only happens when that signal is not `False`.
8. After rebase publication succeeds and the task is ready to be recorded as completed, the host reconciles the parent implementation merge unit through the shared task-scoped sync path using the canonical local target branch as the merge-proof ref. Publication mode can still differ (`--remote` may publish to `origin` host-side before completion is recorded), but completion-time merge proof does not switch to any `origin/*` ref. This lets empty-net-diff, squash-merged, or cherry-picked rebases flip the implementation back to authoritative `merged` state before the next `advance`, `watch`, or `iterate` pass reads the lineage.
9. The canonical temporary worktree and the private rebase checkout are both removed on all
   exit paths (success, failure, exception) via cleanup in the host flow.

## Review invalidation after rebase

`gza advance`, `gza watch`, and `gza show` no longer treat every later completed rebase as invalidating review evidence. They now look at both:

1. whether the rebase completed after the latest completed review
2. whether the rebase task's persisted `changed_diff` signal is not `False`

If the latest completed rebase after the latest review has `changed_diff = 0`, the prior approved review is carried across that rebase. If `changed_diff = 1` or `NULL`, lifecycle behavior stays conservative and requires a fresh review.

That refresh review is resolution-scoped, not a whole-task refresh. The completed rebase now persists pre/post rebase provenance, and lifecycle treats that block as authoritative even if a later stale whole-row task update re-saves inherited review text. If older rows lose that block anyway, a writable maintenance/lifecycle path re-derives it from local reflogs and surviving refs before query-only surfaces such as `gza show` depend on it, then lifecycle validates or repairs the dependent resolution review from that persisted state. Runner review prompts then reconstruct a focused `git range-diff` between the pre-rebase series and the rebased series when possible. If that focused delta cannot be reconstructed from the persisted refs, the prompt fails closed with explicit `resolution delta unavailable` guidance and still suppresses the ordinary whole-implementation diff so reviewers do not accidentally grade the wrong surface.

For legacy verify-only `CHANGES_REQUESTED` reviews, the preserved-rebase path is compatibility bookkeeping only. It does not make the rewritten head mergeable by itself, does not refresh any retired review-coupled clearance state, and does not replace the normal two-gate requirement to have current lifecycle verify evidence plus a merge-permitting current review for the rewritten head.

Resumed or recovered rebase runs are intentionally fail-closed. This includes direct provider resumes and automatic failed-task recovery descendants such as retry-created rebase children. The runner records those baselines with `recovered=True`, so completion persists `changed_diff = 1` and surfaces a warning instead of claiming the diff was preserved from the original pre-rebase state.

The `--resolve` and `--force` flags are accepted for backward compatibility but are no-ops — conflict resolution is always attempted automatically, and existing worktrees are always force-removed before creating a fresh one.

With `--background`, `gza rebase` creates a rebase task via `_create_rebase_task()` and runs it through the standard runner, which already manages its own worktree lifecycle.

## Auto-resolve guardrails

`/gza-rebase --auto` is allowed to resolve straightforward additive conflicts without operator input, but it must not silently choose deletion when symbol liveness is uncertain. Two guardrails now apply:

1. The skill instructions explicitly treat edit-vs-delete and ambiguous two-sided modifications as stop conditions unless the resolver can preserve all still-referenced symbols confidently.
2. The host rejects any provider result that leaves git rebase metadata behind, whether the rebase runs through `invoke_provider_resolve()` or through a standard runner-owned `task_type="rebase"` task. Project verification is agent-side: `/gza-rebase --auto` must run the configured `verify_command` on the final checkout, after any stash restoration, before it reports success.
