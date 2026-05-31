# Review Task Isolation

Review tasks run in isolated git worktrees that only contain git-tracked files.

## How context flows

1. **Host runner** queries the main database via `store.get()`
2. **Host runner** calls `build_prompt()` which includes:
   - Spec file content (if the implementation task has a `spec` field)
   - Ask context from exactly one source when available:
    - `## Original plan:` when a linked plan exists
    - `## Original request:` fallback when no linked plan exists
    - If neither source exists, both sections are intentionally omitted and reviewers should state `No plan or request provided.`
   - `## verify_command result` when `verify_command` is configured for the project
     - The host runner executes the literal command once per autonomous review iteration in the review worktree.
     - The prompt includes pass/fail status, exit status, and trimmed failing output when non-zero.
     - Hung review verification is bounded to 120 seconds; timeouts are converted into a failed `## verify_command result` section with timeout evidence and any partial output captured so the review still runs.
     - Reviews must keep doing the normal code review in the same iteration; verify failure is additional blocker evidence, not a short-circuit.
   - Implementation diff context for `main...{impl_branch}` (small/full/excerpted depending on size thresholds)
   - Improve-lineage context when applicable
     - This is metadata-only coordination context for prior review/improve iterations.
     - It intentionally excludes prior review prose, improve summaries, and copied blocker text.
     - Reviewers must prove current blockers from current code/diff, not from lineage history.
   - Explicit blocker markers when linked review/plan output exists but cannot be loaded on the current machine
3. **Host runner** passes the complete prompt string to Docker/Claude
4. **Claude** receives all context baked into the prompt

Database lookups happen on the host before the worktree is even created. The worktree isolation doesn't affect prompt building.

Current review context logic lives in `_build_context_from_chain()` and `_build_review_diff_context()` in `src/gza/runner.py`.

## Diff base

The review diffs against the branch's **local fork point**, and this is intentional.

The diff range is `{default_branch}...{impl_branch}` (three-dot). By definition `git diff A...B` is
`git diff $(git merge-base A B) B`, so the review base is the merge-base of the local default branch
and the implementation branch — i.e. exactly what the branch changed since it forked. Trunk commits
that landed *after* the fork are excluded, because they are at or before the merge-base.

The bare `{default_branch}` name (e.g. `master`) resolves to the **local** `refs/heads/<default>`,
not `origin/<default>` — gitrevisions resolves heads before remotes. The `get_diff*` helpers in
`src/gza/git.py` pass the range straight to `git diff`, so no separate merge-base computation is
needed; the three-dot operator already does it.

**Review never reasons about origin or remote freshness.** Keeping local trunk current is a separate
concern — the sync layer's job, at defined boundaries (see
[`specs/distributed-sync-engine.md`](../../specs/distributed-sync-engine.md)) — not review's. By the
time review runs, it simply diffs against whatever local trunk is. Do not add origin fallbacks, a
local-vs-origin "pick latest" heuristic, or path/project filtering to the review base.

## What the worktree contains

The worktree is a git checkout of the implementation branch. It contains:
- All git-tracked files from that branch
- `.gza/gza.db` as a host-created point-in-time snapshot of the live task DB
  (copied via SQLite backup API before provider launch, then chmod `0444`)
- `.gza/learnings.md` copied by the host runner for non-internal tasks

## If Claude tries to query gza directly

If Claude runs `gza` commands or reads `.gza/gza.db` during the review, read-only query commands (`show`, `history`, `next`, `search`, `lineage`, and similar read-only views) use a query-only DB open path so they can inspect the frozen snapshot without trying to register the project or backfill schema artifacts. Query-only mode validates the core `tasks` schema up front and fails with a controlled schema error when required task artifacts are damaged; only a narrow set of optional artifacts (for example tags, comments, run steps, and queue-order extras) degrade behind explicit warnings. Plain default-branch `gza unmerged` is now a narrow exception: it refreshes canonical merge truth and therefore fails with a targeted writable-DB error inside read-only review snapshots. Live-target `gza unmerged --target ...` comparisons remain read-only. Snapshot writes still fail with SQLite read-only errors because the file mode is `0444`, and manual-migration requirements are still surfaced explicitly instead of being auto-run. In task containers, prefer `uv run gza ...` in guidance snippets because `gza` may rely on a project-local shim.

If a review complains about task lookups from inside the worktree, check whether the lookup should exist in the spawn-time snapshot and whether the reviewer expected post-spawn host DB changes to appear (they will not). The prompt should still contain everything needed for the review.
