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
   - Implementation diff context for `main...{impl_branch}` (small/full/excerpted depending on size thresholds)
   - Improve-lineage context when applicable
   - Explicit blocker markers when linked review/plan output exists but cannot be loaded on the current machine
3. **Host runner** passes the complete prompt string to Docker/Claude
4. **Claude** receives all context baked into the prompt

Database lookups happen on the host before the worktree is even created. The worktree isolation doesn't affect prompt building.

Current review context logic lives in `_build_context_from_chain()` and `_build_review_diff_context()` in `src/gza/runner.py`.

## What the worktree contains

The worktree is a git checkout of the implementation branch. It contains:
- All git-tracked files from that branch
- `.gza/gza.db` as a host-created point-in-time snapshot of the live task DB
  (copied via SQLite backup API before provider launch, then chmod `0444`)
- `.gza/learnings.md` copied by the host runner for non-internal tasks

## If Claude tries to query gza directly

If Claude runs `gza` commands or reads `.gza/gza.db` during the review, reads will succeed against the frozen snapshot captured at worktree spawn time. Snapshot writes fail with SQLite read-only errors because the file mode is `0444`. In task containers, prefer `uv run gza ...` in guidance snippets because `gza` may rely on a project-local shim.

If a review complains about task lookups from inside the worktree, check whether the lookup should exist in the spawn-time snapshot and whether the reviewer expected post-spawn host DB changes to appear (they will not). The prompt should still contain everything needed for the review.
