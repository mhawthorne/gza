# Review Task Isolation

Review tasks run in isolated git worktrees that only contain git-tracked files.

## How context flows

1. **Host runner** queries the main database via `store.get()`
2. **Host runner** calls `build_prompt()` which includes:
   - Spec file content (if the implementation task has a `spec` field)
   - Ask context:
    - `## Review scope:` when the implementation declares a gradeable review scope (structured `Task.review_scope` first, conservative prompt parsing fallback for legacy sliced tasks)
    - `## Original plan context (out of scope except for the review scope):` as read-only boundary/interface context when a scoped review is plan-backed
    - Otherwise exactly one canonical whole-task ask section:
      - `## Original plan:` when a linked plan exists
      - `## Original request:` fallback when no linked plan exists
    - If neither source exists, ask sections are intentionally omitted and reviewers should state `No plan or request provided.`
   - `## verify_command result` when `verify_command` is configured for the project
     - The host runner executes the literal command once per autonomous review iteration in the review worktree.
     - For `cross-project` reviews, the host runner fans out to each affected project root discovered from the review worktree diff and renders an aggregate status/provenance block first, then one section per affected project.
     - The prompt includes pass/fail/unavailable status, exit status, captured-at time, reviewed branch/head provenance, exact working directory, and trimmed failing output when non-zero, even when every affected project is skipped or unknown.
     - Cross-project reviews persist an aggregate review-verify status that reflects the worst affected-project outcome: `failed` when any affected project verify fails, `unavailable` when any affected project cannot run or is skipped because it has no runnable project root/`verify_command`, and `passed` only when every affected project can run and every runnable affected project verify passes.
     - The reviewed head SHA is captured from the detached review worktree immediately before `verify_command` runs, and that provenance is also persisted on the review task so later lifecycle rules can tell whether the verify evidence still matches the current implementation tip.
     - The exact rendered verify section is persisted on the review task, and the full captured stdout/stderr plus parsed phase results are written to a sibling `.gza/logs/<slug>.review-verify.json` artifact for later audit.
     - Hung autonomous verification is bounded by `autonomous_verify_timeout_seconds` (120 seconds by default). When that budget is exceeded, the runner sends SIGTERM, waits `review_verify_timeout_grace_seconds` (5 seconds by default), then escalates to SIGKILL if the process group is still alive. The failed `## verify_command result` section preserves captured timeout evidence and partial stdout/stderr so the review still runs.
     - Gza's own `./bin/tests` opts into richer timeout diagnostics by adding `--durations=25` to the full unit and functional pytest phases, having the unit and functional pytest suite conftests call the shared `register_sigterm_faulthandler()` helper, and having `python -m gza.test_latency --summary` flush its current summary when lifecycle review verification sends SIGTERM.
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

## Scoped reviews

When `## Review scope:` is present, that section is the only gradeable ask. The accompanying original-plan-context section exists only to explain slice boundaries, sibling ownership, and integration contracts. Missing sibling-slice work is not a blocker unless the current diff breaks an explicit contract described in the scoped ask or context.

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
[`specs/features/distributed-sync-engine.md`](../../specs/features/distributed-sync-engine.md)) — not review's. By the
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
