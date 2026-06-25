# Internal docs

Architecture, design notes, and engineering practices for working on gza itself.
Linked from `AGENTS.md` so these are reachable in one hop instead of by grep.

**Keep this index current**: when you add a doc to `docs/internal/`, add a line here.

## Design & model

- [task-model-canonical.md](task-model-canonical.md) — `db.Task` as the single canonical task model; lifecycle and merge-state semantics.
- [task-vs-agent-context.md](task-vs-agent-context.md) — what the host (task context) vs the agent sandbox can access; **the agent has no network and no `origin/*` refs**.
- [worktree-lifecycle.md](worktree-lifecycle.md) — how task worktrees are created and cleaned up.
- [color-theme-architecture.md](color-theme-architecture.md) — how themes and colors are resolved and applied.

## Workflows

- [advance-workflow.md](advance-workflow.md) — how `gza advance` progresses unmerged tasks through the lifecycle.
- [advance-rebase-flow.md](advance-rebase-flow.md) — how advance resolves the merge source, reconciles local/origin divergence, and falls back to rebase tasks.
- [review-isolation.md](review-isolation.md) — how review tasks are isolated.
- [learnings.md](learnings.md) — LLM-powered per-task learnings summarization.
- [prompt-sanitization.md](prompt-sanitization.md) — provider-facing prompt sanitization.
- [stats-subcommands.md](stats-subcommands.md) — the `gza stats` command surface.

## Practices & ops

- [practices.md](practices.md) — engineering practices (test guardrails, scope discipline, fixtures, subprocess rules).
- [releasing.md](releasing.md) — the release process.
- [release-notes-generator.md](release-notes-generator.md) — how release notes are produced.
- [profiling.md](profiling.md) — profiling gza with py-spy.
- [skills.md](skills.md) — internal skill authoring/usage notes.
- [timeout-budgeting.md](timeout-budgeting.md) — deterministic code-task timeout scaling and verify-profile rationale.

## Known issues / gotchas

- [docker-worktree-bug.md](docker-worktree-bug.md) — the historical Docker + git-worktree bug and the fixed isolation architecture that replaced the workaround.
