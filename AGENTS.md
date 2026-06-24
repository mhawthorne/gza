# Gza

A coding AI agent runner for Claude Code.

**Keep AGENTS.md under 100 lines.** Move details to `docs/`. This file is for critical agent behavioral rules only.

## Essential Commands

Always use `uv run gza` (not `gza` directly or `python -m gza`).

```
gza add "prompt"          # Add a task (default type: implement)
gza work [-b]             # Run next pending task (-b for background)
gza next                  # List pending tasks
gza history               # List recent completed/failed tasks
gza advance               # Progress unmerged tasks through lifecycle
gza lineage <id>          # Show task's ancestor/descendant tree
gza migrate [--status]    # Run manual DB migrations (e.g. v25/v26 task-ID migrations)
```

See `docs/` for detailed documentation:
- [docs/configuration.md](docs/configuration.md) — full command list, all options, config reference
- [docs/skills.md](docs/skills.md) — skill usage and authoring
- [docs/docker.md](docs/docker.md) — Docker setup, custom Dockerfiles, provider auth
- [docs/internal/](docs/internal/README.md) — internal architecture, design notes & practices (index)
## Critical Rules
**Task management**: When the user mentions "task", "add a task", or asks to track something for later, use `uv run gza add "..."`. NEVER edit `etc/todo.txt` or other files manually.

**Default to filing a gza task**: When the user asks for a substantive code change (a feature, fix, or refactor), file it with `uv run gza add` rather than editing inline — do NOT start editing right away. Work in-line only when the user explicitly says to, or for trivial edits. If unsure which way they want it, ask before implementing.

**Config example artifacts**: If you change any discoverable config key/default, regenerate `src/gza/gza.yaml.example` and `src/gza/gza.local.yaml.example` via `uv run gza config example --write` and `uv run gza config example --local --write`, then commit them.

**Failed tasks**: Do NOT run `uv run gza retry ...` or `uv run gza resume ...` unless the user explicitly asks for that exact action.

**Always run from the project root.** Gza uses the current directory to find `gza.yaml` and `.gza/`.

**Do NOT run git commands.** Gza handles branching, committing, and pushing automatically.

**Remote git is host-side only.** Agent context (the worktree, especially in Docker) has no network and no `origin/*` refs. Any operation that reaches origin — fetch, push, force-with-lease, rebase-onto-origin — must run in task context, never in an agent or `rebase` task. See [docs/internal/task-vs-agent-context.md](docs/internal/task-vs-agent-context.md).

**Behavior changes are spec-governed.** Before changing lifecycle/recovery/watch/merge behavior, check `specs/behavior/` — it's the contract; a code/spec mismatch is a bug or spec gap, so land any needed spec edits with the code and keep `gza-behavior-check` green.

**Run /gza-test-and-fix before completing any task.** This runs mypy and pytest, fixes failures, and commits. Do not mark a task done until it passes.

**Test retry circuit breaker**: If the same test fails 3 times with the same error, stop and report instead of retrying.
## Pytest hangs
- If `uv run pytest tests/` produces no new output for about 2 minutes, kill it and bisect by file or class. Do NOT wait it out; CPU usage is a poor liveness signal because an infinite loop also pegs a core.
- When a test drives an iterate-style loop with the worker side mocked (for example `_run_foreground` patched to `MagicMock`), the mock must also mark the spawned task complete or the loop spins forever. See `test_iterate_failed_improve_non_attention_skip_does_not_emit_needs_attention` in `tests/cli/test_execution.py` for the canonical fix shape.

## Don'ts

- Do NOT create summary/documentation files (`IMPLEMENTATION_SUMMARY.md`, `CHANGES.md`, etc.)
- Do NOT create README files unless explicitly requested
- Do NOT create one-off utility scripts in the project root
- Do NOT create setup docs in the project root (use `docs/internal/` if needed)
- Do NOT delete git branches unless explicitly asked
- Do NOT use `python -m pytest` or `pip install` — always `uv run`
- Do NOT use the `sqlite3` CLI — use `gza.db.SqliteTaskStore` programmatically
- Do NOT modify files outside `/workspace/gza/` in Docker unless instructed
- Do NOT remove or weaken the per-test CPU latency bar, 30s hang guard, fail-fast flags, or other load-bearing guardrails to make a failing test pass — see [docs/internal/practices.md](docs/internal/practices.md)

## Architecture

Tasks are stored in SQLite (`.gza/gza.db`). `db.Task` is the single canonical task model/storage API. Task IDs are project-prefixed variable-length decimal strings (e.g. `gza-1234`). Existing DBs may require manual migrations (`gza migrate`) for v25/v26 transitions. `ManualMigrationRequired` is raised on open when a manual migration is pending.

Key modules: `src/gza/db.py` (storage), `src/gza/cli/` (CLI), `src/gza/runner.py` (execution), `src/gza/config.py` (config).

## Code Principles

**Single code path**: Don't duplicate logic across entry points. All entry points for the same operation must use the same underlying mechanism.

**Tests scale with risk.** Write tests for behavior changes in project code. Do not add unit tests for repo scripts, generated/internal metadata, one-off config wording, prompt-only edits, or documentation-only changes unless fixing a concrete regression or guarding a stable user-facing contract. Validate scripts and config syntax ad hoc instead.

**No config-value pinning.** Tests that read a config file and assert a literal value (env var, flag default, workflow setting, version string) have near-zero defect-prevention value — when the config legitimately changes, the test must change in lockstep, with no actual bug ever caught. Don't write them.

❌ `assert 'PYTEST_XDIST_WORKERS: "auto"' in workflow_text`
❌ `assert "timeout = 30" in pyproject_text`

If you need to guard the *behavior* the config enables ("unit tests have a timeout"), assert the behavior — run a slow test, observe the timeout. Not the string.

**Use Explore subagents** for multi-file research (3+ files) instead of sequential reads.

**Use offset/limit** when reading large files (>1000 lines).

## Skills

Edit skills in `src/gza/skills/`, never in `.claude/skills/` (installed artifacts). Install with `gza skills-install`. New skill = new directory with `SKILL.md`. See [docs/skills.md](docs/skills.md).

**Reviewers**: do NOT flag drift between `.claude/skills/` and `src/gza/skills/` as a blocker. The installed copy is gitignored, so no commit can change it — see [docs/internal/practices.md](docs/internal/practices.md).

## Config Fields (new/notable)

- `project_prefix` — prefix for generated task IDs (1–12 chars, lowercase alphanumeric only — no hyphens, since hyphen is the separator in task IDs). Defaults to `project_name`. Task IDs take the form `{prefix}-{decimal_seq}` (e.g. `gza-1234`). Also affects `task.slug`: `YYYYMMDD-{prefix}-{slug}`. Full config reference (incl. `theme`/`colors`): [docs/configuration.md](docs/configuration.md).

## Conventions

- Unix line endings (LF) only
- Temporary files go in `tmp/` (gitignored)
- Rename/refactor in bulk (search-and-replace), not one occurrence at a time
