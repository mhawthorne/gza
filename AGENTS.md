# Gza

A coding AI agent runner for Claude Code.

**Keep AGENTS.md under 100 lines.** Move details to `docs/`. This file is for critical agent behavioral rules only.

## Essential Commands

Always use `uv run gza` (not `gza` directly or `python -m gza`).

```
gza add "prompt"          # Add a task (default type: implement)
gza work [-b]             # Run next pending task (-b for background)
gza next                  # List pending tasks
gza history [--incomplete]  # List recent/failed/unmerged tasks
gza advance               # Progress unmerged tasks through lifecycle
gza lineage <id>          # Show task's ancestor/descendant tree
gza migrate [--status]    # Run manual DB migrations (e.g. v25/v26 task-ID migrations)
```

See `docs/` for detailed documentation:
- [docs/configuration.md](docs/configuration.md) — full command list, all options, config reference
- [docs/skills.md](docs/skills.md) — skill usage and authoring
- [docs/tmux.md](docs/tmux.md) — tmux session attach/detach workflow
- [docs/docker.md](docs/docker.md) — Docker setup, custom Dockerfiles, provider auth

## Critical Rules

**Task management**: When the user mentions "task", "add a task", or asks to track something for later, use `uv run gza add "..."`. NEVER edit `etc/todo.txt` or other files manually.

**Failed tasks**: Do NOT run `uv run gza retry ...` or `uv run gza resume ...` unless the user explicitly asks for that exact action.

**Always run from the project root.** Gza uses the current directory to find `gza.yaml` and `.gza/`.

**Do NOT run git commands.** Gza handles branching, committing, and pushing automatically.

**Run /gza-test-and-fix before completing any task.** This runs mypy and pytest, fixes failures, and commits. Do not mark a task done until it passes.

**Test retry circuit breaker**: If the same test fails 3 times with the same error, stop and report instead of retrying.

## Don'ts

- Do NOT create summary/documentation files (`IMPLEMENTATION_SUMMARY.md`, `CHANGES.md`, etc.)
- Do NOT create README files unless explicitly requested
- Do NOT create one-off utility scripts in the project root
- Do NOT create setup docs in the project root (use `docs/internal/` if needed)
- Do NOT delete git branches unless explicitly asked
- Do NOT use `python -m pytest` or `pip install` — always `uv run`
- Do NOT use the `sqlite3` CLI — use `gza.db.SqliteTaskStore` programmatically
- Do NOT modify files outside `/workspace/gza/` in Docker unless instructed

## Architecture

Tasks are stored in SQLite (`.gza/gza.db`). `db.Task` is the single canonical task model/storage API. Task IDs are project-prefixed variable-length decimal strings (e.g. `gza-1234`). Existing DBs may require manual migrations (`gza migrate`) for v25/v26 transitions. `ManualMigrationRequired` is raised on open when a manual migration is pending.

Key modules: `src/gza/db.py` (storage), `src/gza/cli/` (CLI), `src/gza/runner.py` (execution), `src/gza/config.py` (config).

## Code Principles

**Single code path**: Don't duplicate logic across entry points. All entry points for the same operation must use the same underlying mechanism.

**Write tests for every change.** Tests go in `tests/` with `test_` prefix. Run: `uv run pytest tests/ -v`

**Use Explore subagents** for multi-file research (3+ files) instead of sequential reads.

**Use offset/limit** when reading large files (>1000 lines).

## Skills

Edit skills in `src/gza/skills/`, never in `.claude/skills/` (installed artifacts). Install with `gza skills-install`. New skill = new directory with `SKILL.md`. See [docs/skills.md](docs/skills.md).

## Config Fields (new/notable)

- `project_prefix` — prefix for generated task IDs (1–12 chars, lowercase alphanumeric only — no hyphens, since hyphen is the separator in task IDs). Defaults to `project_name`. Task IDs take the form `{prefix}-{decimal_seq}` (e.g. `gza-1234`). Also affects `task.slug`: `YYYYMMDD-{prefix}-{slug}`. See [docs/configuration.md](docs/configuration.md).
- `theme` — built-in color theme: `default_dark`, `minimal`, `selective_neon`, or `blue`. See [docs/configuration.md](docs/configuration.md).
- `colors` — ad-hoc dict of `field_name: rich_color_string` applied on top of `theme`; overrides always win. Allowed in `gza.local.yaml`.

## Conventions

- Unix line endings (LF) only
- Temporary files go in `tmp/` (gitignored)
- Rename/refactor in bulk (search-and-replace), not one occurrence at a time
