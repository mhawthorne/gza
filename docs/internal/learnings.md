# LLM-Powered Learnings Summarization

After each completed task (on interval), gza can automatically update `.gza/learnings.md` by running an LLM to consolidate patterns from recent completed tasks.

## How It Works

1. An `internal` task is created in the DB with `skip_learnings=True` (to prevent recursion) and run via the standard runner — same as explore/plan/review tasks (worktree, provider, status transitions).
2. The LLM produces bullet-point learnings from recent task outputs; these replace/merge into `.gza/learnings.md`.
3. On any failure (non-zero exit, exception), gza falls back to the existing regex-based extraction.
4. The `internal` task is kept in the DB for observability (visible via `gza history --type internal`).

## Architecture Notes

Always use the provider system (via `runner.run()` or `get_provider()`) for LLM calls — never hardcode provider-specific CLI commands. This keeps gza provider-agnostic across Claude, Codex, and Gemini.

## skip_learnings Field

The `skip_learnings` field on `db.Task`, when `True`, prevents the task's completion from triggering `maybe_auto_regenerate_learnings`. This is set automatically on `internal` learnings tasks to prevent infinite recursion. It can also be set manually on any task type to suppress learnings updates.
