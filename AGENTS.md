# Theo

A coding AI agent runner for Claude Code.

## Usage

```
theo init [project_dir]     # Generate new theo.yaml with defaults
theo work [project_dir]     # Run the next pending task
theo next [project_dir]     # List upcoming pending tasks
theo history [project_dir]  # List recent completed/failed tasks
theo stats [project_dir]    # Show cost and usage statistics
theo validate [project_dir] # Validate theo.yaml configuration
```

Options for `init`:
- `--force` - Overwrite existing theo.yaml file

Options for `stats`:
- `--last N` - Show last N tasks (default: 5)

## Architecture

Tasks are stored in a SQLite database (`.theo/theo.db`), not in YAML files. The database handles task state, history, and coordination.

## Project Structure

Key modules:
- `src/theo/db.py` - SQLite task storage with `Task` class (uses `prompt` field)
- `src/theo/tasks.py` - YAML task storage with `Task` class (uses `description` field) - LEGACY
- `src/theo/cli.py` - CLI commands
- `src/theo/runner.py` - Executes tasks via Claude Code
- `src/theo/config.py` - Configuration loading

**Important**: There are TWO Task classes:
- `db.Task` (SQLite) - The primary storage, uses `prompt` field
- `tasks.Task` (YAML) - Legacy format for `tasks.yaml` files, uses `description` field

## Running in Docker

Theo tasks run inside a Docker container. The container:
- Mounts the project at `/workspace`
- Has Python 3.11+ but limited pre-installed packages
- Use `uv run` for all commands (e.g., `uv run pytest tests/ -v`)

**Do NOT use** `python -m pytest` or `pip install` directly - always use `uv run`.

**Do NOT modify files outside `/workspace/theo/`** unless explicitly instructed. Other directories under `/workspace/` are sibling projects.

## Renaming/Refactoring Tips

When renaming a field across the codebase:
1. Use search-and-replace across files rather than editing one occurrence at a time
2. Check both `tasks.py` and `db.py` for Task-related changes
3. Update tests in bulk, not one test method at a time

## Important Guidelines

- **Do NOT create summary or documentation files** (e.g., `IMPLEMENTATION_SUMMARY.md`, `CHANGES.md`). Just make the code changes and commit them. If summaries are needed, they will be handled separately.
- **Do NOT create README files** unless explicitly requested.

## Creating Tasks from Conversations

When a conversation identifies work to be done, create a theo task rather than implementing inline:

```bash
# Basic task
uv run theo add "description of what needs to be done"

# With task type (plan, implement, review, explore)
uv run theo add --type plan "explore authentication options and propose approach"
uv run theo add --type implement "add user authentication with JWT"

# Auto-review after implementation
uv run theo add --type implement --review "add dark mode toggle"

# Task chaining - implementation based on a plan
uv run theo add --type implement --based-on 5 "implement the approach from task #5"
```

Tips for good task descriptions:
- Be specific about what needs to change and where
- Reference file paths or components when known
- For multi-step work, create a `--type plan` task first
- Use `--review` flag for significant changes that warrant code review
- Use `theo edit <id>` to update a task's prompt instead of deleting and recreating

## Development

After making changes, run the test suite to verify everything works:

```
# Unit tests (fast, no external dependencies)
uv run pytest tests/ -v

# Integration tests (requires Docker and/or API credentials)
uv run pytest tests_integration/ -v -m integration
```

### Testing Guidelines

- **Write tests for every change** - Each feature, bug fix, or enhancement should include corresponding tests
- Tests go in `tests/` with `test_` prefix (e.g., `tests/test_importer.py`)
- Use pytest fixtures for common setup (see existing tests for patterns)
- Test both success cases and error handling
- Run tests after making changes: `uv run pytest tests/ -v`
- For CLI changes, add tests in `tests/test_cli.py` following the existing class structure (e.g., `TestAddCommandWithChaining`)

## Temporary Files

When creating temporary files (e.g., task import files, test data), write them to the `tmp/` directory in the project root. This directory is gitignored and keeps the project clean.
