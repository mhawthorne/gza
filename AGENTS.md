# Gza

A coding AI agent runner for Claude Code.

## Quick Reference

**Task Management - CRITICAL**: When the user mentions "task", "gza task", "add a task", "create a task", or asks to track/remember something for later, ALWAYS use `uv run gza add "..."`. NEVER edit `etc/todo.txt` or any other files manually for task tracking.

## Usage

```
gza init [--project DIR]     # Generate new gza.yaml with defaults
gza work [--project DIR]     # Run the next pending task
gza next [--project DIR]     # List upcoming pending tasks
gza history [--project DIR]  # List recent completed/failed tasks
gza stats [--project DIR]    # Show cost and usage statistics
gza validate [--project DIR] # Validate gza.yaml configuration
```

The `--project` (or `-C`) option specifies the target project directory and can be used with any command. If not specified, the current directory is used.

Options for `init`:
- `--force` - Overwrite existing gza.yaml file

Options for `stats`:
- `--last N` - Show last N tasks (default: 5)

## Architecture

Tasks are stored in a SQLite database (`.gza/gza.db`), not in YAML files. The database handles task state, history, and coordination.

## Project Structure

Key modules:
- `src/gza/db.py` - SQLite task storage with `Task` class (uses `prompt` field)
- `src/gza/tasks.py` - YAML task storage with `Task` class (uses `description` field) - LEGACY
- `src/gza/cli.py` - CLI commands
- `src/gza/runner.py` - Executes tasks via Claude Code
- `src/gza/config.py` - Configuration loading

**Important**: There are TWO Task classes:
- `db.Task` (SQLite) - The primary storage, uses `prompt` field
- `tasks.Task` (YAML) - Legacy format for `tasks.yaml` files, uses `description` field

## Running in Docker

Gza tasks run inside a Docker container. The container:
- Mounts the project at `/workspace`
- Has Python 3.11+ but limited pre-installed packages
- Use `uv run` for all commands (e.g., `uv run pytest tests/ -v`)

**Do NOT use** `python -m pytest` or `pip install` directly - always use `uv run`.

**Do NOT modify files outside `/workspace/gza/`** unless explicitly instructed. Other directories under `/workspace/` are sibling projects.

## Renaming/Refactoring Tips

When renaming a field across the codebase:
1. Use search-and-replace across files rather than editing one occurrence at a time
2. Check both `tasks.py` and `db.py` for Task-related changes
3. Update tests in bulk, not one test method at a time

## Important Guidelines

- **Do NOT create summary or documentation files** (e.g., `IMPLEMENTATION_SUMMARY.md`, `CHANGES.md`, `*_SETUP.md`). Just make the code changes and commit them. If summaries are needed, they will be handled separately.
- **Do NOT create README files** unless explicitly requested.
- **Do NOT create setup/how-to docs in project root**. If you must document something for developers (e.g., release process, setup instructions), place it in `docs/internal/` - never in the project root.
- **Do NOT create one-off utility scripts** in the project root (e.g., `check_syntax.py`, `validate_*.py`, `verify_*.py`). Use existing tools like `uv run pytest` or `uv run python -m py_compile <file>` instead.

## Creating Tasks from Conversations

When a conversation identifies work to be done, create a gza task rather than implementing inline:

```bash
# Basic task
uv run gza add "description of what needs to be done"

# With task type (plan, implement, review, explore)
uv run gza add --type plan "explore authentication options and propose approach"
uv run gza add --type implement "add user authentication with JWT"

# Auto-review after implementation
uv run gza add --type implement --review "add dark mode toggle"

# Task chaining - implementation based on a plan
uv run gza add --type implement --based-on 5 "implement the approach from task #5"
```

Tips for good task descriptions:
- Be specific about what needs to change and where
- Reference file paths or components when known
- For multi-step work, create a `--type plan` task first
- Use `--review` flag for significant changes that warrant code review
- Use `gza edit <id>` to update a task's prompt instead of deleting and recreating

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

## Line Endings

Always use Unix-style line endings (LF, `\n`). Do NOT use Windows-style line endings (CRLF, `\r\n`). This applies to all files in the repository, including shell scripts, Python files, and configuration files.
