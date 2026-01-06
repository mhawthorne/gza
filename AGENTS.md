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

## Development

After making changes, run the test suite to verify everything works:

```
uv run pytest tests/ -v
```
