# Replace direct sqlite3 access in gza skills with Python API calls

## Context

Two gza skills (`gza-task-info` and `gza-task-debug`) shell out to `sqlite3` to query the task database directly. This couples them to the DB schema and bypasses the existing Python API. We want skills to use `from gza.db import SqliteTaskStore` (and helpers) via Python one-liners instead.

## What needs to change

### 1. Add convenience functions to `src/gza/db.py`

Add module-level helper functions that handle store instantiation internally (skills shouldn't need to know about config/paths):

- `get_task(task_id: int) -> dict` — returns task fields as a dict (JSON-serializable)
- `get_task_log_path(task_id: int) -> str | None` — returns log_file path
- `get_task_report_path(task_id: int) -> str | None` — returns report_file path
- `get_baseline_stats(limit: int = 20) -> dict` — returns `{avg_turns, avg_duration, avg_cost}` from last N completed tasks

These functions auto-discover the DB path using the default `.gza/gza.db` relative to cwd (matching what the skills already assume).

### 2. Update `gza-task-info` SKILL.md

- Replace `sqlite3 .gza/gza.db "SELECT ..."` with `python -c "from gza.db import get_task; ..."`
- Replace `allowed-tools: Bash(sqlite3:*)` with `Bash(uv run python -c:*)`
- Keep `Read` and `Bash(git:*)` — file reads and git ops stay as-is

### 3. Update `gza-task-debug` SKILL.md

- Replace the task query with `python -c "from gza.db import get_task; ..."`
- Replace the baseline stats query with `python -c "from gza.db import get_baseline_stats; ..."`
- Replace `allowed-tools: Bash(sqlite3:*)` with `Bash(uv run python -c:*)`
- Keep `Read`, `Bash(git:*)`, `Bash(wc:*)`, `Bash(grep:*)` — log analysis stays file-based

### 4. Tests

Add tests for the new convenience functions in `tests/test_db.py`.

## Files to modify

- `src/gza/db.py` — add convenience functions
- `src/gza/skills/gza-task-info/SKILL.md` — replace sqlite3 with Python API
- `src/gza/skills/gza-task-debug/SKILL.md` — replace sqlite3 with Python API
- `tests/test_db.py` — add tests for new functions

## Verification

- `uv run pytest tests/ -v` passes
- `uv run mypy src/gza/db.py` passes
- Manual: `uv run python -c "from gza.db import get_task; import json; print(json.dumps(get_task(1), indent=2, default=str))"` from a project with `.gza/gza.db`
