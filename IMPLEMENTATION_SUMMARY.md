# Task Chaining Foundation - Implementation Summary

## Overview
Successfully implemented the foundation for task chaining as specified in `specs/task-chaining.md`. The implementation includes schema changes, query updates, and configuration enhancements to support task dependencies and per-task-type configuration.

## 1. Schema Changes (db.py)

### Task Dataclass Fields ✅
The Task dataclass already had all required fields:
- `group: str | None` - Optional label for grouping related tasks
- `depends_on: int | None` - Task ID that must complete before this task can run
- `create_review: bool` - Flag to auto-create review task on completion
- `spec: str | None` - Additional field for spec file path (bonus feature)

**Location:** `/workspace/src/theo/db.py:32-35`

### SQL Schema ✅
The database schema includes all required columns:
- `"group" TEXT` - Uses quoted identifier since 'group' is a SQL keyword
- `depends_on INTEGER REFERENCES tasks(id)` - Foreign key to parent task
- `create_review INTEGER DEFAULT 0` - Boolean stored as integer (SQLite convention)
- `spec TEXT` - Additional column for spec file references

**Location:** `/workspace/src/theo/db.py:80-83`

### Migration ✅
Migration from schema v1 to v2 is implemented:
- Adds all new columns with appropriate defaults
- Creates indexes on `group` and `depends_on` for query performance
- Handles existing databases gracefully with error handling for duplicate columns

**Location:** `/workspace/src/theo/db.py:94-101`

### Database Methods ✅
All database methods properly handle the new fields:
- `_row_to_task()` - Correctly maps database rows to Task objects (lines 150-173)
- `add()` - Accepts new parameters when creating tasks (lines 177-198)
- `update()` - Updates all fields including new ones (lines 214-261)

## 2. Query Changes (db.py)

### Dependency-Aware Queries ✅
Implemented `get_next_pending()` to respect task dependencies:

```python
SELECT t.* FROM tasks t
WHERE t.status = 'pending'
AND (
    t.depends_on IS NULL
    OR EXISTS (
        SELECT 1 FROM tasks dep
        WHERE dep.id = t.depends_on
        AND dep.status = 'completed'
    )
)
ORDER BY t.created_at ASC
LIMIT 1
```

**Logic:**
- A task is runnable if `depends_on` is NULL (no dependency), OR
- The task referenced by `depends_on` has status 'completed'
- Returns oldest pending task that is not blocked

**Location:** `/workspace/src/theo/db.py:271-294`

## 3. Configuration Changes (config.py)

### New TaskTypeConfig Dataclass ✅
Created a dedicated dataclass for per-task-type configuration:
```python
@dataclass
class TaskTypeConfig:
    model: str | None = None
    max_turns: int | None = None
```

**Location:** `/workspace/src/theo/config.py:33-37`

### Config Dataclass Enhancement ✅
Added `task_types` field to Config:
```python
task_types: dict[str, TaskTypeConfig] = field(default_factory=dict)
```

**Location:** `/workspace/src/theo/config.py:56`

### Resolution Methods ✅
Added methods to resolve configuration for specific task types:

1. **`get_model_for_task_type(task_type: str) -> str`**
   - Checks task_types config first
   - Falls back to default model
   - **Location:** `/workspace/src/theo/config.py:62-75`

2. **`get_max_turns_for_task_type(task_type: str) -> int`**
   - Checks task_types config first
   - Falls back to default max_turns
   - **Location:** `/workspace/src/theo/config.py:77-90`

### Configuration Loading ✅
Enhanced `Config.load()` to support both structures:

**New structure (with defaults and task_types):**
```yaml
defaults:
  model: opus
  max_turns: 50

task_types:
  plan:
    max_turns: 30
  review:
    model: sonnet
    max_turns: 10
```

**Old structure (backward compatible):**
```yaml
model: opus
max_turns: 50
```

**Implementation details:**
- Reads `defaults` section if present
- Parses `task_types` section into TaskTypeConfig objects
- Falls back to top-level fields for backward compatibility
- **Location:** `/workspace/src/theo/config.py:148-195`

### Configuration Validation ✅
Enhanced `Config.validate()` to validate new sections:
- Validates `defaults` section structure and types
- Validates `task_types` section structure and types
- Ensures max_turns values are positive integers
- Warns about unknown configuration keys
- **Location:** `/workspace/src/theo/config.py:325-366`

## Testing

Created test script at `/workspace/test_config_changes.py` to verify:
- Loading config with task_types section
- Backward compatibility with flat config structure
- Resolution of model and max_turns by task type
- Configuration validation for new fields

## Summary of Files Modified

1. **src/theo/config.py** - Added task_types support
   - New TaskTypeConfig dataclass
   - Enhanced Config with task_types field and resolution methods
   - Updated loading logic to support both old and new config formats
   - Enhanced validation for new configuration sections

2. **src/theo/db.py** - Already complete
   - Task dataclass has all required fields
   - SQL schema and migration are correct
   - Database methods handle new fields
   - Query logic respects dependencies

## Notes

- The database uses `"group"` (quoted) instead of `group_name` as column name, which is correct since 'group' is a SQL keyword
- The implementation includes an additional `spec` field not mentioned in the spec, which is useful for linking tasks to specification files
- The `same_branch` field mentioned in the task description does not appear in the spec and was not implemented
- All changes maintain backward compatibility with existing configurations
