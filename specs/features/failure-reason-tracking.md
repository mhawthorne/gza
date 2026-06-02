# Failure Reason Tracking

## Problem

When a task fails, gza records `status = 'failed'` but captures no structured information about *why* it failed. The only way to understand a failure is to read through the full log file, which is slow and doesn't support filtering or reporting.

## Solution

Add a `failure_reason` field to tasks that captures a categorized failure reason using a structured marker written by the agent into its log output. gza extracts this marker when the task completes and stores it in the database.

## Failure Reason Categories

Defined in code as a known set, stored as free-text in the DB so new categories can be added without a schema migration.

Initial categories:

| Category | Meaning |
|---|---|
| `MAX_TURNS` | Agent hit the turn limit without completing the task |
| `TEST_FAILURE` | Tests (pytest, mypy, etc.) failed and the agent could not fix them |
| `UNKNOWN` | Default when no marker is found or reason can't be determined |

New categories can be added by updating the validation set in Python code.

## Structured Marker Format

The agent writes a marker to its output when it determines a task has failed:

```
[GZA_FAILURE:REASON]
```

Examples:
```
[GZA_FAILURE:MAX_TURNS]
[GZA_FAILURE:TEST_FAILURE]
```

The marker must appear on its own line. gza scans for the last occurrence of this pattern in the log file (last wins, in case the agent retries and fails differently).

## Schema Change

Add column to `tasks` table:

```sql
ALTER TABLE tasks ADD COLUMN failure_reason TEXT;
```

No index needed — this field is informational and not expected to be a query filter.

## Migration (v10 → v11)

For existing failed tasks:
- Set `failure_reason = 'UNKNOWN'` for all tasks with `status = 'failed'`
- Leave `failure_reason = NULL` for non-failed tasks

```sql
UPDATE tasks SET failure_reason = 'UNKNOWN' WHERE status = 'failed';
```

## Detection Logic

When a task finishes (status changes to `failed`):

1. Scan the log file for the pattern `\[GZA_FAILURE:(\w+)\]`
2. Take the last match
3. Validate the extracted reason against the known category set
4. If valid, store it; if not recognized, store `UNKNOWN`
5. If no marker found, store `UNKNOWN`

## Agent Prompt Changes

Task execution prompts (in skills/CLAUDE.md or equivalent) should instruct the agent to emit the marker when it cannot complete a task:

> If you cannot complete the task, write a failure marker on its own line before your final message:
> `[GZA_FAILURE:REASON]` where REASON is one of: MAX_TURNS, TEST_FAILURE

The agent is not expected to emit a marker for every failure — `UNKNOWN` is an acceptable default.

## Display

`gza unmerged` and `gza history` should display the failure reason alongside status:

```
✗ failed (MAX_TURNS)
✗ failed (TEST_FAILURE)
✗ failed
```

When `failure_reason` is `UNKNOWN` or `NULL`, just show `✗ failed` without a parenthetical.

## Not In Scope

- User-editable failure reasons
- Free-text failure descriptions (use log files for detail)
- Automatic retry based on failure reason (future work)
