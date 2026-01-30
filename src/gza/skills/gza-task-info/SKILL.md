---
name: gza-task-info
description: Gather comprehensive info about a specific gza task including status, branch, commits, and logs
allowed-tools: Read, Bash(sqlite3:*), Bash(git:*)
---

# Gza Task Info

Gather comprehensive information about a specific gza task, including database details, git branch status, commits, and execution logs.

## Process

### Step 1: Get task ID

The user should provide a task ID (e.g., "18", "#42", or just "5"). Extract the numeric ID.

### Step 2: Query task from database

Run sqlite3 query to get all task details:

```bash
sqlite3 .gza/gza.db "SELECT * FROM tasks WHERE id = <ID>;"
```

This will show:
- id, prompt, status, task_type, task_id (slug)
- branch, log_file, report_file
- has_commits, duration_seconds, num_turns, cost_usd
- created_at, started_at, completed_at
- group, depends_on, based_on, spec
- create_review, same_branch, task_type_hint
- output_content, session_id

### Step 3: Check branch status (if task has a branch)

If the task has a branch field set, gather git information:

1. **Check if branch exists:**
   ```bash
   git branch -a | grep <branch-name>
   ```

2. **Show recent commits on the branch:**
   ```bash
   git log <branch-name> --oneline -10
   ```

3. **Check if branch is merged to main:**
   ```bash
   git branch --merged main | grep <branch-name> || echo "Not merged to main"
   ```

4. **Check for uncommitted changes (if on this branch):**
   ```bash
   git diff <branch-name> --stat
   ```

5. **Show branch comparison with main:**
   ```bash
   git log main..<branch-name> --oneline
   ```

### Step 4: Show log file (if exists)

If the task has a log_file field:

1. **Check if log file exists:**
   ```bash
   ls -lh <log_file>
   ```

2. **Show the tail of the log (last 50-100 lines) to see how it ended:**
   ```bash
   tail -100 <log_file>
   ```

   Or if you want to focus on the end:
   ```bash
   tail -50 <log_file>
   ```

### Step 5: Show report file (if exists)

If the task has a report_file field:

1. **Read the entire report:**
   ```bash
   cat <report_file>
   ```

   Or if it's very long, show the first part:
   ```bash
   head -200 <report_file>
   ```

### Step 6: Show output_content (if exists)

If the task has output_content in the database (stored directly), display it.

### Step 7: Summarize the task state

Create a clear, concise summary of the task state. Examples:

**Completed task with commits:**
```
Task #18: completed
Type: implement
Branch: 20260115-add-authentication (3 commits, not yet merged to main)
Duration: 245.3s (4:05)
Cost: $0.42
Prompt: "Add JWT authentication to API endpoints"
```

**Failed task:**
```
Task #23: failed
Type: implement
Duration: 89.2s (1:29)
Cost: $0.15
Prompt: "Fix database migration script"
Log shows: ImportError on line 45 - missing 'alembic' module
```

**Pending task with dependency:**
```
Task #31: pending
Type: implement
Depends on: Task #30 (still in_progress)
Group: metrics-v2
Prompt: "Implement CSV export for metrics data"
```

**Completed task with report:**
```
Task #15: completed (exploration)
Duration: 156.7s (2:37)
Cost: $0.28
Report: Found 3 authentication patterns in codebase (see below)
```

## Important notes

- **Database path**: Always use `.gza/gza.db` (in project root)
- **Handle missing fields**: Not all fields will be populated (e.g., branch might be NULL for failed/pending tasks)
- **Format durations**: Show seconds and human-readable format (e.g., "245.3s (4:05)")
- **Format costs**: Show USD with 2 decimal places (e.g., "$0.42")
- **Log context**: The tail of the log is most important - it shows how the task ended (success, error, timeout, etc.)
- **Branch status**: If branch exists but isn't merged, mention it clearly
- **Dependencies**: If task has depends_on or based_on, show what it's related to

## Tips

- For completed tasks, focus on outcomes (commits, reports, branch status)
- For failed tasks, focus on error details from the log tail
- For pending tasks, focus on dependencies and blocking status
- For in_progress tasks, show the log tail to see current status
- If a task has both report_file and output_content, prefer the file (it might be more recent)
