# Merge Status Tracking

## Problem

Currently, gza tracks whether a task's work has been merged using git-based detection:
- **Diff-based** (default): Checks if `git diff main..<branch>` is empty
- **Commit-based** (`--commits-only`): Uses `git cherry` to find unmerged commits

Both approaches have limitations:

1. **Squash merges confuse diff-based detection**: After `gza merge --squash`, the branch still has commits, but no diff. If main subsequently changes the same files, a diff reappears and the task shows as "unmerged" again.

2. **Task status conflates two concerns**: A task can be `completed` but unmerged, or `failed` but have mergeable partial work. The current `status` field tracks agent completion, not code disposition.

## Solution

Add a separate `merge_status` column to track the code/branch state independently of task status.

## Schema Change

Add column to `tasks` table:

```sql
ALTER TABLE tasks ADD COLUMN merge_status TEXT;
```

Valid values:
- `NULL` - No branch/commits (e.g., explore tasks, or tasks that made no changes)
- `'unmerged'` - Has commits, not yet merged
- `'merged'` - Successfully merged via `gza merge`

Add index for efficient filtering:
```sql
CREATE INDEX idx_tasks_merge_status ON tasks(merge_status);
```

## Migration

Infer `merge_status` for existing tasks:

```python
def migrate_merge_status(db: Database, git: Git) -> None:
    """Set merge_status for existing tasks based on current state."""
    tasks = db.query("SELECT id, has_commits, branch FROM tasks WHERE merge_status IS NULL")

    for task in tasks:
        if not task.has_commits:
            # No commits = no merge status
            merge_status = None
        elif not task.branch:
            # Has commits but no branch recorded = unclear, leave null
            merge_status = None
        elif not git.branch_exists(task.branch):
            # Branch deleted = assume merged
            merge_status = 'merged'
        elif git.is_ancestor(task.branch, 'main'):
            # Branch is ancestor of main = merged
            merge_status = 'merged'
        elif git.diff_is_empty('main', task.branch):
            # No diff = likely squash merged
            merge_status = 'merged'
        else:
            # Has diff = unmerged
            merge_status = 'unmerged'

        db.execute("UPDATE tasks SET merge_status = ? WHERE id = ?",
                   (merge_status, task.id))
```

## Setting merge_status

### On task completion (`gza work`)

```python
# In worker completion logic
if has_commits:
    db.execute("UPDATE tasks SET merge_status = 'unmerged' WHERE id = ?", (task_id,))
# If no commits, leave merge_status as NULL
```

### On merge (`gza merge`)

```python
# After successful merge (any variant: regular, --squash, --rebase)
db.execute("UPDATE tasks SET merge_status = 'merged' WHERE id = ?", (task_id,))
```

## Command Changes

### `gza unmerged`

Replace git-based detection with database query:

```python
# Before
tasks = db.query("""
    SELECT * FROM tasks
    WHERE status = 'completed' AND has_commits = 1
""")
# Then filter using git diff/cherry detection

# After
tasks = db.query("""
    SELECT * FROM tasks
    WHERE merge_status = 'unmerged'
""")
# No git detection needed - trust the database
```

Keep `--commits-only` and `--all` flags for backwards compatibility, but they become no-ops or warnings since detection is no longer git-based.

### `gza history`

Show merge status in output:

```
✓ [#9] (2026-02-20 07:27) Create a movie search page... [implement] [merged]
    branch: feature/create-a-movie-search-page
    stats: 22s | 5 turns | $0.46

⚡ [#10] (2026-02-20 07:31) Create serve.sh script... [implement]
    branch: chore/create-serve-sh
    stats: 1m13s | 25 turns | $0.47
```

Legend:
- `✓` = completed
- `✗` = failed
- `⚡` = unmerged (has commits awaiting merge)
- `[merged]` suffix = explicitly merged

### `gza show <task_id>`

Include merge status in task details:

```
Task #9
==================================================
Status: completed
Merge Status: merged
...
```

## Edge Cases

### Manual merges outside gza

If someone merges a branch manually (not via `gza merge`), the `merge_status` remains `'unmerged'`. Options:

1. **Accept this limitation**: User can run `gza merge --mark-only <task_id>` to update status
2. **Periodic sync**: Add `gza cleanup --sync-merge-status` to re-detect from git state

Recommend option 1 for simplicity.

### Branch deletion

If a branch is deleted (manually or via `gza merge --delete`), the merge_status should already be set. No special handling needed.

### Failed tasks with commits

A failed task can have `has_commits=1` and `merge_status='unmerged'`. This is valid - the task failed but produced mergeable work. `gza unmerged` will show it (possibly with a different indicator).

## Testing

1. **New task flow**: Create task → complete with commits → verify `merge_status='unmerged'`
2. **Merge flow**: Merge task → verify `merge_status='merged'`
3. **Squash merge flow**: `gza merge --squash` → verify `merge_status='merged'`
4. **No commits flow**: Task completes without commits → verify `merge_status=NULL`
5. **Migration**: Existing tasks get correct status inferred
6. **Unmerged query**: Only tasks with `merge_status='unmerged'` appear

## Rollout

1. Add migration to create column with NULL default
2. Run migration script to infer status for existing tasks
3. Update `gza merge` to set `merge_status='merged'`
4. Update worker to set `merge_status='unmerged'` on commit
5. Update `gza unmerged` to use database query
6. Update `gza history` and `gza show` to display merge status
