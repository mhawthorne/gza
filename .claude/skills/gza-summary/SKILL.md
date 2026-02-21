---
name: gza-summary
description: Summarize recent gza task activity and suggest next steps by checking history, unmerged branches, and pending tasks
allowed-tools: Bash(uv run gza history:*), Bash(uv run gza unmerged:*), Bash(uv run gza next:*)
version: 1.0.0
public: true
---

# Gza Summary

Get an overview of recent gza task activity, unmerged work, and pending tasks, then suggest concrete next steps.

## Process

### Step 1: Gather task data

Run all three commands in sequence to collect current state:

**Recent history (completed/failed tasks):**
```bash
uv run gza history
```

**Unmerged work (tasks with branches not yet merged):**
```bash
uv run gza unmerged
```

**Pending tasks (upcoming work):**
```bash
uv run gza next
```

### Step 2: Analyze the output

Review the combined output and identify actionable items:

**From `gza history`:**
- Note any recently failed tasks — these may need to be retried or their errors investigated
- Note recently completed tasks that produced branches (candidates for merging or PR creation)
- Note completed plan/explore tasks whose output hasn't been acted on yet

**From `gza unmerged`:**
- Each branch listed here represents completed work that hasn't been merged to main
- Identify which branches are ready to merge (no conflicts expected) vs which need review first
- Check if any unmerged branches have associated PRs or reviews pending

**From `gza next`:**
- Identify the next pending task(s) — the most immediate work to run with `gza work`
- Note any tasks that are blocked by dependencies or require prerequisite steps (e.g., terraform apply, file uploads)
- Look for tasks that are grouped together and may benefit from being run in sequence

### Step 3: Suggest next steps

Based on the analysis, provide a prioritized list of specific, actionable suggestions. Examples:

**For unmerged branches:**
- "Merge branch `20260115-add-authentication` — task #18 is complete and has 3 commits ready"
- "Create a PR for task #22 (`20260118-fix-api-pagination`) before merging"

**For failed tasks:**
- "Retry failed task #23 — it failed due to a missing module, which may now be installed"
- "Investigate task #25 failure: check the log with `uv run gza log 25 --task`"

**For pending tasks:**
- "Run `gza work` to start the next pending task: #31 (implement CSV export)"
- "Task #28 depends on #27 which is still in_progress — wait before running"

**For infrastructure/ops tasks:**
- "Apply terraform changes before running task #34 (requires infrastructure update)"
- "Upload config files to S3 before running task #36"
- "Review and merge branch X before starting dependent task Y"

**General workflow suggestions:**
- If history shows a completed plan task, suggest creating an implement task based on it
- If there are many unmerged branches, suggest a merge/cleanup session
- If no pending tasks exist, suggest reviewing completed work or adding new tasks

## Output format

Present the summary in three clearly labeled sections:

```
## Recent Activity
[Key highlights from gza history — 3-5 most notable items]

## Unmerged Work
[List of branches/tasks with unmerged commits, if any]

## Pending Tasks
[Next tasks in queue, if any]

## Suggested Next Steps
1. [Most urgent/impactful action]
2. [Second priority action]
3. [Further actions...]
```

Keep the summary concise — focus on what requires human attention or decision-making.

## Important notes

- **Run all three commands** even if one returns no results — the combination gives the full picture
- **Be specific** in suggestions: name the task ID, branch name, or command to run
- **Prioritize** unmerged work and failed tasks over pending tasks (existing work first)
- **Respect dependencies**: don't suggest running a task that depends on an incomplete task
- If all sections are empty (no history, no unmerged, no pending), report that the queue is clean and suggest adding new tasks with `gza add`
