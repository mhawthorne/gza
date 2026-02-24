---
name: gza-task-debug
description: Diagnose why a gza task failed — analyzes logs, detects loops, checks diffs, compares baselines, and suggests fixes
allowed-tools: Read, Bash(uv run python -c:*), Bash(git:*), Bash(wc:*), Bash(grep:*)
version: 1.0.0
public: true
---

# Gza Task Debug

Diagnose why a gza task failed by analyzing logs, detecting agent loops, comparing against baselines, and providing actionable recommendations.

## Process

### Step 1: Get task ID

The user should provide a task ID (e.g., "18", "#42", or just "5"). Extract the numeric ID.

### Step 2: Query task from database

Run a Python one-liner to get all task details as JSON:

```bash
uv run python -c "from gza.db import get_task; import json; print(json.dumps(get_task(<ID>), indent=2, default=str))"
```

Note the following fields for analysis:
- `status` — should be `failed` or `max_turns` (or possibly `completed` if user suspects partial failure)
- `num_turns` — number of agent turns used
- `duration_seconds` — total wall-clock time
- `cost_usd` — API cost
- `log_file` — path to the execution log
- `report_file` — path to the report (if any)
- `branch` — git branch the task worked on

### Step 3: Baseline comparison

Compare the failed task's metrics against the last 20 completed tasks:

```bash
uv run python -c "from gza.db import get_baseline_stats; import json; print(json.dumps(get_baseline_stats(20)))"
```

Calculate how far the failed task deviates:
- If `num_turns` is 2x+ the average → flag as high turns
- If `cost_usd` is 3x+ the average → flag as high cost
- Report the ratio (e.g., "3.2x more turns than average completed tasks")

### Step 4: Loop detection from logs

Do NOT just read the tail — scan the full log for repeated patterns.

If `log_file` is set, run these grep-based checks:

**Repeated file opens (same file opened 5+ times):**
```bash
grep -o 'Reading file: [^ ]*\|reading.*["\x27][^"'\'']*["\x27]\|open.*["\x27][^"'\'']*["\x27]' <log_file> | sort | uniq -c | sort -rn | head -20
```

**Repeated tool invocations (same command run many times):**
```bash
grep -oE '(Bash|Read|Write|Edit|Grep|Glob)\(' <log_file> | sort | uniq -c | sort -rn | head -10
```

**Repeated error strings:**
```bash
grep -iE '(error|failed|exception|traceback)' <log_file> | sort | uniq -c | sort -rn | head -20
```

**Repeated test runs (pytest/jest/etc. invoked repeatedly):**
```bash
grep -cE '(pytest|npm test|jest|rspec|go test)' <log_file>
```

**Same search query repeated:**
```bash
grep -oE 'pattern": "[^"]*"' <log_file> | sort | uniq -c | sort -rn | head -10
```

If any file appears 5+ times, or any error string repeats 5+ times, this strongly indicates a **stuck agent loop**.

### Step 5: Read the log tail

Read the last portion of the log file to understand how the task ended:

```bash
grep -c '' <log_file>
```
(to get total line count, then read the last ~200 lines using offset)

Also read the last 200 lines directly to see the final state — what was the agent doing right before it stopped? Was it:
- Running the same test repeatedly?
- Trying to find a file that doesn't exist?
- Hitting the same error over and over?
- Making good progress but just ran out of turns?

### Step 6: Read the report file

If `report_file` is set, read it for the agent's own summary of what happened.

### Step 7: Check the git diff

If the task has a `branch` with commits:

```bash
git diff main...<branch> --stat
```

This shows:
- How many files were changed
- How many lines were added/deleted

Calculate the **turns-per-file-changed** ratio:
- `num_turns / files_changed`
- **High ratio (10+ turns per file)** → agent was thrashing, not making real progress
- **Low ratio (1-2 turns per file)** → legitimate work, may just need more turns
- **Many files changed (50+)** → scope creep, agent over-engineered

Also check what was actually committed:
```bash
git log main...<branch> --oneline
```

### Step 8: Synthesize the diagnosis

Based on all evidence, determine the root cause. Common failure patterns:

**1. Max-turns with loop (stuck agent)**
- Evidence: Same file opened repeatedly, same error repeated, test run 10+ times
- Cause: Agent got stuck trying to fix a specific error and couldn't break the cycle
- Fix: Add context to the prompt about the specific problem area; break into explore-then-implement

**2. Max-turns with scope creep**
- Evidence: 50+ files changed, many commits, broad prompt
- Cause: Task was too large; agent kept finding more things to do
- Fix: Break into 3-5 smaller focused tasks

**3. Max-turns with missing context**
- Evidence: Agent searching for non-existent files, asking about patterns it can't find, misunderstanding architecture
- Cause: Agent lacked knowledge of codebase structure
- Fix: Update AGENTS.md with relevant docs; use `--based-on` to chain from an explore task

**4. Max-turns with legitimate large task**
- Evidence: Low turns-per-file ratio, consistent progress, no loops, just too much work
- Cause: Task was genuinely too big for current turn limit
- Fix: Increase `--max-turns` or break into sequential subtasks

**5. External failure (test/build errors)**
- Evidence: Consistent error messages from tests/build tools, clear stack traces
- Cause: Broken environment, missing dependencies, or introduced regression
- Fix: Fix the underlying environment issue first; add a setup task

**6. Unnecessary work (over-engineering)**
- Evidence: Many files touched but not required, unrequested features added
- Cause: Prompt was too vague, agent expanded scope
- Fix: Add explicit constraints to the prompt ("only modify X", "do not refactor")

### Step 9: Output the diagnosis

Output a structured diagnosis with these sections:

---

## Task Summary

| Field | Value |
|-------|-------|
| ID | #<id> |
| Status | <status> |
| Prompt | <prompt> |
| Turns | <num_turns> |
| Duration | <duration_seconds>s (<human readable>) |
| Cost | $<cost_usd> |
| Branch | <branch or "none"> |

---

## Baseline Comparison

| Metric | This Task | Avg (last 20) | Ratio |
|--------|-----------|---------------|-------|
| Turns | <num_turns> | <avg_turns> | <ratio>x |
| Duration | <duration>s | <avg_duration>s | <ratio>x |
| Cost | $<cost> | $<avg_cost> | <ratio>x |

---

## Failure Analysis

### Loop Detection
- [List any repeated patterns found, with counts]
- [e.g., "pytest invoked 14 times — strong loop signal"]
- [e.g., "No repeated patterns detected — likely not a loop"]

### Diff Analysis
- Files changed: <N>
- Lines added/deleted: +<A> / -<D>
- Turns per file changed: <ratio>
- [Interpretation: e.g., "High turns-per-file ratio suggests thrashing"]

### Log Summary
[What was the agent doing at the end? What was it trying to accomplish?]

---

## Root Cause

**[One of: Stuck Loop | Scope Creep | Missing Context | Legitimate Large Task | External Failure | Over-engineering]**

[2-3 sentence explanation of what specifically caused the failure]

---

## Recommendations

1. **[Primary recommendation]** — [Specific action to take]
2. **[Secondary recommendation]** — [Specific action to take]
3. [Additional recommendations as needed]

[If applicable: specific prompt text to use, specific AGENTS.md sections to add, specific task splits]

---

### Step 10: Offer to create follow-up tasks

After the diagnosis, ask:

> Would you like me to create replacement tasks based on this analysis?

Suggest concrete replacements based on root cause:

- **If scope creep**: Offer to split into 2-3 focused tasks covering specific parts of the original
- **If missing context**: Offer to create an explore task first, then the implement task with `--based-on`
- **If stuck loop**: Offer a narrower task with explicit constraints and pointers to specific files
- **If legitimate large task**: Offer to rerun with higher turn limit or create sequential subtasks
- **If external failure**: Offer a setup/fix task first, then re-queue the original

Use the format:
```
Would you like me to create N replacement tasks?
1. [Task description] — [why this addresses the root cause]
2. [Task description] — [why this addresses the root cause]
```

If the user says yes, use the `gza-task-add` skill to create each task.

## Important notes

- **Database path**: The Python API auto-discovers `.gza/gza.db` relative to cwd (project root)
- **Log files**: May be absolute or relative paths — try both if the first fails
- **Missing branch**: If `branch` is NULL, skip the git diff analysis
- **No log file**: If `log_file` is NULL or empty, note this and rely on database fields only
- **Format durations**: Show seconds and human-readable (e.g., "245s (4:05)")
- **Format costs**: Show USD with 4 decimal places for small amounts (e.g., "$0.0234")
- **Baseline with few tasks**: If fewer than 5 completed tasks exist, note that baseline is not reliable
