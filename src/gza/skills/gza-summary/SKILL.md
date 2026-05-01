---
name: gza-summary
description: Synthesize operator triage guidance from failed-task history, unimplemented plan and explore work, unmerged branches, and queue state
allowed-tools: Bash(uv run gza history:*), Bash(uv run gza unmerged:*), Bash(uv run gza next:*), Bash(uv run gza advance:*), Bash(uv run gza watch:*)
version: 2.0.0
public: true
---

# Gza Summary

Produce an operator triage summary by combining dedicated gza surfaces, then recommend the next concrete actions.

## Process

### Step 1: Gather task data

Run these commands to collect the canonical source data:

**Failed-task history:**
```bash
uv run gza history --status failed
```

**Unimplemented plan/explore work:**
```bash
uv run gza advance --unimplemented
```

Until `uv run gza unimplemented` exists, `uv run gza advance --unimplemented` is the canonical surface.

**Unmerged code work:**
```bash
uv run gza unmerged
```

**Queue state, including blocked rows:**
```bash
uv run gza next --all
```

**Optional failed-recovery decision surface:**
```bash
uv run gza watch --restart-failed --dry-run
```

### Step 2: Analyze the output

Treat each command as the authoritative surface for one domain:

**From `uv run gza history --status failed`:**
- Treat this as factual failed-task history, including attempts that were later retried or resumed successfully
- Suggest factual follow-up like `uv run gza log <full prefixed task id>` when operators need details
- Use `uv run gza watch --restart-failed --dry-run` when operators need the current failed-task recovery decision surface
- Do not treat this as a replacement for the old mixed-bucket `gza incomplete` output

**From `uv run gza unmerged`:**
- Each listed row is completed code work that still needs merge/review/sync attention
- Prefer gza-native guidance like `uv run gza merge <full prefixed task id>` or `uv run gza sync <full prefixed task id>`

**From `uv run gza advance --unimplemented`:**
- Identify completed `plan` or `explore` rows that do not yet have implementation work
- Suggest `uv run gza implement <full prefixed task id>` only for completed plan rows
- Suggest `uv run gza advance --unimplemented --create` when implement tasks should be queued from listed source rows

**From `uv run gza next --all`:**
- Identify the runnable next task(s) for `uv run gza work`
- Call out blocked rows, including dropped-dependency blockers, as queue state rather than lineage state

**From `uv run gza watch --restart-failed --dry-run` (optional):**
- Use it only when failed tasks exist and the operator needs the failed-recovery decision surface
- Treat it as supplemental guidance, not as the primary summary source

### Step 3: Suggest next steps

Produce a prioritized set of specific recommendations. Keep recommendations mapped back to the dedicated surface they came from.

Examples:

- "Inspect failed task `gza-1234` with `uv run gza log gza-1234` before choosing a recovery action."
- "Queue implementation follow-up for `gza-1250` with `uv run gza advance --unimplemented --create`."
- "Merge unmerged implementation `gza-1301` with `uv run gza merge gza-1301`."
- "Run `uv run gza work` to start the next runnable pending task from the queue."

Avoid broad restatements of task state. Focus on what the operator should do next and why.

## Output format

Present the summary in these sections:

```
## Failed Recovery

## Unimplemented Plans/Explores

## Unmerged Work

## Queue State

## Suggested Next Steps
```

Keep it concise. If a section has no actionable items, say so briefly and move on.

## Important notes

- `/gza-summary` is the synthesized recommendation layer. It is not a canonical replacement for `gza incomplete`.
- `uv run gza history --incomplete` remains a factual unresolved-history filter, not an action-planning summary.
- Prefer gza-native commands in recommendations.
- Use full prefixed task IDs in every command example.
