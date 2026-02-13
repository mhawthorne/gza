# Task Workflow Orchestration

> **Status: Aspirational** - This spec describes planned features. Commands like `gza dashboard`, `gza advance`, `gza summary`, and `gza changelog` have not been implemented yet.

## Overview

This spec describes a higher-level workflow system for managing gza tasks from creation through completion and merge. It covers:

1. **Visibility**: Understanding what's happening across tasks
2. **Progression**: Moving tasks forward through their lifecycle
3. **Orchestration**: Coordinating multi-step task workflows

## Current State

Gza has individual commands for each task operation:
- `gza add` - Create tasks
- `gza work` - Run a single task
- `gza review` - Create/run reviews
- `gza improve` - Address review feedback
- `gza pr` - Create pull requests
- `gza merge` / `gza rebase` - Integrate changes
- `gza ps` - View running workers
- `gza history` - View completed tasks
- `gza unmerged` - View tasks with unmerged work

What's missing:
- **Holistic view**: "What needs my attention right now?"
- **Batch progression**: "Move these 5 tasks forward"
- **Lifecycle automation**: Auto-PR, auto-merge after review passes

---

## Phase 1: Enhanced Visibility

### Problem

The current commands provide fragmented views:
- `gza next` shows pending tasks
- `gza ps` shows running workers
- `gza history` shows recent completions
- `gza unmerged` shows tasks needing merge

A user must run multiple commands to understand the current state.

### Solution: `gza dashboard` / `gza status --all`

A single command that shows the holistic state:

```bash
$ gza dashboard

RUNNING (2 workers):
  #42  implement-auth     running     12m (turn 8, $0.45)
  #45  fix-caching-bug    running      3m (turn 2, $0.08)

NEEDS ATTENTION (3 tasks):
  #40  add-dark-mode      completed   ready to merge (APPROVED)
  #38  refactor-api       completed   needs rebase (conflicts on main)
  #39  update-deps        failed      Claude exited with error (retry?)

WAITING FOR REVIEW:
  #41  add-user-profile   completed   no review yet

PENDING (5 tasks):
  #46  implement-logging
  #47  add-metrics
  ... +3 more (gza next for full list)

RECENT (last 24h):
  #35  fix-typo           merged      1h ago
  #36  add-readme         merged      3h ago
  ... +2 more
```

### Categories

| Category | Source | Criteria |
|----------|--------|----------|
| RUNNING | Workers | Active `gza ps` workers |
| NEEDS ATTENTION | Various | Tasks requiring human action |
| WAITING FOR REVIEW | Completed implements | `auto_review=false`, no review task |
| PENDING | DB | `status=pending` |
| RECENT | DB | `status=completed/merged` in last 24h |

### "Needs Attention" Criteria

A task needs attention if any of these:

1. **Ready to merge**: Completed with APPROVED verdict, no conflicts
2. **Needs rebase**: Branch has conflicts with target branch
3. **Failed**: Task status is `failed`
4. **Max cycles reached**: Review-improve loop exhausted without approval
5. **Waiting for input**: Task asked a question (async HITL)
6. **Review rejected**: Manual review with CHANGES_REQUESTED, no improve task

### New CLI: `gza dashboard`

```bash
# Full dashboard
gza dashboard

# Specific sections
gza dashboard --running      # Only running workers
gza dashboard --attention    # Only tasks needing attention
gza dashboard --summary      # One-line counts per category

# JSON output for scripting
gza dashboard --json
```

### New CLI: `gza summary`

Quick one-liner for shell prompts or periodic checks:

```bash
$ gza summary
2 running | 3 need attention | 5 pending | 4 merged today
```

---

## Phase 2: Batch Progression

### Problem

Moving tasks forward requires multiple manual commands:

```bash
gza work              # Run next pending
gza pr 42             # Create PR
gza review 42 --run   # Run review
gza merge 42          # Merge if approved
```

For multiple tasks, this becomes tedious.

### Solution: `gza advance`

A command that intelligently progresses tasks through their lifecycle:

```bash
# Advance a specific task to the next logical step
gza advance 42

# Advance all tasks that can be progressed
gza advance --all

# Advance with limits
gza advance --all --max 5   # Progress up to 5 tasks

# Dry-run to see what would happen
gza advance --all --dry-run
```

### Task Lifecycle & Progression Logic

```
pending
   │
   ├─[gza work]─→ running ─→ completed
   │                            │
   │                   ┌────────┴────────┐
   │                   │                 │
   │            (no PR needed)    (PR workflow)
   │                   │                 │
   │                   ▼                 ▼
   │               [merge]           [gza pr]
   │                   │                 │
   │                   ▼                 ▼
   │               merged            has PR
   │                                    │
   │                            ┌───────┴───────┐
   │                            │               │
   │                     (auto-review)   (manual review)
   │                            │               │
   │                            ▼               │
   │                      [gza review]          │
   │                            │               │
   │                  ┌─────────┴─────────┐     │
   │                  │                   │     │
   │             APPROVED          CHANGES_REQUESTED
   │                  │                   │
   │                  ▼                   ▼
   │             [merge PR]        [gza improve]
   │                  │                   │
   │                  ▼                   ▼
   │               merged             (loop back)
```

### Advance Logic per Task State

| Current State | Advance Action |
|--------------|----------------|
| `pending` | `gza work` (run the task) |
| `completed`, no PR, no conflicts | `gza merge` or `gza pr` (based on config) |
| `completed`, no PR, has conflicts | Signal "needs rebase" |
| `completed`, has PR, no review | `gza review --run` |
| `completed`, has PR, APPROVED | `gh pr merge` or `gza merge` |
| `completed`, has PR, CHANGES_REQUESTED | `gza improve` + `gza work` |
| `failed` | Signal "needs retry/investigation" |
| `running` | Skip (already in progress) |

### Configuration

```yaml
# gza.yaml
workflow:
  # Auto-create PR after implementation completes
  auto_pr: true  # or false, or "ask"

  # Auto-review after PR creation
  auto_review: true  # already exists

  # Auto-merge after APPROVED
  auto_merge: false  # conservative default

  # Lines changed threshold for auto-review
  review_threshold: 50  # tasks changing >50 lines get reviewed

  # Merge strategy
  merge_strategy: "squash"  # or "merge", "rebase"
```

### `gza advance` Output

```bash
$ gza advance --all --dry-run

Would advance 4 tasks:

  #42 implement-auth (completed)
      → Create PR
      → Run review

  #40 add-dark-mode (APPROVED)
      → Merge PR (squash)

  #46 implement-logging (pending)
      → Run task

  #38 refactor-api (completed, conflicts)
      → SKIP: needs manual rebase

Proceed? [y/N]
```

### Idempotency

`gza advance` should be safe to run repeatedly:
- Already-merged tasks: Skip
- Already-running tasks: Skip
- Tasks with pending reviews: Wait for review to complete

---

## Phase 3: History & Changelog

### Problem

Understanding what happened over time:
- "What tasks completed today?"
- "What changed in the codebase this week?"
- "Give me a summary of recent work"

### Solution: Enhanced History Commands

#### `gza history` enhancements

```bash
# Current: list recent completed/failed tasks
gza history

# New: time-based filtering
gza history --since "1 day"
gza history --since "1 week"
gza history --since "2026-02-01"

# New: include PR/merge status
gza history --with-status
```

Output with `--with-status`:
```
TASK ID                    TYPE       STATUS      PR     MERGED
20260212-implement-auth    implement  completed   #142   ✓
20260212-fix-caching       implement  completed   #143   pending
20260211-add-tests         implement  failed      -      -
20260211-update-deps       implement  completed   -      ✓ (direct)
```

#### `gza changelog`

Generate a summary of changes for a time period:

```bash
$ gza changelog --since "1 week"

# Changes This Week (Feb 5-12, 2026)

## Features
- Add user authentication with JWT (#142)
- Implement dark mode toggle (#140)
- Add usage metrics export (#138)

## Bug Fixes
- Fix caching race condition (#143)
- Resolve login redirect loop (#139)

## Maintenance
- Update dependencies to latest versions
- Refactor API error handling

---
12 tasks completed | 2 failed | 847 lines changed
```

Implementation:
- Parse task prompts/types to categorize (feature, fix, maintenance)
- Aggregate commit messages from task branches
- Use LLM to generate human-readable summary (optional)

---

## Phase 4: Worker Status & Monitoring

### Problem

Understanding what workers are doing right now:
- "Which workers are running?"
- "How long have they been running?"
- "What's their current progress?"

### Current: `gza ps`

Shows running workers with basic info.

### Enhancement: `gza ps --detail`

```bash
$ gza ps --detail

WORKER w-20260212-001
  Task:     #42 implement-auth
  Started:  12 minutes ago
  Turns:    8/20
  Cost:     $0.45
  Status:   Running (last activity: 30s ago)
  Branch:   gza/20260212-implement-auth
  Files:    src/auth/*.py, tests/test_auth.py

WORKER w-20260212-002
  Task:     #45 fix-caching-bug
  Started:  3 minutes ago
  Turns:    2/20
  Cost:     $0.08
  Status:   Running (last activity: 5s ago)
  Branch:   gza/20260212-fix-caching-bug
  Files:    src/cache/redis.py
```

### Enhancement: Worker Health

Detect stalled workers:
- No log output for >5 minutes
- Turn count not increasing
- Process alive but no API calls

```bash
$ gza ps
WORKER        TASK               STATUS     DURATION
w-001         #42 implement-auth stalled    45m (no activity 12m)
w-002         #45 fix-caching    running    3m

! Worker w-001 may be stalled. Consider: gza stop w-001 && gza retry 42
```

---

## Phase 5: Long-Running Workers (Future)

> Note: This is NOT the immediate next step. Documenting for completeness.

### Concept

A single worker that continuously processes tasks:

```bash
# Run until N tasks completed
gza daemon --max-tasks 10

# Run until stopped
gza daemon

# Run until a time limit
gza daemon --max-hours 8
```

### Behavior

1. Worker starts and claims next pending task
2. Executes task through full lifecycle (implement → review → merge)
3. On completion, claims next task
4. Continues until limit reached or no pending tasks

### Considerations

- Session management (fresh session per task vs. shared)
- Error recovery (one bad task shouldn't stop the daemon)
- Priority handling
- Resource limits
- Graceful shutdown

---

## Implementation Phases

### Immediate (Phase 1)

1. **`gza dashboard`**: Holistic view of all task states
2. **`gza summary`**: One-liner for quick status
3. **Attention detection**: Logic to identify tasks needing human action

### Near-term (Phase 2)

4. **`gza advance`**: Intelligent task progression
5. **Workflow configuration**: `auto_pr`, `auto_merge` settings
6. **Batch operations**: Advance multiple tasks

### Later (Phase 3)

7. **`gza history` enhancements**: Time filtering, status display
8. **`gza changelog`**: Summarize changes over time

### Later (Phase 4)

9. **`gza ps` enhancements**: Detailed worker status
10. **Health monitoring**: Stall detection

### Future (Phase 5)

11. **Long-running daemon mode**

---

## Open Questions

### 1. PR creation policy

Should PRs be created automatically for every completed task?

**Options**:
- `auto_pr: true` - Always create PR
- `auto_pr: false` - Manual `gza pr` required
- `auto_pr: "threshold"` - Only for changes > N lines
- `auto_pr: "ask"` - Prompt user (breaks automation)

**Recommendation**: Default `auto_pr: false` for now. Users can enable in config.

### 2. Auto-merge safety

Auto-merging approved PRs is convenient but risky.

**Safeguards**:
- Require CI to pass (if configured)
- Only squash-merge (no merge commits)
- Require N approvals (integrate with GitHub settings)
- Allow blocklist of protected branches

**Recommendation**: Default `auto_merge: false`. Power users can enable.

### 3. Review threshold

When should tasks be auto-reviewed vs. skip review?

**Options**:
- Always review (current default with `--review` flag)
- Review if > N lines changed
- Review if touching certain files (e.g., security-sensitive)
- Never auto-review (manual `gza review` required)

**Recommendation**: `review_threshold: 0` (always review when `--review` specified). Add config option for "auto-review if > N lines".

### 4. Conflict detection

How to detect merge conflicts early?

**Current**: `gza merge` fails if conflicts exist.

**Enhancement**: `gza dashboard` could check for conflicts proactively by:
- Running `git merge-tree` against target branch
- Flagging tasks with conflicts in NEEDS ATTENTION

**Recommendation**: Implement proactive conflict detection in dashboard.

### 5. Failed task handling

What should `gza advance` do with failed tasks?

**Options**:
- Skip and flag for attention
- Auto-retry once
- Auto-retry with `--resume` if session exists

**Recommendation**: Skip and flag. Retries should be explicit since failures often need investigation.

---

## CLI Summary

| Command | Description | Phase |
|---------|-------------|-------|
| `gza dashboard` | Holistic view of all tasks | 1 |
| `gza summary` | One-line status | 1 |
| `gza advance <id>` | Progress a task to next step | 2 |
| `gza advance --all` | Progress all eligible tasks | 2 |
| `gza history --since` | Time-filtered history | 3 |
| `gza changelog` | Generate change summary | 3 |
| `gza ps --detail` | Detailed worker status | 4 |
| `gza daemon` | Long-running worker | 5 |

---

## Configuration Summary

```yaml
# gza.yaml
workflow:
  # PR creation
  auto_pr: false           # true, false, or threshold number

  # Review behavior
  auto_review: false       # Existing flag
  review_threshold: 50     # Lines changed to trigger review

  # Merge behavior
  auto_merge: false        # true or false
  merge_strategy: squash   # squash, merge, rebase

  # Conflict handling
  check_conflicts: true    # Proactive conflict detection
```
