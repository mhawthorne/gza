# Beads Integration

> **Status: Aspirational** - This spec describes a potential future integration. The beads integration has not been implemented.

## Overview

This spec explores replacing gza's homegrown task concept with [beads](https://github.com/anthropics/beads), a distributed git-backed graph issue tracker designed for AI agents.

## Current State

Gza has a `Task` dataclass in `src/gza/db.py` that combines:
- **Work definition**: prompt, task_type, spec, group
- **Dependencies**: depends_on, based_on, same_branch
- **Execution state**: status, branch, session_id, log_file
- **Metrics**: duration_seconds, num_turns, cost_usd, started_at, completed_at

## Proposed Two-Layer Model

### Beads = The "What" (Work Items)

Beads would own:
- Work descriptions and instructions
- Dependency graphs (blocks, parent-child, waits-for)
- Ready work calculation
- Multi-agent coordination (molecules, gates, slots)
- Git-native sync (SQLite → JSONL → git)

### Gza Task = The "Execution" (Agent Runs)

Gza would retain a slimmer task concept focused on execution:

```python
@dataclass
class Task:
    id: int
    bead_ids: list[str]        # References to beads (e.g., ["bd-a1b2"])
    status: str                 # pending, in_progress, completed, failed
    branch: str
    session_id: str            # Claude session for resume
    log_file: str
    started_at: str
    completed_at: str
    duration_seconds: float
    num_turns: int
    cost_usd: float
```

Fields that move to beads:
- `prompt` → bead description/design_notes
- `task_type` → bead issue_type or labels
- `spec` → bead description or linked bead
- `depends_on`, `based_on` → bead dependency graph
- `group` → bead labels or parent issue

## Writing Beads as Agent Instructions

Beads don't have to be written in traditional "issue style". They can contain imperative implementation instructions:

```yaml
title: Add --verbose flag to status command
description: |
  Implement a --verbose flag for the status command.

  When enabled, show:
  - Task creation timestamp
  - Full branch name
  - Session ID if present

  Add tests in test_cli.py covering both modes.
acceptance_criteria: |
  - [ ] Flag parsing works
  - [ ] Output includes all specified fields
  - [ ] Tests pass
```

### Field Conventions

Options for where to put agent instructions:

1. **description only**: Simple, all instructions in one place
2. **description + acceptance_criteria**: Instructions vs. verification split
3. **design_notes for implementation details**: Keep description high-level
4. **Custom label convention**: `gza:prompt` label marks agent-executable beads

## Benefits

1. **Richer dependency modeling**: Full DAG vs. linear chain
2. **Git-native sync**: Issues travel with code
3. **Multi-agent coordination**: Gates/slots/molecules for complex workflows
4. **Merge-friendly**: Hash IDs handle concurrent work
5. **Cleaner separation**: "What to do" vs. "how it ran"
6. **Re-execution**: Same bead, new task (retry without duplicating work item)
7. **Batching**: One task could execute multiple related beads

## Bridge Workflow

```bash
# Find ready work from beads, execute via gza
bd ready --json | gza work --from-beads

# Or gza queries beads directly
gza work  # Internally calls bd ready
```

## Open Questions

1. **One-to-one or one-to-many?** Does one gza task always map to exactly one bead, or can a task execute multiple beads?

2. **Status sync**: When gza marks a task complete, should it auto-close the bead?

3. **Branch association**: Beads don't have built-in branch tracking. Store branch in bead labels/notes, or only in gza task?

4. **Session resume**: Beads has no session concept. Keep session_id only in gza task layer?

5. **Metrics**: Store execution metrics (cost, turns, duration) in bead comments/notes, or only in gza?

## Migration Path

1. Install beads alongside gza
2. Add `bead_ids` field to Task
3. Create bridge: `gza add --from-bead BD-ID`
4. Gradually move work definition to beads
5. Deprecate prompt/depends_on/etc fields in Task
6. Eventually: `gza work` pulls directly from `bd ready`
