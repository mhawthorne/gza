# Review-Improve Loop

## Overview

This spec describes an automated review-improve loop that iterates until a review passes or a maximum cycle count is reached. This builds on the `improve` task type but internalizes the loop within a single task execution.

## Motivation

The manual workflow for getting code review-approved requires multiple commands:

```bash
gza work          # implement
gza work          # review → CHANGES_REQUESTED
gza improve 29    # improve
gza work          # review → CHANGES_REQUESTED
gza improve 29    # improve again
gza work          # review → APPROVED
```

For autonomous operation, we want:

```bash
gza work          # implement + review + improve loop → APPROVED
```

This enables "fire and forget" task execution where the agent iterates until the code meets quality standards.

## Modes of Operation

### Manual Mode (Current + Improve Task)

Separate tasks for each phase, requiring human intervention between steps:

```
implement #29 → [human runs review] → review #30 → [human runs improve] → improve #31 → ...
```

Use cases:
- Human wants to review the review before proceeding
- Human wants to provide additional guidance for improvements
- Complex changes where agent judgment may be insufficient

### Auto Mode (This Spec)

Internal loop within a single task execution:

```
implement #29 (--auto-review)
    ├── [implementation]
    ├── [review cycle 1] → CHANGES_REQUESTED
    ├── [improve cycle 1]
    ├── [review cycle 2] → CHANGES_REQUESTED
    ├── [improve cycle 2]
    ├── [review cycle 3] → APPROVED
    └── status: completed
```

Use cases:
- Routine tasks where review feedback is straightforward
- Batch processing of many tasks overnight
- High confidence in agent's ability to address feedback

## User Interface

### CLI

```bash
# Enable auto-review loop for a task
gza add "Add feature X" --type implement --auto-review

# With custom max cycles (default: 3)
gza add "Add feature X" --type implement --auto-review --max-cycles 5

# Shorthand
gza add "Add feature X" -t implement -a
```

### Configuration

```yaml
# gza.yaml
defaults:
  auto_review: false
  max_review_cycles: 3

task_types:
  implement:
    auto_review: true      # Enable by default for implement tasks
    max_review_cycles: 3
```

## Data Model

### New Task fields

```python
@dataclass
class Task:
    # ... existing fields ...

    auto_review: bool = False       # Enable review-improve loop
    max_review_cycles: int = 3      # Max iterations before stopping
    review_cycle: int = 0           # Current cycle number (0 = not started)
    final_verdict: str | None = None  # APPROVED, CHANGES_REQUESTED, MAX_CYCLES_REACHED
```

### Review artifacts

Each review cycle produces an artifact, stored with cycle number:

```
.gza/reviews/
  20260129-task-29-review-1.md   # Cycle 1: CHANGES_REQUESTED
  20260129-task-29-review-2.md   # Cycle 2: CHANGES_REQUESTED
  20260129-task-29-review-3.md   # Cycle 3: APPROVED
```

These are NOT separate tasks - they're artifacts of the implementation task's execution.

## Runner Behavior

### Loop execution

```python
def _run_with_auto_review(task: Task, config: Config, provider: Provider, ...) -> int:
    # Phase 1: Implementation
    result = _run_implementation(task, ...)
    if result.exit_code != 0:
        return result.exit_code

    # Phase 2: Review-Improve Loop
    for cycle in range(1, task.max_review_cycles + 1):
        store.update_review_cycle(task.id, cycle)

        # Run review
        review_content, verdict = _run_review_phase(task, cycle, ...)

        # Save review artifact
        review_path = f".gza/reviews/{task.task_id}-review-{cycle}.md"
        _save_review(review_path, review_content)

        if verdict == "APPROVED":
            store.update_final_verdict(task.id, "APPROVED")
            return 0

        if cycle == task.max_review_cycles:
            store.update_final_verdict(task.id, "MAX_CYCLES_REACHED")
            print(f"Max review cycles ({task.max_review_cycles}) reached without approval")
            return 0  # Not a failure, just needs attention

        # Run improve phase
        result = _run_improve_phase(task, cycle, review_content, ...)
        if result.exit_code != 0:
            return result.exit_code

    return 0
```

### Review phase

The review phase runs within the same session/context:

1. Switch to "review mode" - read-only, no code changes
2. Generate diff against default branch
3. Compare against plan (if exists)
4. Write review to artifact file
5. Parse verdict from review content

### Improve phase

The improve phase addresses the just-completed review:

1. Read the review artifact from current cycle
2. Make code changes to address feedback
3. Commit changes with message: `Address review feedback (cycle {n})`

### Verdict parsing

Reviews must include a verdict line that can be parsed:

```markdown
**Verdict: APPROVED**
**Verdict: CHANGES_REQUESTED**
**Verdict: NEEDS_DISCUSSION**
```

Parser:
```python
def _parse_verdict(review_content: str) -> str:
    match = re.search(r'\*\*Verdict:\s*(APPROVED|CHANGES_REQUESTED|NEEDS_DISCUSSION)\*\*', review_content)
    if match:
        return match.group(1)
    return "CHANGES_REQUESTED"  # Default to requesting changes if unclear
```

If `NEEDS_DISCUSSION` is returned, treat as `MAX_CYCLES_REACHED` - stop looping, needs human.

## Task Status

### Final states

| `final_verdict` | Meaning |
|-----------------|---------|
| `APPROVED` | Review passed, code is ready |
| `CHANGES_REQUESTED` | Stopped mid-loop (timeout, error) |
| `MAX_CYCLES_REACHED` | Hit max cycles without approval |
| `NEEDS_DISCUSSION` | Review raised questions needing human input |
| `None` | Loop not yet complete |

### Status display

```bash
$ gza status
  ✓ 29. implement Add feature X    completed  APPROVED (3 cycles)
  ✓ 30. implement Add feature Y    completed  MAX_CYCLES_REACHED (3 cycles)
  → 31. implement Add feature Z    in_progress (cycle 2/3)
```

## Interaction with Manual Mode

### Escaping to manual

If auto-review hits `MAX_CYCLES_REACHED`, the user can continue manually:

```bash
$ gza status
  ✓ 29. implement Add feature X    completed  MAX_CYCLES_REACHED (3 cycles)

# Read the final review
$ cat .gza/reviews/20260129-task-29-review-3.md

# Create manual improve task to address remaining issues
$ gza improve 29 --review
```

The `improve` task uses the last review artifact as context.

### Forcing approval

If user determines the code is actually fine despite `CHANGES_REQUESTED`:

```bash
$ gza edit 29 --verdict APPROVED
```

This overrides `final_verdict` without running another review.

## Cost and Performance

### Model selection

Review phases can use a cheaper model:

```yaml
task_types:
  implement:
    model: opus
    review_model: sonnet  # Use for review phases within auto-review loop
```

### Token management

Each review cycle adds to context. Consider:
- Summarizing previous reviews instead of including full content
- Limiting review scope to files changed since last review
- Fresh sessions for each improve phase (vs. continuing same session)

### Timeout handling

```yaml
task_types:
  implement:
    timeout_minutes: 60
    review_timeout_minutes: 10  # Per review cycle
    improve_timeout_minutes: 30  # Per improve cycle
```

If a phase times out, save progress and mark as needing attention.

## Commit Strategy

### During loop

Each improve cycle creates a commit:

```
* abc123 Address review feedback (cycle 2)
* def456 Address review feedback (cycle 1)
* 789xyz Add feature X - initial implementation
```

### After approval

Commits are NOT auto-squashed. The user can squash when merging if desired:

```bash
gh pr merge --squash
```

This preserves the iteration history for those who want it.

## Example Workflow

```bash
# 1. Create task with auto-review
$ gza add "Add user authentication" --type implement --auto-review --max-cycles 3
# Creates task #29

# 2. Run (fire and forget)
$ gza work
Running task #29: Add user authentication
  [1/3] Implementing...
  [1/3] Implementation complete
  [1/3] Running review...
  [1/3] Review: CHANGES_REQUESTED - missing input validation
  [2/3] Improving...
  [2/3] Running review...
  [2/3] Review: CHANGES_REQUESTED - add rate limiting
  [3/3] Improving...
  [3/3] Running review...
  [3/3] Review: APPROVED
✓ Task #29 completed (APPROVED after 3 cycles)

# 3. Check artifacts
$ ls .gza/reviews/
20260129-task-29-review-1.md
20260129-task-29-review-2.md
20260129-task-29-review-3.md

# 4. View final review
$ cat .gza/reviews/20260129-task-29-review-3.md
# ... APPROVED ...
```

## Open Questions

### 1. Session continuity

Should the entire loop run in one session, or start fresh for each phase?

**Options**:
- Single session: Better context retention, but tokens accumulate
- Fresh per phase: Cleaner, but loses nuanced context
- Hybrid: Same session for review, fresh for improve

**Recommendation**: Start fresh for each improve phase, passing explicit context. Reviews are stateless analysis.

### 2. Review scope

Should cycle N review only changes from cycle N-1, or the full diff?

**Recommendation**: Always review full diff against default branch. The reviewer shouldn't need to track incremental changes.

### 3. NEEDS_DISCUSSION handling

If a review returns `NEEDS_DISCUSSION`, should we:
- Stop immediately and require human intervention?
- Try one more improve cycle hoping to resolve?
- Ask the user via a hook?

**Recommendation**: Stop immediately. `NEEDS_DISCUSSION` indicates the agent recognizes it needs human judgment.

### 4. Relationship to `improve` task type

With auto-review loops, is the `improve` task type still needed?

**Yes**, because:
- Manual escape hatch when auto-review needs human guidance
- Explicit intervention for complex cases
- Backward compatibility with existing workflows
- Some users may prefer manual control

### 5. Retry behavior

If a task fails mid-loop (e.g., during cycle 2 improve phase):

```bash
$ gza retry 29
```

Should it:
- Restart from the beginning?
- Resume from where it failed?

**Recommendation**: Resume from current cycle. The implementation and earlier cycles are committed, so restart from the failed phase.
