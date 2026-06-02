# Learning Accumulation

## Overview

This spec describes a system for accumulating and reusing knowledge across task executions. Instead of each task starting fresh, tasks can benefit from patterns, decisions, and insights discovered in previous work.

## Motivation

**Current limitation**: Each task gets a fresh conversation with no memory of past tasks (except via explicit `based_on`/`depends_on` chains). This means:
- Agents rediscover the same patterns repeatedly
- Context about project conventions must be manually specified each time
- Good decisions from task N don't inform task N+1

**Desired outcome**: Tasks benefit from accumulated project knowledge without:
- Unbounded context growth
- Irrelevant noise injection
- Stale or contradictory learnings

## Design

### Core Concept: Incrementally Updated Learnings

Maintain a `.gza/learnings.md` file that accumulates project knowledge over time. Rather than regenerating from scratch each cycle, the LLM receives the current learnings alongside recent task outputs and produces an updated version — adding new patterns, revising stale ones, and preserving stable knowledge that hasn't been contradicted.

**Key properties:**
- **Incremental**: New task outputs refine existing learnings rather than replacing them
- **Stable knowledge persists**: Learnings about untouched areas of the codebase are retained, not discarded
- **Auto-updated**: Runs periodically after every N completed tasks (default: 5)
- **Manually editable**: Users can curate/prune entries — manual edits are preserved as input to the next update
- **Auto-injected**: Included in all task prompts as context

### Learnings File Structure

```markdown
# Project Learnings

Last updated: 2026-02-19 (from 15 recent tasks)

## Testing Patterns
- Use pytest fixtures for database setup to avoid repetition
- Run `uv run pytest tests/ -v` before completing tasks
- Integration tests go in `tests_integration/` with `@pytest.mark.integration`

## Code Style
- Prefer dataclasses over dicts for structured data
- Use type hints for all function parameters
- Keep functions under 50 lines when possible

## Git Workflow
- Never run git commands directly - gza handles commits/branches
- Use `--same-branch` for small follow-up fixes
- Include `Task ID: YYYYMMDD-slug` in all commits

## Architecture Decisions
- SQLite for task storage (not YAML files)
- Rich library for console output with colors
- Docker isolation for task execution (configurable)

## Common Pitfalls
- Don't use `python -m pytest` - always use `uv run pytest`
- Check file exists before editing (use Read tool first)
- Verify line endings are LF, not CRLF
```

### Generation Strategy

**When to regenerate:**

1. **Automatic**: After every 5 completed tasks (configurable via `AUTO_LEARNINGS_INTERVAL`)
2. **On-demand**: Via `gza learnings update` command
3. **Manual**: User directly edits `.gza/learnings.md`

**Extraction approach — LLM summarization as an internal task:**

Learnings regeneration creates an `internal` task and runs it through the standard runner infrastructure. This gives us:

- **DB tracking** — cost, duration, tokens, success/failure per learnings run
- **Log files** — full execution logs for debugging prompt quality
- **Retry** — failed learnings tasks can be resumed via existing mechanisms
- **Observability** — `gza history --type internal` and `gza stats --type internal`
- **Provider-agnostic** — works with Claude, Codex, Gemini via existing provider system

Model selection uses the existing `task_types` config:

```yaml
# gza.yaml — configure model for learnings extraction
task_types:
  internal:
    model: claude-haiku-4-5-20251001  # cheap model recommended
```

If unconfigured, falls back to the project's default model.

**Task creation and execution:**

When learnings regeneration triggers (every N completed tasks or on-demand), the system:
1. Creates an `internal` task in the DB with a summarization prompt
2. Spawns a **background subprocess** to run it via the standard runner — the CLI returns immediately
3. The task's `output_content` contains the LLM's learnings output
4. Parses bullet points from the output and writes `.gza/learnings.md`

```python
def _run_learnings_task(store: SqliteTaskStore, config: Config, recent_tasks: list[Task]) -> list[str] | None:
    """Create an internal task and spawn it in the background."""
    prompt = _build_summarization_prompt(recent_tasks)
    task = store.add(prompt, task_type="internal", skip_learnings=True)

    # Spawn background subprocess using worker-mode entry point.
    # The CLI returns immediately; the learnings task runs asynchronously.
    _spawn_background_worker(config, task.id)
    return None  # Learnings will be written when the background task completes
```

**Non-blocking execution**: The learnings task must never block the foreground CLI command. It spawns as a detached background subprocess using the same worker-mode entry point as `gza work`. The internal task is visible via `gza ps` and `gza history --type internal` while running.

**Key detail**: Internal learnings tasks set `skip_learnings=True` to avoid circular injection (learnings prompt shouldn't include learnings context).

**Summarization prompt:**

The prompt provides both the existing learnings and recent task outputs, asking the LLM to produce an updated version:

```
You are maintaining a knowledge base for a software project. Your job is to
update the project's learnings based on recent completed tasks.

## Current Learnings
{contents of .gza/learnings.md, or "No existing learnings." if empty}

## Recent Completed Tasks (last {N})
{for each: type, prompt, truncated output_content (~1500 chars)}

## Instructions

Update the learnings based on the recent tasks above:
- ADD new patterns, conventions, or pitfalls discovered in recent tasks
- REVISE any existing learnings that are now outdated or wrong based on recent work
- KEEP existing learnings that are still valid, even if not mentioned in recent tasks
- REMOVE learnings only if recent tasks clearly contradict them

Focus on:
- Codebase conventions (naming, structure, idioms)
- Architecture decisions and rationale
- Testing patterns (frameworks, fixtures, assertions)
- Common pitfalls specific to this project
- Workflow preferences (tools, commands)

Do NOT include:
- Task-specific details that don't generalize
- Generic software engineering advice
- Vague platitudes ("write clean code", "test thoroughly")
- Repetitive or near-duplicate entries

Output format: a flat bullet list, one learning per line, starting with "- ".
No headers, no numbering, no sub-lists. Max 25 words per learning.
Each learning should be concrete and actionable — a new developer should
be able to follow it without additional context.
```

**Output parsing:**

When the LLM task succeeds, parse its `output_content` by extracting lines matching `^\s*[-*]\s+(.+)$` (bullet items). Only keep items between 8-160 characters. Do NOT attempt to extract learnings from markdown headers — headers are discarded.

**Fallback — bullet extraction from task outputs:**

If the internal learnings task fails (provider error, timeout, bad output), the system falls back to extracting bullet items from recent task `output_content` fields. Only lines matching bullet syntax (`- ` or `* `) are extracted — markdown headers are discarded entirely. Existing learnings are preserved and new bullet extractions are appended (deduplicated). This ensures learnings still work offline or when credentials are unavailable, though quality will be lower.

**Update logic:**

```python
def regenerate_learnings(store: SqliteTaskStore, config: Config, window: int = 25):
    """Update learnings.md incrementally from recent completed tasks."""
    recent_tasks = store.get_recent_completed(limit=window)
    learnings_path = config.project_dir / ".gza" / "learnings.md"
    existing_learnings = learnings_path.read_text() if learnings_path.exists() else ""

    # Try LLM incremental update
    learnings = _run_learnings_task(store, config, recent_tasks, existing_learnings)

    # Fall back to bullet extraction if LLM failed
    if learnings is None:
        existing_bullets = _extract_existing_file_learnings(learnings_path)
        new_bullets = _extract_bullets_from_tasks(recent_tasks)
        learnings = _dedupe(existing_bullets + new_bullets)

    content = _format_learnings_markdown(learnings, len(recent_tasks))
    learnings_path.write_text(content)
```

### Injection into Task Prompts

**Location**: Append to prompt in `build_prompt()` after spec file but before task type-specific instructions.

```python
def build_prompt(task: Task, config: Config, store: SqliteTaskStore, ...) -> str:
    """Build the prompt for task execution."""
    base_prompt = f"Complete this task: {task.prompt}"

    # Include spec file
    if task.spec:
        # ... existing spec injection logic

    # NEW: Include learnings
    learnings_path = config.project_dir / ".gza" / "learnings.md"
    if learnings_path.exists():
        learnings = learnings_path.read_text()
        base_prompt += f"\n\n{learnings}"

    # Include based_on chain context
    if task.based_on or task.task_type in ("implement", "review"):
        # ... existing context building

    # Task type-specific instructions
    # ...

    return base_prompt
```

**Opt-out**: Tasks can skip learnings injection via flag:

```bash
gza add --no-learnings "one-off experimental task"
```

This adds `skip_learnings: bool` field to Task model.

---

## Configuration

Model selection uses the existing `task_types` config — no new config section needed:

```yaml
# gza.yaml
task_types:
  internal:
    model: claude-haiku-4-5-20251001  # cheap model for learnings extraction
```

Or with provider-scoped config:

```yaml
providers:
  claude:
    task_types:
      internal:
        model: claude-haiku-4-5-20251001
```

The `internal` task type is not user-facing — it only controls model resolution for the learnings LLM call. If unconfigured, the project's default model is used.

### Learnings Parameters

Configurable via `gza.yaml`:

```yaml
# gza.yaml
learnings_window: 25        # Number of recent tasks to include in update prompt (default: 25)
learnings_interval: 5       # Auto-update every N completed tasks (default: 5, 0 to disable)
```

These should be included (commented out) in the default `gza.yaml` template generated by `gza init`, and documented in `docs/configuration.md`.

The `--window` flag on `gza learnings update` overrides `learnings_window` for that invocation.

---

## CLI Commands

### `gza learnings update`

Manually regenerate learnings file:

```bash
# Regenerate from default window (15 tasks)
gza learnings update

# Regenerate from last 30 tasks
gza learnings update --window 30

# Preview without writing
gza learnings update --dry-run
```

### `gza learnings show`

Display current learnings:

```bash
# Show full learnings file
gza learnings show

# Show in pager
gza learnings show --pager
```

### `gza learnings clear`

Delete learnings file (fresh start):

```bash
gza learnings clear
```

---

## Database Schema Changes

Add field to `tasks` table:

```sql
ALTER TABLE tasks ADD COLUMN skip_learnings INTEGER DEFAULT 0;
```

Add table to track regeneration:

```sql
CREATE TABLE IF NOT EXISTS learnings_metadata (
    id INTEGER PRIMARY KEY CHECK (id = 1),  -- Single row
    last_regenerated_at TEXT,
    last_regenerated_from_task_id INTEGER,
    tasks_since_regen INTEGER DEFAULT 0
);
```

---

## Implementation Status

### What exists today

The following infrastructure is in place but producing poor results:

- [x] `.gza/learnings.md` injected into task prompts via `build_prompt()` (`prompts/__init__.py`)
- [x] `skip_learnings` field on Task model, `--no-learnings` CLI flag
- [x] `gza learnings update` and `gza learnings show` commands
- [x] `regenerate_learnings()` with LLM path + regex fallback
- [x] `_run_learnings_task()` creates `internal` task and runs via provider
- [x] Auto-regeneration every 5 completed tasks (`maybe_auto_regenerate_learnings`)
- [x] Delta tracking (added/removed/retained counts) + history log (`.gza/learnings_history.jsonl`)
- [x] Deduplication (case-insensitive stable dedupe)
- [x] Internal tasks tracked in DB, set `skip_learnings=True` to avoid circular injection

### What's broken

1. **Garbage output** — `_extract_learnings_from_output()` has a header extraction path (`_HEADER_RE`) that converts markdown headers into meaningless "Prefer following documented X conventions" strings. This fires on both LLM output and regex fallback output.
2. **Full replacement, not incremental** — `regenerate_learnings()` overwrites learnings from scratch each time. Stable knowledge about untouched parts of the codebase is lost.
3. **Blocking execution** — `maybe_auto_regenerate_learnings()` runs the LLM task synchronously inline after task completion, blocking the CLI for minutes every 5th task.
4. **Weak prompt** — `_build_summarization_prompt()` lacks "Do NOT include" guidance, output format constraints, and doesn't pass existing learnings as context. The LLM returns unstructured markdown that gets mangled by the header regex.
5. **Not configurable** — window size and interval are hardcoded constants, not in gza.yaml or docs.
6. **Fallback destroys existing learnings** — when LLM fails, regex fallback replaces the file entirely instead of preserving existing content.

### Work items 🔜

All items below address the broken state above. They should be implemented together — fixing the prompt without fixing the incremental approach (or vice versa) won't produce good results.

1. **Delete header extraction** — remove the `_HEADER_RE` path from `_extract_learnings_from_output()`. Only keep bullet extraction (`_BULLET_RE`).
2. **Switch to incremental update** — rewrite `_build_summarization_prompt()` to include existing learnings alongside recent tasks, asking the LLM to ADD/REVISE/KEEP/REMOVE rather than regenerate from scratch. See prompt template in Generation Strategy section above.
3. **Improve summarization prompt** — add "Do NOT include" guidance, specify flat bullet format (no headers, no numbering), max 25 words per learning, require concrete/actionable items.
4. **Make learnings task non-blocking** — spawn as detached background subprocess instead of running synchronously inline. Use the same worker-mode entry point as `gza work`. CLI returns immediately after spawning. The background process writes `.gza/learnings.md` on completion.
5. **Make window/interval configurable** — add `learnings_window` (default: 25) and `learnings_interval` (default: 5) to gza.yaml schema. Change `DEFAULT_LEARNINGS_WINDOW` from 15 to 25. Add to default template and docs/configuration.md.
6. **Fix fallback to preserve existing learnings** — when LLM fails, append new bullet extractions to existing learnings (deduplicated) instead of replacing them.

**Validation**: Run `gza learnings update`, verify output is a flat bullet list of concrete learnings with no "Prefer following documented" garbage. Verify existing learnings are preserved/updated, not replaced. Check `gza history --type internal` shows the task. Verify `gza work` returns immediately when learnings regeneration triggers.

### Future enhancements

Not required for the fixes above:

- **Semantic search**: Only inject relevant learnings (requires embeddings)
- **Multiple scopes**: Per-group learnings in addition to global
- **Learning retirement**: Track which learnings are actually used, prune unused ones
- **User feedback**: `gza learnings like <id>` / `gza learnings hide <id>` to curate

---

## Resolved Questions

### 1. Categorization Strategy
**Decision**: Topic headers with bullets (revised from flat list). Learnings are grouped under `## Topic` H2 headers chosen by the LLM (e.g., "Testing Patterns", "Git Workflow"). Longer-term, split into separate files per topic.

### 2. Deduplication
**Decision**: Simple string matching (Option B) as a safety net. With incremental updates, the LLM is also instructed to avoid near-duplicates, so string dedupe is a backstop rather than the primary mechanism.

### 3. Update Frequency
**Decision**: Every 5 completed tasks (`learnings_interval: 5`), configurable via gza.yaml. Set to 0 to disable auto-updates.

### 4. Cost Management
Single LLM call per update. Input is larger now (existing learnings + recent tasks) but still bounded. Use cheapest model via `task_types.internal.model`.

### 5. Learning Quality
**Decision**: Incremental updates address the main quality issue — learnings accumulate over time rather than being regenerated from a small window. The improved prompt with explicit "Do NOT include" guidance and format constraints addresses the garbage output problem.

## Open Questions

### 1. Learnings Growth
With incremental updates, learnings will grow over time since stable knowledge is preserved. Need to decide:
- Should there be a hard cap on number of learnings? (e.g., max 50 items)
- Should the LLM be instructed to consolidate/merge related learnings to keep the list compact?
- At what size does the learnings file become too large for prompt injection?

**Current recommendation**: No hard cap initially. Let the LLM's "REMOVE learnings only if contradicted" instruction naturally keep things reasonable. Monitor file size in practice.

---

## Alternatives Considered

### Alternative 1: Long-Running Agent (One Conversation Per Group)

**Idea**: Resume same conversation across multiple tasks in a group.

**Pros**:
- True continuity, agent "remembers" all previous context
- No need for extraction/summarization

**Cons**:
- Unbounded token growth (cost explosion)
- Session state mismatch (different worktrees, branches)
- Error propagation (bad assumptions compound)
- Provider limitations (Claude `--resume` not designed for this)

**Why rejected**: Too risky, too expensive. Learnings extraction gives us the benefits (knowledge reuse) without the downsides (cost, state drift).

### Alternative 2: Semantic Search with Vector DB

**Idea**: Store all learnings in vector DB, search for relevant ones before each task.

**Pros**:
- Only inject relevant context (no noise)
- Unbounded storage (search finds needles in haystack)
- Natural relevance ranking

**Cons**:
- Added complexity (vector DB dependency)
- Slower task startup (search latency)
- Harder to manually curate
- Overkill for small projects

**Why rejected**: Over-engineered for MVP. Time-windowed approach is simpler and "good enough" for 95% of use cases.

### Alternative 3: Per-Directory Learnings

**Idea**: Separate learning files per directory (backend, frontend, tests, etc.)

**Pros**:
- Domain-specific knowledge scoping
- Smaller context per task

**Cons**:
- Complex injection logic (which file to use?)
- Fragmentation (some learnings apply across domains)
- Manual organization burden

**Why rejected**: Premature optimization. Start with single global file, split later if needed.

---

## Success Metrics

How do we know if learning accumulation is working?

1. **Pattern adoption rate**: Do tasks follow established patterns without being told?
   - Example: If learnings say "use pytest fixtures", do new test tasks use them?

2. **Context efficiency**: Are we explaining less in task prompts?
   - Measure: Average prompt length should decrease over time

3. **Error reduction**: Do tasks make fewer common mistakes?
   - Example: If learnings say "don't run git commands", do tasks still try?

4. **User satisfaction**: Do users find learnings useful?
   - Survey: "Did learnings context help with task quality?" (Yes/No/Unsure)

5. **Cost impact**: Is extraction cost justified by quality improvement?
   - Track: Regeneration cost vs. task success rate / rework rate

---

## Risks and Mitigations

| Risk | Impact | Mitigation |
|------|--------|------------|
| Learnings file grows too large | Token limit exceeded, high cost | Enforce `max_tokens` config, truncate if needed |
| Stale learnings mislead agent | Tasks follow outdated patterns | Time-windowed regeneration keeps it fresh |
| Extraction quality is poor | Noise in learnings file | Manual curation, iterative prompt refinement |
| Users forget to update learnings | Learnings diverge from reality | Auto-regeneration every N tasks |
| Cost of regeneration adds up | Budget concerns for high-volume users | Make regeneration optional, use cheapest model |
| Contradictory learnings confuse agent | Inconsistent behavior | Deduplication, categorization, manual review |

---

## Future Enhancements (Out of Scope)

Ideas for future iterations:

### Group-Aware Learnings

As an enhancement if groups become more heavily used:

1. **Multi-file learnings**: Store learnings per group
   ```
   .gza/learnings/
     default.md      # Ungrouped tasks (time-windowed)
     auth.md         # Auth group learnings
     ui.md           # UI group learnings
     api.md          # API group learnings
   ```

2. **Configurable injection strategy**:
   - `task_group`: Only inject learnings matching task's group (focused context)
   - `both`: Inject default.md + group-specific (comprehensive)
   - `global_only`: Ignore groups, single file (current behavior)

3. **Separate window sizes** for grouped vs ungrouped tasks:
   ```yaml
   learnings:
     default_window_size: 15        # For ungrouped tasks
     group_window_size: 50          # For grouped tasks (or null for unbounded)
     injection_strategy: "task_group"  # or "both", "global_only"
   ```

**Rationale**: Domain-focused groups may benefit from longer history or unbounded learnings, while ungrouped tasks (the default bucket) still need time-windowing to prevent noise accumulation.

### Other Enhancements

4. **Learning attribution**: Track which tasks contributed which learnings (for debugging)
5. **Learning lifecycle**: Mark learnings as "trial", "proven", "retired"
6. **Multi-project learnings**: Share learnings across related projects
7. **Learning templates**: Bootstrap new projects with common learnings
8. **Interactive curation**: `gza learnings review` shows proposed additions, user approves/rejects
9. **Learning analytics**: "Top 10 most-followed learnings", "Learnings that reduced errors"
10. **Negative learnings**: "Don't do X" explicit anti-patterns
11. **Context-aware injection**: Only inject learnings relevant to task type (test tasks get testing learnings)

---

## Example Workflow

### Initial Setup

```bash
# User creates first few tasks manually
gza add "Add user authentication"
gza work

gza add "Write tests for auth"
gza work

# After 5 tasks, learnings auto-generate
# (automatic based on regenerate_every: 5)
```

### Using Learnings

```bash
# .gza/learnings.md now exists with patterns like:
# - Use JWT tokens for authentication
# - Store auth state in SQLite users table
# - Use pytest fixtures for test database setup

# New task benefits from context
gza add "Add password reset feature"
gza work
# ^ Agent sees learnings, follows established auth patterns
```

### Manual Curation

```bash
# User reviews learnings
gza learnings show

# User notices low-quality learning
# Manually edit .gza/learnings.md to remove it

# User adds important pattern manually
echo "- Never expose user emails in API responses" >> .gza/learnings.md
```

### On-Demand Regeneration

```bash
# User completes major refactor, wants fresh learnings
gza learnings update --window 30

# Preview what would be generated
gza learnings update --dry-run
```

---

## Implementation Checklist

### Phase 1-2 (COMPLETE)
- [x] Add `skip_learnings` field to Task model (db.py)
- [x] Add learnings injection to `build_prompt()` (prompts/__init__.py)
- [x] Add `--no-learnings` flag to `gza add` (cli.py)
- [x] Implement regex-based `_extract_learnings_from_output()` (learnings.py)
- [x] Implement `regenerate_learnings()` with delta tracking (learnings.py)
- [x] Add `gza learnings update` and `gza learnings show` commands (cli.py)
- [x] Add auto-regeneration on interval (learnings.py, runner.py)
- [x] Add tests (tests/test_learnings.py)

### Phase 3 (Quality Fixes + Non-Blocking Execution)
- [x] `_run_learnings_task()` creates `internal` task, runs via provider, parses output
- [x] Summarization prompt with task metadata + truncated output_content
- [x] `regenerate_learnings()` tries LLM first, falls back to bullet extraction
- [x] `skip_learnings=True` on internal tasks to avoid circular injection
- [x] Internal tasks tracked in DB — visible via `gza history --type internal`
- [ ] Delete header extraction (`_HEADER_RE` path) from `_extract_learnings_from_output()` — only keep bullet extraction
- [ ] Improve summarization prompt — flat bullets, no headers, max 25 words, concrete/actionable, "Do NOT include" guidance
- [ ] Make learnings task non-blocking — spawn as detached background subprocess instead of synchronous inline execution
- [ ] Add tests for non-blocking behavior + improved extraction (tests/test_learnings.py)
