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

### Core Concept: Time-Windowed Learnings

Generate a `.gza/learnings.md` file from recent completed tasks. This file contains extracted patterns, decisions, and conventions that inform future tasks.

**Key properties:**
- **Bounded size**: Only reflects last N completed tasks (default: 15)
- **Auto-regenerated**: Updates periodically to stay fresh
- **Manually editable**: Users can curate/prune entries
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

**Extraction approach — LLM summarization as a real task via Provider.run():**

The `learn` task type is a first-class non-code task (like `plan`, `explore`, `review`) that runs through the standard runner infrastructure. This gives us:

- **DB tracking** — cost, duration, tokens, success/failure per learnings run
- **Log files** — full execution logs for debugging prompt quality
- **Retry** — failed learnings tasks can be resumed via existing mechanisms
- **Observability** — `gza history --type learn` and `gza stats --type learn`
- **Provider-agnostic** — works with Claude, Codex, Gemini via existing provider system

Model selection uses the existing `task_types` config:

```yaml
# gza.yaml — configure model for learnings extraction
task_types:
  learn:
    model: claude-haiku-4-5-20251001  # cheap model recommended
```

If unconfigured, falls back to the project's default model.

**Task creation and execution:**

When learnings regeneration triggers (every N completed tasks or on-demand), the system:
1. Creates a `learn` task in the DB with a summarization prompt
2. Runs it through the standard non-code task runner (`_run_non_code_task`)
3. The task's `output_content` contains the LLM's learnings output
4. Parses bullet points from the output and writes `.gza/learnings.md`

```python
def _run_learnings_task(store: SqliteTaskStore, config: Config, recent_tasks: list[Task]) -> list[str] | None:
    """Create and run a learn task, return extracted learnings or None."""
    prompt = _build_summarization_prompt(recent_tasks)
    task = store.add(prompt, task_type="learn", skip_learnings=True)  # avoid circular injection

    # Run through standard runner infrastructure
    exit_code = run_non_code_task(task, config, store)

    if exit_code != 0:
        return None  # triggers regex fallback

    # Re-fetch task to get output_content populated by runner
    task = store.get(task.id)
    if task and task.output_content:
        return _parse_bullet_list(task.output_content)
    return None
```

**Key detail**: Learn tasks set `skip_learnings=True` to avoid circular injection (learnings prompt shouldn't include learnings context).

**Summarization prompt:**

```
You are analyzing completed tasks from a software project to extract
reusable learnings for future agents working on this codebase.

Below are the {N} most recently completed tasks.
{for each: type, prompt, truncated output_content (~1500 chars)}

Distill 5-15 key learnings. Focus on:
- Codebase conventions (naming, structure, idioms)
- Architecture decisions and rationale
- Testing patterns (frameworks, fixtures, assertions)
- Common pitfalls specific to this project
- Workflow preferences (tools, commands)

Do NOT include:
- Task-specific details that don't generalize
- Generic software engineering advice

Format: one per line, starting with "- ". Max 25 words each.
```

**Fallback — regex extraction:**

If the learn task fails (provider error, timeout, bad output), the system falls back to regex-based extraction from task `output_content`. This ensures learnings still work offline or when credentials are unavailable.

**Aggregation logic:**

```python
def regenerate_learnings(store: SqliteTaskStore, config: Config, window: int = 15):
    """Regenerate learnings.md from recent completed tasks."""
    recent_tasks = store.get_recent_completed(limit=window)

    # Try LLM summarization via learn task
    learnings = _run_learnings_task(store, config, recent_tasks)

    # Fall back to regex extraction if learn task failed
    if learnings is None:
        learnings = _extract_learnings_regex(recent_tasks)

    learnings = _dedupe(learnings)
    content = _format_learnings_markdown(learnings, len(recent_tasks))

    learnings_path = config.project_dir / ".gza" / "learnings.md"
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
  learn:
    model: claude-haiku-4-5-20251001  # cheap model for learnings extraction
```

Or with provider-scoped config:

```yaml
providers:
  claude:
    task_types:
      learn:
        model: claude-haiku-4-5-20251001
```

The `learn` task type is not user-facing — it only controls model resolution for the learnings LLM call. If unconfigured, the project's default model is used.

Auto-regeneration parameters are constants in `learnings.py`:
- `DEFAULT_LEARNINGS_WINDOW = 15` — number of recent tasks to generate from
- `AUTO_LEARNINGS_INTERVAL = 5` — regenerate every N completed tasks

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

## Implementation Phases

### Phase 1: Manual Learnings ✅ COMPLETE

1. ✅ `learnings.md` injection in `build_prompt()` (`prompts/__init__.py`)
2. ✅ `skip_learnings` field on Task model, `--no-learnings` CLI flag
3. ✅ `gza learnings update` and `gza learnings show` commands

### Phase 2: Regex Extraction + Auto-Regeneration ✅ COMPLETE (but low quality)

1. ✅ `regenerate_learnings()` with regex-based bullet/header extraction
2. ✅ Auto-regeneration every 5 completed tasks (`maybe_auto_regenerate_learnings`)
3. ✅ Delta tracking (added/removed/retained counts) + history log (`.gza/learnings_history.jsonl`)
4. ✅ Deduplication (case-insensitive stable dedupe)

**Problem**: Regex extraction produces low-quality output. Markdown headers become meaningless "Prefer following documented X conventions" entries. Actual codebase knowledge is lost.

### Phase 3: LLM Summarization as Real Task 🔜 NEXT

**Goal**: Replace regex extraction with LLM-powered summarization running as a proper `learn` task through the standard runner infrastructure.

1. Add `learn` as a recognized task type in the runner's non-code task path
2. Add `_run_learnings_task()` to `learnings.py` — creates a `learn` task, runs it via provider, parses output
3. Modify `regenerate_learnings()` to try learn task first, fall back to regex
4. Learn tasks tracked in DB — cost, duration, tokens, success/failure all visible via `gza history`/`gza stats`
5. Failed learn tasks can be retried via existing resume mechanisms
6. Model configured via `task_types.learn.model` (existing config infrastructure)
7. Learn tasks set `skip_learnings=True` to avoid circular learnings injection

**Validation**: Run `gza learnings update`, compare output quality vs regex. Check `gza history --type learn` shows the task.

### Phase 4: Refinements

**Future enhancements** (not required for initial release):

- **Semantic search**: Only inject relevant learnings (requires embeddings)
- **Multiple scopes**: Per-group learnings in addition to global
- **Learning retirement**: Track which learnings are actually used, prune unused ones
- **User feedback**: `gza learnings like <id>` / `gza learnings hide <id>` to curate

---

## Open Questions

### 1. Categorization Strategy

How to organize learnings into sections?

**Option A**: LLM categorizes into predefined categories
```python
CATEGORIES = ["Testing", "Code Style", "Architecture", "Git Workflow", "Common Pitfalls"]
```

**Option B**: LLM generates categories dynamically based on content

**Option C**: No categorization, just flat list

**Recommendation**: Start with Option C (flat list), add categorization in Phase 4 if file gets large.

### 2. Deduplication

How to avoid duplicate learnings?

**Option A**: LLM-based similarity detection
- Compare new learning to existing ones
- Skip if semantically similar (>80% similarity)

**Option B**: Simple string matching
- Lowercase + normalize whitespace
- Skip exact duplicates only

**Option C**: No deduplication, rely on time-window to naturally prune

**Recommendation**: Start with Option B (exact duplicates only), add Option A if accumulation becomes noisy.

### 3. Regeneration Frequency

What's the right balance?

- **Too frequent** (every task): Expensive, learnings churn too fast
- **Too infrequent** (every 50 tasks): Stale learnings, doesn't adapt to recent changes

**Recommendation**: Default `regenerate_every: 5`, make configurable. Users can tune based on their workflow.

### 4. Cost Management

Learning extraction uses a single LLM call per regeneration (consolidation approach, not per-task).

**Estimated cost** (using Haiku at ~$0.001/call): ~$0.001 per regeneration

For 100 tasks with `regenerate_every: 5` = 20 regenerations = ~$0.02 total

**Mitigation**: Use cheapest model via `task_types.learn.model`, single consolidation call instead of per-task extraction, regex fallback when LLM is unavailable.

### 5. Learning Quality

How to ensure extracted learnings are high-quality?

**Problems**:
- LLM may extract trivial patterns ("use print statements for debugging")
- May extract task-specific details that don't generalize
- May miss important implicit patterns

**Mitigations**:
- Refine extraction prompt with examples of good vs bad learnings
- Manual review workflow: `gza learnings review` shows proposed additions before saving
- User can always manually edit `.gza/learnings.md`

**Recommendation**: Start with simple extraction, iterate on prompt quality based on real usage.

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

### Phase 3 (LLM Summarization as Real Task)
- [ ] Add `learn` as recognized non-code task type in runner
- [ ] Add `_run_learnings_task()` to learnings.py — creates task, runs via provider, parses output
- [ ] Build summarization prompt with task metadata + truncated output_content
- [ ] Modify `regenerate_learnings()` to try learn task first, fall back to regex
- [ ] Set `skip_learnings=True` on learn tasks to avoid circular injection
- [ ] Learn tasks tracked in DB — visible via `gza history --type learn` and `gza stats --type learn`
- [ ] Add retry logic for failed learn tasks
- [ ] Add tests for LLM path + fallback (tests/test_learnings.py)
- [ ] Update AGENTS.md with `task_types.learn` documentation
