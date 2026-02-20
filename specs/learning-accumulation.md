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

1. **Automatic**: After every 5 completed tasks
2. **On-demand**: Via `gza learnings update` command
3. **Manual**: User directly edits `.gza/learnings.md`

**Extraction logic:**

```python
def extract_learnings(task: Task, output: str) -> list[str]:
    """Extract learnings from a completed task using LLM."""

    # Use a small/cheap model (Haiku) for extraction
    prompt = f"""
Analyze this completed task and extract 0-3 key learnings that would be useful for future tasks.

Task: {task.prompt}
Type: {task.task_type}

Focus on:
- Reusable patterns (testing, code style, architecture)
- Project-specific conventions (directory structure, naming)
- Common mistakes to avoid
- Tool usage tips (CLI commands, workflows)

Output format: One learning per line, starting with "-"
Be concise (max 20 words per learning).
If nothing significant to learn, output nothing.

Examples:
- Use pytest fixtures for database setup to avoid repetition
- Never run git commands - gza handles all git operations
- Check file exists with Read tool before using Edit tool
"""

    learnings = llm_extract(prompt, model="haiku")
    return [l.strip() for l in learnings.split('\n') if l.strip().startswith('-')]
```

**Aggregation logic:**

```python
def regenerate_learnings(store: SqliteTaskStore, config: Config, limit: int = 15):
    """Regenerate learnings.md from recent completed tasks."""

    recent = store.get_recent_completed(limit=limit, status="completed")

    # Extract learnings from each task
    all_learnings = []
    for task in recent:
        learnings = extract_learnings(task, task.output_content or "")
        all_learnings.extend(learnings)

    # Deduplicate and categorize with LLM
    categorized = categorize_learnings(all_learnings)

    # Format as markdown
    content = f"# Project Learnings\n\n"
    content += f"Last updated: {datetime.now().strftime('%Y-%m-%d')} (from {len(recent)} recent tasks)\n\n"

    for category, items in categorized.items():
        content += f"## {category}\n"
        for item in items:
            content += f"{item}\n"
        content += "\n"

    # Write to file
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

Add to `gza.yaml`:

```yaml
learnings:
  # Enable automatic learning accumulation
  enabled: true

  # Number of recent tasks to generate learnings from
  window_size: 15

  # Auto-regenerate every N completed tasks
  regenerate_every: 5

  # Model to use for extraction (cheap model recommended)
  extraction_model: "haiku"

  # Max tokens for learnings context (safety limit)
  max_tokens: 2000
```

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

### Phase 1: Manual Learnings

**Goal**: Prove the concept with manual curation.

1. Add `learnings.md` injection to `build_prompt()`
2. Create `.gza/learnings.md` manually with initial patterns
3. Verify tasks benefit from context
4. Add `--no-learnings` flag

**Validation**: Run 5-10 tasks, observe if they follow established patterns.

### Phase 2: Automatic Extraction

**Goal**: Auto-generate learnings from completed tasks.

1. Implement `extract_learnings()` using Haiku
2. Implement `regenerate_learnings()` with categorization
3. Add `gza learnings update` command
4. Add `gza learnings show` command

**Validation**: Compare auto-generated learnings vs manual. Refine extraction prompt.

### Phase 3: Automatic Regeneration

**Goal**: Keep learnings fresh without manual intervention.

1. Add `learnings_metadata` table
2. Increment counter on task completion
3. Auto-regenerate when `tasks_since_regen >= regenerate_every`
4. Add configuration options to `gza.yaml`

**Validation**: Run 20+ tasks, verify regeneration happens automatically.

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

Learning extraction uses LLM calls. For 15 tasks × extraction = 15 LLM calls per regeneration.

**Estimated cost** (using Haiku at ~$0.001/call): $0.015 per regeneration

For 100 tasks with `regenerate_every: 5` = 20 regenerations = $0.30 total

**Mitigation**: Use cheapest model (Haiku), make regeneration optional.

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

- [ ] Add `skip_learnings` field to Task model (db.py)
- [ ] Add learnings injection to `build_prompt()` (runner.py)
- [ ] Add `--no-learnings` flag to `gza add` (cli.py)
- [ ] Implement `extract_learnings()` function (new file: learnings.py)
- [ ] Implement `regenerate_learnings()` function (learnings.py)
- [ ] Add `gza learnings update` command (cli.py)
- [ ] Add `gza learnings show` command (cli.py)
- [ ] Add `gza learnings clear` command (cli.py)
- [ ] Add `learnings_metadata` table to schema (db.py)
- [ ] Add auto-regeneration logic to task completion hook (runner.py)
- [ ] Add `learnings` section to Config (config.py)
- [ ] Add tests for extraction (tests/test_learnings.py)
- [ ] Add tests for regeneration (tests/test_learnings.py)
- [ ] Add tests for injection (tests/test_runner.py)
- [ ] Document in README.md
- [ ] Add example `.gza/learnings.md` to docs
