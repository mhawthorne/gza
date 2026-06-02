# Learnings Strategies

## Overview

The learnings system externalizes context that would naturally accumulate in a long-running session — the tribal knowledge about how to work in this repo. Today, learnings are generated from a single source (recent task summaries). This spec introduces pluggable **strategies** for generating learnings, so we can experiment with different approaches and compare their output quality.

## Motivation

The current system feeds task `output_content` (summaries/reports) to an LLM and asks it to extract patterns. This has two problems:

1. **Weak signal**: Summaries are what the agent chose to report *up* to the user, not what it actually learned. The interesting stuff — wrong approaches tried, test failures diagnosed, review feedback addressed — lives in the provider logs or in task lineage relationships, not in the summary.

2. **No lineage awareness**: Each task is treated independently. But the highest-value learnings come from the *corrections* in review/improve cycles: what did the reviewer flag that the original implement didn't get right? That delta is the learning.

We want to try different approaches to extracting learnings without committing to one. The system should support multiple named strategies that can be run independently and compared.

## Design

### Strategy Interface

A learnings strategy is a callable that takes a store, config, and window size, and returns categorized learnings:

```python
class LearningsStrategy(Protocol):
    """Protocol for learnings generation strategies."""

    name: str
    description: str

    def generate(
        self,
        store: SqliteTaskStore,
        config: Config,
        window: int,
        max_items: int,
    ) -> CategorizedLearnings:
        """Generate learnings from task history."""
        ...
```

### Built-in Strategies

#### 1. `summaries` (current behavior)

**Input**: `task.output_content` from the last N completed tasks.

**How it works**: Feeds task summaries/reports to an LLM with the existing incremental update prompt. Falls back to regex bullet extraction on failure.

**Strengths**: Simple, cheap, works with any task type.

**Weaknesses**: Only sees what the agent chose to report. Misses process insights buried in logs. No awareness of task relationships.

#### 2. `lineage`

**Input**: Task chains (implement → review → improve → review → ...) reconstructed from `based_on`/`depends_on` relationships.

**How it works**:

1. Find recent completed root tasks (implement/task) that have review children.
2. For each root, walk the lineage: collect the implement summary, each review's findings, and each improve's summary.
3. Feed the chain to the LLM with a prompt focused on extracting corrections: "What did the review flag that the implementation got wrong? What patterns would prevent this feedback in the future?"
4. Aggregate corrections across chains into learnings.

**Strengths**: Directly captures the "you got this wrong, fix it" signal. Learnings are grounded in actual review feedback, not self-reported summaries. Naturally surfaces recurring issues (if 3 different reviews flag missing test coverage, that's a strong learning).

**Weaknesses**: Only works for tasks that went through review cycles. Misses insights from tasks that succeeded on the first try. More expensive (more tokens per chain).

**Lineage reconstruction**:

```python
def _collect_chains(store: SqliteTaskStore, window: int) -> list[list[Task]]:
    """Find recent implement→review→improve chains."""
    # Get recent root tasks (implement/task type, no based_on parent)
    roots = store.get_recent_completed(limit=window)
    roots = [t for t in roots if t.task_type in ("implement", "task") and t.based_on is None]

    chains = []
    for root in roots:
        chain = [root]
        children = store.get_lineage_children(root.id)
        # Walk the chain: review, improve, review, ...
        for child in children:
            if child.status == "completed" and child.task_type in ("review", "improve"):
                chain.append(child)
        if len(chain) > 1:  # Only include chains with at least one review/improve
            chains.append(chain)
    return chains
```

**LLM prompt focus**:

```
You are analyzing task chains to extract learnings for a software project.
Each chain shows an implementation followed by review feedback and improvements.

Your goal: identify patterns in the review corrections that would help future
implementations get it right the first time, reducing the number of review
cycles needed.

For each chain, look at:
- What did each review flag? (missing tests, wrong patterns, missing docs, etc.)
- What did each improve task have to fix?
- Would a general rule have prevented this feedback?
- How many review cycles did this chain require? What would have reduced that number?

Note: reviews tend to surface a limited number of must-fix issues per pass.
Later reviews are not lower quality — they surface issues that were always there
but got crowded out by earlier findings. Treat all review cycles with equal weight.

## Task Chains
{for each chain: root prompt/summary → review #1 findings → improve #1 summary → review #2 findings → ...}
{note: "This chain required N review/improve cycles."}

## Instructions
Extract learnings that would reduce the number of review cycles needed.
Focus on recurring correction patterns across chains, not one-off issues.
...
```

#### 3. `logs`

**Input**: Provider log files (JSONL conversation transcripts).

**How it works**:

1. For each recent task, read its provider log file (path stored in `task.log_file`).
2. First pass (per-task, cheap model): compress each log into a ~500-1000 char process summary capturing key events — errors encountered, wrong approaches tried and abandoned, retries, eventual solutions, patterns discovered by reading code.
3. Second pass (aggregate): feed all compressed summaries to the learnings LLM with a prompt focused on recurring process patterns across tasks.

**Strengths**: Richest signal source. Captures process knowledge that never makes it into summaries or reviews — the agent trying `pytest` before learning it needs `uv run pytest`, or trying to edit a file before reading it. These patterns repeat across tasks but are invisible to other strategies.

**Weaknesses**: Two-pass approach adds latency and cost. Log format may vary across providers. Quality depends on the first-pass compression retaining the right details.

### Strategy Registry

```python
STRATEGIES: dict[str, LearningsStrategy] = {}

def register_strategy(strategy: LearningsStrategy) -> None:
    STRATEGIES[strategy.name] = strategy

def get_strategy(name: str) -> LearningsStrategy:
    if name not in STRATEGIES:
        raise ValueError(f"Unknown learnings strategy: {name}. Available: {', '.join(STRATEGIES)}")
    return STRATEGIES[name]
```

## CLI Changes

### `gza learnings update`

Add `--strategy` flag:

```bash
# Use default strategy (from config or 'summaries')
gza learnings update

# Use a specific strategy
gza learnings update --strategy lineage

# Compare strategies: run both and write to separate files
gza learnings compare --window 25
# Writes:
#   .gza/learnings-summaries.md
#   .gza/learnings-lineage.md
# Prints a brief diff summary to stdout
```

### `gza learnings compare`

New subcommand for side-by-side comparison:

```bash
# Compare all registered strategies over the last N tasks
gza learnings compare --window 25

# Compare specific strategies
gza learnings compare --strategies summaries,lineage --window 25

# Write to custom output dir
gza learnings compare --output-dir /tmp/learnings-compare
```

Output: one file per strategy named `.gza/learnings-{strategy}.md`, plus a summary printed to stdout showing item counts and topic overlap.

## Configuration

```yaml
# gza.yaml
learnings_strategy: summaries    # default strategy for auto-regeneration (or: lineage, combined)
learnings_max_items: 50
learnings_window: 25
learnings_interval: 5            # auto-update every N completed tasks (default: 5)
```

When using `combined` or running multiple strategies, consider increasing `learnings_interval` (e.g., 20-40) to offset the higher cost of multiple LLM calls per regeneration.

## Implementation Plan

### Phase 1: Strategy abstraction

1. Define `LearningsStrategy` protocol in `learnings.py`
2. Extract current logic into `SummariesStrategy` class
3. Add strategy registry with `get_strategy()` / `register_strategy()`
4. Wire `--strategy` flag through `gza learnings update`
5. `regenerate_learnings()` delegates to the configured strategy
6. All existing tests pass unchanged (default strategy is `summaries`)

### Phase 2: Lineage strategy

1. Implement `_collect_chains()` to reconstruct task lineage
2. Build lineage-specific LLM prompt focused on review corrections
3. Implement `LineageStrategy` class
4. Register as `lineage` strategy
5. Add `gza learnings compare` subcommand
6. Test with real task history, compare output quality

### Phase 3: Combined strategy + aggregation

Run multiple strategies and aggregate results:

1. Implement `CombinedStrategy` that runs a configurable list of sub-strategies
2. Each sub-strategy produces its own `CategorizedLearnings`
3. Feed all results to a final LLM aggregation pass that merges, deduplicates, and selects the best items within `max_items`
4. Register as `combined` strategy
5. Configuration:

```yaml
learnings_strategy: combined
learnings_combined_strategies:   # which strategies to aggregate (default: all registered)
  - summaries
  - lineage
```

The aggregation LLM call adds cost, but running learnings less frequently (every 20-40 tasks instead of every 5) offsets this. The trade-off is higher quality per regeneration at lower frequency.

### Phase 4: Log-based strategy

1. Build log parser to extract conversation transcripts from JSONL provider logs
2. Implement per-task log summarization (compress each task's log to key events: errors, retries, approach changes, discoveries)
3. Build logs-specific LLM prompt focused on process patterns: "What did the agent try that didn't work? What did it eventually learn? What recurring mistakes appear across tasks?"
4. Implement `LogsStrategy` class
5. Register as `logs` strategy
6. Test with real logs, compare against summaries and lineage output

**Cost management**: Logs are large (50-200KB per task). To keep costs reasonable:
- Summarize each task's log individually first (cheaper model), producing a compressed ~500-1000 char process summary per task
- Feed the compressed summaries (not raw logs) to the learnings LLM
- This two-pass approach adds latency but keeps the final learnings call within normal token budgets

## Resolved Questions

### 1. Lineage depth
**Decision**: No cap. Include full chains with equal weight.

Later review cycles are not lower quality — reviews tend to surface a limited number of must-fix issues per pass. A chain that took 4 cycles doesn't mean the later reviews found nitpicks; it means the first review couldn't surface everything at once. Each pass peels back a layer.

The number of cycles itself is a signal: the prompt should note how many cycles a chain took and ask "what patterns would have reduced that number?"

### 2. Log-based strategy feasibility
**Decision**: Implement as Phase 4, after lineage and combined.

The primary use case (repetitive agent errors like `pytest` vs `uv run pytest`) is real. Just extracting tool call failures isn't sufficient — the agent figures it out eventually, so isolated error/retry pairs lack context. The value is in understanding the full conversational flow: why the agent went down a wrong path, what it tried, what eventually worked. A two-pass approach (compress each log first, then extract learnings from compressed summaries) keeps costs manageable.

### 3. Strategy-specific max_items
**Decision**: Same `max_items` for all strategies for now. Revisit later if needed.

The focus is on being able to use and compare different strategies. Per-strategy caps are a tuning knob we can add once we understand how each strategy's output characteristics differ.

### 4. Strategy combination
**Decision**: Support running multiple strategies and aggregating via a final LLM pass.

Rather than picking one best strategy, run all configured strategies and merge results. This is more expensive per regeneration but produces higher quality. Offset cost by running learnings less frequently (increase `learnings_interval` to 20-40).

This also means individual strategies don't need to be perfect — they just need to surface different kinds of signal, and the aggregation step handles dedup and selection.
