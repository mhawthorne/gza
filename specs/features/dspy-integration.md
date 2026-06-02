# DSPy Integration

**Status**: Proposed
**Supersedes**: `specs/llm-judgment-points.md`

## Overview

Replace hand-rolled heuristics at semantic decision points with DSPy modules that provide typed signatures, structured outputs, deterministic fallbacks, and — in a later phase — automatic prompt optimization via MIPROv2.

This spec unifies and extends the judgment-point work proposed in `llm-judgment-points.md` while adding a learnings extraction module and an offline prompt optimization pipeline.

## Motivation

Several places in gza use brittle heuristics (regex, keyword lists, exit codes) to make decisions that require semantic understanding:

| Decision point | Current approach | Limitation |
|---|---|---|
| Branch type inference | Keyword list with regex (`branch_naming.py:7-55`) | Can't classify "Optimize database query for 1M records" as `perf` |
| Review verdict parsing | Regex for `APPROVED\|CHANGES_REQUESTED\|NEEDS_DISCUSSION` (`runner.py:125-136`) | Discards structured feedback (issues, priorities) |
| Task failure analysis | Marker scan for `[GZA_FAILURE:REASON]` (`db.py:528-554`) | Only works when the agent explicitly emits a marker; misses implicit failures |
| Learnings extraction | Bullet regex on output text (`learnings.py:48-65`) | Misses non-bullet insights; extracts noise |
| Learnings deduplication | Case-insensitive exact string match (`learnings.py:77-84`) | "Use pytest fixtures" and "Always use pytest fixtures for setup" are not deduplicated |

### Why DSPy over hand-rolled `claude --print`

The prior spec (`llm-judgment-points.md`) proposed raw subprocess calls to `claude --print` with hand-crafted JSON prompts. DSPy is a better foundation because:

1. **Typed signatures** — input/output fields are declared, not embedded in prompt strings
2. **Structured output parsing** — DSPy handles JSON extraction and validation
3. **Automatic retries with backoff** — built into the DSPy LM abstraction
4. **Prompt optimization** — MIPROv2 can tune instructions from labeled examples
5. **Provider abstraction** — switch between Anthropic, OpenAI, or local models without changing module code

## Design

### Foundation Layer: `src/gza/dspy_modules.py`

A single module file containing all DSPy signatures, modules, and the provider configuration bridge.

#### Provider Configuration

DSPy's LM configuration is bridged from gza's existing `Config` object:

```python
import dspy

def configure_dspy(config: Config) -> None:
    """Configure DSPy LM from gza's provider/model settings."""
    # Map gza provider names to DSPy LM constructors
    provider = config.provider  # "claude", "gemini", etc.
    model = config.model or _default_model_for_provider(provider)

    lm = dspy.LM(
        model=_to_dspy_model_id(provider, model),
        max_tokens=256,   # judgment calls are short
        temperature=0.0,  # deterministic
    )
    dspy.configure(lm=lm)


def _default_model_for_provider(provider: str) -> str:
    """Return a cheap/fast model for judgment calls."""
    return {
        "claude": "claude-haiku-4-5-20251001",
        "gemini": "gemini-2.0-flash",
    }.get(provider, "claude-haiku-4-5-20251001")


def _to_dspy_model_id(provider: str, model: str) -> str:
    """Convert gza provider + model to DSPy model identifier."""
    if provider == "claude":
        return f"anthropic/{model}"
    if provider == "gemini":
        return f"google/{model}"
    return model
```

The key design choice: **judgment calls always use Haiku-class models**, regardless of what the main task uses. A `task_types` override like `review.model: claude-sonnet-4-20250514` only affects the main agent, not the judgment modules. This keeps judgment calls under $0.01 each.

#### Deterministic Fallbacks

Every module has a `fallback()` classmethod that returns a valid output without any LLM call. The calling code follows a consistent pattern:

```python
def classify_branch_type(prompt: str) -> BranchClassification:
    try:
        configure_dspy(config)
        module = BranchClassifier()
        return module(prompt=prompt)
    except Exception:
        return BranchClassifier.fallback(prompt)
```

This guarantees gza works offline, without API keys, or when DSPy is not installed.

#### Optional Dependency

DSPy is an optional dependency. When not installed, all judgment points use their deterministic fallbacks silently:

```python
try:
    import dspy
    HAS_DSPY = True
except ImportError:
    HAS_DSPY = False
```

### Module 1: Branch Type Classification

**Replaces**: keyword list in `branch_naming.py:7-55`

```python
class BranchClassification(dspy.Signature):
    """Classify a coding task into a branch type."""
    prompt: str = dspy.InputField(desc="The task prompt")
    branch_type: str = dspy.OutputField(
        desc="One of: fix, feature, docs, test, perf, refactor, chore"
    )

class BranchClassifier(dspy.Module):
    def __init__(self):
        self.classify = dspy.Predict(BranchClassification)

    def forward(self, prompt: str) -> dspy.Prediction:
        result = self.classify(prompt=prompt)
        # Validate output is in allowed set
        allowed = {"fix", "feature", "docs", "test", "perf", "refactor", "chore"}
        if result.branch_type.lower().strip() not in allowed:
            return self.fallback(prompt)
        result.branch_type = result.branch_type.lower().strip()
        return result

    @staticmethod
    def fallback(prompt: str) -> dspy.Prediction:
        """Deterministic fallback: existing keyword matching."""
        from .branch_naming import _keyword_infer_type
        branch_type = _keyword_infer_type(prompt) or "feature"
        return dspy.Prediction(branch_type=branch_type)
```

**Integration in `branch_naming.py`**:

The existing `infer_type_from_prompt()` function is refactored:
- Current keyword logic is extracted to `_keyword_infer_type()` (private, same code)
- `infer_type_from_prompt()` tries DSPy first, falls back to `_keyword_infer_type()`

```python
def infer_type_from_prompt(prompt: str) -> str | None:
    """Infer branch type, using LLM classification when available."""
    try:
        from .dspy_modules import classify_branch_type
        result = classify_branch_type(prompt)
        return result.branch_type
    except Exception:
        pass
    return _keyword_infer_type(prompt)
```

**Fallback**: Existing keyword matching (preserved as `_keyword_infer_type()`).

### Module 2: Review Verdict Parsing

**Replaces**: regex in `runner.py:125-136`

```python
class ReviewVerdictSignature(dspy.Signature):
    """Extract a structured verdict from a code review."""
    review_content: str = dspy.InputField(desc="The full review markdown")
    verdict: str = dspy.OutputField(
        desc="One of: APPROVED, CHANGES_REQUESTED, NEEDS_DISCUSSION"
    )
    issues: list[str] = dspy.OutputField(
        desc="Specific issues raised, as a list of short strings"
    )
    summary: str = dspy.OutputField(desc="One-line summary of the review")

class ReviewVerdictParser(dspy.Module):
    def __init__(self):
        self.parse = dspy.Predict(ReviewVerdictSignature)

    def forward(self, review_content: str) -> dspy.Prediction:
        # Truncate to avoid token limits on large reviews
        result = self.parse(review_content=review_content[:4000])
        allowed = {"APPROVED", "CHANGES_REQUESTED", "NEEDS_DISCUSSION"}
        if result.verdict.upper().strip() not in allowed:
            return self.fallback(review_content)
        result.verdict = result.verdict.upper().strip()
        return result

    @staticmethod
    def fallback(review_content: str) -> dspy.Prediction:
        """Deterministic fallback: existing regex extraction."""
        import re
        match = re.search(
            r"\*{0,2}Verdict:\s*(APPROVED|CHANGES_REQUESTED|NEEDS_DISCUSSION)\*{0,2}",
            review_content or "",
            re.IGNORECASE,
        )
        verdict = match.group(1).upper() if match else "NEEDS_DISCUSSION"
        return dspy.Prediction(verdict=verdict, issues=[], summary="")
```

**Integration in `runner.py`**:

`_extract_review_verdict()` is updated to try DSPy first. The new structured output (issues list, summary) is returned alongside the verdict for use by downstream code (e.g., auto-generating focused improve tasks).

**Fallback**: Existing regex extraction (preserved inline in `fallback()`).

### Module 3: Failure Analysis

**Replaces**: marker scan in `db.py:528-554`

```python
class FailureAnalysisSignature(dspy.Signature):
    """Analyze why a coding agent task failed."""
    task_output: str = dspy.InputField(desc="Last 3000 chars of task output")
    exit_code: int = dspy.InputField(desc="Process exit code")
    has_changes: bool = dspy.InputField(desc="Whether the task made file changes")
    category: str = dspy.OutputField(
        desc="One of: stuck_loop, partial, already_done, misunderstood, environment, timeout, unknown"
    )
    summary: str = dspy.OutputField(desc="What happened, in one sentence")
    suggestion: str = dspy.OutputField(
        desc="Recommended next step: retry, resume, split, or escalate"
    )

class FailureAnalyzer(dspy.Module):
    def __init__(self):
        self.analyze = dspy.Predict(FailureAnalysisSignature)

    def forward(self, task_output: str, exit_code: int, has_changes: bool) -> dspy.Prediction:
        result = self.analyze(
            task_output=task_output[-3000:],
            exit_code=exit_code,
            has_changes=has_changes,
        )
        return result

    @staticmethod
    def fallback(task_output: str, exit_code: int, has_changes: bool) -> dspy.Prediction:
        """Deterministic fallback: exit code + change heuristics."""
        if exit_code == 124:
            return dspy.Prediction(
                category="timeout", summary="Timed out", suggestion="resume"
            )
        if has_changes:
            return dspy.Prediction(
                category="partial", summary="Partial progress made", suggestion="resume"
            )
        return dspy.Prediction(
            category="unknown",
            summary=f"Exited with code {exit_code}",
            suggestion="retry",
        )
```

**Integration**: Called from the task completion path in `runner.py` when a task fails. The analysis result is stored in the task record and displayed to the user via `gza history`.

**Fallback**: Exit code + has-changes heuristic (current behavior, preserved in `fallback()`).

### Module 4: Learnings Extraction

**Replaces**: bullet regex in `learnings.py:48-65` and case-insensitive dedupe in `learnings.py:77-84`

```python
class LearningsExtractionSignature(dspy.Signature):
    """Extract reusable learnings from a completed coding task."""
    task_prompt: str = dspy.InputField(desc="What the task was asked to do")
    task_output: str = dspy.InputField(desc="Task output/log content")
    learnings: list[str] = dspy.OutputField(
        desc="0-3 reusable patterns, each under 20 words"
    )

class LearningsExtractor(dspy.ChainOfThought):
    """Uses chain-of-thought to reason about what's worth learning."""
    def __init__(self):
        super().__init__(LearningsExtractionSignature)

    @staticmethod
    def fallback(task_prompt: str, task_output: str) -> dspy.Prediction:
        """Deterministic fallback: existing bullet regex extraction."""
        from .learnings import _extract_learnings_from_output
        learnings = _extract_learnings_from_output(task_output)
        return dspy.Prediction(learnings=learnings[:3])
```

#### Semantic Deduplication

**Replaces**: case-insensitive exact match in `learnings.py:77-84`

```python
class SemanticDedupeSignature(dspy.Signature):
    """Determine if two learnings are semantically equivalent."""
    learning_a: str = dspy.InputField()
    learning_b: str = dspy.InputField()
    are_duplicates: bool = dspy.OutputField(
        desc="True if the learnings convey the same information"
    )

class LearningsDeduplicator(dspy.Module):
    def __init__(self):
        self.compare = dspy.Predict(SemanticDedupeSignature)

    def dedupe(self, learnings: list[str]) -> list[str]:
        """Remove semantic duplicates from a list of learnings."""
        unique: list[str] = []
        for candidate in learnings:
            is_dup = False
            for existing in unique:
                result = self.compare(learning_a=candidate, learning_b=existing)
                if result.are_duplicates:
                    is_dup = True
                    break
            if not is_dup:
                unique.append(candidate)
        return unique

    @staticmethod
    def fallback(learnings: list[str]) -> list[str]:
        """Deterministic fallback: case-insensitive exact match."""
        from .learnings import _dedupe
        return _dedupe(learnings)
```

**Note on cost**: Semantic deduplication is O(n²) in LLM calls. For typical regeneration windows (15 tasks × 3 learnings = 45 items max), this means up to ~1000 pairwise comparisons. To keep costs bounded:
- Only run semantic dedupe during `gza learnings update`, not on every task completion
- Auto-regeneration continues to use the existing exact-match dedupe
- Each pairwise call is tiny (<100 tokens), so even 1000 calls ≈ $0.025 with Haiku

### Module 5: Prompt Template Optimization

**New capability** — not in the prior spec.

#### Concept

Use DSPy's MIPROv2 optimizer to tune the instruction text in `src/gza/prompts/templates/*.txt` based on historical task outcomes. The optimizer searches over instruction variations and selects those that maximize a quality metric derived from task history.

#### Training Data

Build a dataset from the SQLite task store:

```python
def build_optimization_dataset(store: SqliteTaskStore) -> list[dspy.Example]:
    """Build training examples from task history."""
    examples = []
    for task in store.get_all_completed():
        example = dspy.Example(
            task_type=task.task_type,
            prompt=task.prompt,
            # Quality signal from multiple sources
            succeeded=task.status == "completed",
            review_verdict=task.review_verdict,  # if reviewed
            failure_reason=task.failure_reason,   # if failed
        ).with_inputs("task_type", "prompt")
        examples.append(example)
    return examples
```

Quality metric:

```python
def task_quality_metric(example, prediction, trace=None) -> float:
    """Score task outcome quality for optimization."""
    score = 0.0
    if prediction.succeeded:
        score += 0.5
    if prediction.review_verdict == "APPROVED":
        score += 0.5
    elif prediction.review_verdict == "CHANGES_REQUESTED":
        score += 0.1
    if prediction.failure_reason and prediction.failure_reason != "UNKNOWN":
        score -= 0.2
    return score
```

#### Optimization Pipeline

```python
def optimize_prompts(store: SqliteTaskStore, config: Config) -> dict[str, str]:
    """Run MIPROv2 optimization over prompt templates."""
    dataset = build_optimization_dataset(store)
    if len(dataset) < 20:
        raise ValueError(
            f"Need at least 20 completed tasks for optimization, have {len(dataset)}"
        )

    # Split into train/val
    train, val = dataset[:int(len(dataset)*0.8)], dataset[int(len(dataset)*0.8):]

    optimizer = dspy.MIPROv2(
        metric=task_quality_metric,
        num_candidates=10,
        num_trials=50,
    )

    # Optimize each template type independently
    optimized = {}
    for template_name in ["task_with_summary", "task_without_summary", "review", "explore", "plan"]:
        template_path = config.project_dir / f"src/gza/prompts/templates/{template_name}.txt"
        if not template_path.exists():
            continue

        module = PromptTemplateModule(template_path)
        optimized_module = optimizer.compile(module, trainset=train, valset=val)
        optimized[template_name] = optimized_module.instruction

    return optimized
```

#### CLI: `gza optimize-prompts`

```
gza optimize-prompts [--min-tasks 20] [--dry-run] [--template NAME]
```

- `--min-tasks`: Minimum completed tasks required (default: 20)
- `--dry-run`: Show proposed changes without writing
- `--template`: Optimize only a specific template

Output: Shows a diff of the original vs. optimized instruction text for each template, and writes the optimized versions to `src/gza/prompts/templates/`.

**Safeguards**:
- Original templates are backed up to `src/gza/prompts/templates/*.txt.bak`
- `--dry-run` is the default in the initial release (must pass `--apply` to write)
- Optimization requires a minimum task history to avoid overfitting to small samples

## Implementation Order

### Phase 1: Foundation + Branch Classification

**Goal**: Validate DSPy integration with the simplest judgment point.

1. Add `dspy` as optional dependency in `pyproject.toml`
2. Create `src/gza/dspy_modules.py` with provider config bridge and `BranchClassifier`
3. Refactor `branch_naming.py`: extract `_keyword_infer_type()`, update `infer_type_from_prompt()`
4. Add tests: mock DSPy LM, verify fallback behavior

**Validation**: Run `gza add` with diverse prompts, compare branch names before/after.

### Phase 2: Review Parsing + Failure Analysis

**Goal**: Structured data from reviews and failures.

1. Add `ReviewVerdictParser` to `dspy_modules.py`
2. Update `_extract_review_verdict()` in `runner.py`
3. Add `FailureAnalyzer` to `dspy_modules.py`
4. Integrate failure analysis into task completion path
5. Store structured failure data in task record

**Validation**: Process existing review files and failure logs; compare structured output to current regex results.

### Phase 3: Learnings Extraction

**Goal**: Better learning quality and deduplication.

1. Add `LearningsExtractor` and `LearningsDeduplicator` to `dspy_modules.py`
2. Update `learnings.py` to use DSPy extraction when available
3. Add `--semantic-dedupe` flag to `gza learnings update`
4. Preserve existing regex extraction as fallback

**Validation**: Compare auto-extracted learnings (regex vs. DSPy) on 20+ completed tasks. Measure noise reduction.

### Phase 4: Prompt Optimization

**Goal**: Data-driven prompt improvement.

1. Add `optimize-prompts` CLI command
2. Implement dataset builder from task history
3. Implement quality metric
4. Wire up MIPROv2 optimizer
5. Add `--dry-run` and `--apply` modes

**Prerequisite**: Requires accumulated task history (at least 20 completed tasks with review verdicts and/or failure reasons).

**Validation**: A/B compare task success rate and review approval rate before/after optimization.

## Cost Estimates

| Module | Input tokens | Output tokens | Cost per call (Haiku) |
|---|---|---|---|
| Branch classification | ~100 | ~10 | < $0.001 |
| Review verdict parsing | ~2000 | ~100 | < $0.005 |
| Failure analysis | ~1500 | ~50 | < $0.003 |
| Learnings extraction | ~1000 | ~100 | < $0.003 |
| Semantic dedupe (per pair) | ~50 | ~10 | < $0.001 |

A full plan → implement → review → improve cycle adds ~4 judgment calls ≈ **$0.01 overhead** on top of the main agent costs.

Prompt optimization is a one-time batch operation: ~50 trials × ~5 templates = 250 LLM calls ≈ **$0.50-1.00 per optimization run**.

## Testing Strategy

### Unit Tests (no API required)

All modules have deterministic fallbacks. Unit tests verify:

1. **Fallback correctness**: Each `fallback()` classmethod returns valid, typed output
2. **Output validation**: Modules reject invalid LLM outputs and fall back gracefully
3. **Provider config bridge**: `configure_dspy()` maps gza providers correctly
4. **Optional dependency**: When `dspy` is not installed, all judgment points use fallbacks silently

```python
# Example test pattern
def test_branch_classifier_fallback():
    result = BranchClassifier.fallback("Fix login crash on empty password")
    assert result.branch_type == "fix"

def test_branch_classifier_rejects_invalid_output(mock_dspy_lm):
    mock_dspy_lm.return_value = "banana"  # invalid branch type
    result = classify_branch_type("some task")
    assert result.branch_type in VALID_BRANCH_TYPES  # fell back
```

### Integration Tests (API required, `@pytest.mark.integration`)

Run against a real LLM to verify end-to-end behavior:

1. **Classification accuracy**: 20 diverse prompts, verify >90% match expected types
2. **Review parsing accuracy**: 10 real review files, verify verdicts match regex baseline
3. **Learnings quality**: Compare DSPy vs. regex extraction on same task outputs

### Regression Tests

Before/after comparison on historical data:

1. Export current heuristic results for all historical tasks
2. Run DSPy modules on same inputs
3. Flag any regressions (cases where DSPy is worse than heuristic)
4. Acceptable threshold: DSPy must match or beat heuristic on ≥95% of cases

## Configuration

No new `gza.yaml` fields are required. DSPy reuses the existing `provider` and `model` fields. The judgment model is hardcoded to Haiku-class (not configurable) to prevent accidental cost spikes.

Future consideration: if users want to override the judgment model, add:

```yaml
dspy:
  judgment_model: "claude-haiku-4-5-20251001"  # default
  enabled: true  # default, set false to disable all DSPy modules
```

## Dependencies

- `dspy` — optional dependency, installed via `pip install gza[dspy]` or `uv add dspy`
- No other new dependencies

## Risks and Mitigations

| Risk | Impact | Mitigation |
|---|---|---|
| DSPy adds latency to every branch/review operation | Slower `gza add` and `gza work` | Haiku is fast (~200ms); fallbacks are instant; async where possible |
| DSPy library is unstable or has breaking changes | Modules break on upgrade | Pin DSPy version; all modules have fallbacks; optional dependency |
| LLM classification disagrees with user expectations | Wrong branch type, wrong verdict | Always allow manual override; log DSPy vs. fallback results for comparison |
| Prompt optimization overfits to small datasets | Worse prompts after optimization | Require minimum 20 tasks; `--dry-run` default; backup original templates |
| Cost creep from many small LLM calls | Unexpected bills | Haiku-class only; cost estimates in `gza stats`; budget cap per session |

## Open Questions

1. **Should DSPy judgment calls be async?** Branch classification happens during `gza add`, which is interactive. Review parsing and failure analysis happen during `gza work`, which is already long-running. Async may not be necessary for the initial implementation.

2. **Should we cache DSPy results?** Branch classification for the same prompt always returns the same result. A simple in-process dict cache would avoid redundant calls. Worth adding if latency becomes noticeable.

3. **How should `gza stats` report DSPy costs?** The judgment calls are cheap but should still be visible. Options: separate line item, or rolled into the task's total cost.
