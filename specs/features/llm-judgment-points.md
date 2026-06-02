# LLM Judgment Points

Replace hardcoded heuristics with targeted LLM calls at decision points where
semantic understanding outperforms pattern matching.

## Problem

Several places in gza use brittle heuristics (regex, keyword lists, exit codes)
to make decisions that require understanding natural language or unstructured
output. These break on edge cases and require ongoing maintenance:

- **Branch type inference** (`branch_naming.py:7-55`): keyword list can't classify
  "Optimize database query for 1M records" as `perf`
- **Review verdict parsing** (`cli.py:608-615`): regex extracts `APPROVED` but
  discards the actual review feedback (specific issues, priorities)
- **Task failure analysis** (`runner.py:881-931`): exit codes can't distinguish
  "got stuck in a loop" from "finished but task was already done"
- **Task context assembly** (`runner.py:223-300`): hardcoded if/else per task type
  breaks when new task types are added

## Approach

Add a lightweight `llm.py` module that makes fast, cheap LLM calls (`claude
--print` with Haiku) at these decision points. Each call is a small, focused
prompt that returns structured output. The existing deterministic workflow stays
the same — these are judgment calls injected into the existing flow, not a
redesign.

## Module: `src/gza/llm.py`

```python
"""Lightweight LLM calls for judgment points in gza.

Each function makes a single, focused call to Claude (via `claude --print`)
using Haiku for speed and cost. All functions have deterministic fallbacks
so gza works without network access or API keys.
"""

import json
import subprocess
from dataclasses import dataclass


def _call(prompt: str, timeout: int = 15) -> str | None:
    """Make a quick LLM call via `claude --print`. Returns None on failure."""
    try:
        result = subprocess.run(
            ["claude", "--print", "--model", "haiku"],
            input=prompt,
            capture_output=True,
            text=True,
            timeout=timeout,
        )
        if result.returncode == 0 and result.stdout.strip():
            return result.stdout.strip()
    except (subprocess.TimeoutExpired, FileNotFoundError):
        pass
    return None
```

## 1. Branch Type Inference

**File:** `branch_naming.py`

**Current:** Keyword list with regex matching (40 lines).

**Change:** Try LLM classification first, fall back to keyword matching.

```python
# llm.py

BRANCH_TYPES = ["fix", "feature", "docs", "test", "perf", "refactor", "chore"]

def classify_branch_type(prompt: str) -> str | None:
    """Classify a task prompt into a branch type."""
    response = _call(
        f"Classify this task into exactly one type: {', '.join(BRANCH_TYPES)}.\n"
        f"Task: {prompt}\n"
        f"Reply with just the type, nothing else."
    )
    if response and response.lower().strip() in BRANCH_TYPES:
        return response.lower().strip()
    return None
```

```python
# branch_naming.py - change to infer_type_from_prompt()

def infer_type_from_prompt(prompt: str) -> str | None:
    from .llm import classify_branch_type
    result = classify_branch_type(prompt)
    if result:
        return result
    # existing keyword logic as fallback
    ...
```

**Fallback:** Existing keyword matching (unchanged).

## 2. Review Verdict Parsing

**File:** `cli.py`

**Current:** Regex extracts one of three verdict strings. Discards everything else.

**Change:** Extract verdict AND structured issues list.

```python
# llm.py

@dataclass
class ReviewVerdict:
    verdict: str  # APPROVED, CHANGES_REQUESTED, NEEDS_DISCUSSION
    issues: list[str]  # specific issues extracted from review
    summary: str  # one-line summary of review

def parse_review(content: str) -> ReviewVerdict | None:
    """Extract structured verdict and issues from review text."""
    response = _call(
        "Extract the review verdict and issues from this code review.\n"
        "Reply in JSON: {\"verdict\": \"APPROVED|CHANGES_REQUESTED|NEEDS_DISCUSSION\", "
        "\"issues\": [\"issue 1\", \"issue 2\"], \"summary\": \"one line\"}\n\n"
        f"Review:\n{content[:4000]}",
        timeout=20,
    )
    if not response:
        return None
    try:
        data = json.loads(response)
        if data.get("verdict", "").upper() in ("APPROVED", "CHANGES_REQUESTED", "NEEDS_DISCUSSION"):
            return ReviewVerdict(
                verdict=data["verdict"].upper(),
                issues=data.get("issues", []),
                summary=data.get("summary", ""),
            )
    except (json.JSONDecodeError, KeyError):
        pass
    return None
```

**Fallback:** Existing regex extraction (unchanged).

**Future use:** The `issues` list enables auto-generating focused improve tasks
instead of a generic "address review feedback" prompt.

## 3. Task Failure Analysis

**File:** `runner.py`

**Current:** Checks exit code and whether files changed. Suggests generic
retry/resume for all failure types.

**Change:** After detecting failure, analyze task output to determine what
happened and suggest the right next step.

```python
# llm.py

@dataclass
class FailureAnalysis:
    category: str     # stuck_loop, partial, already_done, misunderstood, environment, unknown
    summary: str      # what happened
    suggestion: str   # retry, resume, split, or escalate

def analyze_failure(task_output: str, exit_code: int, has_changes: bool) -> FailureAnalysis:
    """Analyze why a task failed and suggest next steps."""
    response = _call(
        "A coding agent task failed. Analyze the output and categorize the failure.\n"
        f"Exit code: {exit_code}\n"
        f"Files changed: {has_changes}\n"
        "Reply in JSON: {\"category\": \"stuck_loop|partial|already_done|misunderstood|environment|unknown\", "
        "\"summary\": \"what happened\", \"suggestion\": \"retry|resume|split|escalate\"}\n\n"
        f"Task output (last 3000 chars):\n{task_output[-3000:]}",
        timeout=20,
    )
    if response:
        try:
            data = json.loads(response)
            return FailureAnalysis(**data)
        except (json.JSONDecodeError, KeyError, TypeError):
            pass
    # Deterministic fallback
    if exit_code == 124:
        return FailureAnalysis("timeout", "Timed out", "resume")
    if has_changes:
        return FailureAnalysis("partial", "Partial progress made", "resume")
    return FailureAnalysis("unknown", f"Exited with code {exit_code}", "retry")
```

**Integration in `runner.py`:**

```python
# After detecting failure (line ~906):
from .llm import analyze_failure

# Read tail of log file for analysis
log_tail = log_file.read_text()[-3000:] if log_file.exists() else ""
analysis = analyze_failure(log_tail, exit_code, worktree_git.has_changes("."))

error_message(f"Task failed: {analysis.summary}")

# Suggest next steps based on analysis
suggestions = []
if analysis.suggestion == "resume":
    suggestions.append((f"gza resume {task.id}", "resume from where it left off"))
elif analysis.suggestion == "retry":
    suggestions.append((f"gza retry {task.id}", "retry from scratch"))
elif analysis.suggestion == "split":
    suggestions.append((f"gza add --based-on {task.id}", "break into smaller tasks"))
elif analysis.suggestion == "escalate":
    suggestions.append((f"gza log {task.id}", "review the log to understand what happened"))
next_steps(suggestions)
```

## 4. Task Context Assembly

**File:** `runner.py`

**Current:** 80-line if/else tree that hardcodes which context to include for
each task type combination.

**Change:** Keep the deterministic chain-walking, but use LLM to decide
relevance when the chain is ambiguous or when new task types are added.

This is the lowest-priority item because the current logic works for the
existing task types. Only worth doing if/when new task types are added and
the if/else tree becomes unwieldy.

```python
# llm.py

def select_relevant_context(
    task_type: str,
    task_prompt: str,
    available_context: dict[str, str],  # label -> content
) -> list[str]:
    """Given available context pieces, return which labels are relevant."""
    labels = list(available_context.keys())
    response = _call(
        f"A '{task_type}' task needs context. Task: {task_prompt[:200]}\n"
        f"Available context: {labels}\n"
        f"Which are relevant? Reply as JSON list of labels."
    )
    if response:
        try:
            selected = json.loads(response)
            return [l for l in selected if l in labels]
        except (json.JSONDecodeError, TypeError):
            pass
    return labels  # fallback: include everything
```

**Not yet recommended.** Document as a future option.

## Implementation Order

1. **Branch type inference** — simplest change, easiest to validate, immediate
   quality improvement
2. **Review verdict parsing** — unlocks auto-generated improve tasks
3. **Failure analysis** — improves UX for failed tasks
4. **Context assembly** — defer until new task types are added

## Cost

Each call uses Haiku and processes <4K tokens. At current pricing (~$0.25/M
input tokens), each judgment call costs well under $0.01. A full
plan-implement-review-improve cycle would add ~4 LLM calls = ~$0.02 overhead
on top of the main Claude Code costs.

## Testing

- Each function has a deterministic fallback, so tests can run without API
  access
- Integration tests can mock `_call()` to return known JSON
- Manual validation: run `gza add` with varied prompts and check branch names
  before/after

## Open Questions

- Should `_call()` use `claude --print` or direct Anthropic API? The CLI is
  simpler but adds a subprocess. Direct API avoids the dependency on the Claude
  CLI being installed but requires API key management separate from the provider
  layer.
- Should results be cached? Branch type inference for the same prompt will
  always return the same result. A simple dict cache in-process would avoid
  redundant calls.
