# GZA Review Prompt Improvements

## Purpose

This document captures review-quality findings observed while using `gza review` on BobbyDigital work on 2026-03-17. The goal is to improve the upstream review prompt so reviews stay sharp on real regressions, enforce project rules consistently, and produce more actionable output with less noise.

This is intended as input for a separate thread in the `gza` project.

---

## What We Observed

Recent reviews were directionally useful and caught real issues, especially around:

- silent fallback behavior with no operator-visible warning
- missing docs updates when config or CLI behavior changed
- missing tests for degradation paths and JSON/aggregate outputs
- inconsistent status/context output when a primary metric was present

The strongest reviews were the ones that:

- tied findings back to explicit project learnings or repo rules
- distinguished true blockers from polish
- required a concrete regression test alongside the fix

The weaker parts were recurring too:

- the same silent-exception rule had to be rediscovered repeatedly across separate reviews
- some suggestions were correct but low leverage relative to the main risk
- reviews sometimes flagged contradictions in prompt/context output, but only as suggestions rather than by using a clearer “misleading operator/LLM signal” heuristic

---

## Repeated Findings Worth Encoding

### 1. Silent fallback paths are high-severity by default

This was the clearest recurring pattern.

Examples observed:

- `except Exception: pass` around multi-project context fallback
- `except Exception: pass` around DB access in `build_context`
- token-query degradation that returned zeros without a warning

Why it matters:

- operators cannot distinguish “no activity” from “broken read path”
- status/chat/sweep output becomes misleading
- degraded behavior compounds when other fallbacks also zero out values

Prompt implication:

- reviewers should treat silent broad exception fallbacks as `Must-Fix` when they affect operator-visible state, unless a warning or equivalent observability already exists

### 2. New behavior needs docs in the same change

This also recurred cleanly.

Examples observed:

- new config shape for project entries
- changed `status` output semantics
- new token/pricing fields exposed in status/chat/sweep/context

Why it matters:

- this repo relies on `AGENTS.md` and specs as active operating docs
- stale docs directly degrade future agent behavior

Prompt implication:

- reviewers should explicitly check whether config fields, CLI surface, task types, or operator-facing behavior changed, and if so require doc updates

### 3. New fallbacks and derived displays need regression tests

Examples observed:

- warning path for token-query failure
- aggregate token totals and JSON serialization
- display branching between token-first and cost fallback

Why it matters:

- these features fail quietly if untested
- display/serialization regressions are easy to miss in manual review

Prompt implication:

- reviewers should ask for tests covering degraded behavior, aggregate outputs, and serialization whenever those paths are introduced or changed

### 4. Prompt/context output should be checked for misleading contradictions

Examples observed:

- token counts present while context still says `Cost today: $0.00`
- no-activity projects rendering as though they had meaningful zero-valued metrics

Why it matters:

- this project feeds LLM-visible context as well as human-visible status
- technically “valid” output can still be materially misleading

Prompt implication:

- reviewers should explicitly evaluate whether new output creates contradictory or misleading signals for either operators or downstream prompts

### 5. Suggestions should stay secondary

Lower-severity suggestions were often useful, but they should stay clearly non-blocking:

- rename shared helpers that look private
- add missing type hints
- reduce duplication by reusing an existing helper
- document rounding behavior

Prompt implication:

- keep `Must-Fix` reserved for correctness, regressions, explicit repo-rule violations, and materially misleading output
- push style/ergonomics issues into `Suggestions`

---

## Recommended Prompt Changes

### 1. Add an explicit “repo rules and learnings” pass

The review prompt should instruct the reviewer to actively look for violations of project-local rules and recurring learnings, not just code-level bugs.

Suggested addition:

> Check changed code against explicit repo guidance in files like `AGENTS.md`, local learnings, and review instructions. If a diff violates a documented project rule, call that out directly and cite the rule in the finding.

### 2. Add a fallback-observability heuristic

Suggested addition:

> Treat broad exception fallbacks, defensive defaults, and “return zero/empty on error” behavior as high risk when they affect user-visible or agent-visible state. If the fallback is not observable through logging or another clear signal, prefer a `Must-Fix`.

### 3. Add a “misleading output” heuristic

Suggested addition:

> Check whether new UI text, JSON fields, or prompt/context text can produce contradictory or misleading signals, even if the code is technically functioning. Flag this when operators or downstream LLM prompts could infer the wrong state.

### 4. Require test guidance that matches the failure mode

Suggested addition:

> When you request a fix, also request the smallest regression test that would have caught the issue. Prefer targeted tests for degraded behavior, serialization, and aggregate calculations over generic “add tests” advice.

### 5. Ask the reviewer to verify docs impact explicitly

Suggested addition:

> If the diff changes config schema, CLI output/flags, task types, or operating assumptions, verify that project docs were updated in the same change. Missing docs should be a required fix when the repo treats docs as operational guidance.

### 6. Sharpen blocker threshold

Suggested addition:

> Reserve `Must-Fix` for correctness issues, real regressions, documented rule violations, missing observability on fallbacks, or materially misleading output. Put naming, cleanup, and low-risk refactors under `Suggestions`.

---

## Proposed Review Checklist

The prompt should bias the reviewer to answer these questions explicitly:

1. Does the diff introduce any silent fallback or broad exception path without observability?
2. Does any new output become misleading when fallback values are shown?
3. Did config, CLI, or operator-facing behavior change without docs updates?
4. Are new degraded paths, aggregate calculations, or serialization fields covered by tests?
5. Are findings ranked correctly, with true blockers separated from polish?

---

## Expected Outcome

If the review prompt is updated along these lines, I would expect:

- fewer missed repo-rule violations
- less repetition across consecutive reviews of the same theme
- more consistent identification of operator-visible regressions
- tighter required-test guidance
- cleaner separation between blocking issues and optional improvements

---

## Concrete BobbyDigital-Derived Cases To Use As Prompt Calibration

Useful examples for prompt tuning or evals:

- broad `except Exception: pass` around status/context DB reads
- fallback-to-zero token/cost behavior with no warning
- token-first display that still emits contradictory cost text
- new config shorthand shipped without corresponding docs update
- new aggregate/json fields added without serialization coverage

These are good calibration cases because they are small, realistic, and representative of the kinds of regressions a review agent should catch reliably.
