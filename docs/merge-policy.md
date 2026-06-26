# Merge Policy

This document defines the review-severity contract and the merge-gate actions that consume it. It is the source of truth for how reviewers, operators, and lifecycle automation should interpret `BLOCKER`, `FOLLOWUP`, and `NIT`.

## Severity Taxonomy

### `BLOCKER`

`BLOCKER` means merge-blocking.

Use `BLOCKER` for:
- Correctness defects.
- Behavior regressions.
- Repository or project-rules violations.
- Missing observability for user-visible or agent-visible fallbacks.
- Misleading output or contradictory signals that could cause an operator or agent to make the wrong decision.
- Unexplained deviations from the provided review scope, plan, or request.
- Missing or incorrect docs/help updates when config, CLI, prompt, or other operator-facing behavior changed and the drift would mislead operators.

### `FOLLOWUP`

`FOLLOWUP` means a real issue or task-worthy improvement that should be tracked, but does not block merge.

Use `FOLLOWUP` for:
- Actionable low-risk debt.
- Adjacent-path hardening or coverage expansion that is worth doing, but is not required to make the current slice safe to ship.
- Non-gating cleanup that should become a tracked task rather than disappear into review prose.

### `NIT`

`NIT` means cosmetic feedback only. It is omitted from canonical review output.

Use `NIT` for:
- Style, wording, or presentation tweaks with no material effect on correctness, behavior, observability, or operator understanding.
- Minor polish suggestions that should not affect verdicts, follow-up creation, or merge decisions.

## Verdict To Action

The current lifecycle consumes review verdicts as follows:

- `APPROVED` -> merge-ready.
- `APPROVED_WITH_FOLLOWUPS` with one or more parsed `FOLLOWUP` findings -> create or reuse follow-up tasks, then merge.
- `CHANGES_REQUESTED` with one or more `BLOCKER` findings -> enter the improve loop.
- Anything else -> fail closed and require attention.

Fail-closed cases include:
- Verdict/finding mismatches.
- Malformed review structure.
- Missing structured severity output.
- `APPROVED_WITH_FOLLOWUPS` with zero parsed follow-ups.
- Reviews whose content cannot be classified safely.

## Backward Compatibility

- Legacy `## Must-Fix` findings parse as `BLOCKER`.
- Legacy `## Suggestions` does not auto-promote items into follow-up tasks.
- Reviews without structured severity are not auto-mergeable.

## Loop Bound

- `max_review_cycles=3` is the current review/improve loop bound.
- When the bound is reached, lifecycle escalates with `review-max-cycles-reached` and requires human intervention instead of continuing to churn.

## Operator Audit Policy

Operators should periodically sample `APPROVED_WITH_FOLLOWUPS` reviews to catch under-grading drift. The goal is to verify that real merge blockers are not being mislabeled as non-gating follow-ups.

## Calibration Examples

TODO: backfill 3-5 real examples once the missing gza-2887 review artifacts are recovered.

Planned examples:
- Broad exception that masks visible state -> `BLOCKER`.
- Adjacent-path coverage sweep -> `FOLLOWUP`.
- Centralization/refactor debt -> `FOLLOWUP`.
