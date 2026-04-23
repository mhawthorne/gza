---
name: gza-plan-improve
description: Refine a draft plan by asking targeted questions, resolving gaps, and rewriting it into an implementation-ready plan
allowed-tools: Read, Bash(uv run gza show:*), Bash(uv run gza log:*), AskUserQuestion
version: 1.0.0
public: true
---

# Gza Plan Improve

Refine a draft plan through a deliberate question loop. Use this when the user has a rough plan, an incomplete completed plan task, or a draft that needs sharper scope, acceptance criteria, sequencing, risks, and test strategy before implementation begins.

## Inputs

Accept one of these inputs:

- Preferred: a full prefixed plan task ID (for example, `gza-1234`)
- Also supported: pasted draft plan text
- Optional: extra constraints, related task IDs, or notes about what feels weak

If the user provides neither a full prefixed plan task ID nor draft plan text, ask for the current draft or plan task first.

Use the full prefixed task ID for all `gza` commands.

## Goal

Produce an improved plan, not just a score.

The skill should:
- identify the highest-leverage gaps in the current draft
- ask concise questions to close those gaps
- confirm assumptions explicitly instead of guessing
- rewrite the plan into a cleaner, more implementation-ready shape
- call out any remaining blockers or open questions

This is different from `/gza-plan-review`:
- `/gza-plan-review` decides `Go` / `No-go`
- `/gza-plan-improve` actively helps the user strengthen the plan first

## Process

### Step 1: Gather the current plan and context

If the input is a full prefixed plan task ID, inspect it with:

```bash
uv run gza show <TASK_ID>
uv run gza log <TASK_ID>
```

Use that output to extract:
- task type and status
- original prompt
- current plan/report content
- nearby context from logs that explains uncertainty, blockers, or assumptions

If the task is not found or is not a `plan` task, stop and explain the mismatch.

If the input is draft text instead of a task ID, use the provided draft as the working plan.

### Step 2: Diagnose the weakest parts first

Evaluate the draft against these plan dimensions:

1. Problem framing
- Is the user problem or objective specific?
- Does the draft explain why the work matters?

2. Scope and boundaries
- What is explicitly in scope?
- What is explicitly out of scope?
- Which files, modules, systems, or surfaces are likely affected?

3. Acceptance criteria
- What observable outcomes define success?
- Are edge cases and failure modes named?
- Would an implementer know when the work is done?

4. Risks and unknowns
- What could cause rework, delay, or the wrong design choice?
- Which unknowns need decisions, investigation, or validation?

5. Dependencies and sequencing
- Are prerequisites, approvals, related tasks, or external systems identified?
- Is the execution order clear enough to avoid backtracking?

6. Test strategy
- Which tests or verification modes are required?
- Which regressions must be guarded against?

Rank the gaps and focus on the smallest set of questions that will most improve the plan.

### Step 3: Run a targeted question loop

Use AskUserQuestion to ask concise, high-value follow-up questions.

Rules for the question loop:
- Ask only what materially improves the plan
- Prefer 1 to 4 questions per round
- Ask about the biggest uncertainty first
- Confirm assumptions explicitly when the draft implies something but does not state it
- Stop asking once the remaining gaps are minor or clearly flagged as open questions

Good question themes:
- exact success criteria
- scope boundaries and non-goals
- risky edge cases
- sequencing and dependency order
- test expectations
- operator-facing docs/help/config impact when relevant

### Step 4: Rewrite the plan

Produce a revised plan with clear headings and direct language. Prefer a structure like:

```text
Plan: <short title>

Objective
- <what problem is being solved>

Scope
- In scope: <items>
- Out of scope: <items>

Assumptions / Inputs
- <assumptions confirmed with user>

Acceptance Criteria
1. <testable success condition>
2. <testable success condition>

Implementation Outline
1. <step>
2. <step>
3. <step>

Risks / Unknowns
- <risk + mitigation or follow-up>

Dependencies
- <task/system/approval + status>

Test Strategy
- <unit/integration/e2e/manual verification as relevant>

Open Questions
- <only unresolved items that genuinely remain>
```

Do not preserve vague wording from the original draft if it can be made concrete.

### Step 5: Close with readiness and next action

After presenting the improved plan, summarize:

- what materially changed
- any blockers or unresolved questions that still matter
- whether the plan now looks ready for `/gza-plan-review` or direct implementation follow-up

If the plan came from a task and is now strong enough, recommend:

```bash
uv run gza show <TASK_ID>
uv run gza log <TASK_ID>
```

and then `/gza-plan-review` for a final quality gate if needed.

If the plan is still too ambiguous after refinement, say so plainly and list the missing decisions.

## Important notes

- Keep the interaction collaborative and specific; avoid broad brainstorming unless the user asks for it.
- Prefer rewriting the plan over merely criticizing it.
- Do not invent technical constraints, dependencies, or acceptance criteria that were not supported by the draft or user answers.
- If the user is really trying to create a new gza task rather than improve a plan draft, prefer `/gza-task-draft`.
- If the user wants a final `Go` / `No-go` decision on a completed plan task, prefer `/gza-plan-review`.
