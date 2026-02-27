---
name: gza-plan-review
description: Run an interactive quality gate for a plan task and produce a go or no-go recommendation before implementation
allowed-tools: Bash(uv run gza show:*), Bash(uv run gza log:*), Bash(uv run gza history:*), AskUserQuestion, Read
version: 1.0.0
public: true
---

# Gza Plan Review

Run an interactive plan quality gate for a specific plan task before implementation starts.

## Inputs

- Required: plan task ID (supports `42` or `#42`)

If the user did not provide a task ID, ask for it before proceeding.

## Process

### Step 1: Inspect the task with built-in gza commands

Run these commands with the provided task ID:

```bash
uv run gza show <TASK_ID>
uv run gza log --task <TASK_ID>
uv run gza history --type plan --last 10
```

Use this output to verify:
- task exists
- task type is `plan`
- task status (prefer `completed` for gate review)
- final plan output and any failure context
- nearby plan/review history for dependencies and precedent

If the task is not found or is not a `plan` task, stop and return a no-go with corrective actions.

### Step 2: Run the interactive quality gate

Use AskUserQuestion to evaluate each gate area. Ask concise, targeted questions and confirm assumptions explicitly.

Gate areas:
1. Scope clarity
- Is the problem statement specific?
- Are boundaries and out-of-scope items explicit?
- Are impacted components/files identified?

2. Acceptance criteria
- Are there concrete, testable success criteria?
- Are behavior, edge cases, and failure modes covered?
- Is "done" unambiguous for implementation and review?

3. Risks
- Are technical/product risks identified and ranked?
- Are mitigation steps or fallback paths defined?
- Are unknowns called out with a way to resolve them?

4. Dependencies
- Are task dependencies (other gza tasks, systems, data, approvals) listed?
- Are dependency states known (ready, blocked, unknown)?
- Is sequencing clear enough to avoid rework?

5. Test strategy
- Are required test types named (unit/integration/e2e as relevant)?
- Are key scenarios and regressions enumerated?
- Is verification command strategy clear for implementation work?

### Step 3: Score and decide

Assign each gate area one status:
- `Pass` - sufficient for implementation
- `Needs work` - gaps exist but are fixable
- `Fail` - critical blocker

Decision rule:
- `Go`: no `Fail` areas and at most one `Needs work`
- `No-go`: any `Fail`, or two or more `Needs work`

### Step 4: Output recommendation and actions

Produce a concise report with:

1. Task context
- Task ID, title/prompt, status, and inspected commands used

2. Gate results
- One line per gate area: status + evidence

3. Recommendation
- `Go` or `No-go`
- One short rationale paragraph

4. Follow-up actions
- Concrete next steps with owners and commands when possible
- Reference existing gza commands to continue workflow:
  - `uv run gza show <TASK_ID>`
  - `uv run gza log --task <TASK_ID>`
  - `uv run gza add --type implement --based-on <TASK_ID> "..."`
  - `uv run gza add --type plan "..."` (for remediation planning)

## Output template

Use this structure:

```text
Plan Review: Task #<TASK_ID>

Task Context
- Type/Status: <type> / <status>
- Prompt: <prompt>
- Inspected with: gza show, gza log --task, gza history --type plan

Quality Gate
- Scope clarity: <Pass|Needs work|Fail> - <evidence>
- Acceptance criteria: <Pass|Needs work|Fail> - <evidence>
- Risks: <Pass|Needs work|Fail> - <evidence>
- Dependencies: <Pass|Needs work|Fail> - <evidence>
- Test strategy: <Pass|Needs work|Fail> - <evidence>

Recommendation
- <Go|No-go>
- Rationale: <short rationale>

Follow-up actions
1. <action with command>
2. <action with command>
3. <action with command>
```

## Important notes

- Keep the gate interactive: ask questions before deciding if evidence is incomplete.
- Use command output as primary evidence; do not guess hidden context.
- Prefer no-go over ambiguous go when acceptance criteria or test strategy are weak.
- If go is granted with one `Needs work`, include an explicit pre-implementation fix action.
