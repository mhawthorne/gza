---
name: gza-explore-summarize
description: Summarize an explore task, extract the important findings, and suggest concrete next steps
allowed-tools: Read, Bash(uv run gza show:*), Bash(uv run gza log:*), AskUserQuestion
version: 1.0.0
public: true
---

# Gza Explore Summarize

Summarize a completed explore task and turn its output into concrete next-step guidance. Use this when an explore task produced useful markdown or notes, but the user wants a faster handoff into planning, implementation, or a narrower follow-up exploration.

## Inputs

- Required: full prefixed explore task ID (for example, `gza-1234`)
- Optional: context about what decision the user is trying to make next

If the user did not provide a full prefixed task ID, ask for it before proceeding.

Use the full prefixed task ID for all `gza` commands.

## Goal

Produce a concise operator-facing synthesis:

- what the explore task found
- what matters most
- what remains uncertain
- what the best next steps are

This skill is not a raw inspection dump. Prefer synthesis and recommended actions over repeating the whole report.

## Process

### Step 1: Inspect the task

Run:

```bash
uv run gza show <TASK_ID>
uv run gza log <TASK_ID>
```

Use the output to verify:
- task exists
- task type is `explore`
- current status
- report or output content
- any execution/log context that changes how confident the findings are

If the task is not found or is not an `explore` task, stop and explain the mismatch.

### Step 2: Read the exploration output

Use the report content from `gza show` or the report file it references.

Extract:
- main findings
- evidence or examples that support those findings
- recommendations already present in the report
- unresolved unknowns, risks, or missing context

If the output is thin, ambiguous, or clearly incomplete, use AskUserQuestion to clarify what decision the user is trying to make from this research.

### Step 3: Synthesize what matters

Summarize the exploration in a compact, decision-oriented way.

Focus on:
- 3 to 7 findings at most
- the implications of those findings
- any disagreements, tradeoffs, or uncertainty that still affect the next step

Do not simply restate the report section by section.

### Step 4: Recommend next actions

Suggest the most appropriate next step based on the exploration output:

- create a `plan` task if the explore task narrowed the problem but design work is still needed
- create an `implement` task if the path is already clear enough to execute
- create another `explore` task if one key unknown still blocks good planning or implementation
- create a `review` or docs-oriented follow-up only if the exploration clearly points there

When helpful, recommend concrete gza commands using the current task as lineage:

```bash
uv run gza add --type plan --based-on <TASK_ID> "..."
uv run gza add --type implement --based-on <TASK_ID> "..."
uv run gza add --type explore --based-on <TASK_ID> "..."
```

Prefer one recommended path plus one fallback, rather than a long menu of possibilities.

### Step 5: Output format

Use this structure:

```text
Explore Summary: Task <TASK_ID>

Task Context
- Status: <status>
- Prompt: <prompt>

Key Findings
- <finding>
- <finding>
- <finding>

What Matters
- <implication or decision-relevant takeaway>

Open Questions
- <remaining uncertainty>

Suggested Next Steps
1. <recommended next action>
2. <optional fallback or follow-up action>

Recommended Commands
- <command>
- <command>
```

Keep it concise. The goal is to help the user decide what to do next without rereading the whole exploration report.

## Important notes

- Use the explore output as primary evidence; do not invent conclusions the report did not support.
- If the report contains clear recommendations, preserve them, but rewrite them more sharply when useful.
- If the exploration does not support a confident implementation path, prefer recommending a `plan` task over jumping straight to `implement`.
- If the user mainly wants raw task details, prefer `/gza-task-info`.
- If the user wants a queue-level overview across many tasks, prefer `/gza-summary`.
