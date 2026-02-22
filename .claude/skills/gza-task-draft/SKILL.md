---
name: gza-task-draft
description: Guide user through deliberate task creation with clarification and refinement before running gza add
allowed-tools: Read, Bash(uv run gza add:*), AskUserQuestion
version: 1.0.0
public: true
---

# Draft Gza Task

Guide the user through a deliberate task creation process: clarify scope, surface risks, draft a prompt, get approval, then run `gza add`.

## Step 1: Read conventions

Read `/workspace/AGENTS.md` — specifically the "Creating Tasks from Conversations" section — to understand task types, flags, and prompt conventions.

## Step 2: Assess description detail

Evaluate how detailed the user's description already is:

**If vague or missing key context** (objective unclear, scope undefined, approach unspecified): ask clarifying questions using AskUserQuestion (max 4 questions per call, 2–4 options each).

Focus on what matters most:
- What is the core objective / what problem does this solve?
- What type of work is this? (explore/plan/implement/review/task)
- Are there constraints, dependencies, or related tasks?
- What does "done" look like?

**If already detailed** (clear objective, known scope, specific requirements): skip to Step 3 directly.

## Step 3: Draft the prompt + surface concerns

Draft a task prompt that:
- States the objective clearly and specifically
- References file paths, modules, or components when known
- Sets scope (what's in and out)
- Includes acceptance criteria for implement tasks
- Is appropriately sized (one task, one objective)

Also identify and surface:
- **Risks**: What could go wrong or cause rework?
- **Ambiguities**: Unclear requirements that could derail the task
- **Alternatives**: Different approaches worth considering
- **Sequencing**: Should this be split (plan first, then implement)? Does it need `--depends-on` or `--based-on`?

Present the draft prompt and concerns clearly to the user.

## Step 4: Propose flags

Based on the discussion, suggest the full `gza add` command:

```
uv run gza add [FLAGS] "prompt"
```

Flags to consider:
- `--type` — task (default), explore, plan, implement, review
- `--review` — auto-create review task after implementation (for significant changes)
- `--group NAME` — group related tasks together
- `--depends-on ID` — task cannot start until another completes
- `--based-on ID` — implementation draws from a previous task's output
- No flag needed for simple tasks

## Step 5: Get approval

Use AskUserQuestion to let the user review the draft:

Options:
- **Approve** — run gza add as proposed
- **Refine prompt** — let user provide corrections (ask what to change, then revise and re-present)
- **Adjust flags** — type, group, or dependencies need changing (ask what, revise, re-present)
- **Split into multiple tasks** — break into smaller tasks (draft each separately)

Repeat Steps 3–5 if refinement is needed.

## Step 6: Run gza add

Once approved, run the command. Show the created task ID and confirm type/group if set.

## Important notes

- **One task, one objective** — if the user describes multiple distinct goals, create multiple tasks
- **Plan before implement** — for complex features, suggest a `--type plan` task first
- **Proactively flag risks** — better to surface ambiguity now than after the task runs
- **Keep prompts specific** — vague prompts produce vague results
