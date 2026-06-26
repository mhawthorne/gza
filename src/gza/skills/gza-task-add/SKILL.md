---
name: gza-task-add
description: Create a well-formed gza task with appropriate type, tags, and prompt
allowed-tools: Read, Bash(uv run gza add:*), AskUserQuestion
version: 1.0.0
public: true
---

# Add Gza Task

Create a well-scoped gza task with the appropriate type and configuration.

## Process

### Step 1: Understand gza task conventions

Read `/workspace/AGENTS.md` to understand:
- Task types (task, explore, plan, implement, review)
- Task format conventions
- When to use each type

### Step 2: Gather task requirements

Ask the user what they want to accomplish. Use AskUserQuestion to gather:

1. **What needs to be done?** - The core objective or problem to solve
2. **Task type** - Present options:
   - `task` (default) - General purpose task
   - `explore` - Research, investigation, or discovery work
   - `plan` - Planning and design work that produces a specification
   - `implement` - Code implementation based on clear requirements
   - `review` - Code review or quality assessment
   - `improve` - Address review feedback and/or unresolved task comments on an implementation. Runs from review findings, from unresolved comments, or from both; comments-only improve is supported when no usable review exists (use `uv run gza improve <impl-id> --run`)

3. **Additional context** (optional):
   - Should this be tagged to relate it to other tasks? (--tag TAG, repeatable)
   - Does this depend on another task? (--depends-on ID)
   - For implement tasks: Should auto-create a review task? (--review)
   - For chained work: Is this based on a previous task's output? (--based-on ID)

### Step 3: Generate the task prompt

Create a clear, specific prompt that:
- States the objective clearly
- Includes relevant context (file paths, components, constraints)
- Is scoped appropriately for the task type
- For `plan` tasks: Explains what needs to be designed/explored
- For `implement` tasks: Specifies what to build, clear acceptance criteria, and any important constraints
- For `review` tasks: Identifies what to review and what to look for

For `implement` tasks, include at least these prompt elements when they are relevant:
- The exact behavior to change
- Non-goals or explicitly out-of-scope areas
- Relevant files, modules, or subsystems when known
- Required tests or failure modes to cover
- Required docs/help/config updates when operator-facing behavior changes

If those details are unknown and the work is ambiguous or cross-cutting, recommend a `plan` task first instead of writing a vague `implement` task.

### Step 4: Run uv run gza add

Execute the command with appropriate flags:

```bash
uv run gza add [FLAGS] "prompt text"
```

**Important: there is no `--prompt` flag.** `gza add` takes the prompt as a positional argument or via `--prompt-file FILE`. If you pass `--prompt "text"`, argparse prefix-matches it to `--prompt-file` and tries to open your text as a filename — you will get `[Errno 63] File name too long`.

**Single-line prompts:** pass as the positional arg.

```bash
uv run gza add "fix the thing"
```

**Multi-line prompts: always Write a tempfile and use `--prompt-file`.**

Do **not** use `"$(cat <<'EOF' ... EOF)"` heredoc-in-command-substitution. It is fragile — backticks, `$`, parens, or indented code blocks in the body cause the EOF terminator to be missed, and the prompt body leaks out as bash commands. The failure mode silently mangles the prompt or fails with confusing "command not found" errors. The tempfile path is always safe.

1. Use the `Write` tool to put the prompt body in a file under `/tmp/` (e.g. `/tmp/gza-prompt-<short-name>.md`).
2. Pass it to `gza add`:
   ```bash
   uv run gza add --type implement --prompt-file /tmp/gza-prompt-<short-name>.md
   ```

Common flag combinations:
- Basic task: `uv run gza add "description"`
- Exploration: `uv run gza add --type explore "what to investigate"`
- Planning: `uv run gza add --type plan "what to design"`
- Implementation: `uv run gza add --type implement "what to build"`
- Implementation with review: `uv run gza add --type implement --review "what to build"`
- Tagged tasks: `uv run gza add --tag auth --type implement "add login endpoint"` (repeatable; use multiple `--tag` flags)
- Dependent task: `uv run gza add --depends-on gza-5 "build on task gza-5's foundation"`
- Based-on task: `uv run gza add --type implement --based-on gza-5 "implement the approach from task gza-5"`
- Based-on with default prompt: `uv run gza add --type implement --based-on gza-5` (opens editor with default: "Implement plan from task gza-5: <plan-task-slug>")

### Step 5: Confirm success

After running the command:
1. Show the task ID that was created
2. Confirm the task details (type, tags if set)
3. If a review task was auto-created, note that as well

## Tips for good task prompts

- **Be specific**: Reference concrete files, functions, or components when possible
- **Include context**: Explain the "why" not just the "what"
- **Set scope**: Make clear what's in scope and what's not
- **For implement tasks**: Include acceptance criteria, non-goals, required tests, and docs/help impact when relevant
- **For multi-step work**: Create a `plan` task first, then `implement` tasks based on it
- **Use dependencies**: Connect related tasks with `--depends-on` or `--based-on`
- **Enable reviews**: Add `--review` flag for significant implementation work

## Examples

**Exploration:**
```bash
uv run gza add --type explore "investigate how authentication is currently implemented and identify areas for improvement"
```

**Planning:**
```bash
uv run gza add --type plan "design a task chaining system that allows tasks to reference previous task outputs"
```

**Implementation (multi-line prompt):**

For any prompt longer than one line, write it to a tempfile first and pass `--prompt-file`. Do **not** use `"$(cat <<'EOF' ... EOF)"` — heredoc-in-command-substitution is fragile (backticks, `$`, parens, or indented code blocks in the body break it, and the failure mode silently mangles the prompt).

1. Write `/tmp/gza-prompt-<short-name>.md` with the prompt body.
2. Run:
   ```bash
   uv run gza add --type implement --review --prompt-file /tmp/gza-prompt-<short-name>.md
   ```

Example prompt body to put in the file:

```
Add JWT authentication to the API endpoints in src/api/routes.py.

Acceptance criteria:
- Authenticated endpoints accept valid JWT bearer tokens.
- Invalid and expired tokens return the expected error response.
- Add targeted regression coverage for token validation paths.

Non-goals:
- Do not redesign session management.
- Do not change unrelated auth middleware behavior.
```

**Tagged workflow:**
```bash
uv run gza add --tag metrics --type plan "design metrics collection system"
uv run gza add --tag metrics --type implement --depends-on gza-c "implement metrics collector"
uv run gza add --tag metrics --type implement --depends-on gza-d "add metrics export to CSV/JSON"
```

**Improve workflow (addressing review findings and/or unresolved comments):**
```bash
# After a review requests changes, create an improve task
uv run gza improve gza-t --run  # where gza-t is the implementation task ID
uv run gza improve gza-t --run --review  # auto-create review after improvements

# Comments-only improve: when no usable review exists but unresolved task
# comments do, improve still runs using comments-only feedback. Add comments
# first via `gza comment <task_id> "<text>"`, then:
uv run gza improve gza-t --run
```

## Important notes

- **Always use `uv run gza add`** - Never edit task files manually
- **One task, one objective** - If there are multiple distinct goals, create multiple tasks
- **Use task chaining** - Connect related work with dependencies rather than creating one massive task
- **Review significant changes** - Add `--review` flag for implementations that warrant code review
