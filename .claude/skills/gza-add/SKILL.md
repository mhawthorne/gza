---
name: gza-add
description: Create a well-formed gza task with appropriate type, group, and prompt
allowed-tools: Read, Bash(uv run gza add:*), AskUserQuestion
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

3. **Additional context** (optional):
   - Should this be grouped with related tasks? (--group NAME)
   - Does this depend on another task? (--depends-on ID)
   - For implement tasks: Should auto-create a review task? (--review)
   - For chained work: Is this based on a previous task's output? (--based-on ID)

### Step 3: Generate the task prompt

Create a clear, specific prompt that:
- States the objective clearly
- Includes relevant context (file paths, components, constraints)
- Is scoped appropriately for the task type
- For `plan` tasks: Explains what needs to be designed/explored
- For `implement` tasks: Specifies what to build and acceptance criteria
- For `review` tasks: Identifies what to review and what to look for

### Step 4: Run gza add

Execute the command with appropriate flags:

```bash
uv run gza add [FLAGS] "prompt text"
```

Common flag combinations:
- Basic task: `uv run gza add "description"`
- Exploration: `uv run gza add --type explore "what to investigate"`
- Planning: `uv run gza add --type plan "what to design"`
- Implementation: `uv run gza add --type implement "what to build"`
- Implementation with review: `uv run gza add --type implement --review "what to build"`
- Grouped tasks: `uv run gza add --group auth --type implement "add login endpoint"`
- Dependent task: `uv run gza add --depends-on 5 "build on task 5's foundation"`
- Based-on task: `uv run gza add --type implement --based-on 5 "implement the approach from task #5"`

### Step 5: Confirm success

After running the command:
1. Show the task ID that was created
2. Confirm the task details (type, group if set)
3. If a review task was auto-created, note that as well

## Tips for good task prompts

- **Be specific**: Reference concrete files, functions, or components when possible
- **Include context**: Explain the "why" not just the "what"
- **Set scope**: Make clear what's in scope and what's not
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

**Implementation:**
```bash
uv run gza add --type implement --review "add JWT authentication to the API endpoints in src/api/routes.py"
```

**Grouped workflow:**
```bash
uv run gza add --group metrics --type plan "design metrics collection system"
uv run gza add --group metrics --type implement --depends-on 12 "implement metrics collector"
uv run gza add --group metrics --type implement --depends-on 13 "add metrics export to CSV/JSON"
```

## Important notes

- **Always use `uv run gza add`** - Never edit task files manually
- **One task, one objective** - If there are multiple distinct goals, create multiple tasks
- **Use task chaining** - Connect related work with dependencies rather than creating one massive task
- **Review significant changes** - Add `--review` flag for implementations that warrant code review
