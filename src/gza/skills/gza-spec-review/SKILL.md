---
name: gza-spec-review
description: Run an interactive quality gate for a spec file and produce a go or no-go recommendation before implementation
allowed-tools: Read, Glob, Grep, Bash(ls:*), Bash(git log:*), Bash(git blame:*), Bash(uv run *--help*), AskUserQuestion
version: 1.0.0
public: true
---

# Gza Spec Review

Run an interactive quality gate for a specific spec file before implementation starts.

## Inputs

- Required: path to a spec file (e.g. `specs/foo.md`)

If the user did not provide a spec path, ask for it before proceeding.

Normalize spec paths:
- If input is just a filename like `foo.md`, expand to `specs/foo.md`
- If the file does not exist, stop and report the error

## Process

### Step 1: Read and understand the spec

Read the spec file in full. Identify:
- The core problem/feature being described
- Concrete claims: file paths, command names, config fields, function signatures, workflow steps
- Acceptance criteria (explicit or implied)
- Dependencies on other specs, tasks, or systems

### Step 2: Verify claims against the codebase

For each concrete claim in the spec:

1. **File paths** - Does the spec reference files that exist?
   - Search with Glob for referenced paths
   - Flag paths that don't exist and aren't plausible future additions

2. **Command/option names** - Does the spec describe CLI commands or flags?
   - Compare against `uv run gza --help` and `uv run gza <command> --help`
   - Flag commands that were renamed or flags that changed

3. **Config fields** - Does the spec reference configuration options?
   - Search `src/gza/config.py` for referenced field names
   - Flag fields that were renamed or removed

4. **Code patterns** - Does the spec describe specific functions, classes, or modules?
   - Search with Grep for referenced identifiers
   - Flag identifiers that no longer exist

5. **Workflow steps** - Does the spec describe a multi-step process?
   - Verify each step is still how the feature works
   - Flag steps that have been simplified, reordered, or changed

Use git blame/log for context when unsure if something is aspirational vs outdated:
- Recent specs are more likely aspirational
- Old specs with recent code changes around the feature are more likely outdated

### Step 3: Run the interactive quality gate

Use AskUserQuestion to evaluate each gate area. Ask concise, targeted questions and confirm assumptions explicitly.

Gate areas:
1. Scope clarity
- Is the problem statement specific?
- Are boundaries and out-of-scope items explicit?
- Are impacted components/files identified?

2. Accuracy
- Do concrete claims (paths, commands, config, code patterns) match the current codebase?
- Are there outdated references that need updating?
- Is the spec aspirational, current, or a mix?

3. Acceptance criteria
- Are there concrete, testable success criteria?
- Are behavior, edge cases, and failure modes covered?
- Is "done" unambiguous for implementation and review?

4. Risks & dependencies
- Are technical/product risks identified and ranked?
- Are dependencies (other specs, tasks, systems, data) listed?
- Are dependency states known (ready, blocked, unknown)?

5. Test strategy
- Are required test types named (unit/integration/e2e as relevant)?
- Are key scenarios and regressions enumerated?
- Is verification approach clear for implementation work?

### Step 4: Score and decide

Assign each gate area one status:
- `Pass` - sufficient for implementation
- `Needs work` - gaps exist but are fixable
- `Fail` - critical blocker

Decision rule:
- `Go`: no `Fail` areas and at most one `Needs work`
- `No-go`: any `Fail`, or two or more `Needs work`

### Step 5: Output recommendation and actions

Produce a concise report.

## Output template

Use this structure:

```text
Spec Review: <spec path>

Spec Context
- File: <spec path>
- Last modified: <date from git log>
- Summary: <one-line summary of what the spec describes>

Codebase Verification
- <claim 1>: <verified|missing|outdated|aspirational> - <details>
- <claim 2>: <verified|missing|outdated|aspirational> - <details>
- ...

Quality Gate
- Scope clarity: <Pass|Needs work|Fail> - <evidence>
- Accuracy: <Pass|Needs work|Fail> - <evidence>
- Acceptance criteria: <Pass|Needs work|Fail> - <evidence>
- Risks & dependencies: <Pass|Needs work|Fail> - <evidence>
- Test strategy: <Pass|Needs work|Fail> - <evidence>

Recommendation
- <Go|No-go>
- Rationale: <short rationale>

Follow-up actions
1. <action>
2. <action>
3. <action>
```

## Important notes

- Keep the gate interactive: ask questions before deciding if evidence is incomplete.
- Distinguish aspirational (planned) from outdated (wrong) content - aspirational claims are not failures.
- Use codebase verification as primary evidence; do not guess hidden context.
- Prefer no-go over ambiguous go when accuracy or acceptance criteria are weak.
- If go is granted with one `Needs work`, include an explicit pre-implementation fix action.
