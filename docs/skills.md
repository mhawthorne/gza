# Skills

Gza provides Claude Code skills that enhance the agent's ability to work with gza tasks. Install them into your project with:

```bash
# Install all public skills
gza skills-install

# Install specific skills
gza skills-install gza-task-add gza-task-info

# Install including dev (non-public) skills
gza skills-install --dev

# List available skills
gza skills-install --list
```

Skills are installed to `.claude/skills/` in your project directory and become available as slash commands in Claude Code.

---

## gza-task-add

**Create a well-formed gza task with appropriate type, tags, and prompt.**

Use `/gza-task-add` when you want to add a task to the gza queue during a Claude Code session. The skill reads your project's AGENTS.md conventions, asks clarifying questions, constructs a well-scoped prompt, and runs `uv run gza add` with the right flags.

**Key behaviors:**
- Asks about task type (`explore`, `plan`, `implement`, `review`, `improve`)
- Prompts for optional flags: `--tag`, `--depends-on`, `--based-on`, `--review`
- Generates a specific prompt (not vague) and shows the task ID on success
- Always uses `uv run gza add` — never edits task files manually

**Example output:**

```bash
uv run gza add --type implement --review "add JWT authentication to src/api/routes.py"
# Created task gza-16 (implement)
```

---

## gza-task-draft

**Guide user through deliberate task creation with clarification and refinement before running `uv run gza add`.**

Use `/gza-task-draft` when the task idea needs more thinking before committing. Unlike `gza-task-add`, this skill explicitly surfaces risks, ambiguities, and alternative approaches before finalizing the task.

**Key behaviors:**
- Skips clarification if the description is already detailed; asks questions if it's vague
- Drafts the prompt and shows it to you for approval before running anything
- Flags risks (e.g., "this might need a plan task first"), dependencies, and sequencing concerns
- Supports refinement loops: approve, edit, adjust flags, or split into multiple tasks

**When to prefer over `gza-task-add`:**
- You have a rough idea but haven't thought through the scope
- The task is complex enough to warrant splitting into plan + implement
- You want to review the exact prompt before it runs

---

## gza-task-info

**Gather comprehensive info about a specific gza task including status, branch, commits, and logs.**

Use `/gza-task-info` to inspect any task by ID. Pulls data from the database, git branch, and execution logs into a single summary.

**Key behaviors:**
- Queries `.gza/gza.db` for all task fields (status, cost, duration, branch, etc.)
- Shows recent commits on the task's branch and whether it's merged to main
- Displays the execution log tail (most useful for diagnosing failures)
- Shows report content for plan/explore/review tasks
- Formats duration as `245.3s (4:05)` and cost as `$0.42`

**Example summary:**

```
Task gza-i: completed
Type: implement
Branch: 20260115-add-authentication (3 commits, not yet merged to main)
Duration: 245.3s (4:05)
Cost: $0.42
Prompt: "Add JWT authentication to API endpoints"
```

---

## gza-task-fix

**Run an escalation rescue workflow for stuck review/improve churn using first-class `gza fix` context and closure-ledger handoff.**

Use `/gza-task-fix` when an implementation is stuck after repeated `CHANGES_REQUESTED`, `max_cycles_reached`, or repeated failed improve attempts.

**Key behaviors:**
- Starts from a full prefixed task ID (for example, `gza-1234`) and resolves to the root implementation lineage
- Uses `uv run gza fix <task_id>` instead of ad hoc manual repair steps
- Enforces bounded blocker-driven scope with explicit rescue guardrails
- Requires machine-readable closure ledger output (`fix_result` plus blocker entries)
- Requires a fresh independent review after code-changing rescue runs

---

## gza-task-debug

**Diagnose why a gza task failed — analyzes logs, detects loops, checks diffs, compares baselines, and suggests fixes.**

Use `/gza-task-debug` when a task has failed and you need to understand why before deciding whether to resume, retry, or rewrite the prompt.

---

## gza-summary

**Synthesize operator triage guidance from the canonical gza surfaces.**

Use `/gza-summary` when you want a synthesized "what should I do next?" view without reviving `gza incomplete` as a mixed-bucket CLI command.

**Key behaviors:**
- Runs `uv run gza history --status failed`, `uv run gza advance --unimplemented`, `uv run gza unmerged`, and `uv run gza next --all`
- Optionally uses `uv run gza watch --restart-failed --dry-run` when failed-task recovery needs a decision surface
- Treats `uv run gza history --status failed` as factual failed-attempt history, not an unresolved-only recovery queue
- Distinguishes factual history filters from recommendation synthesis
- Suggests gza-native follow-up commands such as `uv run gza work`, `uv run gza merge <id>`, `uv run gza sync <id>`, and `uv run gza log <id>`

**Output sections:**

```
## Failed Recovery
## Unimplemented Plans/Explores
## Unmerged Work
## Queue State
## Suggested Next Steps
```

---

## gza-rebase

**Rebase current branch on main, with interactive conflict resolution.**

Use `/gza-rebase` when your branch has fallen behind main and needs rebasing before merge. Handles conflict resolution interactively, explaining each conflict and asking for approval before editing.

**Key behaviors:**
- Checks for uncommitted changes before starting (stops if any exist)
- Lets you choose between `origin/main` (default) or local `main` as the rebase target
- For each conflict: explains what both sides are doing, proposes a resolution, asks for approval, edits the file, verifies Python syntax, and stages the file
- Supports `--auto` mode for automation: resolves conflicts using best judgment, aborts on low-confidence conflicts
- Never force-pushes automatically — shows the push command for you to run

**After rebase:**

```bash
git push --force-with-lease
```

---

## gza-code-review-interactive

**Review changes on current branch and output a structured review. Optionally post to PR with --pr flag.**

Use `/gza-code-review-interactive` for a focused review of the changes on your current branch.

---

## gza-code-review-full

**Comprehensive pre-release code review assessing test coverage, code duplication, and component interactions.**

Use `/gza-code-review-full` before a release or when you want a full quality assessment of the codebase. Reviews 10 dimensions including unit test coverage, functional test coverage, code duplication, and component interactions. Writes findings to `reviews/<timestamp>-code-review-full.md`.

---

## gza-test-and-fix

**Run mypy and pytest, fix any errors found in files changed on the current branch, then commit all fixes.**

Use `/gza-test-and-fix` before declaring any task complete. Only fixes errors in changed files (compared against `main`), never touches unrelated files.

---

## gza-plan-review

**Run an interactive quality gate for a plan task and produce a go or no-go recommendation before implementation.**

Use `/gza-plan-review` to evaluate a completed plan task before creating an implementation task from it.

---

## gza-plan-improve

**Refine a draft plan by asking targeted questions, resolving gaps, and rewriting it into an implementation-ready plan.**

Use `/gza-plan-improve` when a plan exists but is still rough. It asks the smallest useful set of follow-up questions, sharpens scope and acceptance criteria, and produces a stronger revised plan instead of only grading the current one.

**Key behaviors:**
- Accepts either a full prefixed plan task ID like `gza-1234` or pasted draft plan text
- Reuses the same core quality dimensions as plan review: scope, acceptance criteria, risks, dependencies, and test strategy
- Focuses the question loop on the highest-leverage gaps first
- Rewrites the plan into a clearer structure with explicit assumptions, sequencing, and open questions
- Points the user to `/gza-plan-review` afterward when they want a final go/no-go decision

---

## gza-explore-summarize

**Summarize an explore task, extract the important findings, and suggest concrete next steps.**

Use `/gza-explore-summarize` when an explore task produced useful markdown but you want a faster handoff into action. It reads the explore task output, identifies the decision-relevant findings, and recommends whether the next move should be `plan`, `implement`, or a narrower follow-up `explore` task.

**Key behaviors:**
- Starts from a full prefixed explore task ID like `gza-1234`
- Synthesizes the report into key findings, implications, and remaining uncertainty
- Avoids raw dump output in favor of operator-facing summary
- Recommends the most likely next workflow and concrete `uv run gza add --based-on <TASK_ID>` commands
- Falls back to clarifying questions only when the report is too thin or the desired decision is unclear

---

## gza-docs-review

**Review documentation for accuracy, completeness, and missing information that users may need.**

Use `/gza-docs-review` to audit documentation before a release or after adding new features. Verifies docs against CLI `--help` output, checks for missing commands, and reviews specs for accuracy. Writes findings to `reviews/<timestamp>-docs-review.md`.

---

## Authoring Skills

### Where Skills Live

The source of truth for all `/gza-*` skills is:

```
src/gza/skills/<skill-name>/SKILL.md
```

**Always edit skills in `src/gza/skills/`, never in `.claude/skills/`.** The `.claude/skills/` copies are installed artifacts that get overwritten by `gza skills-install`.

New skills are auto-discovered — adding a directory under `src/gza/skills/` with a `SKILL.md` file is sufficient. No registry to update.

### SKILL.md Frontmatter

```yaml
---
name: gza-task-add
description: Create a well-formed gza task with appropriate type, group, and prompt
allowed-tools: Read, Bash(uv run gza add:*), AskUserQuestion
version: 1.0.0
public: true
---
```

| Field | Required | Description |
|-------|----------|-------------|
| `name` | Yes | Skill identifier, must match the directory name |
| `description` | Yes | Short description shown in skill listings |
| `allowed-tools` | Yes | Comma-separated list of tools the skill may use |
| `version` | Yes | Semantic version string |
| `public` | No | `true` to expose via `skills-install`. Defaults to `false` |

### Public vs Private

- **`public: true`** — included when running `gza skills-install`
- **`public: false` (or omitted)** — internal/developer-only, not installed by default

### Adding a New Skill

1. Create `src/gza/skills/<your-skill-name>/SKILL.md`
2. Add required frontmatter and skill instructions
3. Set `public: true` if user-facing
4. Run `gza skills-install` to install into the project
