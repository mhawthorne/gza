# Skills

Gza provides Claude Code skills that enhance the agent's ability to work with gza tasks. Install them into your project with:

```bash
# Install all skills
gza claude-install-skills

# Install specific skills
gza claude-install-skills gza-task-add gza-task-info

# List available skills
gza claude-install-skills --list
```

Skills are installed to `.claude/skills/` in your project directory and become available as slash commands in Claude Code.

---

## gza-task-add

**Create a well-formed gza task with appropriate type, group, and prompt.**

Use `/gza-task-add` when you want to add a task to the gza queue during a Claude Code session. The skill reads your project's AGENTS.md conventions, asks clarifying questions, constructs a well-scoped prompt, and runs `gza add` with the right flags.

**Key behaviors:**
- Asks about task type (`task`, `explore`, `plan`, `implement`, `review`)
- Prompts for optional flags: `--group`, `--depends-on`, `--based-on`, `--review`
- Generates a specific prompt (not vague) and shows the task ID on success
- Always uses `uv run gza add` — never edits task files manually

**Example output:**

```bash
uv run gza add --type implement --review "add JWT authentication to src/api/routes.py"
# Created task #42 (implement)
```

---

## gza-task-draft

**Guide user through deliberate task creation with clarification and refinement before running gza add.**

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
Task #18: completed
Type: implement
Branch: 20260115-add-authentication (3 commits, not yet merged to main)
Duration: 245.3s (4:05)
Cost: $0.42
Prompt: "Add JWT authentication to API endpoints"
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

## gza-summary

**Summarize recent gza task activity and suggest next steps.**

Use `/gza-summary` for a quick status overview: what completed recently, what's unmerged, and what's pending. Returns a prioritized list of suggested next actions.

**Key behaviors:**
- Runs `gza history`, `gza unmerged`, and `gza next` to collect current state
- Highlights failed tasks, unmerged branches, and blocked pending tasks
- For plan/explore/review tasks, includes the report file path so you can open it directly
- Suggests specific commands to run (e.g., `gza work`, `git merge`, `uv run gza log 25 --task`)

**Output sections:**

```
## Recent Activity
## Unmerged Work
## Pending Tasks
## Suggested Next Steps
```
