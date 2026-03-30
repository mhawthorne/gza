# Gza

A coding AI agent runner for Claude Code.

## Quick Reference

**Task Management - CRITICAL**: When the user mentions "task", "gza task", "add a task", "create a task", or asks to track/remember something for later, ALWAYS use `uv run gza add "..."`. NEVER edit `etc/todo.txt` or any other files manually for task tracking.

## Usage

**IMPORTANT**: Always use `uv run gza` to run gza commands. Do NOT use `gza` directly or `python -m gza` - these will fail.

```
gza init [--project DIR]              # Generate new gza.yaml with defaults
gza work [--project DIR]              # Run the next pending task
gza next [--project DIR]              # List upcoming pending tasks
gza history [--project DIR]           # List recent completed/failed tasks
gza stats [--project DIR]             # Show cost and usage statistics
gza lineage <task-id> [--project DIR] # Show full ancestor/descendant tree for a task
gza validate [--project DIR]          # Validate gza.yaml configuration
gza skills-install [SKILLS...]        # Install gza Claude Code skills to project
```

See [Configuration Reference](docs/configuration.md) for the full command list and all options.

The `--project` (or `-C`) option specifies the target project directory and can be used with any command. If not specified, the current directory is used.

Options for `init`:
- `--force` - Overwrite existing gza.yaml file

Options for `history`:
- `--last N` / `-n N` - Show last N tasks (default: 5)
- `--type TYPE` - Filter by task type (explore, plan, implement, review, improve, rebase, internal)
- `--days N` - Show only tasks from the last N days
- `--start-date YYYY-MM-DD` - Show only tasks on or after this date
- `--end-date YYYY-MM-DD` - Show only tasks on or before this date
- `--status STATUS` - Filter by status (completed, failed, unmerged)
- `--incomplete` - Show only unresolved tasks (failed or unmerged)
- `--lineage-depth N` - Expand lineage N levels for each task

**Note**: `gza history` also reconciles any orphaned `in_progress` tasks (tasks whose worker process is no longer alive) to `failed (WORKER_DIED)` before listing. This is a side effect that may change task statuses in the database. If you use `gza history` in monitoring scripts that require a read-only view, be aware that orphaned task statuses will be updated on each invocation.

Options for `stats`:
- `--last N` / `-n N` - Show last N tasks (default: 5)
- `--all` - Show all tasks (no limit)
- `--type TYPE` - Filter by task type (explore, plan, implement, review, improve, rebase, internal)
- `--days N` - Show only tasks from the last N days
- `--start-date YYYY-MM-DD` - Show only tasks on or after this date
- `--end-date YYYY-MM-DD` - Show only tasks on or before this date

Options for `lineage`:
- `<task-id>` - ID of any task in the lineage (the root is resolved automatically)
- Displays the full ancestor/descendant tree with status, relationship labels, cost/token stats, and marks the target task with `→`

## Installing Claude Code Skills

Gza provides custom Claude Code skills that enhance the agent's ability to work with gza tasks. Install them with:

```bash
# List available skills
gza skills-install --list

# Install all skills
gza skills-install

# Install specific skills
gza skills-install gza-task-add gza-task-info

# Force overwrite existing skills
gza skills-install --force
```

Available skills:
- `gza-task-add`: Create well-formed gza tasks with appropriate types and groups
- `gza-task-draft`: Guide through deliberate task creation with clarification, risk surfacing, and prompt refinement before running gza add
- `gza-task-info`: Gather comprehensive info about specific gza tasks including status, branch, commits, and logs
- `gza-task-debug`: Diagnose why a gza task failed — analyzes logs, detects loops, checks diffs, compares baselines, and suggests fixes
- `gza-task-run`: Run a gza task inline in the current conversation, using the same prompt as background execution
- `gza-task-improve`: Address review comments for a task inline — reads the most recent review, fixes must-fix items, runs verify, and commits
- `gza-rebase`: Rebase current branch on main, with interactive conflict resolution

The skills are installed to `.claude/skills/` in your project directory.

## Architecture

Tasks are stored in a SQLite database (`.gza/gza.db`), not in YAML files. The database handles task state, history, and coordination.

## Configuration

Gza is configured via `gza.yaml` in the project root. Key fields:

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `project_name` | string | (required) | Project name, used for branch prefixes and Docker image naming |
| `verify_command` | string | `""` | Shell command run before finishing any `task`/`implement`/`improve` task. Agents are instructed to run this and fix any errors. Example: `uv run mypy src/ && uv run pytest tests/ -x -q` |
| `max_turns` | int | 50 | Maximum conversation turns per task |
| `timeout_minutes` | int | 10 | Maximum time per task in minutes |
| `use_docker` | bool | true | Whether to run Claude in a Docker container |
| `branch_mode` | string | `"multi"` | `"single"` (reuse one branch) or `"multi"` (new branch per task) |
| `provider` | string | `"claude"` | AI provider: `"claude"`, `"gemini"`, or `"codex"` |
| `model` | string | `""` | Provider-specific model name (optional) |
| `task_types` | dict | `{}` | Per-task-type overrides for `model` and `max_turns` |
| `branch_strategy` | string or dict | `"monorepo"` | Branch naming strategy (presets: `monorepo`, `conventional`, `simple`, `date_slug`) |
| `work_count` | int | 1 | Number of tasks to run in a single work session |
| `advance_create_reviews` | bool | `true` | When `true` **and** `advance_requires_review` is also `true`, `gza advance` automatically creates a review task and spawns a worker for completed implement tasks that have no review yet. Has no effect when `advance_requires_review` is `false` (tasks merge directly without a review in that case). |
| `advance_requires_review` | bool | `true` | When `true`, `gza advance` refuses to merge an implement task that has no passing (APPROVED) review. Set both flags to `false` for legacy merge-without-review behavior |
| `max_resume_attempts` | int | `1` | Maximum number of times `gza advance` will auto-resume a failed task (for MAX_STEPS or MAX_TURNS failures). Can be overridden per-run with `--max-resume-attempts N`. |
| `max_review_cycles` | int | `3` | Maximum number of review/improve cycles before `gza advance` stops and flags a task for human intervention. Overridable per-run with `--max-review-cycles N`. |
| `merge_squash_threshold` | int | `0` | When > 0, `gza advance` squash-merges branches with this many commits or more. `0` disables auto-squash (default). `1` = always squash. Overridable per-run with `--squash-threshold N`. |

### Tmux Configuration

Tasks can run inside tmux sessions so you can attach interactively with `gza attach`. Configure under the `tmux:` key in `gza.yaml`:

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `tmux.enabled` | bool | `true` | Run tasks inside tmux sessions. Set `false` to disable tmux (e.g. in CI or environments where tmux is unavailable). |
| `tmux.auto_accept_timeout` | float | `10.0` | Seconds of quiescence (no new output) before the proxy auto-sends Enter to accept a tool prompt. Must be > 0. |
| `tmux.max_idle_timeout` | float | `300.0` | Seconds of quiescence before the proxy assumes the session is stuck, sends Ctrl-C + EOF, and exits. Must be > 0. |
| `tmux.detach_grace` | float | `5.0` | Seconds to wait after a human detaches before auto-accept resumes. Gives users time to reattach after an accidental detach. Must be > 0. |
| `tmux.terminal_size` | list[int] | `[200, 50]` | Terminal dimensions `[cols, rows]` for the tmux session. Both values must be positive integers. |

Example:
```yaml
tmux:
  enabled: true
  auto_accept_timeout: 10
  max_idle_timeout: 300
  detach_grace: 5
  terminal_size: [200, 50]
```

Use `gza attach <worker-id>` or `gza attach <task-id>` to connect to a running task's tmux session.

### LLM-Powered Learnings Summarization

After each completed task (on interval), gza can automatically update `.gza/learnings.md` by running an LLM to consolidate patterns from recent completed tasks.

**How it works**:
1. An `internal` task is created in the DB with `skip_learnings=True` (to prevent recursion) and run via the standard runner — same as explore/plan/review tasks (worktree, provider, status transitions).
2. The LLM produces bullet-point learnings from recent task outputs; these replace/merge into `.gza/learnings.md`.
3. On any failure (non-zero exit, exception), gza falls back to the existing regex-based extraction.
4. The `internal` task is kept in the DB for observability (visible via `gza history --type internal`).

**Architecture note**: Always use the provider system (via `runner.run()` or `get_provider()`) for LLM calls — never hardcode provider-specific CLI commands. This keeps gza provider-agnostic across Claude, Codex, and Gemini.

**`skip_learnings` field** (on `db.Task`): When `True`, the task's completion will not trigger `maybe_auto_regenerate_learnings`. This is set automatically on `internal` learnings tasks to prevent infinite recursion. It can also be set manually on any task type to suppress learnings updates.

### verify_command

The `verify_command` field ensures agents always run project-specific verification before finishing. When set, gza injects the following instruction into `task`, `implement`, and `improve` prompts:

> Before finishing, run the following verification command and fix any errors: `<verify_command>`

This is NOT added to `explore`, `plan`, `review`, or `internal` tasks since those don't produce code changes.

Example `gza.yaml`:
```yaml
project_name: myproject
verify_command: 'uv run mypy src/ && uv run pytest tests/ -x -q'
```

## Project Structure

Key modules:
- `src/gza/db.py` - SQLite task storage with `Task` class (uses `prompt` field)
- `src/gza/cli.py` - CLI commands
- `src/gza/runner.py` - Executes tasks via Claude Code
- `src/gza/config.py` - Configuration loading

**Important**: `db.Task` is the single canonical task model/storage API.

## Running in Docker

Gza tasks run inside a Docker container. The container:
- Mounts the project at `/workspace`
- Has Python 3.11+ but limited pre-installed packages
- Use `uv run` for all commands (e.g., `uv run pytest tests/ -v`)

**Do NOT use** `python -m pytest` or `pip install` directly - always use `uv run`.

**Do NOT use the `sqlite3` CLI** — it may not be installed. To query the database programmatically:
```python
from pathlib import Path
from gza.db import SqliteTaskStore, Task
store = SqliteTaskStore(Path('.gza/gza.db'))
task = store.get(42)  # or store.get_all(), store.get_pending(), store.get_by_task_id('slug')
```

**Do NOT modify files outside `/workspace/gza/`** unless explicitly instructed. Other directories under `/workspace/` are sibling projects.

## Renaming/Refactoring Tips

When renaming a field across the codebase:
1. Use search-and-replace across files rather than editing one occurrence at a time
2. Check `db.py` and all call sites for Task-related changes
3. Update tests in bulk, not one test method at a time

## Code Reuse

**Single code path principle**: When the same behavior is needed in multiple places, implement it once and call it from all locations. Don't copy logic.

Example: If `--run` means "create then immediately execute", it should call the same execution path as `gza work`, not duplicate the post-completion logic (PR posting, auto-review, etc.). Otherwise bugs appear where one path works and another doesn't.

Signs you're violating this:
- Copy-pasting code between functions
- Adding "if task_type == X" checks in multiple places for the same behavior
- Post-completion hooks that only run from some entry points
- A background/automated version of a command that uses a different mechanism than the foreground/manual version

**Multiple entry points, same mechanism**: Many operations (rebase, review, improve, work) can be triggered from multiple entry points: direct CLI invocation, `--background` flag, `gza advance`, Docker, etc. All entry points for the same operation must use the same underlying mechanism. For example, `gza rebase --background` must create a rebase task and run it through the standard runner — the same path `gza advance` uses when it detects conflicts. Do not create a separate "background rebase" implementation that bypasses the runner.

## Important Guidelines

- **Always run gza commands from your starting directory** - Do not `cd` to other directories before running gza commands unless explicitly instructed. Gza uses the current directory to find `gza.yaml`, `.gza/`, and the task database. Running from the wrong directory will target the wrong project or fail.
- **Do NOT run git commands** - Gza handles all git operations (branching, committing, pushing) automatically after your task completes. Just make the code changes and let gza commit them. If git fails with "not a git repository" (e.g., in a cleaned-up worktree), do not attempt `git init` or `--git-dir` workarounds — report the issue and stop.
- **Run /gza-test-and-fix before completing any task** - You MUST invoke the `/gza-test-and-fix` skill before declaring a task complete. This runs mypy and pytest, automatically fixes any failures in files changed on the current branch, and commits the fixes. Do not mark a task as done until `/gza-test-and-fix` passes cleanly.
- **Test retry circuit breaker** - If the same test fails 3 times with the same error, stop and report the issue instead of continuing to retry. Looping on unfixable tests wastes turns and budget.
- **Do NOT delete git branches** unless explicitly asked to. Branches should be preserved for history and reference.
- **Do NOT create summary or documentation files** (e.g., `IMPLEMENTATION_SUMMARY.md`, `CHANGES.md`, `*_SETUP.md`). Just make the code changes and commit them. If summaries are needed, they will be handled separately.
- **Do NOT create README files** unless explicitly requested.
- **Do NOT create setup/how-to docs in project root**. If you must document something for developers (e.g., release process, setup instructions), place it in `docs/internal/` - never in the project root.
- **Do NOT create one-off utility scripts** in the project root (e.g., `check_syntax.py`, `validate_*.py`, `verify_*.py`). Use existing tools like `uv run pytest` or `uv run python -m py_compile <file>` instead.
- **Use offset/limit when reading large files** - When reading files that might be large (>1000 lines), use the `offset` and `limit` parameters on the Read tool. If you get a file-too-large error, retry with `limit=500` and navigate using `offset`.
- **Use Explore subagents for multi-file research** - When you need to understand code across 3+ files before making changes (e.g., tracing a feature, reviewing an implementation, understanding call sites), delegate the exploration to an Agent tool with `subagent_type: Explore` rather than sequentially reading files yourself. This runs in parallel and keeps your main context clean. Especially useful for review tasks and pre-implementation research.

## Creating Tasks from Conversations

When a conversation identifies work to be done, create a gza task rather than implementing inline:

```bash
# Basic task
uv run gza add "description of what needs to be done"

# With task type (plan, implement, review, explore)
uv run gza add --type plan "explore authentication options and propose approach"
uv run gza add --type implement "add user authentication with JWT"

# Auto-review after implementation
uv run gza add --type implement --review "add dark mode toggle"

# Task chaining - implementation based on a plan
uv run gza add --type implement --based-on 5 "implement the approach from task #5"

# Improve task to address review feedback (runs immediately by default)
uv run gza improve 29  # where 29 is the implementation task ID (runs immediately)
uv run gza improve 29 --review  # auto-create a review after improvements
uv run gza improve 29 --queue  # add to queue without executing

# Create and run a review task (runs immediately by default, with optional PR posting)
uv run gza review 42            # creates review, runs it, auto-posts to PR if exists
uv run gza review 42 --no-pr   # creates review, runs it, skips PR posting
uv run gza review 42 --pr      # creates review, runs it, errors if no PR found
uv run gza review 42 --queue   # creates review, adds to queue without executing

# Create a pull request (caches PR number for future reviews)
uv run gza pr 42                    # creates PR from implementation task #42
```

Tips for good task descriptions:
- Be specific about what needs to change and where
- Reference file paths or components when known
- For multi-step work, create a `--type plan` task first
- Use `--review` flag for significant changes that warrant code review
- Use `gza edit <id>` to update a task's prompt instead of deleting and recreating

## Review and PR Workflow

Gza supports automated code review and PR integration:

### Review Tasks

Reviews run immediately by default and automatically post to PRs when available:

```bash
# Create and run review (auto-posts to PR if exists)
gza review <task-id>

# Skip PR posting
gza review <task-id> --no-pr

# Require PR to exist
gza review <task-id> --pr

# Add to queue without executing
gza review <task-id> --queue
```

### How it Works

1. **PR Creation**: When you create a PR with `gza pr`, the PR number is cached in the task
2. **Review Execution**: When you run `gza review --run`, the review completes and outputs to `.gza/reviews/{task-id}.md`
3. **Auto-Posting**: If a PR exists for the implementation task, the review is automatically posted as a PR comment
4. **PR Discovery**: Gza finds PRs via:
   - Cached `pr_number` field (fastest)
   - Branch lookup via `gh pr view` (fallback)

### Improve Task Commits

Improve task commits include `Gza-Review: #<id>` trailers for traceability:

```
Improve implementation based on review #30

Address input validation issues identified in code review.

Task ID: 20260211-improve-authentication
Gza-Review: #30
```

### Full Workflow Example

```bash
# 1. Create and implement a plan
gza add --type plan "Design feature X"
gza work

gza add --type implement --based-on 1 --review "Implement per plan"
gza work    # Implements code
gza work    # Runs auto-created review

# 2. Create PR (stores PR number)
gza pr 2

# 3. Run review (auto-posts to PR)
gza review 2

# 4. If changes requested, iterate
gza improve 2  # Runs immediately, commits include "Gza-Review: #3" trailer

gza review 2  # New review auto-posts to PR
```

## Building Skills

Gza skills are Claude Code skills that agents can invoke during task execution. Here's how to author and ship them.

### Where Skills Live

The source of truth for all `/gza-*` skills is:

```
src/gza/skills/<skill-name>/SKILL.md
```

For example: `src/gza/skills/gza-task-add/SKILL.md`

**Always edit skills in `src/gza/skills/`, never in `.claude/skills/`.** The `.claude/skills/` copies are installed artifacts — they get overwritten by `gza skills-install`.

When gza is installed, the `skills/` directory is included as package data. There is **no registry to update** — adding a new directory under `src/gza/skills/` with a `SKILL.md` file is sufficient for it to be discovered automatically.

### Installing Skills into a Project

After creating or editing skills in `src/gza/skills/`, install them into the project with:

```bash
gza skills-install
```

This copies skills from the installed gza package into the project's `.claude/skills/` directory, where Claude Code picks them up. Use `--force` to overwrite existing skills.

### SKILL.md Frontmatter Fields

Each `SKILL.md` begins with YAML frontmatter:

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
| `public` | No | Set to `true` to expose the skill via `skills-install`. Defaults to `false` |

### Public vs. Private Skills

The `public` field controls which skills are exposed to end users:

- **`public: true`** — Skill is included when running `gza skills-install`
- **`public: false` (or omitted)** — Skill is internal/developer-only and not installed by default

The function `get_available_skills(public_only=True)` in `src/gza/skills_utils.py` is what `skills-install` uses to determine which skills to expose.

### Adding a New Skill

1. Create a directory: `src/gza/skills/<your-skill-name>/`
2. Create `SKILL.md` with required frontmatter and skill instructions
3. Set `public: true` if the skill should be user-facing
4. The skill is automatically discovered — no registry update needed

## Development

After making changes, run the test suite to verify everything works:

```
# Unit tests (fast, no external dependencies)
uv run pytest tests/ -v

# Integration tests (requires Docker and/or API credentials)
uv run pytest tests_integration/ -v -m integration
```

### Testing Guidelines

- **Write tests for every change** - Each feature, bug fix, or enhancement should include corresponding tests
- Tests go in `tests/` with `test_` prefix (e.g., `tests/test_importer.py`)
- Use pytest fixtures for common setup (see existing tests for patterns)
- Test both success cases and error handling
- Run tests after making changes: `uv run pytest tests/ -v`
- For CLI changes, add tests in `tests/test_cli.py` following the existing class structure (e.g., `TestAddCommandWithChaining`)

## Temporary Files

When creating temporary files (e.g., task import files, test data), write them to the `tmp/` directory in the project root. This directory is gitignored and keeps the project clean.

## Line Endings

Always use Unix-style line endings (LF, `\n`). Do NOT use Windows-style line endings (CRLF, `\r\n`). This applies to all files in the repository, including shell scripts, Python files, and configuration files.
