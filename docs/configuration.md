# Gza Configuration Reference

This document provides a comprehensive reference for all configuration options available in Gza.

## Configuration File (gza.yaml)

The main configuration file is `gza.yaml` in your project root directory.

### Required Configuration

| Option | Type | Description |
|--------|------|-------------|
| `project_name` | String | Project name used for branch prefixes and Docker image naming |

### Optional Configuration

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `tasks_file` | String | `tasks.yaml` | Path to legacy tasks file |
| `log_dir` | String | `.gza/logs` | Directory for log files |
| `use_docker` | Boolean | `true` | Whether to run Claude in Docker container |
| `docker_image` | String | `{project_name}-gza` | Custom Docker image name |
| `docker_volumes` | List | `[]` | Custom Docker volume mounts (e.g., `["/host:/container:ro"]`) |
| `timeout_minutes` | Integer | `10` | Maximum time per task in minutes |
| `branch_mode` | String | `multi` | Branch strategy: `single` or `multi` |
| `max_turns` | Integer | `50` | Maximum conversation turns per task |
| `worktree_dir` | String | `/tmp/gza-worktrees` | Directory for git worktrees |
| `work_count` | Integer | `1` | Number of tasks to run in a single work session |
| `provider` | String | `claude` | AI provider: `claude`, `codex`, or `gemini` |
| `providers` | Dict | `{}` | Provider-scoped model/task-type config (preferred) |
| `model` | String | *(empty)* | Legacy global model fallback (compatible) |
| `task_types` | Dict | `{}` | Legacy global per-task fallback (compatible) |
| `claude` | Dict | *(see below)* | Claude-specific configuration section |
| `claude.fetch_auth_token_from_keychain` | Boolean | `false` | Fetch OAuth token from macOS Keychain for Docker (macOS only) |
| `claude.args` | List | `["--allowedTools", "Read", "Write", "Edit", "Glob", "Grep", "Bash"]` | Arguments passed to Claude Code CLI |
| `claude_args` | List | *(deprecated)* | Use `claude.args` instead |

### Branch Naming Strategy

Configure branch naming with the `branch_strategy` option. Three presets are available:

```yaml
# Preset: monorepo (default)
# Generates: {project}/{task_id}
# Example: myproject/20260108-add-feature
branch_strategy: monorepo

# Preset: conventional
# Generates: {type}/{slug}
# Example: feature/add-feature
branch_strategy: conventional

# Preset: simple
# Generates: {slug}
# Example: add-feature
branch_strategy: simple
```

Or use a custom pattern:

```yaml
branch_strategy:
  pattern: "{type}/{slug}"
  default_type: feature
```

**Available pattern variables:**

| Variable | Description |
|----------|-------------|
| `{project}` | Project name |
| `{task_id}` | Full task ID (YYYYMMDD-slug) |
| `{date}` | Date portion (YYYYMMDD) |
| `{slug}` | Slug portion |
| `{type}` | Inferred or default type |

**Branch types** are automatically inferred from task prompts:

| Type | Trigger Keywords |
|------|-----------------|
| `docs` | documentation, document, doc, docs, readme |
| `test` | tests, test, spec, coverage |
| `perf` | performance, optimize, speed |
| `refactor` | refactor, restructure, reorganize, clean |
| `fix` | fix, bug, error, crash, broken, issue |
| `chore` | chore, update, upgrade, bump, deps, dependencies |
| `feature` | feat, feature, add, implement, create, new |

### Docker Volume Mounts

Mount additional directories or files from the host into the Docker container using `docker_volumes`. This is useful for providing access to datasets, model files, or other resources.

```yaml
docker_volumes:
  - "~/datasets:/datasets:ro"          # Tilde expanded automatically
  - "/Users/x/models:/models"
  - "/tmp/cache:/cache"
```

**Volume format:** Each volume uses Docker's standard volume syntax:
- `source:destination` - Mount `source` from host to `destination` in container
- `source:destination:mode` - Add mode flags like `ro` (read-only) or `rw` (read-write)

**Common mount modes:**
- `ro` - Read-only mount (recommended for input data)
- `rw` - Read-write mount (default if omitted)
- `z` - SELinux label sharing (for container isolation)
- `Z` - SELinux exclusive label (for single container)

**Environment variable override:**
```bash
# Override config with comma-separated volumes
export GZA_DOCKER_VOLUMES="~/data:/data:ro,~/models:/models"
gza work
```

**Notes:**
- Volumes are only used when `use_docker: true`
- Tilde (`~`) in source paths is automatically expanded to your home directory
- The workspace is always mounted at `/workspace` automatically
- Config validation warns about common syntax errors but doesn't block invalid formats

### Provider-Scoped Configuration

Preferred approach for multi-provider setups:

```yaml
provider: claude
providers:
  claude:
    model: claude-sonnet-4-5
    task_types:
      review:
        model: claude-haiku-4-5
        max_turns: 20
  codex:
    model: o4-mini
```

### Task Types Configuration (Legacy-Compatible)

Override settings per task type:

```yaml
task_types:
  explore:
    model: claude-sonnet-4-5
    max_turns: 20
  plan:
    model: claude-opus-4
    max_turns: 30
  review:
    max_turns: 15
```

Valid task types: `task`, `explore`, `plan`, `implement`, `review`, `improve`

Top-level `task_types` and `model` are still supported for backward compatibility. They are used as fallbacks when no provider-scoped value exists.

### Resolution Precedence

Provider selection:
1. `task.provider`
2. `provider` (already merged with `GZA_PROVIDER`)

Model selection:
1. `task.model`
2. `providers.<effective_provider>.task_types.<task_type>.model`
3. `providers.<effective_provider>.model`
4. `task_types.<task_type>.model` (legacy fallback)
5. `model` / `defaults.model` / `GZA_MODEL` (legacy fallback)
6. Provider runtime default (if no model resolved)

Max turns selection:
1. `providers.<effective_provider>.task_types.<task_type>.max_turns`
2. `task_types.<task_type>.max_turns` (legacy fallback)
3. `max_turns` / `defaults.max_turns` / `GZA_MAX_TURNS`

---

## Environment Variables

All `gza.yaml` options can be overridden via environment variables:

| Environment Variable | Maps To | Description |
|---------------------|---------|-------------|
| `GZA_USE_DOCKER` | `use_docker` | Override Docker usage (`true`/`false`) |
| `GZA_TIMEOUT_MINUTES` | `timeout_minutes` | Override task timeout |
| `GZA_BRANCH_MODE` | `branch_mode` | Override branch strategy |
| `GZA_MAX_TURNS` | `max_turns` | Override max conversation turns |
| `GZA_WORKTREE_DIR` | `worktree_dir` | Override worktree directory |
| `GZA_WORK_COUNT` | `work_count` | Override tasks per session |
| `GZA_PROVIDER` | `provider` | Override AI provider |
| `GZA_MODEL` | `model` | Override global legacy model fallback |

### Providers and Models

Gza supports multiple AI providers for task execution:

| Provider | Status | Description |
|----------|--------|-------------|
| `claude` | **Supported** | Claude Code CLI (default) |
| `codex` | **Supported** | OpenAI Codex CLI |
| `gemini` | *Experimental* | Gemini CLI - partially implemented |

Set your provider in `gza.yaml`:

```yaml
provider: claude
model: claude-sonnet-4-5  # optional: override the default model
```

Or via environment variable:

```bash
export GZA_PROVIDER=claude
export GZA_MODEL=claude-sonnet-4-5
```

`GZA_MODEL` is provider-agnostic and applies as a global legacy fallback, so it can override to a model that doesn't match the selected provider if set manually.

### Provider Credentials

**Claude:**

| Variable | Description |
|----------|-------------|
| `ANTHROPIC_API_KEY` | API key for Claude (alternative to OAuth) |

Claude supports two authentication methods:

1. **OAuth** (subscription): Run `claude login` on host. Credentials stored in `~/.claude/` are mounted into Docker. Uses your Claude Max subscription.
2. **API Key**: Set `ANTHROPIC_API_KEY` in `~/.gza/.env`. Uses pay-per-token API pricing.

**Important:** `ANTHROPIC_API_KEY` takes precedence over OAuth. If you have both configured and want to use your subscription, comment out or remove `ANTHROPIC_API_KEY` from your `.env` files.

**Codex:**

| Variable | Description |
|----------|-------------|
| `CODEX_API_KEY` | API key for OpenAI Codex (alternative to OAuth) |

Codex authentication priority:
1. OAuth (`~/.codex/auth.json`) - preferred, uses ChatGPT pricing
2. `CODEX_API_KEY` - fallback, uses standard OpenAI API pricing

**Gemini:**

| Variable | Description |
|----------|-------------|
| `GEMINI_API_KEY` | Primary API key for Gemini |
| `GOOGLE_API_KEY` | Alternative API key (Vertex AI) |
| `GOOGLE_APPLICATION_CREDENTIALS` | Path to service account JSON file |
| `GEMINI_SHELL_ENABLED` | Enable shell commands (`true`) |

---

## Dotenv Files (.env)

Environment variables can be set in `.env` files:

| Location | Scope |
|----------|-------|
| `~/.gza/.env` | User-level (applies to all projects) |
| `.env` | Project-level (overrides everything) |

**Precedence order** (highest to lowest):

1. **Project `.env`** - Overrides all other sources
2. **Shell environment** - Variables exported in your shell
3. **`~/.gza/.env`** - Only sets values not already defined

This means if you have `ANTHROPIC_API_KEY` set in your shell, you don't need `~/.gza/.env` at all. The home `.env` file uses `setdefault` behavior, so it won't override existing environment variables.

**Format:**

```
ANTHROPIC_API_KEY=sk-ant-...
GZA_MAX_TURNS=100
GZA_TIMEOUT_MINUTES=15
```

---

## Command-Line Arguments

### Global Options

All commands support these options:

| Option | Description |
|--------|-------------|
| `--project`, `-C` | Target project directory (default: current directory) |
| `--help`, `-h` | Show help for command |

```bash
gza <command> [options]
gza -C /path/to/project <command>
```

### work

Run tasks from the queue.

```bash
gza work [task_id...] [options]
```

| Option | Description |
|--------|-------------|
| `task_id` | Specific task ID(s) to run (can specify multiple) |
| `--no-docker` | Run Claude directly instead of in Docker |
| `--count N`, `-c N` | Number of tasks to run before stopping |
| `--background`, `-b` | Run worker in background |
| `--max-turns N` | Override max_turns setting for this run |

### add

Add a new task.

```bash
gza add [prompt] [options]
```

| Option | Description |
|--------|-------------|
| `prompt` | Task prompt (opens $EDITOR if not provided) |
| `--edit`, `-e` | Open $EDITOR to write the prompt |
| `--type TYPE` | Set task type: `task`\|`explore`\|`plan`\|`implement`\|`review`\|`improve` |
| `--branch-type TYPE` | Set branch type hint for naming |
| `--explore` | Create explore task (shorthand) |
| `--group NAME` | Set task group |
| `--based-on ID` | Base on previous task |
| `--depends-on ID` | Set dependency on another task |
| `--review` | Auto-create review task on completion |
| `--same-branch` | Continue on depends_on task's branch |
| `--spec FILE` | Path to spec file for context |
| `--prompt-file FILE` | Read prompt from file (for non-interactive use) |

### edit

Edit an existing task.

```bash
gza edit <task_id> [options]
```

| Option | Description |
|--------|-------------|
| `--group NAME` | Move task to group (empty `""` removes) |
| `--based-on ID` | Set dependency |
| `--explore` | Convert to explore task |
| `--task` | Convert to regular task |
| `--review` | Enable automatic review task creation on completion |
| `--prompt TEXT` | Set new prompt directly (use `-` for stdin) |
| `--prompt-file FILE` | Read new prompt from file |

### log

View task or worker logs.

```bash
gza log <identifier> [options]
```

| Option | Description |
|--------|-------------|
| `--task`, `-t` | Look up by task ID (mutually exclusive with -s, -w) |
| `--slug`, `-s` | Look up by task slug (mutually exclusive with -t, -w) |
| `--worker`, `-w` | Look up by worker ID (mutually exclusive with -t, -s) |
| `--turns` | Show full conversation turns |
| `--follow`, `-f` | Follow log in real-time |
| `--tail N` | Show last N lines |
| `--raw` | Show raw JSON lines |

**Note:** One of `--task`, `--slug`, or `--worker` is required.

### stats

Show task statistics.

```bash
gza stats [options]
```

| Option | Description |
|--------|-------------|
| `--last N` | Show last N tasks (default: 5) |

### pr

Create a pull request for a completed task.

```bash
gza pr <task_id> [options]
```

| Option | Description |
|--------|-------------|
| `--title TITLE` | Override auto-generated PR title |
| `--draft` | Create as draft PR |

### delete

Delete a task.

```bash
gza delete <task_id> [options]
```

| Option | Description |
|--------|-------------|
| `--yes`, `-y` | Skip confirmation prompt |
| `--force`, `-f` | Deprecated alias for `--yes` |

### clean

Archive old log and worker files to free up disk space. By default, files are moved to `.gza/archive/` rather than deleted.

```bash
gza clean [options]
```

| Option | Description |
|--------|-------------|
| `--days N` | Archive files older than N days (default: 30) |
| `--purge` | Delete archived files instead of archiving (default threshold: 365 days) |
| `--dry-run` | Show what would be archived/deleted without doing it |

**Example usage:**

```bash
# Preview what would be archived (default: 30 days)
gza clean --dry-run

# Archive files older than 30 days
gza clean

# Archive files older than 7 days
gza clean --days 7

# Delete files from archive older than 365 days
gza clean --purge

# Delete archived files older than 60 days
gza clean --purge --days 60
```

The clean command archives files from:
- `.gza/logs/` - Task execution logs
- `.gza/workers/` - Worker metadata files

Files are moved to `.gza/archive/` based on their modification time. Use `--purge` to permanently delete archived files.

### import

Import tasks from a YAML file.

```bash
gza import [file] [options]
```

| Option | Description |
|--------|-------------|
| `file` | YAML file to import from |
| `--dry-run` | Preview without creating tasks |
| `--force`, `-f` | Skip duplicate detection |

### status

Show tasks in a group.

```bash
gza status <group>
```

### ps

Show running workers.

```bash
gza ps [options]
```

| Option | Description |
|--------|-------------|
| `--all`, `-a` | Include completed/failed workers |
| `--quiet`, `-q` | Only show worker IDs |
| `--json` | Output as JSON |

### stop

Stop workers.

```bash
gza stop [worker_id] [options]
```

| Option | Description |
|--------|-------------|
| `worker_id` | Worker ID to stop |
| `--all` | Stop all running workers |
| `--force` | Force kill (SIGKILL) |

### validate

Validate configuration.

```bash
gza validate
```

### show

Show details of a specific task.

```bash
gza show <task_id>
```

### resume

Resume a failed task from where it left off. The AI continues with the existing conversation context.

```bash
gza resume <task_id> [options]
```

| Option | Description |
|--------|-------------|
| `--no-docker` | Run Claude directly instead of in Docker |
| `--background`, `-b` | Run worker in background |
| `--queue`, `-q` | Add task to queue without executing immediately |
| `--max-turns N` | Override max_turns setting for this run |

### retry

Retry a failed or completed task from scratch. Starts a fresh conversation.

```bash
gza retry <task_id> [options]
```

| Option | Description |
|--------|-------------|
| `--no-docker` | Run Claude directly instead of in Docker (only with --background) |
| `--background`, `-b` | Run worker in background |
| `--queue`, `-q` | Add task to queue without executing immediately |
| `--max-turns N` | Override max_turns setting for this run |

### merge

Merge a completed task's branch into the current branch.

```bash
gza merge <task_id> [task_id...] [options]
```

| Option | Description |
|--------|-------------|
| `task_id` | Task ID(s) to merge (can specify multiple) |
| `--squash` | Squash commits into a single commit |
| `--rebase` | Rebase onto current branch instead of merging |
| `--delete` | Delete the branch after successful merge |
| `--remote` | Fetch from origin and rebase against remote (requires --rebase) |
| `--mark-only` | Mark branch as merged without performing actual merge (deletes branch) |

### unmerged

List tasks with branches that haven't been merged to main.

```bash
gza unmerged
```

### groups

List all task groups with their task counts.

```bash
gza groups
```

### history

List recent completed or failed tasks.

```bash
gza history [options]
```

| Option | Description |
|--------|-------------|
| `--limit N`, `-n N` | Number of tasks to show (default: 10) |
| `--all` | Show all tasks (no limit) |
| `--status STATUS` | Filter by status: `completed`, `failed`, or `unmerged` |
| `--type TYPE` | Filter by task type: `task`, `explore`, `plan`, `implement`, `review`, `improve` |

### checkout

Checkout a task's branch, removing any stale worktree if needed.

```bash
gza checkout <task_id_or_branch> [options]
```

| Option | Description |
|--------|-------------|
| `task_id_or_branch` | Task ID or branch name to checkout |
| `--force`, `-f` | Force removal of worktree even if it has uncommitted changes |

### diff

Show git diff for a task's changes with colored output and pager support.

```bash
gza diff [task_id] [diff_args...]
```

| Option | Description |
|--------|-------------|
| `task_id` | Task ID to diff (optional, uses current branch if omitted) |
| `diff_args` | Arguments passed to git diff (use `--` before options like `--stat`) |

### rebase

Rebase a task's branch onto a target branch.

```bash
gza rebase <task_id> [options]
```

| Option | Description |
|--------|-------------|
| `--onto BRANCH` | Branch to rebase onto (defaults to current branch) |
| `--remote` | Fetch from origin and rebase against remote target branch |
| `--force`, `-f` | Force remove worktree even if it has uncommitted changes |

### cleanup

Clean up stale worktrees, old logs, and worker metadata.

```bash
gza cleanup [options]
```

| Option | Description |
|--------|-------------|
| `--worktrees` | Only clean up stale worktrees |
| `--logs` | Only clean up old log files |
| `--workers` | Only clean up stale worker metadata |
| `--days N` | Remove items older than N days (default: 30) |
| `--keep-unmerged` | Keep logs for tasks that are still unmerged |
| `--dry-run` | Show what would be cleaned without doing it |

### improve

Create an improve task to address review feedback on an implementation.

```bash
gza improve <impl_task_id> [options]
```

| Option | Description |
|--------|-------------|
| `impl_task_id` | Implementation task ID to improve |
| `--review` | Auto-create review task on completion |
| `--queue`, `-q` | Add task to queue without executing immediately |
| `--background`, `-b` | Run worker in background |
| `--no-docker` | Run Claude directly instead of in Docker |
| `--max-turns N` | Override max_turns setting for this run |

The improve command finds the most recent review for the implementation task and creates a new task that continues on the same branch to address the review feedback.

### review

Create and run a review task for an implementation. Runs immediately by default.

```bash
gza review <task_id> [options]
```

| Option | Description |
|--------|-------------|
| `task_id` | Implementation task ID to review |
| `--queue`, `-q` | Add task to queue without executing immediately |
| `--background`, `-b` | Run worker in background |
| `--no-docker` | Run Claude directly instead of in Docker |
| `--no-pr` | Do not post review to PR even if one exists |
| `--pr` | Require PR to exist (error if not found) |
| `--open` | Open the review file in $EDITOR after completion |

When a PR exists for the implementation task, the review is automatically posted as a PR comment.

### next

List upcoming pending tasks.

```bash
gza next [options]
```

Shows pending tasks that are ready to run (dependencies satisfied). Tasks blocked by dependencies are listed separately.

---

## Task Types

Gza supports several task types, each with distinct behavior:

| Type | Purpose | Output Location |
|------|---------|-----------------|
| `task` | General work (default) | Code changes on branch |
| `explore` | Research and investigation | `.gza/explorations/{task_id}.md` |
| `plan` | Design and architecture | `.gza/plans/{task_id}.md` |
| `implement` | Build per a plan | Code changes on branch |
| `review` | Evaluate implementation | `.gza/reviews/{task_id}.md` |
| `improve` | Address review feedback | Code changes on same branch |

**Typical workflow:**

1. `plan` - Design the approach, saved to `.gza/plans/`
2. `implement --based-on <plan_id> --review` - Build per plan, auto-create review
3. `review` runs automatically, saved to `.gza/reviews/`
4. If changes requested: `improve <impl_id>` addresses feedback on same branch

**Per-type configuration:**

Override settings for specific task types in `gza.yaml`:

```yaml
task_types:
  explore:
    model: claude-sonnet-4-5
    max_turns: 20
  plan:
    model: claude-opus-4
    max_turns: 30
  review:
    max_turns: 15
```

---

## Task Lifecycle

Tasks move through these states:

```
pending → in_progress → completed
                     ↘ failed
```

| State | Description |
|-------|-------------|
| `pending` | Task is queued and waiting to run |
| `in_progress` | A worker is currently executing the task |
| `completed` | Task finished successfully |
| `failed` | Task encountered an error or timed out |

**Recovering from failures:**

- Use `gza resume <task_id>` to continue from where the task left off (preserves conversation context)
- Use `gza retry <task_id>` to start completely fresh

**Dependencies:**

Tasks with `depends_on` set will remain pending until their dependency completes. Use `gza status <group>` to see dependency chains.

---

## Configuration Precedence

Configuration is resolved in the following order (highest to lowest priority):

1. **Command-line arguments**
2. **Environment variables** (`GZA_*`)
3. **Project `.env` file**
4. **Home `.env` file** (`~/.gza/.env`)
5. **`gza.yaml` file**
6. **Hardcoded defaults**

---

## File Locations

### Project Files

| Path | Purpose |
|------|---------|
| `gza.yaml` | Main configuration file |
| `.env` | Project-specific environment variables |
| `.gza/` | Local state directory (add to `.gitignore`) |
| `.gza/gza.db` | SQLite task database |
| `.gza/logs/` | Task execution logs |
| `.gza/workers/` | Worker metadata |
| `etc/Dockerfile.claude` | Generated Docker image for Claude |
| `etc/Dockerfile.codex` | Generated Docker image for Codex |
| `etc/Dockerfile.gemini` | Generated Docker image for Gemini |

> **Note:** The `.gza/` directory contains machine-specific state and should be added to `.gitignore`. Run `echo ".gza/" >> .gitignore` after initializing your project.

### Home Directory

| Path | Purpose |
|------|---------|
| `~/.gza/.env` | User-level environment variables |
| `~/.claude/` | Claude OAuth credentials |
| `~/.codex/` | Codex OAuth credentials |
| `~/.gemini/` | Gemini OAuth credentials |

---

## Example Configuration

```yaml
# gza.yaml
project_name: my-app

# Execution settings
use_docker: true
timeout_minutes: 15
max_turns: 80
work_count: 3

# Custom volume mounts (optional)
docker_volumes:
  - "/Users/x/datasets:/datasets:ro"
  - "/Users/x/models:/models"

# AI provider
provider: claude
model: claude-sonnet-4-5

# Branch settings
branch_mode: multi
branch_strategy: conventional

# Task type overrides
task_types:
  explore:
    max_turns: 20
  review:
    max_turns: 15
```

---

## Troubleshooting

### Task stuck in "in_progress"

If a worker crashed or was killed, tasks may be stuck in `in_progress` state:

```bash
# Check for running workers
gza ps

# If no workers are running but task shows in_progress, the worker crashed
# Resume or retry the task:
gza resume <task_id>
# or
gza retry <task_id>
```

### "No pending tasks" but tasks exist

Tasks with unmet dependencies won't be picked up. Check:

```bash
gza next          # Shows pending tasks and their dependencies
gza status <group>  # Shows dependency chain status
```

### Claude Code not found

Gza requires Claude Code CLI to be installed:

```bash
# Install Claude Code
npm install -g @anthropic-ai/claude-code

# Verify installation
claude --version

# Authenticate
claude login
```

### API key not working

Check credential precedence:

```bash
# See what's set
echo $ANTHROPIC_API_KEY

# Check .env files
cat .env
cat ~/.gza/.env
```

Project `.env` overrides shell variables, which override `~/.gza/.env`.

### Docker permission errors

On Linux, your user may need to be in the docker group:

```bash
sudo usermod -aG docker $USER
# Log out and back in
```

### Task times out before completion

Increase the timeout in `gza.yaml`:

```yaml
timeout_minutes: 30  # default is 10
```

Or per-task-type:

```yaml
task_types:
  implement:
    timeout_minutes: 45
```

### Worker won't stop

If `gza stop` doesn't work, force kill:

```bash
gza stop <worker_id> --force
```

Or stop all workers:

```bash
gza stop --all --force
```
