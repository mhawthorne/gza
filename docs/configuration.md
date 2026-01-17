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
| `timeout_minutes` | Integer | `10` | Maximum time per task in minutes |
| `branch_mode` | String | `multi` | Branch strategy: `single` or `multi` |
| `max_turns` | Integer | `50` | Maximum conversation turns per task |
| `worktree_dir` | String | `/tmp/gza-worktrees` | Directory for git worktrees |
| `work_count` | Integer | `1` | Number of tasks to run in a single work session |
| `provider` | String | `claude` | AI provider: `claude` or `gemini` |
| `model` | String | *(empty)* | Provider-specific model name override |
| `claude_args` | List | `["--allowedTools", "Read", "Write", "Edit", "Glob", "Grep", "Bash"]` | Arguments passed to Claude Code CLI |

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

### Task Types Configuration

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

Valid task types: `task`, `explore`, `plan`, `implement`, `review`

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
| `GZA_MODEL` | `model` | Override model name |

### Providers and Models

Gza supports multiple AI providers for task execution:

| Provider | Status | Description |
|----------|--------|-------------|
| `claude` | **Supported** | Claude Code CLI (default) |
| `gemini` | *Experimental* | Gemini CLI - partially implemented, coming soon |

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

### Provider Credentials

**Claude:**

| Variable | Description |
|----------|-------------|
| `ANTHROPIC_API_KEY` | API key for Claude (alternative to OAuth) |

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

### add

Add a new task.

```bash
gza add [prompt] [options]
```

| Option | Description |
|--------|-------------|
| `prompt` | Task prompt (opens $EDITOR if not provided) |
| `--edit`, `-e` | Open $EDITOR to write the prompt |
| `--type TYPE` | Set task type: `task`\|`explore`\|`plan`\|`implement`\|`review` |
| `--branch-type TYPE` | Set branch type hint for naming |
| `--explore` | Create explore task (shorthand) |
| `--group NAME` | Set task group |
| `--based-on ID` | Base on previous task |
| `--depends-on ID` | Set dependency on another task |
| `--review` | Auto-create review task on completion |
| `--same-branch` | Continue on depends_on task's branch |
| `--spec FILE` | Path to spec file for context |

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

### log

View task or worker logs.

```bash
gza log <identifier> [options]
```

| Option | Description |
|--------|-------------|
| `--task`, `-t` | Look up by task ID |
| `--slug`, `-s` | Look up by task slug |
| `--worker`, `-w` | Look up by worker ID |
| `--turns` | Show full conversation turns |
| `--follow`, `-f` | Follow log in real-time |
| `--tail N` | Show last N lines |
| `--raw` | Show raw JSON lines |

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
| `--force`, `-f` | Skip confirmation prompt |

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

### retry

Retry a failed or completed task from scratch. Starts a fresh conversation.

```bash
gza retry <task_id> [options]
```

| Option | Description |
|--------|-------------|
| `--no-docker` | Run Claude directly instead of in Docker |
| `--background`, `-b` | Run worker in background |

### merge

Merge a completed task's branch into the current branch.

```bash
gza merge <task_id> [options]
```

| Option | Description |
|--------|-------------|
| `--squash` | Squash commits into a single commit |
| `--rebase` | Rebase onto current branch instead of merging |
| `--delete` | Delete the branch after successful merge |

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
| `.gza/gza.db` | SQLite task database |
| `.gza/logs/` | Task execution logs |
| `.gza/workers/` | Worker metadata |
| `etc/Dockerfile.claude` | Generated Docker image for Claude |
| `etc/Dockerfile.gemini` | Generated Docker image for Gemini |

### Home Directory

| Path | Purpose |
|------|---------|
| `~/.gza/.env` | User-level environment variables |
| `~/.claude/` | Claude OAuth credentials |
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
