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
| `.env` | Project-level (overrides user-level) |

**Format:**

```
ANTHROPIC_API_KEY=sk-ant-...
GZA_MAX_TURNS=100
GZA_TIMEOUT_MINUTES=15
```

---

## Command-Line Arguments

### Global

```bash
gza <command> [project_dir]
```

- `project_dir` - Target project directory (default: current directory)

### work

Run tasks from the queue.

```bash
gza work [task_id] [options]
```

| Option | Description |
|--------|-------------|
| `task_id` | Specific task ID to run |
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
gza validate [project_dir]
```

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
