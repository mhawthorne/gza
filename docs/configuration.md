# Gza Configuration Reference

This document provides a comprehensive reference for all configuration options available in Gza.

## Configuration File (gza.yaml)

The main configuration file is `gza.yaml` in your project root directory.
You can optionally add `gza.local.yaml` for machine-local overrides.

### Required Configuration

| Option | Type | Description |
|--------|------|-------------|
| `project_name` | String | Project name used for branch prefixes and Docker image naming |

### Optional Configuration

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `project_prefix` | String | *(project_name)* | Short prefix for task IDs (1–12 chars, lowercase alphanumeric only — no hyphens, since hyphen is the separator in task IDs like `gza-1234`). Defaults to `project_name`. |
| `tasks_file` | String | `tasks.yaml` | Path to legacy tasks file |
| `log_dir` | String | `.gza/logs` | Directory for log files |
| `use_docker` | Boolean | `true` | Whether to run Claude in Docker container |
| `docker_image` | String | `{project_name}-gza` | Custom Docker image name |
| `docker_volumes` | List | `[]` | Custom Docker volume mounts (e.g., `["/host:/container:ro"]`) |
| `timeout_minutes` | Integer | `10` | Maximum time per task in minutes |
| `branch_mode` | String | `multi` | Branch strategy: `single` or `multi` |
| `max_steps` | Integer | `50` | Maximum conversation steps per task (preferred) |
| `max_turns` | Integer | `50` | Legacy alias for `max_steps` |
| `worktree_dir` | String | `/tmp/gza-worktrees` | Directory for git worktrees |
| `work_count` | Integer | `1` | Number of tasks to run in a single work session |
| `provider` | String | `claude` | AI provider: `claude`, `codex`, or `gemini` |
| `task_providers` | Dict | `{}` | Route task types to providers (e.g., `review: claude`) |
| `providers` | Dict | `{}` | Provider-scoped model/task-type config (preferred) |
| `model` | String | *(empty)* | Legacy global model fallback (compatible) |
| `task_types` | Dict | `{}` | Legacy global per-task fallback (compatible) |
| `claude` | Dict | *(see below)* | Claude-specific configuration (see [Claude Configuration](#claude-configuration)) |
| `claude_args` | List | *(deprecated)* | Use `claude.args` instead |
| `tmux` | Dict | *(see below)* | Tmux session configuration (see [Tmux Sessions](tmux.md)) |
| `review_diff_small_threshold` | Integer | `500` | Total changed-line cutoff (`added + removed`) below which review prompts include full inline diff |
| `review_diff_medium_threshold` | Integer | `2000` | Total changed-line cutoff above `review_diff_small_threshold`; larger diffs use targeted excerpts instead of full inline diff |
| `review_context_file_limit` | Integer | `12` | Maximum number of changed files to include in targeted excerpt mode for large review diffs |
| `iterate_max_iterations` | Integer | `3` | Default iterate action budget when `gza iterate` omits `--max-iterations` |
| `watch` | Dict | `{batch: 5, poll: 300, max_idle: null, max_iterations: 10}` | Defaults for `gza watch` loop behavior |
| `learnings_window` | Integer | `25` | Number of recent completed tasks to include in the learnings update prompt |
| `learnings_interval` | Integer | `5` | Auto-update learnings every N completed tasks; set to `0` to disable auto-updates |
| `theme` | String | *(none)* | Built-in color theme: `default_dark`, `minimal`, `selective_neon`, or `blue`. Override with `gza.local.yaml`. |
| `colors` | Dict | `{}` | Ad-hoc map of `field_name: rich_color_string` applied on top of `theme` (highest priority). Allowed in `gza.local.yaml`. |

### Local Overrides (gza.local.yaml)

Use `gza.local.yaml` for machine-specific settings that should not be committed.

- Merge behavior: deep merge for dictionaries, replace for scalars/lists
- Precedence: `gza.yaml` < `gza.local.yaml` < env vars
- Guardrails: only approved keys can be overridden in `gza.local.yaml`

Example:

```yaml
# gza.local.yaml
use_docker: false
timeout_minutes: 30
docker_volumes:
  - ~/datasets:/datasets:ro
providers:
  claude:
    task_types:
      review:
        model: claude-haiku-4-5
```

Inspect effective values and source attribution:

```bash
gza config
gza config --json
```

### Themes and Colors

Gza supports color themes that control the appearance of all CLI output.

#### Built-in themes

| Theme | Description |
|-------|-------------|
| `default_dark` | Light gray / white palette optimized for dark terminal backgrounds |
| `minimal` | Extends `default_dark` with selective semantic color — red for errors, green for success, yellow for warnings, dim for secondary info |
| `selective_neon` | Minimal overrides — bright neon highlights on task IDs and headings |
| `blue` | Monochromatic blue palette |

Set a theme in `gza.yaml` or `gza.local.yaml`:

```yaml
theme: default_dark
```

#### Ad-hoc color overrides

The `colors` key lets you override individual fields on top of (or instead of) a theme. Values are [Rich color strings](https://rich.readthedocs.io/en/stable/appendix/colors.html) — hex (`#ff99cc`), ANSI names (`cyan`), or modifiers (`bold`, `dim`).

```yaml
theme: blue
colors:
  task_id: "#ff0000"
  prompt: bold green
```

Overrides apply to every output context that has the named field. For example, setting `task_id` changes it in `gza history`, `gza show`, `gza next`, `gza lineage`, and `gza unmerged` simultaneously.

#### Override priority

From highest to lowest:

1. `colors` in config (ad-hoc per-field overrides)
2. Theme domain-specific overrides (e.g. the theme's `task` or `show` dict)
3. Theme base overrides (cross-cutting fields like `task_id`, `prompt`, `stats`)
4. Dataclass field defaults (hardcoded in `src/gza/colors.py`)

#### Available color fields

**Base fields** (shared across most output contexts): `task_id`, `prompt`, `stats`, `branch`, `label`, `value`, `heading`

**Domain-specific fields** — these only affect their respective command:

| Context | Fields |
|---------|--------|
| Task history | `success`, `failure`, `unmerged`, `orphaned`, `lineage`, `header` |
| Status | `completed`, `failed`, `pending`, `in_progress`, `unmerged`, `dropped`, `stale`, `unknown`, `running` |
| Work output | `step_header`, `assistant_text`, `tool_use`, `error`, `todo_pending`, `todo_in_progress`, `todo_completed` |
| Show | `section`, `status_pending`, `status_running`, `status_completed`, `status_failed`, `status_default` |
| Unmerged | `review_approved`, `review_changes`, `review_discussion`, `review_none` |
| Lineage | `task_type`, `annotation`, `connector`, `type_label`, `relationship`, `target_highlight` |
| Next | `type`, `blocked`, `index` |


### Branch Naming Strategy

Configure branch naming with the `branch_strategy` option. Several presets are available:

```yaml
# Preset: project_date_slug (default)
# Generates: {project}/{date}-{slug}
# Example: myproject/20260108-add-feature
branch_strategy: project_date_slug

# Preset: conventional
# Generates: {type}/{slug}
# Example: feature/add-feature
branch_strategy: conventional

# Preset: simple
# Generates: {slug}
# Example: add-feature
branch_strategy: simple

# Preset: date_slug
# Generates: {date}-{slug}
# Example: 20260108-add-feature
branch_strategy: date_slug
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
| `{prefix}` | Project prefix (used in task slugs) |
| `{task_id}` | Short task id (`{prefix}-{decimal}`, for example `gza-1234`) |
| `{task_slug}` | Full task slug (`YYYYMMDD-{prefix}-{slug}`) |
| `{date}` | Date portion of the task slug (YYYYMMDD) |
| `{slug}` | Bare slug with `{prefix}-` stripped (e.g. `add-feature`) |
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

**Notes:**
- Volumes are only used when `use_docker: true`
- Tilde (`~`) in source paths is automatically expanded to your home directory
- The workspace is always mounted at `/workspace` automatically
- Config validation warns about common syntax errors but doesn't block invalid formats

### Claude Configuration

Nested under the `claude:` key in `gza.yaml`:

```yaml
claude:
  fetch_auth_token_from_keychain: false
  args: ["--allowedTools", "Read", "Write", "Edit", "Glob", "Grep", "Bash"]
```

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `fetch_auth_token_from_keychain` | Boolean | `false` | Fetch OAuth token from macOS Keychain for Docker (macOS only) |
| `args` | List | `["--allowedTools", "Read", "Write", "Edit", "Glob", "Grep", "Bash"]` | Arguments passed to Claude Code CLI |

### Tmux Sessions

Tmux behavior is provider-specific. By default, Claude background workers run in pipe mode (no tmux proxy), while Codex/Gemini can run in tmux when enabled. Claude interactive attach still uses tmux for kill/resume handoff sessions. Set `GZA_ENABLE_TMUX_PROXY=1` to force legacy Claude tmux proxy mode. See [Tmux Sessions](tmux.md) for full details.

```yaml
tmux:
  enabled: true
  auto_accept_timeout: 10
  max_idle_timeout: 300
  detach_grace: 5
  terminal_size: [200, 50]
```

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `enabled` | Boolean | `false` | Run background tasks inside tmux sessions. Requires tmux to be installed |
| `auto_accept_timeout` | Float | `10.0` | Seconds of quiescence before auto-accepting a tool prompt |
| `max_idle_timeout` | Float | `300.0` | Seconds of total inactivity before assuming stuck and killing the session |
| `detach_grace` | Float | `5.0` | Seconds to wait after a human detaches before resuming auto-accept |
| `terminal_size` | List | `[200, 50]` | Terminal dimensions `[cols, rows]` |

### Provider-Scoped Configuration

Preferred approach for multi-provider setups:

```yaml
provider: claude
task_providers:
  review: claude
  implement: codex
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

Valid task types: `explore`, `plan`, `implement`, `review`, `improve`, `rebase`, `internal`

Top-level `task_types` and `model` are still supported for backward compatibility. They are used as fallbacks when no provider-scoped value exists.

### Resolution Precedence

Provider selection:
1. `task.provider`
2. `task_providers.<task_type>`
3. `provider`

Model selection:
1. `task.model`
2. `providers.<effective_provider>.task_types.<task_type>.model`
3. `providers.<effective_provider>.model`
4. `task_types.<task_type>.model` (legacy fallback)
5. `model` / `defaults.model` (legacy fallback)
6. Provider runtime default (if no model resolved)

Max steps selection:
1. `providers.<effective_provider>.task_types.<task_type>.max_steps`
2. `providers.<effective_provider>.task_types.<task_type>.max_turns` (legacy)
3. `task_types.<task_type>.max_steps` (legacy fallback)
4. `task_types.<task_type>.max_turns` (legacy fallback)
5. `max_steps` / `defaults.max_steps`
6. `max_turns` / `defaults.max_turns` (legacy fallback)

---

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
| `.env` | Project-level (overrides shell and user-level) |
| `.gza/.env` | Worktree-level (highest priority; shared across worktrees via symlink) |

These files are for **provider credentials only** (API keys, tokens). Gza configuration should go in `gza.yaml` or `gza.local.yaml`, not in `.env` files.

**Credential precedence** (highest to lowest):

1. **`.gza/.env`** - Highest priority; overrides all other sources (useful for worktree setups where `.gza/` is symlinked to share credentials)
2. **Project `.env`** - Overrides shell environment and user-level defaults
3. **Shell environment** - Variables exported in your shell
4. **`~/.gza/.env`** - Only sets values not already defined

This means if you have `ANTHROPIC_API_KEY` set in your shell, a project `.env` or `.gza/.env` will override it. The home `.env` file uses `setdefault` behavior, so it won't override existing environment variables.

**Format:**

```
ANTHROPIC_API_KEY=sk-ant-...
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
| `task_id` | Specific full prefixed task ID(s) to run (for example `gza-1234`; can specify multiple) |
| `--no-docker` | Run Claude directly instead of in Docker |
| `--count N`, `-c N` | Number of tasks to run before stopping |
| `--background`, `-b` | Run worker in background |
| `--max-turns N` | Override max_turns setting for this run |
| `--force` | Skip dependency merge precondition checks (run even if depends_on output is not yet merged) |

### add

Add a new task.

```bash
gza add [prompt] [options]
```

| Option | Description |
|--------|-------------|
| `prompt` | Task prompt (opens $EDITOR if not provided) |
| `--edit`, `-e` | Open $EDITOR to write the prompt |
| `--type TYPE` | Set task type: `explore`\|`plan`\|`implement`\|`review`\|`improve` (default: `implement`) |
| `--branch-type TYPE` | Set branch type hint for naming |
| `--explore` | Create explore task (shorthand) |
| `--group NAME` | Set task group |
| `--based-on ID` | Base on previous task by full prefixed task ID (e.g. `gza-1234`) |
| `--depends-on ID` | Set dependency on another task by full prefixed task ID (e.g. `gza-1234`) |
| `--review` | Auto-create review task on completion |
| `--same-branch` | Continue on depends_on task's branch |
| `--spec FILE` | Path to spec file for context |
| `--prompt-file FILE` | Read prompt from file (for non-interactive use) |
| `--model MODEL` | Override model for this task (e.g., `claude-3-5-haiku-latest`) |
| `--provider PROVIDER` | Override provider for this task (`claude`, `codex`, or `gemini`) |
| `--no-learnings` | Skip injecting `.gza/learnings.md` context into this task's prompt |
| `--next` | Mark the new task urgent and bump it to the front of the urgent lane (same as add + queue bump) |

### edit

Edit an existing task.

```bash
gza edit <task_id> [options]
```

| Option | Description |
|--------|-------------|
| `task_id` | Full prefixed task ID to edit (e.g. `gza-1234`) |
| `--group NAME` | Move task to group (empty `""` removes) |
| `--based-on ID` | Set lineage/parent relationship using a full prefixed task ID (branch inheritance and context; e.g. `gza-1234`) |
| `--depends-on ID` | Set execution dependency using a full prefixed task ID (blocks task until dependency completes; e.g. `gza-1234`) |
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
| `identifier` | Full prefixed task ID (e.g. `gza-1234`), slug, or worker ID |
| `--slug`, `-s` | Interpret identifier as task slug (supports partial match) |
| `--worker`, `-w` | Interpret identifier as worker ID |
| `--steps` | Show compact step timeline |
| `--steps-verbose` | Show verbose step timeline with substeps |
| `--turns` | Deprecated alias for `--steps-verbose` |
| `--follow`, `-f` | Follow log in real-time |
| `--tail N` | Show last N lines |
| `--raw` | Show raw JSON lines |
| `--page` | Pipe output through `$PAGER` (default: `less -R`); skipped for `--follow` and `--raw` |

By default, the identifier is treated as a full task ID (for example `gza-1234`).
If no main task log exists yet, `gza log` can fall back to worker startup logs in `.gza/workers/*-startup.log`.

### stats

Show analytics subcommands for reviews and iteration activity.

#### stats reviews

```bash
gza stats reviews [options]
```

| Option | Description |
|--------|-------------|
| `--days N` | Show only tasks from the last N days (default: 14) |
| `--start-date YYYY-MM-DD` | Show only tasks on or after this date |
| `--end-date YYYY-MM-DD` | Show only tasks on or before this date (default: today) |
| `--issues` | Show per-model must-fix and suggestion counts |

#### stats iterations

```bash
gza stats iterations [options]
```

| Option | Description |
|--------|-------------|
| `-n, --last N` | Limit output to the N most recent implementation rows |
| `--hours N` | Show tasks with activity in the last N hours (cannot combine with `--days`/`--start-date`/`--end-date`) |
| `--days N` | Show only tasks from the last N days (default: 14) |
| `--start-date YYYY-MM-DD` | Show only tasks on or after this date |
| `--end-date YYYY-MM-DD` | Show only tasks on or before this date (default: today) |
| `--all`, `--all-time` | Show stats across all time (cannot combine with date-window flags) |

`stats iterations` uses completion-aware activity windows:
- Completed implementation/review/improve tasks use `completed_at`.
- Incomplete tasks fall back to `created_at`.
- A row is included when the implementation or any linked review/improve task has activity in the selected window.

### pr

Create a pull request for a completed task.

```bash
gza pr <task_id> [options]
```

| Option | Description |
|--------|-------------|
| `task_id` | Full prefixed task ID for the completed task to open as a PR (e.g. `gza-1234`) |
| `--title TITLE` | Override auto-generated PR title |
| `--draft` | Create as draft PR |

### delete

Delete a task.

```bash
gza delete <task_id> [options]
```

| Option | Description |
|--------|-------------|
| `task_id` | Full prefixed task ID to delete (e.g. `gza-1234`) |
| `--yes`, `-y` | Skip confirmation prompt |
| `--force`, `-f` | Deprecated alias for `--yes` |

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

### group

Show tasks in a specific group.

```bash
gza group <group>
```

### status

List active workers and startup failures (alias for `ps`).

```bash
gza status
```

### attach

Attach to a running task. Claude uses an interactive kill/resume handoff session; Codex/Gemini attach read-only. See [Tmux Sessions](tmux.md) for details.

```bash
gza attach <worker_id_or_task_id>
```

| Option | Description |
|--------|-------------|
| `worker_id_or_task_id` | Worker ID (from `gza ps`) or full prefixed task ID (e.g. `gza-1234`) |

### ps

Show active workers and startup failures.

```bash
gza ps [options]
```

| Option | Description |
|--------|-------------|
| `--all`, `-a` | Include all completed/failed workers (not just startup failures) |
| `--quiet`, `-q` | Only show worker IDs |
| `--json` | Output as JSON |

Runtime reconciliation notes:
- Task lifecycle state is derived from the DB `tasks` table (`status`, `started_at`, `running_pid`), while worker metadata is a process index.
- On CLI startup, `in_progress` tasks are reconciled and auto-failed as:
  - `WORKER_DIED` when `running_pid` is missing/invalid or the PID is no longer alive.
  - `TIMEOUT` when runtime exceeds configured `timeout_minutes`.
- `gza ps` merges worker rows and DB in-progress tasks by task ownership, so healthy background runs appear as one active task row.

### kill

Kill a running task.

```bash
gza kill [task_id] [options]
```

| Option | Description |
|--------|-------------|
| `task_id` | Full prefixed task ID to kill (optional if `--all` is used; e.g. `gza-1234`) |
| `--all` | Kill all running tasks |
| `--force`, `-9` | Send SIGKILL immediately (skip SIGTERM) |

Sends SIGTERM and waits 3 seconds; escalates to SIGKILL if the process is still alive. Sets the task status to `failed` with `failure_reason=KILLED`.

### validate

Validate configuration.

```bash
gza validate
```

### config

Show effective configuration and source attribution (`base`, `local`, `env`, `default`).

```bash
gza config
gza config --json
```

### show

Show details of a specific task.

```bash
gza show <task_id> [options]
```

| Option | Description |
|--------|-------------|
| `task_id` | Full prefixed task ID to show (e.g. `gza-1234`) |
| `--full` | Show full output without truncation |
| `--page` | Pipe output through `$PAGER` (default: `less -R`); skipped for `--prompt`, `--output`, and `--path` modes |

When a task has a branch, `gza show` also reports active worktree information:
- `Worktree: <path>` when the task branch is currently checked out in an active worktree
- `Warning: Worktree lookup failed: ...` when git worktree metadata could not be read

### resume

Resume a failed task from where it left off. The AI continues with the existing conversation context.

```bash
gza resume <task_id> [options]
```

| Option | Description |
|--------|-------------|
| `task_id` | Full prefixed task ID to resume (e.g. `gza-1234`) |
| `--no-docker` | Run Claude directly instead of in Docker |
| `--background`, `-b` | Run worker in background |
| `--queue`, `-q` | Add task to queue without executing immediately |
| `--max-turns N` | Override max_turns setting for this run |
| `--force` | Skip dependency merge precondition checks when starting the resumed task |

### retry

Retry a failed or completed task from scratch. Starts a fresh conversation.

```bash
gza retry <task_id> [options]
```

| Option | Description |
|--------|-------------|
| `task_id` | Full prefixed task ID to retry (e.g. `gza-1234`) |
| `--no-docker` | Run Claude directly instead of in Docker (only with --background) |
| `--background`, `-b` | Run worker in background |
| `--queue`, `-q` | Add task to queue without executing immediately |
| `--max-turns N` | Override max_turns setting for this run |
| `--force` | Skip dependency merge precondition checks when starting the retry task |

### mark-completed

Manually complete a task when automation failed. Defaults are task-type aware:
- `task`, `implement`, `improve` default to git-verified completion
- `explore`, `plan`, `review` default to status-only completion

```bash
gza mark-completed <task_id> [--verify-git | --force]
```

| Option | Description |
|--------|-------------|
| `task_id` | Full prefixed task ID to mark as completed (e.g. `gza-1234`) |
| `--verify-git` | Validate branch and commits before completion |
| `--force` | Status-only completion (for non-code tasks or stale in_progress recovery) |

`force-complete` is deprecated. Use `mark-completed <task_id> --force`.

### merge

Merge a completed task's branch into the current branch.

```bash
gza merge <task_id> [task_id...] [options]
```

| Option | Description |
|--------|-------------|
| `task_id` | Full prefixed task ID(s) to merge (e.g. `gza-1234`; can specify multiple) |
| `--all` | Merge all unmerged done tasks (task_ids optional when used) |
| `--squash` | Squash commits into a single commit |
| `--rebase` | Rebase onto current branch instead of merging |
| `--delete` | Delete the branch after successful merge |
| `--remote` | Fetch from origin and rebase against remote (requires --rebase) |
| `--mark-only` | Mark branch as merged without performing actual merge (deletes branch) |
| `--resolve` | Auto-resolve conflicts using AI when rebasing (requires --rebase) |

### unmerged

List tasks with branches that haven't been merged to main.

```bash
gza unmerged [options]
```

| Option | Description |
|--------|-------------|
| `--commits-only` | Use commit-based detection (git cherry) instead of diff-based |
| `--all` | Include failed tasks and check git directly for commits |

For each unmerged implementation, output includes:
- Branch diff/commit summary.
- A `lineage:` branch-rendered tree showing related task IDs and types (implementation, review, improve).
- A `review:` freshness classification:
  - `no review` when no completed review exists.
  - `reviewed` when the latest completed review still reflects current code.
  - `review stale` when code-changing work (for example an improve task) happened after the latest review.

### groups

List all task groups with their task counts.

```bash
gza groups
```

### history

List recent completed or failed tasks.

By default, `gza history` excludes `internal` tasks. Use `--type internal` to view internal task history.

```bash
gza history [options]
```

| Option | Description |
|--------|-------------|
| `--last N`, `-n N` | Show last N tasks (default: 5) |
| `--type TYPE` | Filter by task type: `explore`, `plan`, `implement`, `review`, `improve`, `rebase`, `internal` |
| `--days N` | Show only tasks from the last N days |
| `--start-date YYYY-MM-DD` | Show only tasks on or after this date |
| `--end-date YYYY-MM-DD` | Show only tasks on or before this date |
| `--status STATUS` | Filter by status: `completed`, `failed`, or `unmerged` |
| `--incomplete` | Show only unresolved tasks (failed or unmerged) |
| `--lineage-depth N` | Render root-deduplicated lineage trees up to N levels |

### checkout

Checkout a task's branch, removing any stale worktree if needed.

```bash
gza checkout <task_id_or_branch> [options]
```

| Option | Description |
|--------|-------------|
| `task_id_or_branch` | Full prefixed task ID or branch name to checkout (e.g. `gza-1234` or `feat/auth`) |
| `--force`, `-f` | Force removal of worktree even if it has uncommitted changes |

### diff

Show git diff for a task's changes with colored output and pager support.

```bash
gza diff [task_id] [diff_args...]
```

| Option | Description |
|--------|-------------|
| `task_id` | Full prefixed task ID to diff (e.g. `gza-1234`; optional, uses current branch if omitted) |
| `diff_args` | Arguments passed to git diff (use `--` before options like `--stat`) |

### rebase

Rebase a task's branch onto a target branch.

```bash
gza rebase <task_id> [options]
```

| Option | Description |
|--------|-------------|
| `task_id` | Full prefixed task ID to rebase (e.g. `gza-1234`) |
| `--onto BRANCH` | Branch to rebase onto (defaults to current branch) |
| `--remote` | Fetch from origin and rebase against remote target branch |
| `--resolve` | Auto-resolve rebase conflicts using `/gza-rebase` in the active provider runtime |
| `--force`, `-f` | Force remove worktree even if it has uncommitted changes |
| `--background`, `-b` | Run rebase in background |

When `--resolve` is used, gza runs the active task provider (`claude`, `codex`, or `gemini`) and sends the `/gza-rebase --auto` prompt. If the `gza-rebase` skill is missing for that runtime, gza fails fast with an install command such as:

```bash
uv run gza skills-install --target codex gza-rebase --project /path/to/project
```

### clean

Clean up stale worktrees, old logs, worker metadata, and archives.

```bash
gza clean [options]
```

| Option | Description |
|--------|-------------|
| `--worktrees` | Only clean up stale worktrees |
| `--workers` | Only clean up stale worker metadata and startup logs |
| `--logs` | Only clean up old log files |
| `--backups` | Only clean up old backup files |
| `--days N` | Remove items older than N days (default: from config cleanup_days, or 30) |
| `--keep-unmerged` | Keep logs for tasks that are still unmerged |
| `--archive` | Archive old log and worker files instead of deleting |
| `--purge` | Delete previously archived files (default: older than 365 days) |
| `--force` | Skip confirmation prompt before removing worktrees |
| `--dry-run` | Show what would be cleaned without doing it |

### improve

Create an improve task to address review feedback on an implementation.

```bash
gza improve <impl_task_id> [options]
```

| Option | Description |
|--------|-------------|
| `impl_task_id` | Full prefixed task ID (implement, improve, or review — auto-resolves to root implementation; e.g. `gza-1234`) |
| `--review-id ID` | Explicit full prefixed review task ID to base the improve on (overrides auto-pick of most recent completed review; e.g. `gza-1234`) |
| `--review` | Auto-create review task on completion |
| `--queue`, `-q` | Add task to queue without executing immediately |
| `--background`, `-b` | Run worker in background |
| `--no-docker` | Run Claude directly instead of in Docker |
| `--max-turns N` | Override max_turns setting for this run |
| `--model MODEL` | Override model for this task |
| `--provider PROVIDER` | Override provider for this task |
| `--force` | Skip dependency merge precondition checks when running the improve task |

The improve command finds the most recent review for the implementation task and creates a new task that continues on the same branch to address the review feedback.

### review

Create and run a review task for an implementation. Runs immediately by default.

```bash
gza review <task_id> [options]
```

| Option | Description |
|--------|-------------|
| `task_id` | Full prefixed task ID (implement, improve, or review — auto-resolves to root implementation; e.g. `gza-1234`) |
| `--queue`, `-q` | Add task to queue without executing immediately |
| `--background`, `-b` | Run worker in background |
| `--no-docker` | Run Claude directly instead of in Docker |
| `--no-pr` | Do not post review to PR even if one exists |
| `--pr` | Require PR to exist (error if not found) |
| `--open` | Open the review file in $EDITOR after completion |
| `--model MODEL` | Override model for this task |
| `--provider PROVIDER` | Override provider for this task |

When a PR exists for the implementation task, the review is automatically posted as a PR comment.

### next

List upcoming pending tasks.

```bash
gza next [options]
```

Shows pending tasks that are ready to run (dependencies satisfied). Tasks blocked by dependencies are listed separately.

### queue

Inspect and manage runnable pending queue ordering.

```bash
gza queue
gza queue bump <task_id>
gza queue unbump <task_id>
```

| Option | Description |
|--------|-------------|
| `task_id` | Full prefixed task ID to bump/unbump (e.g. `gza-1234`) |

Queue pickup ordering is urgent-first. `queue bump` moves a task to the front of the urgent lane (next pickup), then remaining tasks stay FIFO by creation time within each lane.
`gza queue` shows tasks that default worker pickup can run (internal and dependency-blocked pending tasks are excluded).

### implement

Create an implementation task from a completed plan task.

```bash
gza implement <plan_task_id> [prompt] [options]
```

| Option | Description |
|--------|-------------|
| `plan_task_id` | Full prefixed completed plan task ID to implement (e.g. `gza-1234`) |
| `prompt` | Implementation prompt (defaults to plan-derived prompt) |
| `--review` | Auto-create review task on completion |
| `--group NAME` | Set task group |
| `--depends-on ID` | Set dependency on another task by full prefixed task ID (e.g. `gza-1234`) |
| `--same-branch` | Continue on depends_on task's branch instead of creating new |
| `--branch-type TYPE` | Set branch type hint for branch naming |
| `--model MODEL` | Override model for this task |
| `--provider PROVIDER` | Override provider for this task |
| `--no-learnings` | Skip injecting learnings context |
| `--queue`, `-q` | Add task to queue without executing immediately |
| `--background`, `-b` | Run worker in background |
| `--no-docker` | Run Claude directly instead of in Docker |
| `--max-turns N` | Override max_turns setting for this run |
| `--force` | Skip dependency merge precondition checks when running the implement task |

### advance

Intelligently progress unmerged tasks through their lifecycle. Handles review creation, improve tasks, merging, and resuming failed tasks.

```bash
gza advance [task_id] [options]
```

| Option | Description |
|--------|-------------|
| `task_id` | Specific full prefixed task ID to advance (e.g. `gza-1234`; omit to advance all eligible) |
| `--dry-run` | Preview actions without executing them |
| `--max N` | Limit the number of tasks to advance |
| `--no-docker` | Run workers directly instead of in Docker |
| `--force` | Skip dependency merge precondition checks when advance starts workers |
| `--unimplemented` | List completed plans/explores with no implementation task yet |
| `--create` | With `--unimplemented`: create queued implement tasks for listed tasks |
| `--auto`, `-y` | Skip confirmation and execute immediately |
| `--batch B` | Stop after spawning B background workers |
| `--no-resume-failed` | Skip auto-resume of failed tasks |
| `--max-resume-attempts N` | Override max_resume_attempts config value |
| `--max-review-cycles N` | Override max_review_cycles config value |
| `--new` | Start new pending tasks to fill remaining `--batch` slots (requires `--batch`) |
| `--type TYPE` | Only advance tasks of this type (`plan` or `implement`) |
| `--squash-threshold N` | Squash-merge branches with N or more commits (0 disables) |

### iterate

Run an automated implementation lifecycle loop (review/improve/resume/rebase).

```bash
gza iterate <impl_task_id> [options]
```

| Option | Description |
|--------|-------------|
| `impl_task_id` | Full prefixed implementation task ID to iterate (e.g. `gza-1234`) |
| `--max-iterations N` | Maximum iterate actions (default: `iterate_max_iterations` or `3`) |
| `--dry-run` | Preview what would happen without executing |
| `--no-docker` | Run Claude directly instead of in Docker |
| `--force` | Skip dependency merge precondition checks when iterate starts workers |

### watch

Continuously maintain a target number of concurrent workers.

```bash
gza watch [options]
```

| Option | Description |
|--------|-------------|
| `--batch N` | Target concurrent workers (default: `watch.batch` or `5`) |
| `--poll SECS` | Poll interval in seconds (default: `watch.poll` or `300`) |
| `--max-idle SECS` | Exit after consecutive idle time (default: `watch.max_idle`, no limit when unset) |
| `--max-iterations N` | Iterate loop cap for implement tasks (default: `watch.max_iterations` or `10`) |
| `--dry-run` | Show what each cycle would do without executing |
| `--quiet` | Write events to `.gza/watch.log` only |

### learnings

Manage project learnings accumulated from completed tasks.

```bash
gza learnings show
gza learnings update
```

| Subcommand | Description |
|------------|-------------|
| `show` | Display the current learnings file |
| `update` | Regenerate learnings from recent completed tasks |

### refresh

Refresh cached diff stats for unmerged tasks.

```bash
gza refresh [task_id] [options]
```

| Option | Description |
|--------|-------------|
| `task_id` | Full prefixed task ID to refresh (e.g. `gza-1234`; omit to refresh all unmerged tasks) |
| `--include-failed` | Also refresh failed tasks that have branches |

### migrate

Run pending manual database migrations. This includes v25 (INTEGER primary keys to project-prefixed base36 TEXT IDs) and v26 (base36 TEXT IDs to project-prefixed decimal IDs like `gza-1234`).

```bash
gza migrate [--status] [--dry-run] [--yes/-y]
```

| Option | Description |
|--------|-------------|
| `--status` | Show current schema version and list pending migrations without running anything |
| `--dry-run` | Preview what the migration would change without writing any data |
| `--yes`, `-y` | Skip the confirmation prompt and run migrations immediately |

When run without flags, `gza migrate` prompts for confirmation before applying migrations. Each migration is atomic (wrapped in BEGIN/COMMIT/ROLLBACK) and creates a pre-migration backup (for example, `<db_path>.backup.pre-v25.db` and `<db_path>.backup.pre-v26.db`). It is safe to re-run: calling it on an already-migrated database is a no-op.

On successful migration, the backup path is printed to stdout so you can locate it for rollback if needed.

Task IDs start at `{prefix}-1` for new databases (there is no `{prefix}-0`) and are variable-length decimal (`{prefix}-{n}`).

Task ID validation is format-based (`{prefix}-{decimal}`) and does not require the prefix to match your current `project_prefix`. A mismatched but valid full ID is accepted by parsing and then fails later as "not found" if it does not exist in the current project database.

If a `ManualMigrationRequired` error appears when running any other command, run `gza migrate` to upgrade the database schema.

### set-status

Manually force a task's status.

```bash
gza set-status <task_id> <status>
```

`task_id` must be a full prefixed task ID (for example `gza-1234`).

Valid statuses: `pending`, `in_progress`, `completed`, `failed`, `dropped`.

### sync-report

Sync report file content from disk into the database `output_content` field. Useful when report files have been edited manually.

```bash
gza sync-report <task_id>
```

`task_id` must be a full prefixed task ID (for example `gza-1234`).

Examples use `gza-1234`, and validation is format-based (`{prefix}-{decimal}`) with variable-length decimal suffixes.

---

## Task Types

Gza supports several task types, each with distinct behavior:

| Type | Purpose | Output Location |
|------|---------|-----------------|
| `explore` | Research and investigation | `.gza/explorations/{task_id}.md` |
| `plan` | Design and architecture | `.gza/plans/{task_id}.md` |
| `implement` | Build per a plan (default) | Code changes on branch |
| `review` | Evaluate implementation | `.gza/reviews/{task_id}.md` |
| `improve` | Address review feedback | Code changes on same branch |
| `internal` | gza-owned provider workflows (for example learnings/PR drafting) | `.gza/internal/{task_id}.md` |

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

Any state can be manually set to `dropped` via `gza set-status`.
```

| State | Description |
|-------|-------------|
| `pending` | Task is queued and waiting to run |
| `in_progress` | A worker is currently executing the task |
| `completed` | Task finished successfully |
| `failed` | Task encountered an error or timed out |
| `dropped` | Task was manually dropped (via `gza set-status`) |

**Recovering from failures:**

- Use `gza resume <task_id>` to continue from where the task left off (preserves conversation context)
- Use `gza retry <task_id>` to start completely fresh
- `PREREQUISITE_UNMERGED`: the resolved completed dependency branch is not reachable from the default branch (`main` in most repos). Merge the dependency (`gza merge <dependency_task_id>`) and then retry (`gza retry <task_id>`). Use `--force` only when you intentionally want to bypass this guard.

**Dependencies:**

Tasks with `depends_on` set will remain pending until their dependency completes. Use `gza group <group>` to see dependency chains.

---

## Configuration Precedence

Configuration is resolved in the following order (highest to lowest priority):

1. **Command-line arguments**
2. **`gza.local.yaml` file** (if present)
3. **`gza.yaml` file**
4. **Hardcoded defaults**

Provider credentials (API keys) have their own precedence — see [Dotenv Files](#dotenv-files-env) above.

---

## File Locations

### Project Files

| Path | Purpose |
|------|---------|
| `gza.yaml` | Main configuration file |
| `gza.local.yaml` | Local machine overrides (gitignored) |
| `.env` | Project-specific environment variables |
| `.gza/.env` | Worktree-level credentials (highest priority; shared via symlink) |
| `.gza/` | Local state directory (add to `.gitignore`) |
| `.gza/gza.db` | SQLite task database |
| `.gza/logs/` | Task execution logs |
| `.gza/workers/` | Worker metadata and startup logs |
| `etc/Dockerfile.claude` | Generated Docker image for Claude |
| `etc/Dockerfile.codex` | Generated Docker image for Codex |
| `etc/Dockerfile.gemini` | Generated Docker image for Gemini |

> **Note:** `.gza/` and `gza.local.yaml` are machine-specific and should be gitignored.

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
gza group <group>  # Shows tasks in a group
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

### Task won't stop

If `gza kill` doesn't work, force kill:

```bash
gza kill <task_id> --force
```

Or kill all running tasks:

```bash
gza kill --all --force
```
