# Gza Configuration Reference

This document provides a comprehensive reference for all configuration options available in Gza.

## Configuration Files

Gza reads configuration from three YAML layers:

1. `~/.gza/config.yaml` for per-user defaults across all projects on the machine
2. `gza.yaml` in the project root for committed project configuration
3. `gza.local.yaml` for machine-local project overrides

`gza.yaml` is still required for project discovery and must define `project_name`.

### Required Configuration

| Option | Type | Description |
|--------|------|-------------|
| `project_name` | String | Project name used for branch prefixes and Docker image naming |

### Optional Configuration

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `project_id` | String | *(local DB: `default`; shared DB: required)* | Project identity used to scope rows in shared DB mode. In shared mode, `project_id: default` is invalid and omitting `project_id` is an error. `gza init` persists a stable readable `project_id` for new projects. Older shared-DB configs that never persisted one must add the legacy derived ID explicitly, or run `gza migrate --import-local-db` to persist that identity while importing a legacy local DB, even when shared mode is inherited from `~/.gza/config.yaml`, `gza.local.yaml`, or `GZA_DB_PATH`. |
| `project_prefix` | String | *(project_name)* | Short prefix for task IDs (1–12 chars, lowercase alphanumeric only — no hyphens, since hyphen is the separator in task IDs like `gza-1234`). Defaults to `project_name`. |
| `db_path` | String | `.gza/gza.db` | Task DB path. Shared DB mode is opt-in via an explicit path (for example `~/.gza/gza.db`). `gza init` now asks you to choose local vs shared DB mode; in non-interactive runs you must pass `--db local` or `--db shared` (optionally with `--db-path PATH`). If a legacy local `.gza/gza.db` exists when using shared mode, either import it with `gza migrate --import-local-db` or pin the project back to local with `db_path: .gza/gza.db`. `GZA_DB_PATH` overrides this value at runtime. |
| `tasks_file` | String | `tasks.yaml` | Path to legacy tasks file |
| `log_dir` | String | `.gza/logs` | Directory for log files |
| `use_docker` | Boolean | `true` | Whether to run Claude in Docker container |
| `enforce_project_scope` | Boolean | `true` | Fail code-task commits that stage paths outside the project subtree or declared in-repo local deps. Tasks tagged `cross-project` may span multiple discovered project roots, including new roots declared by changed `gza.yaml` files on the branch, but still fail if they touch paths outside all discovered or branch-declared project roots. |
| `docker_image` | String | `{project_name}-gza` | Custom Docker image name |
| `docker_setup_command` | String | `""` | Pre-warm hook run synchronously in Docker before provider CLI starts |
| `docker_volumes` | List | `[]` | Custom Docker volume mounts (e.g., `["/host:/container:ro"]`) |
| `timeout_minutes` | Integer | `10` | Maximum time per task in minutes |
| `inner_verify_command` | String | `""` | Optional fast verification command for code-task edit loops; `verify_command` remains the final gate |
| `branch_mode` | String | `multi` | Branch strategy: `single` or `multi` |
| `max_steps` | Integer | `50` | Maximum conversation steps per task (preferred) |
| `max_turns` | Integer | `50` | Legacy alias for `max_steps` |
| `worktree_dir` | String | `/tmp/gza-worktrees` | Directory for git worktrees |
| `work_count` | Integer | `1` | Number of tasks to run in a single work session |
| `provider` | String | `claude` | AI provider: `claude`, `codex`, or `gemini` |
| `task_providers` | Dict | `{}` | Route task types to providers (e.g., `review: claude`) |
| `providers` | Dict | `{}` | Provider-scoped model/task-type config (preferred) |
| `model` | String | *(empty)* | Default model fallback (compatible) |
| `reasoning_effort` | String | *(empty)* | Default reasoning effort fallback (Codex) |
| `task_types` | Dict | `{}` | Task-type fallback configuration (compatible) |
| `claude` | Dict | *(see below)* | Claude-specific configuration (see [Claude Configuration](#claude-configuration)) |
| `claude_args` | List | *(deprecated)* | Use `claude.args` instead |
| `tmux` | Dict | *(see below)* | Tmux session configuration (see [Tmux Sessions](tmux.md)) |
| `review_diff_small_threshold` | Integer | `500` | Total changed-line cutoff (`added + removed`) below which review prompts include full inline diff |
| `review_diff_medium_threshold` | Integer | `2000` | Total changed-line cutoff above `review_diff_small_threshold`; larger diffs use targeted excerpts instead of full inline diff |
| `review_context_file_limit` | Integer | `12` | Maximum number of changed files to include in targeted excerpt mode for large review diffs |
| `autonomous_verify_timeout_seconds` | Integer | `120` | Timeout for lifecycle/automation-initiated `verify_command` runs |
| `review_verify_timeout_grace_seconds` | Float | `5` | Grace period after SIGTERM before autonomous review verification escalates to SIGKILL; accepts values `>= 1` second |
| `code_task_diff_timeout_medium_threshold` | Integer | `400` | Reviewable diff-size threshold where code tasks move from the base timeout to the medium scaled timeout |
| `code_task_diff_timeout_large_threshold` | Integer | `1200` | Reviewable diff-size threshold where code tasks move from the base timeout to the large scaled timeout; must be `>= code_task_diff_timeout_medium_threshold` after defaults and overrides are merged |
| `code_task_diff_timeout_medium_minutes` | Integer | `30` | Timeout budget used for medium-sized code diffs |
| `code_task_diff_timeout_large_minutes` | Integer | `45` | Timeout budget used for large code diffs; must be `>= code_task_diff_timeout_medium_minutes` after defaults and overrides are merged |
| `code_task_diff_timeout_cap_minutes` | Integer | `45` | Hard maximum applied to code-task budgets after base timeout resolution and diff-size scaling |
| `pr_integration` | Boolean | `true` | Enable GitHub PR discovery/comment/create flows; set `false` to skip all `gh`-backed PR operations for the project |
| `advance_create_plan_reviews` | Boolean | `true` | Auto-create `plan_review` tasks for completed non-held plans; when disabled, lifecycle parks for manual plan-review creation instead |
| `require_plan_review_before_implement` | Boolean | `true` | Require an approved `plan_review` before lifecycle automation materializes implementation work from a plan |
| `max_plan_review_cycles` | Integer | `2` | Cap for `plan_review` / `plan_improve` loops before lifecycle automation parks for discussion |
| `max_plan_slices` | Integer or null | `null` | Optional cap on how many implementation slices one approved plan review may materialize automatically |
| `plan_slice_target_timeout_minutes` | Integer or null | `code_task_diff_timeout_cap_minutes` | Optional reviewer sizing budget for one implementation slice; when unset it derives from code-task timeout sizing |
| `recommend_rebase_behind_commits` | Integer | `1` | Deprecated compatibility key; accepted but ignored by current lifecycle planning |
| `max_noop_improve_cycles` | Integer | `1` | Cap for consecutive no-op improves before lifecycle automation stops for discussion |
| `max_failed_closing_review_retries` | Integer | `3` | Max consecutive failed closing-review attempts before a lineage is parked as `needs_attention`; set `0` to escalate immediately on first failure |
| `max_concurrent` | Integer | explicit `watch.batch` or `5` | Hard global ceiling on concurrently running task-executing processes across `work`, `watch`, `advance`, iterate/recovery helpers, and internal task runners. Explicit `max_concurrent` wins; otherwise an explicitly configured `watch.batch` becomes the cap; if `watch.batch` is omitted, the fallback remains `5` |
| `iterate_max_iterations` | Integer | `3` | Default iterate iteration budget when `gza iterate` omits `--max-iterations` (1 iteration = code-change task [implement/improve] + review) |
| `main_checkout_isolate` | Boolean | `false` | When true, `gza watch` stages merges in a dedicated detached checkout, then fast-forwards the real default branch only after the isolated merge lands cleanly |
| `watch` | Dict | `{batch: 2, poll: 300, no_activity_timeout: 60, max_idle: null, max_iterations: 10, recovery_slots: 1}` | Defaults for `gza watch` loop behavior |
| `learnings_window` | Integer | `25` | Number of recent completed tasks to include in the learnings update prompt |
| `learnings_interval` | Integer | `5` | Auto-update learnings every N completed tasks; set to `0` to disable auto-updates |
| `theme` | String | `minimal` | Built-in color theme: `default_dark`, `minimal`, `selective_neon`, or `blue`. Override with `gza.local.yaml`. |
| `no_color` | Boolean | `false` | Disable all color/theming, even on a TTY. Equivalent to persistent `NO_COLOR=1`; effective behavior is `no_color OR NO_COLOR`. |
| `colors` | Dict | `{}` | Ad-hoc map of `field_name: rich_color_string` applied on top of `theme` (highest priority). Allowed in `gza.local.yaml`. |

### Project Scope Enforcement

When `gza.yaml` lives below the git repo root, Gza treats that subdirectory as the default write boundary for code tasks. Commits that stage files outside the project subtree fail with `PROJECT_SCOPE_VIOLATION` unless one of these is true:

- `enforce_project_scope: false`
- The task has the reserved `cross-project` tag and every changed path falls under a discovered sibling project root, or under a new project root declared by a changed `gza.yaml` on the branch

In-repo local dependencies resolved from `uv.lock` widen the allowed write scope automatically. Out-of-repo local dependencies stay read-only in Docker via auto-injected bind mounts.

For `cross-project` tasks, execution still stays anchored to the configured project root. Verification is per affected project: Gza discovers changed project roots from the branch diff, including project configs added on the branch itself, then runs each affected project's own `verify_command` from that project's root. Affected projects with no `verify_command` are reported as skipped rather than silently treated as passing.

### Local Overrides (gza.local.yaml)

Use `gza.local.yaml` for machine-specific settings that should not be committed.

- Merge behavior: deep merge for dictionaries, replace for scalars/lists
- Precedence: `~/.gza/config.yaml` < `gza.yaml` < `gza.local.yaml` < `GZA_DB_PATH` (for `db_path`) < other env vars
- Guardrails: only approved keys can be overridden in `gza.local.yaml`

### User Defaults (`~/.gza/config.yaml`)

Use `~/.gza/config.yaml` for per-user defaults that should apply to every Gza project on the machine.

- Merge behavior: deep merge for dictionaries, replace for scalars/lists
- Purpose: machine-wide defaults such as shared DB location, provider/model defaults, Docker settings, watch/tmux defaults, and UI preferences
- Project discovery: unsupported. `~/.gza/config.yaml` does not replace `gza.yaml`, and it cannot supply `project_name`.
- Validation: invalid or unknown keys are hard errors because this file affects every project on the machine

Allowed keys:
`db_path`, `use_docker`, `enforce_project_scope`, `docker_image`, `docker_volumes`, `docker_setup_command`, `timeout_minutes`, `max_steps`, `max_turns`, `worktree_dir`, `work_count`, `interactive_worktree_dir`, `provider`, `task_providers`, `model`, `reasoning_effort`, `defaults`, `task_types`, `providers`, `claude`, `tmux`, `chat_text_display_length`, `verify_command`, `inner_verify_command`, `watch`, `iterate_max_iterations`, `advance_create_reviews`, `advance_create_plan_reviews`, `require_review_before_merge`, `require_plan_review_before_implement`, `pr_integration`, `max_resume_attempts`, `max_review_cycles`, `max_plan_review_cycles`, `max_noop_improve_cycles`, `max_plan_slices`, `plan_slice_target_timeout_minutes`, `main_checkout_isolate`, `merge_squash_threshold`, `cleanup_days`, `review_diff_small_threshold`, `review_diff_medium_threshold`, `review_context_file_limit`, `autonomous_verify_timeout_seconds`, `review_verify_timeout_grace_seconds`, `code_task_diff_timeout_medium_threshold`, `code_task_diff_timeout_large_threshold`, `code_task_diff_timeout_medium_minutes`, `code_task_diff_timeout_large_minutes`, `code_task_diff_timeout_cap_minutes`, `recommend_rebase_behind_commits` (deprecated no-op), `learnings_window`, `learnings_interval`, `learnings_max_items`, `theme`, `no_color`, `colors`

Disallowed keys:
`project_name`, `project_id`, `project_prefix`, `tasks_file`, `log_dir`, `branch_strategy`, `branch_mode`

Shared DB example:

```yaml
# ~/.gza/config.yaml
db_path: ~/.gza/gza.db
```

`gza init` is inheritance-aware:

- If `~/.gza/config.yaml` already sets `db_path`, choosing shared mode leaves project `gza.yaml` without an active `db_path` so the project inherits that user-level shared DB.
- If no user-level shared default exists, choosing shared mode writes `db_path: ~/.gza/gza.db` into the project unless you pass `--db-path`.
- Choosing local mode writes `db_path: .gza/gza.db` explicitly, including when a user-level shared default exists, so the project opts out visibly.
- Non-interactive `gza init` no longer silently defaults to local DB mode; pass `--db local` or `--db shared` in automation.

`GZA_DB_PATH` is the supported environment override for the task database path. This is useful for one-off runs against a shared DB:

```bash
GZA_DB_PATH=~/.gza/gza.db uv run gza next
```

Example:

```yaml
# gza.local.yaml
use_docker: false
timeout_minutes: 30
inner_verify_command: ./bin/tests --quick
docker_volumes:
  - ~/datasets:/datasets:ro
providers:
  claude:
    task_types:
      review:
        model: claude-haiku-4-5
        reasoning_effort: low
  codex:
    task_types:
      implement:
        timeout_minutes: 45
```

Inspect effective values and source attribution:

```bash
gza config
gza config --json
gza config keys
gza config keys --json
gza config example
gza config example --local
gza config example --check
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

Disable all color persistently:

```yaml
no_color: true
```

`NO_COLOR` remains supported and wins whenever it is set. Effective behavior is a logical OR: if either `no_color: true` or `NO_COLOR` is present, Gza emits plain text with no ANSI color codes.

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
| Unmerged | `review_approved`, `review_followups`, `review_changes`, `review_discussion`, `review_none`, `merge_conflicts` |
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

### Docker Pre-Warm Hook (`docker_setup_command`)

Use `docker_setup_command` in `gza.yaml` for one-time container environment preparation before the agent starts.

```yaml
docker_setup_command: "uv sync"
```

How it runs:
- Runs synchronously inside the container before the provider CLI process starts.
- Runs on a single process once per provider invocation, so setup does not race with parallel tool calls or subagents.
- Is concatenated with the internal shim installer in the container entrypoint and evaluated before provider startup.

What to put here:
- Python (`uv`): `uv sync`
- Python (`Poetry`): `poetry install --no-interaction`
- Python (`pip`): `pip install -e .`
- Node-side prep: `npm ci`
- Mixed stacks: chained setup such as `uv sync && npm ci`

Why this matters:
- Without `docker_setup_command`, dependency installs are often lazy on the first CLI invocation.
- If the agent parallelizes initial commands, those first lazy installs can race and fail (for example, shared wheel staging collisions).
- Pre-warming in `docker_setup_command` makes later `uv run ...`/CLI invocations no-op for environment setup and avoids first-use install contention.

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
  plan_review: codex
  plan_improve: codex
  implement: codex
providers:
  claude:
    model: claude-sonnet-4-5
    task_types:
      review:
        model: claude-haiku-4-5
        reasoning_effort: low
        max_turns: 20
  codex:
    model: o4-mini
    reasoning_effort: medium
    task_types:
      plan_review:
        timeout_minutes: 45
      plan_improve:
        timeout_minutes: 30
```

### Task Types Configuration (Legacy-Compatible)

Override settings per task type:

```yaml
task_types:
  explore:
    model: claude-sonnet-4-5
    reasoning_effort: low
    max_turns: 20
  plan:
    model: claude-opus-4
    reasoning_effort: medium
    max_turns: 30
  plan_review:
    reasoning_effort: high
    timeout_minutes: 45
  plan_improve:
    reasoning_effort: medium
    timeout_minutes: 30
  review:
    reasoning_effort: high
    max_turns: 15
```

Valid task types: `explore`, `plan`, `plan_review`, `plan_improve`, `implement`, `review`, `improve`, `fix`, `rebase`, `internal`

The generic task-type routing keys apply to plan-review work the same way they apply to existing task types:
`task_providers.plan_review`, `task_types.plan_review`, and `providers.<provider>.task_types.plan_review` route and shape `plan_review`; `plan_improve` uses the corresponding `plan_improve` keys.

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

Reasoning effort selection:
1. `providers.<effective_provider>.task_types.<task_type>.reasoning_effort`
2. `providers.<effective_provider>.reasoning_effort`
3. `task_types.<task_type>.reasoning_effort` (task-type fallback)
4. `reasoning_effort` / `defaults.reasoning_effort` (default fallback)
5. Provider runtime default (if no reasoning effort resolved)

Max steps selection:
1. `providers.<effective_provider>.task_types.<task_type>.max_steps`
2. `providers.<effective_provider>.task_types.<task_type>.max_turns` (legacy)
3. `task_types.<task_type>.max_steps` (legacy fallback)
4. `task_types.<task_type>.max_turns` (legacy fallback)
5. `max_steps` / `defaults.max_steps`
6. `max_turns` / `defaults.max_turns` (legacy fallback)

Timeout selection:
1. `providers.<effective_provider>.task_types.<task_type>.timeout_minutes`
2. `task_types.<task_type>.timeout_minutes`
3. `timeout_minutes`
4. Hardcoded default (`10`)

For `implement`, `improve`, `fix`, and `rebase`, the resolved base timeout can then be scaled up by reviewable diff size:

- If the diff reaches `code_task_diff_timeout_medium_threshold`, use at least `code_task_diff_timeout_medium_minutes`.
- If the diff reaches `code_task_diff_timeout_large_threshold`, use at least `code_task_diff_timeout_large_minutes`.
- After base timeout resolution and any diff-size scaling, cap the final code-task budget at `code_task_diff_timeout_cap_minutes`, even if a task-type or provider override is higher.
- The large threshold/minutes checks are enforced on the resolved config, even when only one side is set in `gza.yaml`, `~/.gza/config.yaml`, or `gza.local.yaml`.
- If Gza cannot compute the reviewable diff safely, it falls back to the normal resolved timeout and logs that fallback.

### Verification Profiles

Code tasks support two verification tiers:

```yaml
verify_command: ./bin/tests
inner_verify_command: ./bin/tests --quick
```

- `verify_command` remains the required final gate before a code task reports success.
- `inner_verify_command` is optional and is intended for fast edit-loop checks during implementation.
- When `inner_verify_command` is unset, agents should prefer targeted tests during editing and still run `verify_command` once after the last code change.
- Autonomous review verification is separate and remains bounded by `autonomous_verify_timeout_seconds`.
- When autonomous review verification times out, Gza sends SIGTERM to the verify process group, waits `review_verify_timeout_grace_seconds`, then escalates to SIGKILL if the process tree is still alive.

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
reasoning_effort: medium  # optional: Codex reasoning effort override
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
| `--pr` | Request auto-create/reuse of a GitHub PR after successful code-task completion; evaluated at completion time and skipped without failing when PRs are unavailable |
| `--tag TAG` | Only pick pending tasks matching tag filters when no task IDs are specified (repeatable) |
| `--all-tags` | With repeated `--tag` values, require all requested tags instead of the default any-tag matching |

`uv run gza work` starts pending tasks only. It does not run failed-task recovery (`resume` / `retry` / manual-review parking) and it does not progress review/rebase/merge lifecycle work for already-started lineages. If recovery candidates exist, `work` leaves them untouched and starts from the pending lane anyway.

### work / advance / watch operating surface

Use this matrix when deciding "what will this command actually touch?"

| Command | Starts new pending tasks? | Runs failed-task recovery? | Runs review / merge lifecycle? | Continuous loop? |
|--------|----------------------------|----------------------------|--------------------------------|------------------|
| `uv run gza work` | Yes. Pending lane only. | No. | No. | No. |
| `uv run gza advance` | No by default. Yes with `--new` after lifecycle/recovery planning. | Yes. Shared bounded `resume` / `retry` / manual-review decisions. | Yes. Review, improve, rebase, merge, held-plan follow-up, and related lifecycle work. | No. |
| `uv run gza watch` | Yes. Maintains the configured batch from the pending lane. | Yes. Uses the same bounded recovery policy as `advance`. | Yes. Reuses the same lifecycle planner and follow-on actions. | Yes. |

Two operator-facing queue surfaces show these sets separately:

- `uv run gza next` and `uv run gza queue` show recovery, lifecycle actions, and pending as distinct sections.
- Recovery lane entries belong to `advance` / `watch`, not `work`.
- Lifecycle-action entries belong to `advance` / `watch`, not `work`.
- Pending lane entries belong to `work` / `watch`.

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
| `--tag TAG` | Add task tag (repeatable) |
| `--based-on ID` | Base on previous task by full prefixed task ID (e.g. `gza-1234`) |
| `--depends-on ID` | Set dependency on another task by full prefixed task ID (e.g. `gza-1234`) |
| `--review` | Auto-create review task on completion |
| `--hold-for-review` | For `--type plan`, require manual review before any automatic implementation follow-up |
| `--pr` | Request auto-create/reuse of a GitHub PR after successful code-task completion; evaluated at completion time and skipped without failing when PRs are unavailable |
| `--same-branch` | Continue on depends_on task's branch |
| `--spec FILE` | Path to spec file for context |
| `--review-scope TEXT` | For direct implement tasks, set the authoritative gradeable review boundary |
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
| `--add-tag TAG` | Add one or more tags (repeatable, mutually exclusive with other tag mutation flags) |
| `--remove-tag TAG` | Remove one or more tags (repeatable, mutually exclusive with other tag mutation flags) |
| `--clear-tags` | Remove all tags from task (mutually exclusive with other tag mutation flags) |
| `--set-tags CSV` | Replace all tags with comma-separated tags (mutually exclusive with other tag mutation flags) |
| `--based-on ID` | Set lineage/parent relationship using a full prefixed task ID (branch inheritance and context; e.g. `gza-1234`) |
| `--depends-on ID` | Set execution dependency using a full prefixed task ID (blocks task until dependency completes; e.g. `gza-1234`) |
| `--clear-depends-on` | Remove the execution dependency (mutually exclusive with `--depends-on`) |
| `--explore` | Convert to explore task |
| `--task` | Convert to regular task |
| `--review` | Enable automatic review task creation on completion |
| `--hold-for-review`, `--no-hold-for-review` | For plan tasks, require or release manual review before automatic implementation follow-up |
| `--auto-implement` | Compatibility alias for `--no-hold-for-review`; retained for existing scripts |
| `--pr` | Request auto-create/reuse of a GitHub PR after successful code-task completion; evaluated at completion time and skipped without failing when PRs are unavailable |
| `--prompt TEXT` | Set new prompt directly (use `-` for stdin) |
| `--prompt-file FILE` | Read new prompt from file |
| `--model MODEL` | Override model for this task |
| `--provider PROVIDER` | Override provider for this task |
| `--no-learnings` | Skip injecting learnings context |

Pending tasks may use any supported edit flag. Non-pending tasks may only use tag mutation flags (`--add-tag`, `--remove-tag`, `--clear-tags`, or `--set-tags`).
Pending plan tasks may use `--hold-for-review` or `--no-hold-for-review`. Completed plan tasks may also use `--no-hold-for-review` (preferred) or `--auto-implement` (compatibility alias) to release a hold-for-review plan without rerunning it.
All other edit flags (`--based-on`, `--depends-on`, `--clear-depends-on`, `--explore`, `--task`, `--review`, `--pr`, `--prompt`, `--prompt-file`, `--model`, `--provider`, `--no-learnings`, and completed-plan `--hold-for-review`) remain pending-only.

Non-conflicting edit mutations can be combined in one invocation. Tag mutation flags remain mutually exclusive with each other.

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
| `--verbose` | Keep formatted provider rendering, but expand generic fallback events with pretty JSON payloads |
| `--conversation-only` | Show only the provider conversation transcript stream |
| `--ops-only` | Show only the gza operational stream |
| `--failure`, `-F` | Show failure-focused diagnostics for failed tasks (reason, summary, marker, agent explanation, and last verify/result context) |
| `--page` | Pipe output through `$PAGER` (default: `less -R`); skipped for `--follow` and `--raw` |

By default, the identifier is treated as a full task ID (for example `gza-1234`).
For split-layout task logs, `gza log` merges `.gza/logs/<slug>.log` (provider conversation transcript) with `.gza/logs/<slug>.ops.jsonl` (runner/provider operational events) in chronological order.
If no main task log exists yet, `gza log` can fall back to startup logs and their paired startup ops siblings, including worker startup logs in `.gza/workers/*-startup.log` and `.gza/workers/*-startup.ops.jsonl`.
Top-level provider `{"type":"error"}` events are rendered in normal log output; if the provider embeds a nested `error.message` JSON payload, `gza log` shows the readable message and keeps the full payload inline.
When stream metadata is present, `gza log` also shows model parity in-session: configured model (from `gza/info`) vs provider-reported model, including a warning on mismatch or an explicit note when the provider does not echo a model.
`--verbose` does not switch to raw JSONL. It preserves the curated formatted output and only expands generic fallback events so unknown provider payloads stay inspectable without losing the higher-level rendering.

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

`gza pr` reuses an open PR when the branch already has one. If the most recent associated PR is closed or merged while the branch is still unmerged, `gza pr` creates a new PR and updates the cached `pr_number`.

`gza pr` does not reconcile or close stale GitHub PRs after manual or squash merges outside GitHub. Run `uv run gza sync` after those merges to refresh cached PR state and close stale open PRs only when `origin/<default-branch>` proves the branch changes already landed.

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
| `--poll [SECS]` | Refresh output every `SECS` seconds (default: `5` when flag is present without a value) |
| `--recent-minutes MINUTES` | In poll mode, keep first-seen terminal rows visible when they ended within the last `MINUTES` (default: `1`, `0` disables) |

Runtime reconciliation notes:
- Task lifecycle state is derived from the DB `tasks` table (`status`, `started_at`, `running_pid`), while worker metadata is a process index.
- On CLI startup, `gza ps` only prune dead worker metadata; it does not reconcile or auto-fail `in_progress` DB tasks.
- DB reconciliation for stale `in_progress` tasks still happens on mutating lifecycle commands such as `gza work`, where tasks may be auto-failed as:
  - `WORKER_DIED` when `running_pid` is missing/invalid or the PID is no longer alive.
  - `TIMEOUT` when runtime exceeds configured `timeout_minutes`.
- `gza ps` merges worker rows and DB in-progress tasks by task ownership, so healthy background runs appear as one active task row.
- When available, `gza ps` shows the full stored execution model ID in a dedicated `MODEL` column; tasks that have never run render `-`.
- `gza ps` shows the task merge unit in a dedicated `MERGE UNIT` column as `<merge-unit-id> / <owner-task-id>`; tasks not attached to a merge unit render `-`.

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

### preflight

Run a live provider/model sanity check before queueing real work. `gza preflight`
loads config, resolves the provider/model pairs real tasks would use, runs the
provider credential preflight, then sends a trivial prompt and reports a PASS/FAIL
table. The command exits non-zero if any resolved pair fails. When the live
round-trip is rejected by the provider, the failure detail prefers the provider's
model/config error text over the startup command breadcrumb so operators see the
actionable rejection reason.

```bash
gza preflight
gza preflight --task-type review
gza preflight --provider codex --model o4-mini
gza preflight --docker
gza preflight --no-docker
```

By default the execution path follows `use_docker` from config. Use `--docker`
or `--no-docker` to override that for the check. Docker preflights require API
keys in the container environment; OAuth or keychain-backed credentials do not
propagate into containers.

### config

Show effective configuration and source attribution (`base`, `local`, `env`, `default`).

```bash
gza config
gza config --json
gza config keys
gza config keys --json
```

`gza config keys` prints the discoverable config-key registry in a readable table with these columns:
- `KEY`
- `TYPE`
- `DEFAULT`
- `DESCRIPTION`

`gza config keys --json` prints a machine-readable payload:
- `keys[]` entries with `key`, `type`, `required`, `default`, and `description`

`gza config example` renders the generated commented `gza.yaml` example from the same
`CONFIG_KEY_REGISTRY` that powers `gza config keys`, docs parity, and `gza init`.
Use `--local` for the machine-local override flavor, `--output PATH` to write somewhere else,
`--write` to regenerate the committed example artifact, and `--check` to fail on drift in CI.

#### Discoverable Config Keys

The following keys are currently discoverable via `gza config keys`:

```text
advance_create_reviews
advance_mode
require_review_before_merge
branch_mode
branch_strategy
branch_strategy.default_type
branch_strategy.pattern
chat_text_display_length
claude.args
claude.fetch_auth_token_from_keychain
claude_args
cleanup_days
colors.*
defaults.max_steps
defaults.max_turns
defaults.model
defaults.reasoning_effort
docker_image
docker_setup_command
docker_volumes
interactive_worktree_dir
iterate_max_iterations
learnings_interval
learnings_max_items
learnings_window
log_dir
max_resume_attempts
max_review_cycles
max_noop_improve_cycles
max_failed_closing_review_retries
max_steps
max_turns
main_checkout_isolate
merge_squash_threshold
model
pr_integration
reasoning_effort
project_name
project_prefix
provider
providers.*.model
providers.*.reasoning_effort
providers.*.task_types.*.max_steps
providers.*.task_types.*.max_turns
providers.*.task_types.*.model
providers.*.task_types.*.reasoning_effort
providers.*.task_types.*.timeout_minutes
review_context_file_limit
autonomous_verify_timeout_seconds
review_verify_timeout_grace_seconds
recommend_rebase_behind_commits
review_diff_medium_threshold
review_diff_small_threshold
task_providers.*
task_types.*.max_steps
task_types.*.max_turns
task_types.*.model
task_types.*.reasoning_effort
task_types.*.timeout_minutes
tasks_file
theme
timeout_minutes
tmux.auto_accept_timeout
tmux.detach_grace
tmux.enabled
tmux.max_idle_timeout
tmux.terminal_size
use_docker
verify_command
inner_verify_command
max_concurrent
code_task_diff_timeout_medium_threshold
code_task_diff_timeout_large_threshold
code_task_diff_timeout_medium_minutes
code_task_diff_timeout_large_minutes
code_task_diff_timeout_cap_minutes
watch.batch
watch.failure_backoff_initial
watch.failure_backoff_max
watch.failure_halt_after
watch.max_idle
watch.no_activity_timeout
watch.no_progress_cycles
watch.max_iterations
watch.poll
watch.recovery_slots
watch.restart_failed_batch
work_count
worktree_dir
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
| `--prompt` | Print only the fully built prompt text for this task and exit |
| `--output` | Print only the raw output/report content and exit |
| `--path` | Print only the report file path and exit |
| `--metadata-only` | Show the normal metadata/detail view without the `Prompt:` and `Output:` blocks |
| `--page` | Pipe output through `$PAGER` (default: `less -R`); skipped for `--prompt`, `--output`, and `--path` modes |

`--metadata-only` is incompatible with `--prompt`, `--output`, `--path`, and `--full`.

The standard metadata block includes PR intent and cached PR details when known:
- `Create PR: yes|no` showing whether the task is configured to auto-create or reuse a PR on successful completion.
- `PR Number: <N>` when a cached PR number is present on the task.
- `PR State: <open|closed|merged|...>` when a cached PR state is present on the task.

When a task's lineage extends beyond the selected row, `gza show` also includes:
- `Lifecycle: ...` directly under `Status:` summarizing the current unit-of-work state using the same shared recovery planning handoff and advance lifecycle classifier as `gza advance`, `gza watch`, and `gza iterate`. For completed plan lineages with implement descendants, `show` summarizes the newest implement descendant instead of the plan-level `implement task already exists` skip, so already-merged implementations render as terminal. Needs-attention lifecycle outcomes reuse the shared `reason=...` policy slug format, and git/default-branch resolution or later shared-classifier Git/context failures are surfaced as an explicit lifecycle-unavailable message instead of guessed fallback state.
- A `Lineage:` tree where every node includes its current task status, failed-task reason when relevant, and merge state for completed code tasks.

When a task has a branch, `gza show` also reports active worktree information:
- `Worktree: <path>` when the task branch is currently checked out in an active worktree
- `Warning: Worktree lookup failed: ...` when git worktree metadata could not be read

When execution provenance is known, `gza show` also includes:
- `Execution Mode: worker_background` for detached worker runs
- `Execution Mode: worker_foreground` for foreground worker runs
- `Execution Mode: foreground_inline` for `gza run-inline` runner-managed foreground runs
- `Execution Mode: manual` for legacy tasks that were manually forced to `in_progress` in older releases
- `Execution Mode: skill_inline` for inline skill runs (for example `gza-task-run`)
- `Provider: <provider>` and `Model: <full-model-id>` from the stored execution record (`-` when unset)

For completed `review` tasks, `gza show` also includes:
- `Verdict: <APPROVED|CHANGES_REQUESTED|NEEDS_DISCUSSION|...>` when parseable from review output.
- `Score: <N>/100` when a derived `review_score` is available.
- Review-verify audit fields when autonomous review verification ran, including status, exit status, capture time, branch/head/base provenance, working directory, the persisted `## verify_command result` section, the latest `verify_command_output` artifact path, and an `Artifacts:` list covering every stored task artifact with missing-file visibility.

### artifact

Print the latest matching stored task artifact content or path.

```bash
gza artifact <task_id> [options]
```

| Option | Description |
|--------|-------------|
| `task_id` | Full prefixed task ID to inspect (e.g. `gza-1234`) |
| `--kind KIND` | Filter artifacts by kind (for example `verify_command_output`) |
| `--latest` | Select the latest matching artifact (default behavior) |
| `--path` | Print only the resolved absolute artifact path when the latest row has a content file |

By default, `gza artifact` selects the newest matching artifact row. If that latest row is metadata-only (for example a no-output verify result), or if its stored file is missing, both default content retrieval and `--path` fail clearly instead of silently falling back to an older content-bearing artifact.

### run-inline

Run a specific task in the foreground through the same runner path used by workers.

```bash
gza run-inline <task_id> [options]
```

| Option | Description |
|--------|-------------|
| `task_id` | Full prefixed task ID to run inline (e.g. `gza-1234`) |
| `--resume` | Resume from the stored provider session instead of starting fresh |
| `--no-docker` | Run provider directly instead of in Docker |
| `--max-turns N` | Override max_turns setting for this run |
| `--force` | Skip dependency merge precondition checks when starting the run |

`run-inline` uses provider capabilities: Claude runs in terminal-attached interactive foreground mode, while providers without interactive foreground support run in observe-only mode.

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

Retry a failed or completed task by creating a new attempt with a fresh conversation. Implement retries may fork a fresh branch; same-branch follow-up retries stay attached to the shared branch.

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

For immediate-start commands that hand work off to detached workers (`work`, `resume`, `retry`, `implement`, `extract`, `review`, `improve`, `fix`, `iterate`, `rebase`, and `advance --new`), any parent-side validation or startup-preparation failure is reported on the caller's stderr before the worker detaches. Success, queue, and no-op status messages remain on stdout.

### mark-completed

Manually complete a task when automation failed. Defaults are task-type aware:
- `task`, `implement`, `improve` default to git-verified completion
- `explore`, `plan`, `review` default to status-only completion

```bash
gza mark-completed <task_id> [--verify-git | --force] [--reason <text>]
```

| Option | Description |
|--------|-------------|
| `task_id` | Full prefixed task ID to mark as completed (e.g. `gza-1234`) |
| `--verify-git` | Validate branch and commits before completion |
| `--force` | Status-only completion (for non-code tasks or stale in_progress recovery) |
| `--reason <text>` | Persist a completion reason to `task.completion_reason` |

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
| `--mark-only` | Mark branch as merged without performing the git merge or deleting the branch |
| `--no-followups` | Skip materializing review FOLLOWUP tasks after a successful merge or mark-only |
| `--resolve` | Auto-resolve conflicts using AI when rebasing (requires --rebase) |

`gza merge` only performs the local git merge/rebase path and updates local merge state. Merge units are the canonical persisted merge-truth model; compatibility task-row `merge_status` is dual-written from the selected unit during the migration window. Task selectors resolve through merge-unit membership first, so `uv run gza merge <review-task-id>` and same-branch follow-up task IDs merge the shared implementation branch/unit they belong to. Successful merges and `--mark-only` now also materialize review FOLLOWUP findings into implement tasks by default, reusing existing deterministic follow-up tasks when they were already created by `advance`, `watch`, or `iterate`; pass `--no-followups` to skip that step. Merged units also persist a `merge_source` provenance label (`manual` for this command). `gza merge` does not reconcile GitHub PR state. After merge, run `uv run gza sync` to refresh cached PR metadata and close any stale still-open PRs when remote default-branch state proves the changes already landed.

### merged

List merged merge units, optionally filtered by merge-source provenance and merge time.

```bash
uv run gza merged [options]
```

| Option | Description |
|--------|-------------|
| `--source SOURCE` | Filter by recorded merge source: `manual`, `advance`, `watch`, `github_pr`, or `external` |
| `--last-days N` | Only show units merged in the last N days |
| `--since DATE` | Only show units merged on or after `YYYY-MM-DD` or another ISO timestamp |
| `--json` | Output structured JSON rows |
| `--fields CSV` | Projection field override (for example `merge_unit_id,merge_source,branch`) |
| `--list-fields` | List valid `--fields` values for this command and exit |

`uv run gza merged` is the audit surface for persisted merge provenance. It reads canonical merge-unit state and renders merged units newest-first, with default columns for unit ID, owner task, source, merge timestamp, and source branch. Use it to answer questions like `uv run gza merged --source manual --last-days 7`.

### lineage

Show a task's lineage from the selected task outward.

```bash
uv run gza lineage <task_id> [--full | --parents-only | --children-only]
```

By default, `uv run gza lineage <task_id>` keeps the existing children-focused view: it renders the selected task and its descendants. When the selected task has immediate parents, the default output also prints a short parent hint so resume/retry or dependency ancestry is visible without switching modes.

| Option | Description |
|--------|-------------|
| `task_id` | Full prefixed task ID to inspect (for example `gza-1234`) |
| `--full` | Show both ancestor lineage and descendant lineage |
| `--parents-only` | Show only the ancestor chain for the selected task |
| `--children-only` | Show only the selected task and its descendants |

### unmerged

List tasks with merge units that have not been merged to the default branch.

```bash
uv run gza unmerged [options]
```

| Option | Description |
|--------|-------------|
| `-n N` | Show last N unmerged tasks (default: 5, `0` for all) |
| `--fetch` | Fetch `origin` before the canonical default-branch refresh so `origin/<default>` merge evidence is current. Has no effect with `--into-current` or `--target` |
| `--into-current` | Compare against the current branch using live git checks instead of cached default-branch `merge_status`; query-only and never persists reconciliation results |
| `--target BRANCH` | Compare against the specified branch using live git checks instead of cached default-branch `merge_status`; query-only and never persists reconciliation results |
| `--json` | Output JSON rows from the unified query API |
| `--fields CSV` | Projection field override (for example `id,prompt`). In text mode, one field prints bare values and multiple fields print `field: value` blocks; in JSON mode rows stay structured objects |
| `--list-fields` | List valid `--fields` values for this command and exit |

`uv run gza unmerged` is the daily merge-truth command. In the default-branch view, it opens the task store read/write, backfills merge units when needed, refreshes canonical branch-cohort merge truth from local git plus any already-present `origin/<default-branch>` remote-tracking ref, persists merge-unit state and diff stats for the real default target branch, dual-writes compatibility task merge fields, and then prints the reconciled default-branch unmerged list. Same-branch improve/fix/rebase/review follow-ups may validly keep `merge_status = NULL` because the owning implementation row carries the shared branch merge truth while all related rows remain attached to the same merge unit.

This is the deliberate narrow exception to the usual read-only query convention: only plain default-branch `uv run gza unmerged` mutates, because its entire purpose is to answer the canonical question "what still needs to be merged?" without leaving stale cached rows behind.

By default, plain `uv run gza unmerged` does not initiate network I/O. It reuses any already-present `origin/<default-branch>` remote-tracking ref if one exists locally, but otherwise relies on local branch state. Pass `--fetch` to opt into the older fetch-then-reconcile behavior and refresh `origin/*` first.

Deleted local feature branches are not treated as merge proof by themselves. Canonical reconciliation keeps them unmerged and visible until the target branch is explicitly proven to contain the changes, for example via a surviving `origin/<feature>` ref or merged PR metadata from `gza sync`.

If the canonical default-branch refresh cannot persist because the database is read-only, `uv run gza unmerged` fails with a targeted error instead of silently falling back to stale split-brain behavior.

With `--into-current` or `--target`, `uv run gza unmerged` always does ad hoc live git comparisons and leaves the database unchanged. If a live branch comparison or diff-stat refresh fails for any branch, the command exits non-zero and does not print a potentially stale unmerged list from fallback state.

`uv run gza unmerged` now builds an unmerged-specific query preset and then renders that result through the shared query projection/presentation path. The default text view is the slim operator-focused summary. Any explicit `--fields` switches to the generic projection renderer in either text or JSON mode, so `uv run gza unmerged --fields id` prints bare IDs and `uv run gza unmerged --json --fields id,prompt,merge_unit_id,merge_unit_state,source_branch,target_branch` returns structured rows that expose both task and merge-unit context.

During execution, the command logs concise progress for the refresh, query, and render phases. Those lines include counts for how many candidate tasks are being refreshed, how many task rows the query scans, and how many filtered rows are rendered.

For each unmerged implementation in the default text view, output includes:
- Branch diff/commit summary.
- When live merge analysis detects unresolved conflicts, a dedicated `merge: has conflicts` line.
- A `lineage:` branch-rendered tree showing only the selected branch-owner task and its descendants.
- A `review:` freshness classification:
  - `no review` when no completed review exists.
  - `reviewed` when the latest completed review still reflects current code.
  - `review stale` when code-changing work (for example an improve task) happened after the latest review.
- When a completed review has a stored derived score, verdict badges include it as `(<score>)`, for example `✓ approved (82)`.

### history

List recent completed or failed tasks.

By default, `gza history` excludes `internal` tasks. Use `--type internal` to view internal task history.

```bash
gza history [options]
```

| Option | Description |
|--------|-------------|
| `--last N`, `-n N` | Show last N tasks (default: 5) |
| `--type TYPE` | Filter by task type: `explore`, `plan`, `plan_review`, `plan_improve`, `implement`, `review`, `improve`, `fix`, `rebase`, `internal` |
| `--type-not TYPE` | Exclude the given task type |
| `--days N` | Show only tasks from the last N days |
| `--start-date YYYY-MM-DD` | Show only tasks on or after this date |
| `--end-date YYYY-MM-DD` | Show only tasks on or before this date |
| `--status STATUS` | Filter by status: `completed`, `failed`, or `unmerged` |
| `--status-not STATUS` | Exclude the given status |
| `--tag TAG` | Filter by tag (repeatable; matches any requested tag by default) |
| `--tag-not TAG` | Exclude by tag (repeatable; uses the same all-tags vs any-tag matching mode as `--tag`) |
| `--all-tags` | With repeated `--tag` and/or `--tag-not` values, require all requested tags instead of the default any-tag matching |
| `--lineage-depth N` | Render root-deduplicated lineage trees up to N levels |
| `--date-field FIELD` | Date field for date filters: `created`, `completed`, or `effective` (default: `effective`) |
| `--fields CSV` | Projection field override. In text mode, one field prints bare values and multiple fields print `field: value` blocks; in JSON mode rows stay structured objects |
| `--list-fields` | List valid `--fields` values for this command and exit |
| `--json` | Output JSON rows from the unified query API |

Positive and negative filters on the same field are applied in order: include matches for the positive flag first, then drop anything matching the corresponding `--...-not` flag. If the same value appears in both, the negative filter wins and that row is excluded.

Default human output also shows each task's stored execution model. Projection/JSON output supports `model` (and `provider`) fields, so `gza history --fields id,status,model` and `gza history --json` include them directly.

### incomplete

Show unresolved task lineages that still need attention.

```bash
gza incomplete [options]
```

| Option | Description |
|--------|-------------|
| `--last N`, `-n N` | Show last N unresolved rows (default: 5; use `0` for all) |
| `--type TYPE` | Filter by task type: `explore`, `plan`, `plan_review`, `plan_improve`, `implement`, `review`, `improve`, `fix`, `rebase`, `internal` |
| `--days N` | Show only unresolved rows from the last N days |
| `--date-field FIELD` | Date field for `--days`: `created`, `completed`, or `effective` (default: `effective`) |
| `--tree` | Render unresolved lineages as trees instead of one-line summaries |
| `--verbose` | In one-line mode, show owner task details beneath each unresolved lineage |
| `--blocked-by-dropped` | Switch to pending tasks blocked by dropped dependencies instead of unresolved lineages |
| `--fields CSV` | Projection field override. In text mode, one field prints bare values and multiple fields print `field: value` blocks; in JSON mode rows stay structured objects |
| `--list-fields` | List valid `--fields` values for this command and exit |
| `--json` | Output JSON rows from the unified query API |

Use `gza incomplete` for unresolved lineage triage. Use the more specific command surfaces when you want one domain only:

Projected `next_action` values come from the shared live lifecycle planner. Cleanly mergeable branches continue to the normal review or merge actions even when they are behind the target branch. Completed held plan tasks surface `awaiting_human` until you run `uv run gza implement <plan-id>` or `uv run gza edit <plan-id> --no-hold-for-review` (preferred; `--auto-implement` also works). Those held-plan rows now carry `reason=awaiting-human-review`. If an approved plan review has partial implement descendants but no durable materialization record, lifecycle now parks with `reason=plan-review-materialization-repair-needed` instead of silently treating the partial prefix as complete. Needs-attention rows now carry an explicit subject task, so `gza incomplete` roots attention rows at the plan/explore/implementation the operator should inspect instead of inferring that identity from lineage ownership alone. If older or malformed action data is missing that subject, the shared resolver falls back conservatively and emits a warning instead of silently re-inferring identity.

`uv run gza incomplete --list-fields` prints the unresolved-lineage projection set. `uv run gza incomplete --blocked-by-dropped --list-fields` prints the blocked-dropped task projection set.

Default text output stays to one wrapped line per lineage: the owner prompt is reduced to its first non-empty line and truncated, and `| unresolved: ...` appears only when multiple unresolved tasks remain for the same owner, summarized as task IDs plus failure/completion status.

| Need | Command |
|--------|-------------|
| Unmerged code work | `uv run gza unmerged` |
| Completed `plan`/`explore` work without implementation | `uv run gza advance --unimplemented` |
| Failed-task history | `uv run gza history --status failed` |
| Pending queue state | `uv run gza next` or `uv run gza next --all` |
| Synthesized next-step guidance | `/gza-summary` |

### search

Search task prompts by substring.

```bash
gza search <term> [options]
```

| Option | Description |
|--------|-------------|
| `term` | Substring to match in task prompt text |
| `--last N`, `-n N` | Show last N matching tasks (default: 10; use `0` for all) |
| `--status CSV` | Filter statuses (comma-separated) |
| `--status-not CSV` | Exclude statuses (comma-separated) |
| `--type CSV` | Filter task types (comma-separated) |
| `--type-not CSV` | Exclude task types (comma-separated) |
| `--days N` | Show only matches from the last N days |
| `--start-date YYYY-MM-DD` | Show only matches on or after this date |
| `--end-date YYYY-MM-DD` | Show only matches on or before this date |
| `--date-field FIELD` | Date field for date filters: `created`, `completed`, or `effective` (default: `created`) |
| `--lineage-of TASK_ID` | Restrict to the canonical lineage containing TASK_ID |
| `--lineage-of-not TASK_ID` | Exclude the canonical lineage containing TASK_ID |
| `--related-to TASK_ID` | Deprecated alias for `--lineage-of` |
| `--related-to-not TASK_ID` | Deprecated alias for `--lineage-of-not` |
| `--root CSV` | Restrict by lineage root IDs (comma-separated) |
| `--root-not CSV` | Exclude lineage root IDs (comma-separated) |
| `--tag TAG` | Filter by tag (repeatable; matches any requested tag by default) |
| `--tag-not TAG` | Exclude by tag (repeatable; uses the same all-tags vs any-tag matching mode as `--tag`) |
| `--all-tags` | With repeated `--tag` and/or `--tag-not` values, require all requested tags instead of the default any-tag matching |
| `--fields CSV` | Projection field override. In text mode, one field prints bare values and multiple fields print `field: value` blocks; in JSON mode rows stay structured objects |
| `--list-fields` | List valid `--fields` values for this command and exit |
| `--json` | Output JSON rows from the unified query API |

Text output ends with a summary footer such as `Showing results 1-9 out of 55`.
Positive and negative filters on the same field are applied in order: include matches for the positive flag first, then drop anything matching the corresponding `--...-not` flag. If the same value appears in both, the negative filter wins and that row is excluded.
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

Clean up stale worktrees, old logs, task artifacts, worker metadata, and archives.

```bash
gza clean [options]
```

| Option | Description |
|--------|-------------|
| `--worktrees` | Only clean up stale worktrees |
| `--workers` | Only clean up stale worker metadata and startup logs |
| `--logs` | Only clean up old log files (conversation `.log` and paired `.ops.jsonl` siblings together) and live task artifact files; archived artifacts are left for `--purge` |
| `--backups` | Only clean up old backup files |
| `--days N` | Remove items older than N days (default: from config cleanup_days, or 30) |
| `--keep-unmerged` | Keep logs and task artifacts for tasks that are still unmerged |
| `--archive` | Archive old log, live task artifact, and worker files instead of deleting; already archived artifacts are skipped |
| `--purge` | Delete previously archived log, artifact, and worker files (default: older than 365 days) |
| `--force` | Skip confirmation prompt before removing worktrees |
| `--dry-run` | Show what would be cleaned without doing it |

### comment

Add a comment to a task.

```bash
gza comment <task_id> <text> [options]
```

| Option | Description |
|--------|-------------|
| `task_id` | Full prefixed task ID to comment on (e.g. `gza-1234`) |
| `text` | Comment text to add |
| `--author NAME` | Optional author name recorded with the comment |

When task comments exist, `gza show` also includes a `Comments:` section.
When tasks have comments, `gza history` includes a `comments: N` indicator.

### improve

Create an improve task to address review feedback on an implementation.

```bash
gza improve <impl_task_id> [options]
```

| Option | Description |
|--------|-------------|
| `impl_task_id` | Full prefixed task ID (implement, improve, review, or fix — auto-resolves to root implementation; e.g. `gza-1234`) |
| `--review-id ID` | Explicit full prefixed review task ID to base the improve on (overrides auto-pick of most recent completed review; e.g. `gza-1234`) |
| `--review` | Auto-create review task on completion; if the branch already has an open PR, push same-branch improve commits first |
| `--pr` | Request auto-create/reuse of a GitHub PR after successful code-task completion; evaluated at completion time and skipped without failing when PRs are unavailable |
| `--queue`, `-q` | Add task to queue without executing immediately |
| `--background`, `-b` | Run worker in background |
| `--no-docker` | Run Claude directly instead of in Docker |
| `--max-turns N` | Override max_turns setting for this run |
| `--model MODEL` | Override model for this task |
| `--provider PROVIDER` | Override provider for this task |
| `--force` | Skip dependency merge precondition checks when running the improve task |

The improve command finds the most recent review for the implementation task and creates a new task that continues on the same branch to address the review feedback.
When no completed review exists, improve can use unresolved task comments as feedback context.
If no completed review exists, or a review exists but unresolved comments do, improve still runs using comments-only feedback.
When an improve run completes with `--review`, gza performs one narrow PR check before the follow-up review: if GitHub can confirm that the branch already has an open PR, gza pushes any new same-branch commits first so the review sees the published code. If GitHub is unavailable, lookup fails, or no live PR exists, improve preserves the normal auto-review flow.

### fix

Create and optionally run a fix rescue task for a stuck implementation lifecycle.

```bash
gza fix <task_id> [options]
```

| Option | Description |
|--------|-------------|
| `task_id` | Full prefixed task ID (implement, improve, review, or fix — auto-resolves to root implementation; e.g. `gza-1234`) |
| `--review` | Auto-create review task on completion; if the branch already has an open PR, push same-branch fix commits first |
| `--queue`, `-q` | Add task to queue without executing immediately |
| `--background`, `-b` | Run worker in background |
| `--no-docker` | Run Claude directly instead of in Docker |
| `--max-turns N` | Override max_turns setting for this run |
| `--model MODEL` | Override model for this task |
| `--provider PROVIDER` | Override provider for this task |
| `--force` | Skip dependency precondition checks when running the fix task |

When a fix run completes with `--review`, gza performs the same narrow PR check before the follow-up review: if GitHub can confirm that the branch already has an open PR, gza pushes any new same-branch commits first so the review sees the published code. If GitHub is unavailable, lookup fails, or no live PR exists, fix preserves the normal auto-review flow.

### review

Create and run a review task for an implementation. Runs immediately by default.

```bash
gza review <task_id> [options]
```

| Option | Description |
|--------|-------------|
| `task_id` | Full prefixed task ID (implement, improve, review, or fix — auto-resolves to root implementation; e.g. `gza-1234`) |
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

`gza next` now renders three distinct sections:

- Recovery lane: visible failed-task recovery and manual-attention lineages that `uv run gza advance` / `uv run gza watch` act on ahead of ordinary pending pickup.
- Lifecycle actions: actionable review/rebase/merge/materialization work that `uv run gza advance` / `uv run gza watch` would run ahead of pending pickup.
- Pending lane: pending tasks that `uv run gza work` / `uv run gza watch` can start, with blocked dependencies separated as before.

Use `--tag TAG` (repeatable) to scope all three sections to matching tags. Repeated tags use
any-tag matching by default; add `--all-tags` to require every requested tag.

### queue

Inspect and manage pending queue ordering.

```bash
gza queue
gza queue bump <task_id>
gza queue unbump <task_id>
gza queue move <task_id> <position>
gza queue next <task_id>
gza queue clear <task_id>
```

| Option | Description |
|--------|-------------|
| `task_id` | Full prefixed task ID to reorder (for example `gza-1234`) |
| `position` | 1-based explicit queue position for `queue move` |
| `--tag TAG` | Only list recovery, lifecycle, and pending lanes matching tag filters (repeatable; the pending lane uses the same scoped pickup order as `uv run gza watch --tag TAG`; if a matching lineage is blocked by an out-of-scope derived child, queue reports the blocker without starting it) |
| `--all-tags` | With repeated `--tag` values, require all requested tags instead of the default any-tag matching |
| `-n, --limit N` | Show first N runnable tasks (default: 10; blocked tasks are always shown; use `0`, `-1`, or `--all` for all runnable tasks) |
| `--all` | Show all runnable tasks (blocked tasks are always shown) |

Queue pickup ordering is:
1. Explicit `queue_position` values in ascending order
2. Urgent lane, with `queue bump` moving a task to the front of that lane
3. FIFO by creation time for the remaining runnable tasks

Use `gza queue next <task_id>` to make a task the next ordered item, or `gza queue move <task_id> <position>` to assign positions like 1, 2, and 3 without having to order every task. Use `gza queue clear <task_id>` to remove explicit ordering and fall back to lane/FIFO behavior.
When `queue move`, `queue next`, or `queue clear` include `--tag` filters, explicit ordering is shared across all tasks matching that tag scope, even when some tasks have additional unrelated tags.
Those commands fail closed when the target task does not match the provided tag scope (`any` semantics by default, `all` with `--all-tags`) and do not mutate queue ordering in that case.
When no tag scope is provided, queue-position edits keep existing exact tag-set bucket behavior.
`gza queue` also renders three distinct sections:

- Recovery lane first, showing failed-task recovery or manual-attention lineages that belong to `uv run gza advance` / `uv run gza watch`.
- Lifecycle actions second, showing actionable review/rebase/merge/materialization work that belong to `uv run gza advance` / `uv run gza watch`.
- Pending lane third, showing the actual queue ordering that `uv run gza work` / `uv run gza watch` use for new pending starts.

Within the pending lane, runnable pending tasks appear first and pending tasks blocked by unsatisfied direct dependencies appear at the bottom. Internal tasks remain excluded.
By default, `gza queue` shows the first 10 runnable tasks plus all blocked tasks. Use `-n 0`, `-n -1`, or `--all` to show all runnable tasks too.
To treat a tag as a release slice, assign tasks with `uv run gza add --tag release-1.2 ...` and inspect them with `uv run gza queue --tag release-1.2`. That command is the canonical preview for what `uv run gza watch --tag release-1.2` will consider and in what order for the pending lane, with any same-scope recovery lane entries shown separately above it.
Internally, queue-style task listing is routed through the unified task query layer so queue, next, and API consumers can share the same filter/order semantics.

### implement

Create implementation from a completed plan task. When the latest completed plan source
already has an approved valid `plan_review` manifest, `gza implement <plan-id>` prefers
materializing the reviewed slices instead of creating one legacy monolithic implement task.
If the latest completed approved review exists but its effective manifest is invalid or missing,
including a malformed stored override artifact, the command exits non-zero instead of silently
creating a monolithic implement task. If no
completed approved review exists, the command keeps the single-task compatibility fallback; when
that fallback bypasses a completed non-approved review, it prints a warning. Re-running
`gza implement <plan-id>` after those reviewed slices already exist reuses the same non-dropped
slice tasks instead of recreating or dropping them, and the command output says `Reused`
rather than `Created`.

```bash
gza implement <plan_task_id> [prompt] [options]
```

| Option | Description |
|--------|-------------|
| `plan_task_id` | Full prefixed completed plan task ID to implement (e.g. `gza-1234`) |
| `prompt` | Implementation prompt (defaults to plan-derived prompt) |
| `--review` | Auto-create review task on completion |
| `--pr` | Request auto-create/reuse of a GitHub PR after successful code-task completion; evaluated at completion time and skipped without failing when PRs are unavailable |
| `--tag TAG` | Add task tag (repeatable) |
| `--same-branch` | Continue on depends_on task's branch instead of creating new |
| `--branch-type TYPE` | Set branch type hint for branch naming |
| `--review-scope TEXT` | Set the authoritative gradeable review boundary for the new implementation task |
| `--model MODEL` | Override model for this task |
| `--provider PROVIDER` | Override provider for this task |
| `--no-learnings` | Skip injecting learnings context |
| `--queue`, `-q` | Add task to queue without executing immediately |
| `--background`, `-b` | Run worker in background |
| `--no-docker` | Run Claude directly instead of in Docker |
| `--max-turns N` | Override max_turns setting for this run |
| `--force` | Skip dependency merge precondition checks when running the implement task |

When `uv run gza implement <plan-id>` is used to approve a held completed plan, it also clears that plan's hold so the completed plan no longer remains in `uv run gza incomplete`.
Using `uv run gza edit <plan-id> --no-hold-for-review` also clears the hold, but that path releases the completed plan back into the automated `plan_review` lifecycle rather than directly creating implementation work.

### plan-review

Create and optionally run a `plan_review` task for a completed plan source.

```bash
gza plan-review <task_id> [options]
```

| Option | Description |
|--------|-------------|
| `task_id` | Full prefixed task ID for a completed `plan` or `plan_improve` source |
| `--rerun` | Create a fresh review attempt even if a completed review already exists |
| `--edit-slices` | Open a completed approved `plan_review` by review ID in `$EDITOR`, validate the edited manifest, and persist it as a review-tied override |
| `--materialize` | Materialize implementation slices exactly once from a completed approved `plan_review` by review ID |
| `--queue`, `-q` | Add task to queue without executing immediately |
| `--background`, `-b` | Run worker in background |
| `--no-docker` | Run Claude directly instead of in Docker |
| `--max-turns N` | Override max_turns setting for this run |
| `--model MODEL` | Override model for this task |
| `--provider PROVIDER` | Override provider for this task |
| `--force` | Skip dependency merge precondition checks when running the plan review task |

`--edit-slices` and `--materialize` operate on a completed `plan_review` task ID rather than a plan source ID. `--edit-slices` persists a validated override artifact tied to that review, and `--materialize` reuses an existing non-dropped slice set for the same review/manifest instead of creating duplicates. If a stored override artifact is malformed or no longer validates, both commands exit non-zero with an invalid override-manifest error and create no implementation tasks.
The primary unattended path is still `plan -> plan_review -> materialized implement slices`; this command is the direct manual surface for reruns, slice overrides, and explicit materialization.

### plan-improve

Create and optionally run a `plan_improve` task from a completed `plan_review`
whose verdict is `CHANGES_REQUESTED`.

```bash
gza plan-improve <task_id> [options]
```

| Option | Description |
|--------|-------------|
| `task_id` | Full prefixed completed `CHANGES_REQUESTED` `plan_review` task ID to revise |
| `--queue`, `-q` | Add task to queue without executing immediately |
| `--background`, `-b` | Run worker in background |
| `--no-docker` | Run Claude directly instead of in Docker |
| `--max-turns N` | Override max_turns setting for this run |
| `--model MODEL` | Override model for this task |
| `--provider PROVIDER` | Override provider for this task |
| `--force` | Skip dependency merge precondition checks when running the plan improve task |

`gza plan-improve` rejects pending, in-progress, approved, or discussion-only plan
reviews and creates no revised-plan task in those states.

### extract

Create a new implementation task from a selected subset of file changes on a source task branch or explicit branch.

```bash
gza extract [PATH ...] [options]
gza extract SOURCE [PATH ...] [options]
gza extract --branch BRANCH [PATH ...] [options]
gza extract --commit REV [--commit REV ...] [PATH ...] [options]
gza extract SOURCE --files-from FILE [options]
```

| Option | Description |
|--------|-------------|
| `SOURCE` | Full prefixed completed/failed code task ID to extract from; omit to use the current branch |
| `PATH ...` | Repo-relative selected files to extract; omit to extract all changed files from the source diff |
| `--branch BRANCH` | Source branch to extract from (alternative to `SOURCE`; defaults to current branch when omitted) |
| `--commit REV` | Committed git revision to extract from (repeatable; applied in the order provided) |
| `--per-commit` | With `--commit`: create one extracted task per selected commit, preserving the provided commit order for task creation; with `--background`, workers still start in parallel |
| `--files-from FILE` | Read newline-delimited selected files from file |
| `--prompt TEXT` | Additional operator intent appended to the drafted prompt |
| `--review` | Auto-create review task on completion |
| `--pr` | Request auto-create/reuse of a GitHub PR after successful code-task completion; evaluated at completion time and skipped without failing when PRs are unavailable |
| `--tag TAG` | Add task tag (repeatable) |
| `--branch-type TYPE` | Set branch type hint for branch naming |
| `--base-branch BRANCH` | Override base branch used for source diff and new task branch creation |
| `--model MODEL` | Override model for this task |
| `--provider PROVIDER` | Override provider for this task |
| `--no-learnings` | Skip injecting learnings context |
| `--queue`, `-q` | Add task to queue without executing immediately |
| `--background`, `-b` | Run worker in background |
| `--no-docker` | Run Claude directly instead of in Docker |
| `--max-turns N` | Override max_turns setting for this run |
| `--force` | Skip dependency merge precondition checks when running the extracted implement task |

`gza extract` accepts exactly one source selector: `SOURCE`, `--branch BRANCH`, or one or more `--commit REV` flags. When multiple commits are provided, extraction applies them in the exact order given on the command line. Without `--per-commit`, that ordered commit set becomes one extraction bundle and one implement task. With `--per-commit`, `gza extract` creates one extracted implement task per selected commit, still preserving the provided order for task creation and extraction manifests. If `--background` is also set, those extracted tasks are then handed to the generic background worker spawner, so execution starts in parallel rather than as a serialized commit-by-commit run.

The drafted extract prompt leads with the best available description of the work itself. Task-based extraction prefers specific source-task prompt content after filtering generated extraction scaffolding and provenance boilerplate, branch-based extraction falls back to selected diff/file context, and commit-based extraction uses commit subjects when they provide a clearer summary. Source task IDs, branch/base refs, and commit SHAs remain in the prompt as secondary provenance context.

At run time, branch/task-based extracted tasks re-derive their selected patch from the current `source_base_ref...source_branch` diff before seeding the worktree. Commit-based extracted tasks re-derive their patch from the stored committed revisions in manifest order. If that refreshed diff is empty, or if the refreshed selected-path patch is already present on the current base and only later selected-path edits remain, the task completes successfully without invoking the agent.

### advance

Intelligently progress unmerged tasks through their lifecycle. Handles review creation, improve tasks, merging, and shared automatic failed-task recovery (resume/retry).

```bash
uv run gza advance [task_id] [options]
```

| Option | Description |
|--------|-------------|
| `task_id` | Specific full prefixed task ID to advance (e.g. `gza-1234`; omit to advance all eligible) |
| `--dry-run` | Preview actions without executing them |
| `--max N` | Limit the number of tasks to advance |
| `--no-docker` | Run workers directly instead of in Docker |
| `--force` | Skip dependency merge precondition checks when advance starts workers |
| `--unimplemented` | List completed plan/explore source rows that still need an implementation path |
| `--create` | With `--unimplemented`: queue implement tasks for the listed source rows |
| `--auto`, `-y` | Skip confirmation and execute immediately |
| `--batch B` | Stop after spawning B background workers |
| `--no-resume-failed` | Skip automatic failed-task recovery decisions (resume/retry/manual-review) |
| `--max-resume-attempts N` | Override `max_resume_attempts`: `0` disables automatic failed-task recovery; any positive value enables the fixed bounded shared resume/retry policy |
| `--max-review-cycles N` | Override max_review_cycles config value |
| `--new` | Start new pending tasks to fill remaining `--batch` slots (requires `--batch`) |
| `--type TYPE` | Only advance tasks of this type (`plan` or `implement`) |
| `--squash-threshold N` | Squash-merge branches with N or more commits (0 disables) |

`uv run gza advance` is the explicit non-looping lifecycle command. It evaluates recovery and existing lineage lifecycle work first. It does not start fresh pending work unless you opt into `--new`, and when `--new` is present the pending lane only fills whatever `--batch` capacity remains after recovery/lifecycle actions.

`--unimplemented` stays restricted to `plan` and `explore` lineages and only lists completed
source rows that still need an implementation path. Completed `explore` roots with an active
pending or in-progress `plan`/`implement` descendant are intentionally suppressed here; find that
queued follow-up work through `uv run gza next`, `uv run gza next --all`, or other queue surfaces.
It never shows `implement` tasks directly.
Only completed plan rows are directly runnable with `uv run gza implement <id>`; use `uv run gza advance --unimplemented --create` to queue implement tasks
for listed explore rows.

When the shared advance/recovery engine decides a task must be skipped for human intervention, `uv run gza advance` prints a dedicated `Needs attention` section. Each entry includes the task id, task type, short prompt, a stable `reason=...` policy slug, and the underlying skip text. This section is shown in the normal pre-confirmation preview and in `--dry-run` output, including when there is otherwise no actionable work to advance.
Held completed plans use `next_action = awaiting_human` with `reason=awaiting-human-review`, plus guidance to review the plan and then either run `uv run gza implement <plan-id>` for a one-off approval or `uv run gza edit <plan-id> --no-hold-for-review` to restore the normal automatic follow-up path (`--auto-implement` remains a compatibility alias).

### iterate

Run an automated implementation lifecycle loop (review/improve/resume/rebase).

```bash
uv run gza iterate <impl_task_id> [options]
```

| Option | Description |
|--------|-------------|
| `impl_task_id` | Full prefixed implementation task ID to iterate (e.g. `gza-1234`) |
| `--max-iterations N` | Maximum iterate iterations (1 iteration = code-change task [implement/improve] + review; default: `iterate_max_iterations` or `3`) |
| `--dry-run` | Preview what would happen without executing |
| `--no-docker` | Run Claude directly instead of in Docker |
| `--force` | Skip dependency merge precondition checks when iterate starts workers |

If `impl_task_id` names a failed implementation whose recovery-only lineage already ends in a completed retry/resume descendant, iterate plans from that completed descendant instead of surfacing a stale `recovery child already completed` skip. If that descendant is already merged or has no remaining commits to land (`empty` merge state) or only has commits already present on target (`redundant` merge state), iterate reports that no remaining lifecycle action is needed. Directly iterating an implementation whose current-target merge state is already terminal also exits early with the same no-op outcome instead of resurfacing historical failed review/improve/rebase side-quests.

When a human runs `uv run gza iterate` against a failed implementation or failed improve chain that has already hit the automatic max-resume cap, iterate now prints a warning to stderr and proceeds with the manual resume. The same manual-only warning path also applies when an older failed task is blocked by a newer failed recovery descendant: iterate warns, reroutes the resume through that newer failed descendant, and still leaves scheduler- or worker-launched `--auto-iterate` runs blocked on the shared `newer-recovery-descendant-needs-attention` stop. These warnings are emitted before either foreground execution or `--background` worker handoff returns. If iterate cannot evaluate the completed-task `--background` preflight at all, it emits a degraded-check warning to `stderr` before detaching instead of failing silently, including for `--auto-iterate`.

Before `--background` detaches, iterate now also evaluates the current lifecycle decision for completed implementations. If the decision is a true no-worker outcome (for example already merged, no remaining commits to land, plain `merge` readiness, max review cycles reached, waiting on existing work, or another shared skip/needs-attention outcome), iterate prints that decision synchronously to the caller and exits without creating a worker row. `merge_with_followups` is not treated as a no-op here: background iterate still detaches into the normal iterate worker so the shared follow-up task materialization path runs before the implementation becomes merge-ready.

If that manual resume completes successfully, operator-facing lifecycle readouts move forward from the completed resume descendant instead of leaving the capped failed row as the active unit of work. The older failed row remains in task history, but shared lineage/lifecycle surfaces treat it as recovered: owner-row listings continue from the descendant, and `uv run gza show` on the older failed row renders `Lifecycle: recovered, ...` based on the descendant's next action or terminal state.

When iterate stops with `max_cycles_reached`, it now prints review-iteration accounting with:
- task `completed` review-iteration count
- configured `max_review_cycles`
- `consumed_this_invocation` cycles

When iterate stops on a shared human-required outcome, it also prints the same `Needs attention: <task> ... reason=...` line used by `uv run gza advance`, including the same single-line shortened prompt formatting, so operators can see the exact policy boundary that stopped automation.

### watch

Continuously maintain a target number of concurrent workers.

```bash
uv run gza watch [options]
```

Press `Ctrl+C` to stop the watch loop cleanly. `gza` exits with status `130`
and leaves any in-flight workers running. Press `Ctrl+C` a second time if you
need to break out promptly from a long or blocked watch pass.

| Option | Description |
|--------|-------------|
| `--batch N` | Target concurrent workers, capped by `max_concurrent` (default: `watch.batch` or `2`; when `max_concurrent` is unset, an explicitly configured `watch.batch` also becomes the global cap, otherwise the fallback global cap remains `5`) |
| failure backoff | After each newly observed non-auto-resumable failure, `gza watch` logs an exponential cooldown using `watch.failure_backoff_initial` and `watch.failure_backoff_max`, and exits when `watch.failure_halt_after` is reached |
| `--poll SECS` | Poll interval in seconds (default: `watch.poll` or `300`) |
| `--max-idle SECS` | Exit after consecutive idle watch-loop time (default: `watch.max_idle`, no limit when unset) |
| `--max-iterations N` | Iterate loop cap for implement tasks launched by watch (default: `watch.max_iterations` or `10`) |
| `--recovery-slots N` | Slots per watch pass reserved for worker-consuming failed-task recovery before pending pickup (default: `watch.recovery_slots` or `1`) |
| `--recovery-only` | Send the full batch to failed-task recovery; pending pickup waits until recovery drains |
| `--pending-only` | Disable failed-task recovery and spend all watch slots on pending pickup |
| `--max-resume-attempts N` | Override `max_resume_attempts` for this watch run: `0` disables automatic failed-task recovery; any positive value enables the fixed bounded shared policy used by both plain watch and the recovery lane |
| `--dry-run` | Show what watch would do without executing; with `--recovery-only`, print the full failed-recovery report and exit |
| `--show-skipped` | With `--recovery-only`, include skipped failed tasks in the dry-run recovery report and live watch logs |
| `--quiet` | Write events to `.gza/watch.log` only |
| `--[no-]auto-restart-on-drift` | When installed `gza` code changes while watch is running, re-exec at the next watch-pass boundary to load the new code without waiting for running or pending work to drain (default: enabled) |
| `--tag TAG` | Only advance, resume, and start tasks matching tag filters (repeatable); use `uv run gza queue --tag TAG` to preview matching recovery candidates, lifecycle actions, and the pending pickup order. Scoped watch reports out-of-scope derived blockers but does not start them |
| `--all-tags` | With repeated `--tag` values, require all requested tags instead of the default any-tag matching |

`uv run gza watch` combines the two surfaces above: it runs recovery decisions, review/rebase/merge lifecycle work, and pending-lane pickup in one loop. Recovery and pending are still distinct sets even when watch is driving both.
Each watch pass also emits one counted `Lifecycle actions (...)` summary line before execution when actionable lifecycle work is queued for that pass, so operators can see the planned advance work without switching to `uv run gza advance --dry-run`.

When `main_checkout_isolate: true`, watch preflights a dedicated detached checkout reset to the default-branch tip and executes merge attempts there. If the isolated merge succeeds, watch then fast-forwards the real default-branch ref to that detached merge commit and syncs any attached default-branch checkout back to a clean state before marking the task merged. If the initial refresh fails because that checkout is stale or conflicted, watch rebuilds it once from scratch before giving up on merge actions for that watch pass. The integration checkout does not directly check out the shared default-branch ref, so an operator checkout already on that branch stays clean. Conflict rebases still run on task branches via standard rebase tasks.

Watch also has a separate worker-silence threshold:

```yaml
watch:
  no_activity_timeout: 60
  max_idle: null
```

`watch.no_activity_timeout` controls when watch reconciliation marks a silent registered worker for a pending or in-progress task `NO_ACTIVITY` because its task log or startup evidence has stopped receiving writes. `watch.max_idle` keeps its existing meaning: it exits the `gza watch` loop itself after consecutive idle cycles. These settings are independent.

`watch.no_progress_cycles` sets the restart-safe no-progress backstop threshold for `gza watch`. When watch selects the same unchanged worker-launch or recovery action for the same merge unit or lineage across that many cycles without durable progress, it parks the subject with `watch-no-progress-backstop` instead of respawning the no-op forever.

When tag filters are active, watch emits an explicit scope line to console and `.gza/watch.log`:
`INFO      scope: tags=<comma-separated-tags> mode=any|all`.

Manual-operator advance outcomes such as `needs_discussion`, `max_cycles_reached`, exhausted failed-task recovery, and improve-recovery stop reasons are surfaced as `ATTENTION` lines in watch output instead of one-shot deduped `SKIP` lines. Watch reuses the same formatted task line as `uv run gza advance`, including the stable `reason=...` policy slug. Inline `ATTENTION` is emitted only when an attention row is newly visible for the current watch session or when that row's message changes from the previous watch pass; unchanged inline reminders are suppressed. Each watch pass still prints a counted `Needs attention (...)` roundup for the full current visible set, so unchanged rows stay operator-visible even when no new inline `ATTENTION` line appears. Attention row identity comes from the action's declared subject task, so held plans and similar follow-up gates render against the task the operator should inspect. Guarded pending routing skips use the same centralized attention path on the first observed guarded skip, then follow the same unchanged-inline suppression while remaining present in the roundup. Successful watch-managed merges now surface as exactly one structured `MERGE <owner-task-id> -> <target>` line per landed merge unit at the moment the merge lands; watch suppresses the shared cosmetic `Merging...`, `Successfully squash merged`, and `✓ Reconciled ...` success chatter, but still prints squash-reconcile warning/failure guidance when origin reconciliation diverges. The logged task ID is the merge-unit owner/leader only. Canonical merge credit for attached members persists on the shared merge unit (`state == merged`), while the compatibility task-row `merge_status` field remains owner-scoped during the migration window. For fix-handoff reasons such as `review-max-cycles-reached`, `automatic-recovery-disabled`, `retry-limit-reached`, and `retryable-provider-error`, `uv run gza advance` and `uv run gza iterate` also print `Recommended next step: uv run gza fix <task-id>`. Lineages parked because automatic recovery hit `retry-limit-reached` or `retryable-provider-error` stay parked until a human changes the lineage state; watch does not re-select them for a fresh iterate worker in the meantime. Treat `manual-review-required` as a legacy alias rather than a current parked reason. Ordinary wait/skip states keep the existing `SKIP` dedupe behavior.

Multiline watch log messages are rendered with continuation indentation so wake, repair, and recovery output stays readable in both stdout and `.gza/watch.log`. `WAKE` lines now include a `live workers:` block when running workers can be identified, listing active task IDs and any anonymous workers that do not currently map to a live task row.

When watch detects that the installed `gza` package fingerprint has changed since startup, it logs the drift immediately and, by default, re-execs itself at the next watch-pass boundary without waiting for running or pending work to drain. Detached workers keep running, and the replacement watch process reconciles them after it auto-resumes. The re-exec is treated as a continuation of the already-approved watch session, so it skips the first-pass confirmation prompt. Pass `--no-auto-restart-on-drift` to keep the manual-restart warning instead.

If a watch-time merge attempt fails only because the task branch is already merged into the target branch, watch runs the shared branch-truth reconciliation path, marks the task merged, and logs the repair as informational reconciliation instead of surfacing a misleading merge failure.

`uv run gza watch --recovery-only --dry-run` is the recovery inspection surface for this mode. It prints the failed-task decision report for the current scope, showing actionable `resume` and `retry` decisions plus any shared `Needs attention` rows by default, then exits without entering the normal watch loop. Plain `uv run gza watch` now reserves a recovery lane by default: default `watch.recovery_slots = 1` means each watch pass allocates up to one slot to worker-consuming failed-task recovery before pending pickup, with the remaining `batch - 1` slots left for pending work. The rule is uniform for worker-consuming recovery: at batch 1, default plain watch gives the single slot to worker-consuming recovery first; use `--pending-only` or `watch.recovery_slots: 0` when you intentionally want pending-only behavior on a single slot. `--recovery-only` is the other extreme (`recovery_slots = batch`) and suppresses pending pickup until actionable recovery drains, even for direct reconcile actions that do not consume a worker slot. `max_resume_attempts` is still the shared recovery toggle (`0` disables automatic recovery, any positive value enables the same fixed bounded policy). Ordinary skipped tasks stay hidden by default; pass `--show-skipped` to include those non-attention skips with launch mode and attempt counts in both the dry-run report and live watch logs. Failed `review` / `improve` / `rebase` rows whose structured target implementation is already merged are omitted entirely from this recovery surface rather than being counted as skipped. Deprecated compatibility aliases remain accepted for now: `--restart-failed` maps to `--recovery-only`, `--restart-failed-batch` maps to `--recovery-slots`, and `watch.restart_failed_batch` maps to `watch.recovery_slots`.

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

### sync

Explicitly reconcile branch-scoped task state across local git, fetched remote default-branch git state, and GitHub PR metadata.

```bash
uv run gza sync [task_id ...] [options]
```

| Option | Description |
|--------|-------------|
| `task_id` | Full prefixed task ID(s) whose branch cohorts should be synced (e.g. `gza-1234`; omit to use the bounded default candidate set) |
| `--dry-run` | Show intended DB writes and PR cleanup without making changes |
| `--git-only` | Only reconcile merge status and diff stats; skip GitHub PR sync |
| `--pr-only` | Only reconcile PR metadata and stale-PR cleanup; skip git diff refresh |
| `--no-fetch` | Skip `git fetch origin`; stale-PR auto-close is disabled without a fresh fetch |

Use `uv run gza unmerged` for the daily "what still needs to be merged?" check. `uv run gza sync` remains the broader explicit branch and PR reconciliation command. When `pr_integration: true`, it also performs project-level `gh`-backed PR discovery/comment/create flows. Set `pr_integration: false` to disable those PR operations. It:
- dedupes work by branch, writing shared branch metadata back to every same-branch task row that carries commits while persisting merge status only on the merge-owning row
- refreshes cached `merge_status`, `diff_*` stats, `pr_number`, `pr_state`, `pr_last_synced_at`, and `sync_last_synced_at`
- discovers PRs by branch for bounded candidates that need PR reconciliation
- auto-closes stale open PRs only after posting a comment and only when a fresh `origin/<default-branch>` fetch proves the branch content is already present upstream

The only GitHub-side exceptions outside `uv run gza sync` are improve and fix completion with `--review`: before auto-running the follow-up review, gza may do a narrow branch-scoped live-PR check and push for that same branch when `pr_integration: true`. With `pr_integration: false`, those same branch-scoped PR checks are skipped. It does not replace `uv run gza sync` for broader cache refresh, merge-state reconciliation, or stale-PR cleanup.

Default `uv run gza sync` scope is intentionally bounded. It includes unresolved branches, tasks with known or unknown open-PR cache state, and recently touched PR-intended work. Pass explicit task IDs to force-sync specific branch cohorts outside that default window. That bounded scope is acceptable because `uv run gza sync` is no longer the primary daily merge-truth refresh surface.

Branch candidates selected by the default scope are cooldown-filtered by `sync_last_synced_at`. When the bounded candidate set is empty only because that cache is still warm, `uv run gza sync` prints an explicit cooldown message instead of a generic "No sync candidates". During active reconciliation it also emits concise `[sync] ...` progress lines for fetch/auth steps and per-branch cohort progress.

### tv

Live multi-task log dashboard.

```bash
gza tv [task_id ...] [options]
```

| Option | Description |
|--------|-------------|
| `task_id` | Optional task IDs to watch (default: auto-select running tasks) |
| `--number N`, `-n N` | Fixed slot count (equivalent to `--min N --max N`) |
| `--min N` | Minimum slot count in auto-select mode (default: 1) |
| `--max N` | Maximum slot count in auto-select mode (default: 4) |

Each task panel title includes the task ID, type, status, and, when known, the full stored execution model ID before the elapsed/steps/token stats cluster.

### migrate

Run pending manual database migrations. This includes v25 (INTEGER primary keys to project-prefixed base36 TEXT IDs) and v26 (base36 TEXT IDs to project-prefixed decimal IDs like `gza-1234`).

```bash
gza migrate [--status] [--dry-run] [--yes/-y] [--import-local-db]
```

| Option | Description |
|--------|-------------|
| `--status` | Show current schema version and list pending migrations without running anything |
| `--dry-run` | Preview what the migration would change without writing any data |
| `--yes`, `-y` | Skip the confirmation prompt and run migrations immediately |
| `--import-local-db` | Import legacy project-local `.gza/gza.db` rows into active shared `db_path`; if shared mode still omits `project_id`, persist the legacy derived identity once before import and record an idempotent marker |

When run without flags, `gza migrate` prompts for confirmation before applying migrations. Each migration is atomic (wrapped in BEGIN/COMMIT/ROLLBACK) and creates a pre-migration backup (for example, `<db_path>.backup.pre-v25.db` and `<db_path>.backup.pre-v26.db`). It is safe to re-run: calling it on an already-migrated database is a no-op.

On successful migration, the backup path is printed to stdout so you can locate it for rollback if needed.

Task IDs start at `{prefix}-1` for new databases (there is no `{prefix}-0`) and are variable-length decimal (`{prefix}-{n}`).

Task ID validation is format-based (`{prefix}-{decimal}`) and does not require the prefix to match your current `project_prefix`. A mismatched but valid full ID is accepted by parsing and then fails later as "not found" if it does not exist in the current project database.

If a `ManualMigrationRequired` error appears when running any other command, run `gza migrate` to upgrade the database schema.

When shared DB mode is active (explicit `db_path`) and a legacy local `.gza/gza.db` is detected, task commands stop with an explicit message until you either run `gza migrate --import-local-db` or pin the project back to local with `db_path: .gza/gza.db`.

### set-status

Override a task's status for recovery or correction.

```bash
uv run gza set-status <task_id> <status> [--reason <text>]
```

`task_id` must be a full prefixed task ID (for example `gza-1234`).

Allowed targets:

- `failed`, from any source status that `set-status` already supports
- `dropped`, from any source status that `set-status` already supports
- `pending`, only from `dropped`, to revive an abandoned task

Disallowed lifecycle transitions point operators at the canonical commands:

- `completed` is rejected as a target. To complete a task, use
  `uv run gza mark-completed <task_id>`.
- `failed -> pending` is rejected. Use `uv run gza retry <task_id>` to re-run
  with the normal worker-registry reset and failure-reason cleanup.
- `in_progress -> pending` is rejected. Use `uv run gza resume <task_id>` to
  reattach to active work, or settle the task as `failed`/`dropped` if the
  worker is gone.
- `completed -> pending` is rejected. Create a new task with `uv run gza add`
  for new work, or use `uv run gza set-status <task_id> failed --reason '...'`
  to revert a falsely completed task.
- `in_progress` is not a valid target. That state is set by a running worker,
  not by manual operator action.

`--reason` stores a failure reason for `failed` tasks. `dropped` and `pending`
accept `--reason` but ignore it and emit a warning.

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
| `fix` | Rescue stuck implementation lifecycle or repeated review regressions | Code changes on implementation branch |
| `internal` | gza-owned provider workflows (for example learnings/PR drafting) | `.gza/internal/{task_id}.md` |

**Typical workflow:**

1. `plan` - Design the approach, saved to `.gza/plans/`
2. `implement --based-on <plan_id> --review --pr` - Build per plan, auto-create review, and request PR creation/reuse at successful completion for later review comments when PRs are available
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

- Use `uv run gza resume <task_id>` to continue from where the task left off (preserves conversation context)
- Use `uv run gza retry <task_id>` to create a new retry attempt (`implement` retries fork fresh; same-branch follow-ups stay on the shared branch)
- `PREREQUISITE_UNMERGED`: the resolved completed dependency is not yet marked merged to the default branch (`main` in most repos). Merge the dependency (`uv run gza merge <dependency_task_id>`); after that, `uv run gza watch --restart-failed` can pick the task up automatically, or you can retry it manually with `uv run gza retry <task_id>`. Use `--force` only when you intentionally want to bypass this guard.

**Dependencies:**

Tasks with `depends_on` set will remain pending until their dependency completes. Use tag-scoped views such as `gza search --tag <tag>` or `gza queue --tag <tag>` to inspect related chains.

---

## Configuration Precedence

Configuration is resolved in the following order (highest to lowest priority):

1. **Command-line arguments**
2. **Environment variables** (`GZA_DB_PATH` for `db_path`, then other env-specific overrides)
3. **`gza.local.yaml` file** (if present)
4. **`gza.yaml` file**
5. **`~/.gza/config.yaml` file** (if present)
6. **Hardcoded defaults**

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
| `.gza/gza.db` | Default project-local SQLite task database |
| `~/.gza/gza.db` | Optional shared SQLite task database (set `db_path` explicitly) |
| `.gza/logs/` | Task transcripts (`*.log`) and paired operational logs (`*.ops.jsonl`) |
| `.gza/workers/` | Worker metadata and startup logs |
| `etc/Dockerfile.claude` | Generated Docker image for Claude |
| `etc/Dockerfile.codex` | Generated Docker image for Codex |
| `etc/Dockerfile.gemini` | Generated Docker image for Gemini |

> **Note:** `.gza/` and `gza.local.yaml` are machine-specific and should be gitignored.

### Home Directory

| Path | Purpose |
|------|---------|
| `~/.gza/config.yaml` | User-level Gza defaults shared across projects |
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
docker_setup_command: "uv sync"
timeout_minutes: 15
max_turns: 80
autonomous_verify_timeout_seconds: 180
review_verify_timeout_grace_seconds: 5
verify_command: ./bin/tests
inner_verify_command: ./bin/tests --quick
code_task_diff_timeout_medium_threshold: 500
code_task_diff_timeout_large_threshold: 1500
max_noop_improve_cycles: 1
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
  implement:
    timeout_minutes: 30
providers:
  codex:
    task_types:
      implement:
        timeout_minutes: 45
```

---

## Troubleshooting

### Task stuck in "in_progress"

If a worker crashed or was killed, tasks may be stuck in `in_progress` state:

```bash
# Check for running workers
uv run gza ps

# If no workers are running but task shows in_progress, the worker crashed
# Resume or retry the task:
uv run gza resume <task_id>
# or
uv run gza retry <task_id>
```

### "No pending tasks" but tasks exist

Tasks with unmet dependencies won't be picked up. Check:

```bash
uv run gza next          # Shows pending tasks and their dependencies
uv run gza search --tag <tag>  # Shows tasks with a tag slice
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

For large code tasks, prefer leaving the base timeout modest and tuning the diff-size scaling knobs instead:

```yaml
timeout_minutes: 15
code_task_diff_timeout_medium_threshold: 500
code_task_diff_timeout_medium_minutes: 30
code_task_diff_timeout_large_threshold: 1500
code_task_diff_timeout_large_minutes: 45
code_task_diff_timeout_cap_minutes: 45
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
