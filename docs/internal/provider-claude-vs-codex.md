# Provider divergence: Claude vs Codex

How gza handles the differences between executing Claude Code and OpenAI Codex
as the underlying coding agent.

## Architecture

A single abstract base class encapsulates the per-provider behavior; the runner
only deals with the base class.

- Base class: `Provider` in `src/gza/providers/base.py:645-718`
- Concrete subclasses: `ClaudeProvider` (`src/gza/providers/claude.py`),
  `CodexProvider` (`src/gza/providers/codex.py`), `GeminiProvider`
- Factory: `get_provider(config)` in `src/gza/providers/base.py:892-908`
  dispatches on `config.provider`
- Common result type: `RunResult` dataclass in
  `src/gza/providers/base.py:626-642`

There is no inline `if provider == "claude"` branching in the runner — all
divergence is hidden behind the `Provider` interface.

## Provider selection

- Config field: `provider` (top-level in `gza.yaml`)
  - Default: `"claude"` (`DEFAULT_PROVIDER` at `src/gza/config.py:55`)
  - Allowed: `claude`, `codex`, `gemini`
- Per-task-type override: `task_providers.*` — looked up via
  `config.get_provider_for_task(task.task_type)` at `src/gza/runner.py:1060`
- Per-invocation CLI flag: `--provider {claude|codex|gemini}` on `add`, `edit`,
  `improve`, `fix`, `implement`, `extract` in `src/gza/cli/main.py`

## Command construction

### Claude (`src/gza/providers/claude.py:678-691`)

```
timeout {N}m claude \
  -p - \
  --output-format stream-json \
  --verbose \
  --model {model} \
  --max-turns {max_steps} \
  [--resume {session_id}] \
  {config.claude.args...}
```

### Codex (`src/gza/providers/codex.py:253-609`)

Non-resume:

```
codex -c check_for_update_on_startup=false \
  exec --json \
  --dangerously-bypass-approvals-and-sandbox \
  --skip-git-repo-check \
  -C {work_dir} \
  [-m {model}] \
  [-c model_reasoning_effort={effort}] \
  -
```

Resume — note this is a *different subcommand*, not just a different flag:

```
codex -c check_for_update_on_startup=false \
  exec resume \
  --json \
  --dangerously-bypass-approvals-and-sandbox \
  {thread_id} \
  -
```

The shared base args are `CODEX_HEADLESS_EXEC_BASE_ARGS` at
`src/gza/providers/codex.py:55-62`.

## Prompt and working directory

| Aspect | Claude | Codex |
|---|---|---|
| Prompt input | `-p -` flag, prompt on stdin | Trailing `-` arg, prompt on stdin |
| Working dir | Subprocess `cwd=work_dir` (`claude.py:865`) | `-C {work_dir}` CLI flag (`codex.py:258`) |
| Docker workspace | Mounted at `/workspace` (`base.py:333`) | Same — but the absolute `/workspace` path is passed via `-C` (`codex.py:528`) |

## Session and resume semantics

| Aspect | Claude | Codex |
|---|---|---|
| Session-ID name | `session_id` | `thread_id` |
| Emitted in | `system.init` event (`claude.py:1021-1025`) | `thread.started` event (`codex.py:732-738`), fallback in `result` |
| Resume mechanism | `--resume {session_id}` flag | `exec resume {thread_id}` subcommand |
| Interactive foreground | Supported via PTY (`supports_interactive_foreground = True`, `claude.py:471`; `os.write(master_fd, ...)` at `claude.py:1074`) | Not supported — `interactive` parameter is explicitly ignored (`codex.py:462`) |

## Output parsing

Separate renderers in `src/gza/providers/log_renderers.py:7-9`, selected by
provider name.

### Claude (`src/gza/providers/claude.py:1200+`)

- Event types: `system`, `assistant`, `user`, `result`
- Renderer: `ClaudeLogRenderer._handle()` at `log_renderers.py:129`
- Step counting: unique message IDs (`log_rendering`:256-262)
- Token fields: `input_tokens`, `output_tokens`, `cache_creation_input_tokens`,
  `cache_read_input_tokens`

### Codex (`src/gza/providers/codex.py:717-900+`)

- Top-level event types handled via the shared registry include `error`,
  `thread.started`, `turn.started`, `turn.failed`, `turn.completed`,
  `item.started`, `item.updated`, and `item.completed`
- Item types inside `item.completed`: `agent_message`, `collab_tool_call`,
  `command_execution`, `file_change`, `mcp_tool_call`, `reasoning`,
  `todo_list`, `web_search`
- Replay/live handler membership for both top-level events and nested
  `item.completed` item types is derived from the shared Codex registries in
  `codex.py`, with drift tests covering registry-to-dispatch alignment
- Renderer: `CodexLogRenderer._handle()` at `log_renderers.py:120`
- Step counting: per `agent_message` item (`codex.py:853-854`)
- Token fields: `input_tokens`, `output_tokens`, `cached_input_tokens`

Both normalize into the shared `RunResult` (exit code, duration, step/turn
counts, token counts, cost, error type, session ID).

## Approvals and sandboxing

- Claude: no explicit approval-bypass flag; permission to run tools is implicit
  (the `--allowedTools` approach is configured via `config.claude.args`)
- Codex: requires `--dangerously-bypass-approvals-and-sandbox`
  (`codex.py:60, 515, 588`) to run headless — without it, Codex would prompt
  interactively for tool execution
- Codex also disables update-check prompts that can hang headless runs:
  `-c check_for_update_on_startup=false` (`codex.py:514, 587`)

## Auth and environment

### Claude (`src/gza/providers/claude.py:452-460`)

- npm package: `@anthropic-ai/claude-code`
- CLI command: `claude`
- Config dir: `.claude` (OAuth)
- Env var: `ANTHROPIC_API_KEY`
- **Priority: OAuth > API key** (`claude.py:481-483`)
- macOS keychain sync: yes (`claude.py:399-449`,
  `config.claude.fetch_auth_token_from_keychain`)
- Login command: `claude login`

### Codex (`src/gza/providers/codex.py:278-317`)

- npm package: `@openai/codex` (pinned at `0.128.0` in `etc/Dockerfile.codex:16`)
- CLI command: `codex`
- Config dir: `.codex` (only mounted when in OAuth mode)
- Env vars: `CODEX_API_KEY` or `OPENAI_API_KEY` (`codex.py:270, 290-291`)
- **Priority: API key > OAuth** (`codex.py:265, 275`) — inverse of Claude
- macOS keychain sync: no
- Login command: `codex login`

## Docker

Two separate images:

- `etc/Dockerfile.claude` — `npm install -g @anthropic-ai/claude-code`,
  `CMD ["claude"]`, config mount `.claude`
- `etc/Dockerfile.codex` — `npm install -g @openai/codex@0.128.0`,
  `CMD ["codex"]`, config mount `.codex` (conditional on auth mode)

Shared Docker plumbing — including OAuth config-dir mount logic — lives in
`_get_config_dir_volume_args()` at `src/gza/providers/base.py:106-132`.

## Skills, hooks, MCP

No provider-specific divergence. Skills in `src/gza/skills/` are
provider-agnostic and work across all providers. Only the log renderers and
the provider classes themselves know which CLI they're talking to.

## Quick reference

| Aspect | Claude | Codex |
|---|---|---|
| CLI binary | `claude` | `codex` |
| npm package | `@anthropic-ai/claude-code` | `@openai/codex@0.128.0` |
| JSON output flag | `--output-format stream-json` | `--json` |
| Prompt-on-stdin | `-p -` | trailing `-` |
| Step budget | `--max-turns N` | (implicit) |
| Model flag | `--model` | `-m` |
| Working dir | subprocess `cwd=` | `-C` |
| Resume | `--resume {sid}` flag | `exec resume {tid}` subcommand |
| Session ID name | `session_id` | `thread_id` |
| Session ID source event | `system.init` | `thread.started` |
| Auth priority | OAuth > API key | API key > OAuth |
| Config dir | `.claude` | `.codex` |
| API key env var | `ANTHROPIC_API_KEY` | `CODEX_API_KEY` / `OPENAI_API_KEY` |
| Keychain sync | yes (macOS) | no |
| Approval bypass | implicit | `--dangerously-bypass-approvals-and-sandbox` |
| Update-check suppression | n/a | `-c check_for_update_on_startup=false` |
| Interactive foreground | yes (PTY) | no |
| Reasoning effort | n/a | `-c model_reasoning_effort=` |
