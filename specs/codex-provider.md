# OpenAI Codex Provider

Add support for OpenAI's Codex CLI as an alternative coding agent provider.

## Overview

Codex is OpenAI's CLI coding agent (`@openai/codex`), similar to Claude Code. It supports JSONL output via `--json` flag, making integration straightforward given the existing provider abstraction.

## CLI Comparison

| Aspect | Claude | Codex |
|--------|--------|-------|
| Package | `@anthropic-ai/claude-code` | `@openai/codex` |
| CLI | `claude -p - --output-format stream-json` | `codex exec --json -` |
| Event types | `assistant`, `result` | `turn.started`, `item.completed`, `turn.completed` |
| Tool calls | `type: "tool_use"` in message content | `item.type: "command_execution"` |
| Usage stats | In `message.usage` and `result` | In `turn.completed.usage` |
| Resume | `--resume <session_id>` | `codex exec resume` subcommand |
| Sandbox | N/A (uses allowed tools) | `--sandbox workspace-write` |
| Prompt input | stdin with `-p -` | stdin with `-` argument |
| Max turns | `--max-turns N` | Not available - must track manually |
| Credentials | `ANTHROPIC_API_KEY` or OAuth | `OPENAI_API_KEY` or OAuth |

## JSONL Event Format

Codex outputs these event types:

```jsonl
{"type":"thread.started","thread_id":"..."}
{"type":"turn.started"}
{"type":"item.completed","item":{"id":"item_0","type":"reasoning","text":"..."}}
{"type":"item.completed","item":{"id":"item_1","type":"agent_message","text":"..."}}
{"type":"item.started","item":{"id":"item_2","type":"command_execution","command":"...","status":"in_progress"}}
{"type":"item.completed","item":{"id":"item_2","type":"command_execution","command":"...","aggregated_output":"...","exit_code":0,"status":"completed"}}
{"type":"turn.completed","usage":{"input_tokens":21063,"cached_input_tokens":17024,"output_tokens":246}}
```

## Implementation

### 1. Create `src/gza/providers/codex.py`

Implement the `Provider` interface:

```python
class CodexProvider(Provider):
    @property
    def name(self) -> str:
        return "Codex"

    def check_credentials(self) -> bool:
        # Check OPENAI_API_KEY or ~/.codex config
        ...

    def verify_credentials(self, config: Config) -> bool:
        # Run `codex --version` and check for auth errors
        ...

    def run(self, config, prompt, log_file, work_dir, resume_session_id=None) -> RunResult:
        # Build and execute command, parse output
        ...
```

### 2. Command Construction

```python
cmd = [
    "timeout", f"{config.timeout_minutes}m",
    "codex", "exec", "--json",
    "--sandbox", "workspace-write",
    "-C", str(work_dir),
    "-",  # Read prompt from stdin
]

# Model selection
if config.model:
    cmd.extend(["-m", config.model])
```

For resume:
```python
cmd = [
    "timeout", f"{config.timeout_minutes}m",
    "codex", "exec", "resume",
    "--json",
    "--last",  # or specific session ID
]
```

### 3. Output Parsing

Map Codex events to the existing logging format:

```python
def parse_codex_output(line: str, data: dict) -> None:
    event = json.loads(line)
    event_type = event.get("type")

    if event_type == "thread.started":
        data["thread_id"] = event.get("thread_id")

    elif event_type == "turn.started":
        data["turn_count"] = data.get("turn_count", 0) + 1
        print(f"  [turn {data['turn_count']}]")

    elif event_type == "item.completed":
        item = event.get("item", {})
        item_type = item.get("type")

        if item_type == "command_execution":
            command = item.get("command", "")
            if len(command) > 80:
                command = command[:77] + "..."
            print(f"  → Bash {command}")

        elif item_type == "agent_message":
            text = item.get("text", "").strip()
            if text:
                first_line = text.split("\n")[0][:80]
                print(f"  {first_line}")

        elif item_type == "reasoning":
            # Optional: show reasoning
            pass

    elif event_type == "turn.completed":
        usage = event.get("usage", {})
        data["input_tokens"] = data.get("input_tokens", 0) + usage.get("input_tokens", 0)
        data["output_tokens"] = data.get("output_tokens", 0) + usage.get("output_tokens", 0)
        data["cached_tokens"] = data.get("cached_tokens", 0) + usage.get("cached_input_tokens", 0)
```

### 4. Register Provider

In `src/gza/providers/base.py`:

```python
from .codex import CodexProvider

def get_provider(config: Config) -> Provider:
    providers = {
        "claude": ClaudeProvider,
        "gemini": GeminiProvider,
        "codex": CodexProvider,
    }
    ...
```

### 5. Configuration

Update `src/gza/config.py` to accept `"codex"` as a valid provider value.

## Missing Features

### Max Turns

Codex doesn't have a `--max-turns` flag. Options:

1. **Accept the limitation** - rely on timeout only
2. **Manual tracking** - count `turn.started` events and kill process when limit reached
3. **Feature request** - OpenAI may add this in future

Recommend option 1 for initial implementation, with a warning in docs.

### Cost Calculation

Codex doesn't report cost directly. Calculate from tokens using OpenAI pricing:

```python
CODEX_PRICING = {
    "gpt-5.2-codex": (2.50, 10.00),  # per million tokens
    "gpt-5.3-codex": (2.50, 10.00),
    "o3": (10.00, 40.00),
    "default": (2.50, 10.00),
}
```

### Docker Support

Similar to Claude/Gemini - create Docker config:

```python
def _get_docker_config(image_name: str) -> DockerConfig:
    return DockerConfig(
        image_name=image_name,
        npm_package="@openai/codex",
        cli_command="codex",
        config_dir=".codex",
        env_vars=["OPENAI_API_KEY"],
    )
```

## Testing

Add tests in `tests/test_providers.py`:

- `test_codex_check_credentials`
- `test_codex_verify_credentials`
- `test_codex_run_basic`
- `test_codex_output_parsing`

## Future Considerations

- MCP server support (`codex mcp-server`)
- Review mode (`codex review`)
- Cloud tasks integration (`codex cloud`)
