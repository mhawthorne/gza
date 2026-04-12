# Tmux Sessions

Tmux behavior is provider-specific:

- Claude background workers default to pipe mode (no tmux proxy).
- Codex and Gemini background workers can run in tmux when `tmux.enabled: true`.
- Claude interactive attach uses a dedicated tmux session that performs a kill/resume handoff.

## How It Works

By default, Claude background execution does not use `gza-tmux-proxy`. `gza attach` for Claude now:

1. Preflights tmux session creation.
2. Stops the running Claude worker.
3. Launches `gza.attach_wrapper` in a fresh tmux session.
4. Runs `claude --resume <session_id>` interactively.
5. On tmux detach (`Ctrl-B D`), auto-resumes background pipe-mode execution.

## Attaching to a Task

```bash
# By task ID
gza attach 42

# By worker ID (from gza ps)
gza attach w-20260301-143025
```

When you attach:
1. Claude: current worker is stopped and replaced by an interactive resume session.
2. Codex/Gemini: attach is read-only observe mode.
3. You can type only in Claude interactive attach sessions.

For Claude:
- `Ctrl-B D` (detach) auto-resumes in background.
- Normal interactive Claude exit also auto-resumes in background.

### Attaching from inside tmux

If you're already in a tmux session, `gza attach` uses `tmux switch-client` instead of `attach-session`, so you don't need to worry about nested sessions. When the task ends and its session is destroyed, you're automatically switched back to your previous session. You can also Ctrl-B D to detach at any time, which returns you to your original session while the task continues autonomously.

This requires tmux 3.2+ for the automatic switch-back. On older versions, you'll be detached from tmux when the task session ends.

## Claude Proxy Compatibility Mode

Set `GZA_ENABLE_TMUX_PROXY=1` to force legacy Claude tmux proxy behavior.

In compatibility mode, Claude background workers run through `gza-tmux-proxy` and `detach_grace` applies to proxy auto-accept behavior.

## Provider Behavior

Tmux attach works differently depending on the AI provider:

| Provider | Attach Mode | What You Can Do |
|----------|-------------|-----------------|
| Claude | Interactive kill/resume handoff | Type messages, approve/deny tools, redirect approach |
| Codex | Observe only | Watch terminal output (read-only) |
| Gemini | Observe only | Watch terminal output (read-only) |

Codex and Gemini run in headless mode and don't accept mid-run input. Attaching to these sessions is like a richer version of `gza log -f`. The session attaches with the `-r` (read-only) flag to prevent stray keystrokes from interfering.

```
$ gza attach gza-17
Attaching to task gza-17 (provider: codex)...
Note: Codex runs in headless mode. You can observe output but cannot
interact. Use Ctrl-B D to detach.
To intervene, stop this task (gza kill gza-17) and re-run with Claude.
```

## Configuration

Configure tmux behavior in `gza.yaml`:

```yaml
tmux:
  enabled: true              # default: false
  auto_accept_timeout: 10    # seconds before auto-accept when detached
  max_idle_timeout: 300      # seconds before assuming stuck (5 min)
  detach_grace: 5            # seconds after detach before auto-accept resumes
  terminal_size: [200, 50]   # [columns, rows]
```

Tmux sessions are opt-in (`tmux.enabled: true`). For Claude, this mainly affects compatibility proxy mode (`GZA_ENABLE_TMUX_PROXY=1`) and interactive attach session sizing.

## Log Capture

In tmux mode, log capture works differently from print mode:

- **Terminal output** is captured via `tmux pipe-pane` to `.gza/logs/{task_id}.log`
- **Worker lifecycle events** (start/stop/attach/detach/resume) are logged in `.gza/logs/{task_id}.log` as JSONL entries
- **Proxy events** (when using compatibility mode) are logged to `.gza/logs/{task_id}-proxy.log`
- **`gza log`** tails the raw terminal capture, or you can `gza attach` for the live view

## Requirements

Tmux must be installed on the host:

```bash
# macOS
brew install tmux

# Verify
tmux -V
```

If tmux is not found at startup, gza prints a warning and falls back to bare subprocess execution.
