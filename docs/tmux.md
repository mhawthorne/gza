# Tmux Sessions

Gza runs every background task inside a tmux session. This means you can attach to any running task to observe or take interactive control, without needing to decide upfront which tasks might need intervention.

## How It Works

Each background task launches inside a tmux session with a proxy process (`gza-tmux-proxy`) sitting between the terminal and Claude Code. Claude runs in **interactive mode** (not `-p` print mode), and the proxy auto-accepts tool prompts when no human is attached.

```
tmux session: gza-{task_id}
┌───────────────────────────────────┐
│  gza-tmux-proxy  ◄──►  claude    │
│       │              (interactive)│
│   [if attached]                   │
│       ▼                           │
│  human terminal                   │
└───────────────────────────────────┘
```

**Detached (default):** The proxy monitors for output quiescence. When Claude stops producing output for `auto_accept_timeout` seconds (default: 10), the proxy sends Enter to accept the pending tool prompt. The task runs fully autonomously.

**Attached:** When you connect via `gza attach`, the proxy detects a tmux client and stops auto-accepting. You get the full Claude Code interactive TUI and can type messages, approve or deny tool calls, or redirect the approach. Ctrl-B D to detach.

## Attaching to a Task

```bash
# By task ID
gza attach 42

# By worker ID (from gza ps)
gza attach w-20260301-143025
```

When you attach:
1. Auto-accept stops immediately — you have full control
2. You see the live Claude Code interface
3. You can type messages, approve/deny tools, guide the task
4. Ctrl-B D to detach and return to autonomous mode

When you detach:
1. A grace period starts (`detach_grace` seconds, default: 5)
2. After the grace period, auto-accept resumes
3. The task continues autonomously

The grace period prevents accidental auto-accepts if you briefly disconnect and want to reattach.

### Attaching from inside tmux

If you're already in a tmux session, `gza attach` uses `tmux switch-client` instead of `attach-session`, so you don't need to worry about nested sessions. When the task ends and its session is destroyed, you're automatically switched back to your previous session. You can also Ctrl-B D to detach at any time, which returns you to your original session while the task continues autonomously.

This requires tmux 3.2+ for the automatic switch-back. On older versions, you'll be detached from tmux when the task session ends.

## Safety Timeouts

The proxy has two layers of protection against hung sessions:

**Auto-accept timeout** (`auto_accept_timeout`, default: 10s): When detached and no output for this duration, sends Enter to accept a pending prompt. This is the normal autonomous operation.

**Max idle timeout** (`max_idle_timeout`, default: 300s): When detached and no output for this duration, the proxy assumes the session is stuck. It sends Ctrl-C, then EOF, and exits. The task is marked failed with a `stuck_idle` reason.

## Provider Behavior

Tmux attach works differently depending on the AI provider:

| Provider | Attach Mode | What You Can Do |
|----------|-------------|-----------------|
| Claude | Interactive | Type messages, approve/deny tools, redirect approach |
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
  enabled: true              # default: true
  auto_accept_timeout: 10    # seconds before auto-accept when detached
  max_idle_timeout: 300      # seconds before assuming stuck (5 min)
  detach_grace: 5            # seconds after detach before auto-accept resumes
  terminal_size: [200, 50]   # [columns, rows]
```

Tmux sessions are opt-in. Set `tmux.enabled: true` in your `gza.yaml` to enable them. See the [configuration reference](configuration.md) for all options.

## Log Capture

In tmux mode, log capture works differently from print mode:

- **Terminal output** is captured via `tmux pipe-pane` to `.gza/logs/{task_id}.log`
- **Proxy events** (auto-accepts, attach/detach, timeouts) are logged to `.gza/logs/{task_id}-proxy.log` in JSONL format
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
