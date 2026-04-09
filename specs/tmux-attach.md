# Tmux Session Attach for Running Tasks

## Problem

Gza tasks run non-interactively (`claude -p`) in detached subprocesses. When a task goes sideways — stuck in a loop, choosing the wrong approach, or needing guidance — you can only watch via `gza log -f` and wait for it to fail. There's no way to intervene.

You can't predict ahead of time which tasks will need intervention. Requiring a `--tmux` flag at launch time defeats the purpose — by the time you know you need to attach, the task is already running.

## Proposed Solution

Always run tasks inside tmux sessions. Claude runs in **interactive mode** (not `-p`) with a **proxy layer** that auto-accepts tool calls when no human is attached. When you attach, you get the full Claude Code interactive UI and can guide the task directly.

```bash
# Task is already running autonomously...
gza ps
#  WORKER         TASK                            STATUS   DURATION
#  w-20260227-1   42: add-caching                 running  5m 23s
#  w-20260227-2   43: fix-auth-bug                running  2m 10s

# Something looks wrong in the logs...
gza log -w w-20260227-1 -f
#  [turn 8] Edit src/cache.py ...
#  [turn 9] Edit src/cache.py ...  (same file again?)

# Attach to intervene
gza attach w-20260227-1
# → tmux attach-session -t gza-42
# You're now in the live Claude Code interactive session.
# Type guidance, approve/deny tool calls, redirect the approach.
# Ctrl-B D to detach — task continues autonomously.
```

## Design Goals

1. **Always attachable**: Every task runs in tmux. No upfront decision required.
2. **Autonomous by default**: Tasks run unattended with no human interaction needed.
3. **Seamless transition**: Attaching/detaching is instant and doesn't disrupt the task.
4. **No hang risk**: A detached session never blocks waiting for input.

---

## Provider Compatibility

Tmux attach with interactive supervision only works with providers that have an interactive terminal UI. Not all do.

| Provider | Execution Mode | Interactive TUI? | Tmux Attach? |
|----------|---------------|-------------------|--------------|
| **Claude** | `claude -p -` (print mode) | Yes — full TUI when run without `-p` | **Full support** — switch to interactive mode, proxy mediates |
| **Codex** | `codex exec --json --dangerously-bypass-approvals-and-sandbox` | No — `exec` is a headless batch API | **Observe only** — can watch output but not interact |
| **Gemini** | `gemini -p <prompt> --yolo` | Limited — `--yolo` auto-approves everything | **Observe only** — nothing to approve/guide |

### Behavior by provider

**Claude (full support):**
- Runs in interactive mode inside tmux
- Proxy auto-accepts when detached, passes through when attached
- Human can type messages, approve/deny tools, redirect approach

**Codex / Gemini (observe only):**
- Still runs in tmux for consistency (makes `gza attach` always available)
- Runs in the existing headless mode (`codex exec` / `gemini -p --yolo`) inside the tmux session
- Attaching shows live terminal output (like a fancier `gza log -f`)
- But there is no interactive prompt to type into — the provider doesn't accept mid-run input
- `gza attach` prints a clear notice:

```
$ gza attach 43
Attaching to task #43 (provider: codex)...
Note: Codex runs in headless mode. You can observe output but cannot
interact. Use Ctrl-B D to detach.
To intervene, stop this task (gza kill 43) and re-run with Claude.
```

### Why still use tmux for non-interactive providers?

1. **Consistent UX** — `gza attach` always works, users don't need to remember which provider a task uses
2. **Better log tailing** — seeing the real terminal output is richer than the parsed `gza log` view
3. **Future-proofing** — if Codex or Gemini add interactive modes, tmux infrastructure is already in place
4. **Process management** — tmux gives us named sessions, clean shutdown, and session persistence for free

---

## Architecture

### The Proxy Process

Each task runs under a lightweight proxy (`gza-tmux-proxy`) that sits between the tmux PTY and Claude Code. The proxy is the key piece — it makes the session behave differently based on whether a human is attached.

```
┌─────────────────────────────────────┐
│  tmux session: gza-42               │
│                                     │
│  ┌───────────┐    ┌──────────────┐  │
│  │ gza-tmux  │◄──►│ claude       │  │
│  │ -proxy    │    │ (interactive)│  │
│  └─────┬─────┘    └──────────────┘  │
│        │                            │
│    [if attached]                    │
│        │                            │
│  ┌─────▼─────┐                      │
│  │  human    │                      │
│  │  terminal │                      │
│  └───────────┘                      │
└─────────────────────────────────────┘
```

**Detached behavior (autonomous):**
- Claude runs interactively and prompts for tool approvals
- Proxy detects no tmux clients attached
- Proxy auto-sends approval keystrokes within a short timeout
- Task progresses without human involvement

**Attached behavior (supervised):**
- Human attaches via `gza attach`
- Proxy detects a tmux client is now connected
- Proxy stops auto-accepting and passes all I/O through to the human
- Human can type messages, approve/deny tools, redirect Claude
- On detach, proxy resumes auto-accepting

### Attach Detection

The proxy periodically checks for attached clients:

```python
def has_human_attached(session_name: str) -> bool:
    """Check if any tmux client is attached to this session."""
    result = subprocess.run(
        ["tmux", "list-clients", "-t", session_name, "-F", "#{client_name}"],
        capture_output=True, text=True,
    )
    return bool(result.stdout.strip())
```

This is polled at a short interval (200ms). The cost is trivial.

### Auto-Accept Logic

When detached, the proxy must handle Claude's interactive prompts. Claude Code's interactive mode can block on:

1. **Tool approval prompts** — waiting for the user to accept/reject a tool call
2. **End-of-turn input** — waiting for the user to type a follow-up message

For tool approvals, the proxy sends an accept keystroke. For end-of-turn (Claude has finished and is waiting for more input), the proxy does nothing — Claude should not reach this state during a task because gza provides the full prompt upfront. If it does happen (e.g., Claude finishes early and waits), the proxy should send an EOF / empty response after a configurable timeout to let Claude wrap up.

**Prompt detection strategy:**

Rather than parsing Claude's terminal output (fragile), use a simpler heuristic:

- Monitor the PTY for output quiescence (no new output for N seconds)
- When quiescent and detached, send a single newline/enter keystroke
- Claude Code treats Enter on an approval prompt as "accept" (the default)
- If Claude was already working (not prompting), the keystroke is harmless — Claude Code ignores stray input during execution

A more robust approach (if the simple heuristic proves insufficient):

- Parse the terminal screen buffer via `tmux capture-pane -p -t gza-42`
- Look for known prompt patterns: `Allow?`, `(Y/n)`, the input prompt `>`
- Act accordingly based on what's detected

**Recommendation:** Start with the quiescence heuristic. Fall back to screen-buffer parsing if needed.

### Timeout / Anti-Hang Safety

The critical edge case: Claude asks a question just as the human detaches, or Claude enters end-of-turn while detached.

Safety rules:
- **Auto-accept timeout**: If detached and no output for `auto_accept_timeout` seconds (default: 10), send Enter
- **Maximum idle timeout**: If detached and no output for `max_idle_timeout` minutes (default: 5), assume the session is stuck — send Ctrl-C, then EOF, and mark the task for retry
- **Attach grace period**: When a human detaches, wait `detach_grace_seconds` (default: 5) before resuming auto-accept, in case of accidental detach/reattach

---

## Implementation

### 1. Changes to `_spawn_background_worker` (`cli.py`)

Replace the bare `subprocess.Popen` with a tmux session launch:

```python
def _spawn_background_worker(args, config, task_id=None):
    # ... existing task claiming logic ...

    session_name = f"gza-{task.id}"

    # Build the inner command that runs inside tmux
    inner_cmd = [
        sys.executable, "-m", "gza", "work", "--worker-mode",
        "--tmux-session", session_name,  # tells worker it's in tmux
    ]
    if task_id:
        inner_cmd.append(str(task_id))
    inner_cmd.extend(["--project", str(config.project_dir.absolute())])

    # Wrap in proxy
    proxy_cmd = [
        sys.executable, "-m", "gza.tmux_proxy",
        "--session", session_name,
        "--auto-accept-timeout", str(config.tmux.auto_accept_timeout),
        "--", *inner_cmd,
    ]

    # Launch in a detached tmux session
    tmux_cmd = [
        "tmux", "new-session", "-d",
        "-s", session_name,
        "-x", "200", "-y", "50",  # reasonable terminal size
        "--", *proxy_cmd,
    ]

    subprocess.run(tmux_cmd, check=True)

    # Get the PID of the proxy process from tmux
    pid = _get_tmux_session_pid(session_name)

    # ... existing worker registration ...
```

### 2. The Proxy (`src/gza/tmux_proxy.py`)

New module. Core loop:

```python
class TmuxProxy:
    def __init__(self, session_name: str, auto_accept_timeout: float = 10.0):
        self.session_name = session_name
        self.auto_accept_timeout = auto_accept_timeout
        self.last_output_time = time.monotonic()

    def run(self, cmd: list[str]):
        """Launch cmd in a PTY, mediating I/O based on attach state."""
        # Fork a PTY for the inner command (claude)
        pid, fd = pty.fork()
        if pid == 0:
            os.execvp(cmd[0], cmd)

        # Parent: proxy loop
        self._proxy_loop(pid, fd)

    def _proxy_loop(self, child_pid: int, pty_fd: int):
        while True:
            # select() on pty_fd for output, stdin for human input
            readable, _, _ = select.select([pty_fd, sys.stdin], [], [], 0.2)

            for fd in readable:
                if fd == pty_fd:
                    data = os.read(pty_fd, 4096)
                    if not data:
                        return  # child exited
                    sys.stdout.buffer.write(data)
                    sys.stdout.buffer.flush()
                    self.last_output_time = time.monotonic()
                elif fd == sys.stdin:
                    data = os.read(sys.stdin.fileno(), 4096)
                    os.write(pty_fd, data)

            # Auto-accept when detached and quiescent
            if not self._has_human() and self._is_quiescent():
                os.write(pty_fd, b"\n")
                self.last_output_time = time.monotonic()

    def _has_human(self) -> bool:
        result = subprocess.run(
            ["tmux", "list-clients", "-t", self.session_name, "-F", "#{client_name}"],
            capture_output=True, text=True,
        )
        return bool(result.stdout.strip())

    def _is_quiescent(self) -> bool:
        return (time.monotonic() - self.last_output_time) > self.auto_accept_timeout
```

This is a simplified sketch. The real implementation will need:
- Proper signal forwarding (SIGWINCH for terminal resize, SIGTERM for shutdown)
- Child process reaping
- Robust PTY handling (termios settings, raw mode)
- Logging of auto-accept events to the task log

### 3. Changes to Claude Provider (`providers/claude.py`)

When running inside a tmux session, switch from `-p` mode to interactive:

```python
def _build_cmd(self, config, resume_session_id=None, tmux_mode=False):
    cmd = ["claude"]

    if tmux_mode:
        # Interactive mode — no -p flag
        # Use --allowedTools to pre-approve safe tools
        cmd.extend(config.claude.args)  # includes --allowedTools
        cmd.extend(["--max-turns", str(config.max_steps)])
        cmd.extend(["--verbose"])
        if resume_session_id:
            cmd.extend(["--resume", resume_session_id])
        # Prompt is typed into stdin by the proxy, not piped
    else:
        # Existing -p mode for non-tmux execution
        cmd.extend(["-p", "-", "--output-format", "stream-json", "--verbose"])
        # ... existing flags ...

    return cmd
```

**Important:** In interactive mode, `--output-format stream-json` is not used — output is the normal Claude Code TUI. Log capture changes: instead of parsing JSONL from stdout, the proxy writes a separate event log, or we rely on Claude Code's own session storage for post-hoc analysis.

### 4. Log Capture in Tmux Mode

Since we lose JSONL streaming in interactive mode, log capture works differently:

- **tmux pipe-pane**: `tmux pipe-pane -t gza-42 "cat >> .gza/logs/42.log"` captures raw terminal output
- **Proxy event log**: The proxy writes structured events (auto-accepts, attach/detach, timeouts) to `.gza/logs/42-proxy.log`
- **Claude session data**: Claude Code stores its own session data which can be queried post-hoc for token counts, tool calls, etc.
- **`gza log`**: In tmux mode, `gza log -f` can either tail the raw capture or just suggest `gza attach` instead

### 5. New CLI Command: `gza attach`

```python
# Providers where the human can interact (type messages, approve/deny)
INTERACTIVE_PROVIDERS = {"claude"}

# Providers that run headless — attach is observe-only
OBSERVE_ONLY_PROVIDERS = {"codex", "gemini"}

def cmd_attach(args, config):
    """Attach to a running task's tmux session."""
    worker_id = args.worker_id
    registry = WorkerRegistry(config.workers_path)
    store = SqliteTaskStore(config.db_path)

    worker = registry.get(worker_id)
    if not worker:
        # Try interpreting as task ID
        worker = registry.find_by_task_id(int(worker_id))

    if not worker or worker.status != "running":
        print(f"No running worker found for: {worker_id}")
        return 1

    session_name = f"gza-{worker.task_id}"

    # Verify tmux session exists
    result = subprocess.run(
        ["tmux", "has-session", "-t", session_name],
        capture_output=True,
    )
    if result.returncode != 0:
        print(f"No tmux session found: {session_name}")
        print("This task may have been started without tmux support.")
        return 1

    # Check provider and warn if observe-only
    task = store.get(worker.task_id)
    provider = (task.provider or config.provider or "claude").lower()

    if provider in OBSERVE_ONLY_PROVIDERS:
        print(f"Attaching to task #{worker.task_id} (provider: {provider})...")
        print(f"Note: {provider.title()} runs in headless mode. You can observe")
        print("output but cannot interact. Use Ctrl-B D to detach.")
        print(f"To intervene, stop this task (gza kill {worker.task_id}) and re-run with Claude.")
        print()
        # Attach read-only since interaction is pointless
        os.execvp("tmux", ["tmux", "attach-session", "-r", "-t", session_name])
    else:
        print(f"Attaching to task #{worker.task_id} (provider: {provider})...")
        print("You have full interactive control. Ctrl-B D to detach.")
        print()
        os.execvp("tmux", ["tmux", "attach-session", "-t", session_name])
```

Accepts task ID directly: `gza attach 42`, worker ID: `gza attach w-20260227-1`.

For observe-only providers, attaches with `-r` (read-only) since sending keystrokes to a headless process would be harmful.

### 6. Configuration (`gza.yaml`)

```yaml
tmux:
  enabled: true                    # default: true (always use tmux)
  auto_accept_timeout: 10         # seconds of quiescence before auto-accept
  max_idle_timeout: 300           # seconds before assuming stuck (5 min)
  detach_grace: 5                 # seconds after detach before auto-accept resumes
  terminal_size: [200, 50]        # columns x rows for the tmux session
```

Setting `tmux.enabled: false` falls back to the current bare-subprocess behavior for environments where tmux is unavailable (CI, Docker without tmux).

### 7. Worker Lifecycle Changes

**Start:**
1. `gza run` claims task, launches tmux session with proxy + claude
2. Worker registered with `tmux_session` field in metadata
3. Proxy starts Claude in interactive mode, feeds the prompt via stdin
4. Claude begins working, proxy auto-accepts tool calls

**Running (detached):**
- Fully autonomous
- Proxy auto-accepts on quiescence (Claude only; Codex/Gemini are already fully autonomous)
- `gza log` shows raw output or points to `gza attach`
- `gza ps` shows tmux session status and provider, so users know what `attach` will give them:

```
WORKER          TASK                      PROVIDER  ATTACH    DURATION
w-20260227-1    42: add-caching           claude    interact  5m 23s
w-20260227-2    43: fix-auth-bug          codex     observe   2m 10s
```

**Running (attached):**
- Human sees live Claude Code TUI
- Can type messages, approve/deny tools, guide the task
- Ctrl-B D to detach, task continues autonomously

**Completion:**
- Claude finishes, proxy detects child exit
- Proxy writes final status, exits
- tmux session closes automatically
- Worker marked completed in registry

**Stuck (safety net):**
- No output for `max_idle_timeout` while detached
- Proxy sends Ctrl-C, waits briefly, sends EOF
- If still stuck, proxy kills Claude and exits
- Task marked failed with `stuck_idle` reason

---

## Prompt Delivery in Interactive Mode

In `-p` mode, the full prompt is piped via stdin. In interactive mode, the prompt must be delivered differently.

**Approach:** The proxy types the prompt into Claude's stdin after startup:

```python
def deliver_prompt(self, pty_fd: int, prompt: str):
    """Wait for Claude to be ready, then deliver the prompt."""
    # Wait for Claude's initial ready prompt (e.g., the ">" input marker)
    self._wait_for_ready(pty_fd)

    # Send prompt — for long prompts, use a temp file reference
    if len(prompt) > 2000:
        prompt_file = self._write_prompt_file(prompt)
        # Tell Claude to read the prompt file
        message = f"Read and execute the instructions in {prompt_file}\n"
        os.write(pty_fd, message.encode())
    else:
        os.write(pty_fd, prompt.encode())
        os.write(pty_fd, b"\n")
```

For long prompts (which gza prompts typically are), writing to a temp file and telling Claude to read it is more reliable than trying to type thousands of characters into a PTY.

---

## Dependencies

- **tmux**: Required on the host. Check at startup:
  ```python
  def check_tmux():
      result = subprocess.run(["tmux", "-V"], capture_output=True, text=True)
      if result.returncode != 0:
          raise RuntimeError("tmux is required for gza. Install with: brew install tmux")
  ```
- No other new dependencies. The proxy uses only stdlib (`pty`, `select`, `os`, `subprocess`).

---

## Open Questions

1. **Should `gza attach` accept task ID, worker ID, or both?**

   Recommendation: Both. `gza attach 42` (task ID) is the common case. `gza attach w-20260227-1` (worker ID) for when you have it from `gza ps`.

2. **What about `gza log -f` in tmux mode?**

   With interactive mode, we lose the structured JSONL stream. Options: (a) `gza log -f` just does `gza attach` in read-only mode, (b) capture terminal output separately, (c) keep a separate structured log via proxy events. Recommendation: (a) for now, with proxy event log for post-hoc analysis.

3. **Should the prompt file approach use Claude's `/read` command or a user message?**

   Claude Code supports reading files naturally via tool calls. Sending "Read and execute the instructions in /path/to/prompt.md" as the first user message should work — Claude will use the Read tool to load it. This is simpler than trying to invoke `/read` which is an interactive slash command.

4. **How to handle `--allowedTools` in interactive mode?**

   Currently gza passes `--allowedTools Read Write Edit Glob Grep Bash` which auto-approves these tools. In interactive mode, this means Claude won't prompt for these tools at all (the proxy doesn't need to auto-accept them). It will only prompt for tools outside this list or for potentially dangerous operations. This is the right default — safe tools are auto-approved, risky operations can be caught when attached.

5. **What about Docker-mode tasks?**

   Docker tasks run Claude inside a container. Tmux could run on the host wrapping the docker command, or inside the container. Recommendation: Run tmux on the host, wrapping `docker run -it ...` (adding `-it` for interactive). The proxy mediates the docker container's PTY. This keeps tmux management simple and host-side.

6. **Should observe-only providers even use tmux?**

   Using tmux for Codex/Gemini adds consistency but also adds the tmux dependency for users who don't use Claude. Options: (a) always use tmux for all providers (consistent, simple), (b) only use tmux for Claude, fall back to bare subprocess for others. Recommendation: (a) — the overhead is negligible and `gza attach` having a consistent "always works" contract is worth more than avoiding a tmux dependency.

7. **Session naming collisions?**

   If task 42 is re-run (retry/resume), the old tmux session may still exist. Use `tmux kill-session -t gza-42` before starting a new one, or use `gza-42-{attempt}` naming. Recommendation: Kill any existing session with that name before starting.

---

## Future Enhancements

1. **Read-only attach mode**: `gza attach --readonly 42` — observe without ability to interfere. Implemented via `tmux attach -r`.

2. **Multi-pane dashboard**: `gza dashboard` opens a tmux window with multiple panes, one per running task. Quick overview of all activity.

3. **Attach notifications**: When you attach, the proxy could inject a system message to Claude: "A human operator has joined the session." This lets Claude adjust behavior (e.g., ask questions it would otherwise guess on).

4. **Policy-based auto-accept**: Instead of accepting everything when detached, apply rules: accept file edits, accept reads, reject git pushes, reject deletions of non-generated files, etc. Configured via `tmux.auto_accept_policy` in `gza.yaml`.

5. **Session recording**: Use `tmux pipe-pane` or `script` to record full terminal sessions for replay/audit. `gza replay 42` plays back the session.

6. **Integration with async-human-in-the-loop**: When Claude asks a structured `<<GZA_QUESTION>>`, the proxy could detect this pattern in the terminal output and send a notification instead of auto-accepting, giving the human a chance to attach and answer.
