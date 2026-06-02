# Interactive Attach via Kill/Resume

## Problem

The current tmux attach system runs Claude in interactive mode with a proxy that auto-accepts tool calls by blindly sending Enter keystrokes. This is fragile — Claude sometimes presents multiple-choice prompts, asks clarifying questions, or enters states where Enter alone doesn't do the right thing. The proxy has no understanding of what Claude is actually asking.

Meanwhile, the `-p` (pipe) mode used for non-tmux execution is robust and deterministic — structured JSON output, no prompt guessing — but offers zero interactivity. If a task goes sideways, you can only watch it fail.

## Proposed Solution

Run tasks in `-p` mode by default for reliability. When a human wants to intervene, `gza attach` stops the running worker and resumes the same Claude session interactively inside tmux. When the human detaches, a new background worker resumes the task in `-p` mode.

This is fundamentally just two `gza resume` operations with different flags — no new state machine, no signal files, no proxy process.

```bash
# Task is running autonomously in -p mode...
gza ps
#  WORKER         TASK                            STATUS   DURATION
#  w-20260227-1   42: add-caching                 running  5m 23s

# Something looks wrong — attach interactively
gza attach 42
# Worker is stopped, Claude session resumed interactively in tmux.
# You're now in a live Claude Code session with full conversation history.
# Type guidance, approve/deny tool calls, redirect the approach.
# Ctrl-B D to detach — a new background worker resumes in -p mode.
```

### Why this is better than the proxy approach

1. **No keystroke simulation** — `-p` mode doesn't prompt, so there's nothing to auto-accept
2. **Structured output** — JSONL streaming is preserved for unattended execution, giving reliable step counting, session ID capture, and log parsing
3. **Real interactivity** — when attached, you get the actual Claude Code TUI, not a proxy pretending to be you
4. **Clean separation** — unattended = pipe mode (robust), attended = interactive mode (full-featured)
5. **Resilient by design** — killing and resuming workers is already a well-tested path (`gza kill` + `gza resume`)

## Design

### Execution modes

| Mode | Claude flags | Output | Interactivity |
|------|-------------|--------|---------------|
| **Unattended** (default) | `-p - --output-format stream-json` | Structured JSONL | None — fully autonomous |
| **Interactive** (attached) | `--resume <session-id>` | TUI | Full — human controls Claude |

### The attach flow

`gza attach 42` is effectively `gza kill 42` + `gza resume 42 --interactive`:

1. Look up worker metadata, find the running worker for task 42
2. Stop the worker (SIGTERM to the `gza work` process)
3. Look up the task's session ID from the DB
4. Launch `claude --resume <session-id>` in a tmux session and attach
5. Log lifecycle event: "Interactive session started"

### The detach flow

When the human detaches (Ctrl-B D) or Claude exits:

1. The tmux session ends (Claude is killed or has exited)
2. Log lifecycle event: "Interactive session ended"
3. If the task is not complete, spawn a new background worker: `gza resume 42` (which resumes in `-p` mode)

The "on detach, resume in background" step could be:
- **Automatic**: a wrapper script inside the tmux session handles this
- **Manual**: user runs `gza resume 42` themselves after detaching

Recommendation: automatic. The tmux session runs a small wrapper that launches Claude, and after Claude exits or the session is detached, it invokes `gza resume -b` if the task is still in progress.

### Session ID is the key

The entire approach depends on Claude's `--resume` flag reliably picking up where the previous session left off. This is already proven — `gza retry` and `gza resume` use it today. The session ID is persisted to the task record as soon as Claude emits it.

Killing Claude mid-turn is not a new risk. It already happens when:
- Max turns is reached
- Network dies
- User runs `gza kill`
- Process is OOM-killed

`--resume` handles all of these.

### Lifecycle logging

Worker lifecycle events are logged to the task's log file:

```
[2026-04-02 16:05:23] Worker w-20260402-160523 started (pipe mode)
[2026-04-02 16:10:45] Worker w-20260402-160523 stopped (interactive attach)
[2026-04-02 16:10:46] Interactive session started (session: abc123...)
[2026-04-02 16:12:30] Interactive session ended (detached)
[2026-04-02 16:12:31] Worker w-20260402-161231 started (pipe mode, resumed)
[2026-04-02 16:18:00] Worker w-20260402-161231 completed (exit 0)
```

This gives a full timeline of the task's execution across worker restarts and interactive sessions.

### Foreground attach

For foreground tasks (`gza work` without `-b`), the same mechanism works — the foreground worker is stopped, interactive session runs in tmux, and on detach a new *background* worker resumes the task. The user's original terminal is freed.

### Provider compatibility

Interactive attach only works with providers that support `--resume` and have an interactive TUI. For non-interactive providers (Codex, Gemini), `gza attach` falls back to observe-only mode (existing behavior — attach to tmux read-only to watch output).

## Configuration

No new config fields needed. The existing `tmux` config section is reused for the interactive session's terminal size. Auto-accept timeouts become irrelevant since unattended mode uses `-p`.

## What this replaces

This spec supersedes the proxy-based auto-accept approach described in `specs/tmux-attach.md` for Claude tasks. Specifically:

- **Removed:** `TmuxProxy` keystroke simulation, auto-accept logic, quiescence detection
- **Kept:** tmux session management, `gza attach` CLI command, observe-only mode for non-interactive providers
- **Kept:** `tmux pipe-pane` log capture for the interactive session

## Open questions

1. **Race condition on kill:** What if Claude is mid-write when killed? `--resume` should handle this, but worth verifying that Claude doesn't corrupt its session state on SIGTERM.

2. **Multiple attach cycles:** Can you attach/detach multiple times during a single task? Should work — each cycle is just a stop/resume. But each resume adds overhead (Claude re-reads context). Worth testing with long conversations.

3. ~~**Step counting across mode switches:**~~ Resolved — step counting from JSONL covers the `-p` portions. For interactive sessions, track attach count, wall clock duration, and note it in task stats. Exact step counts during interactive mode aren't needed — the primary purpose of max turns (preventing runaway cost) is still served since `-p` mode enforces it during unattended execution.

4. ~~**Auto-resume on detach:**~~ Resolved — detach always auto-resumes in background. Killing is a separate action (`gza kill`). Detach means "I'm done intervening, carry on."

5. ~~**`gza attach --observe` for Claude tasks:**~~ Resolved — not needed. `gza log -f` already covers observation. No reason to duplicate functionality.
