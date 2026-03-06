# Task Context vs Agent Context

## Definitions

- **Task context**: The gza orchestration process running on the host. Handles DB writes, file setup, worktree creation, and post-task operations (learnings, status updates). Always runs on the host filesystem with access to `config.project_dir`.

- **Agent context**: The provider process (claude/codex/gemini) running inside a worktree, possibly inside Docker. Only sees files that exist in the worktree directory. Has no access to `config.project_dir` unless files are explicitly copied in.

## What lives where

| Resource | Location | Accessible in agent context? |
|---|---|---|
| `.gza/` (DB, learnings, logs) | `config.project_dir` | No — gitignored, not in worktree |
| `.claude/skills/` | Installed into worktree via `ensure_all_skills()` | Yes |
| `docs/internal/` | In git, checked out in worktree | Yes |
| `.gza/learnings.md` | `config.project_dir/.gza/` | **No** — must be copied into worktree |
| Summary files | Worktree dir created in task context; agent writes there; read back after | Yes |
| `~/.codex`, `~/.claude` | Host home dir, mounted into Docker via `config_dir` | Yes (in Docker) |

## How existing resources flow between contexts

### Skills (`ensure_all_skills`)

Skills are **installed from the gza package** into the worktree's `.claude/skills/` directory before the provider launches. This is not a copy from `.gza/` — it's an install from bundled skill definitions. See `runner.py:1403-1408`.

### Summary files

The task context creates a summary **directory** in the worktree and tells the agent (via the prompt) to write its summary there. After the agent finishes, the summary is **read back from the worktree** into the project. The flow is worktree → project, not the other way around.

### Provider config dirs (`~/.claude`, `~/.codex`)

Each provider has a `config_dir` setting (e.g., `".claude"`, `".codex"`) that controls whether its home directory is mounted into Docker. For example, Codex with OAuth sets `config_dir=".codex"`, which mounts `~/.codex` → `/home/gza/.codex` in the container. This gives the agent access to auth credentials and provider-level settings. This is the **provider's own config**, not the project's `.gza/` directory.

### Docker volume mapping

In Docker, the worktree is mounted as `/workspace`. So any file copied into the worktree in task context becomes visible at `/workspace/<path>` in the container. This means copying `.gza/learnings.md` into the worktree works for both native and Docker modes — no special Docker handling needed.

## The learnings gap

The prompt tells agents to consult `.gza/learnings.md`, but this file only exists in task context. Since `.gza/` is gitignored, it is not present in worktrees. Agents see the instruction but can't find the file.

The learnings *write* path is fine — `regenerate_learnings()` runs in task context after the agent finishes, writing to `config.project_dir/.gza/learnings.md` using output stored in the DB.

### Fix

Copy `.gza/learnings.md` (and eventually the full learnings directory) into the worktree in task context before launching the provider. The copy is read-only from the agent's perspective — learnings are only written by the `learn` task type, which writes in task context. No need to copy back.

## Rules of thumb

1. If the agent needs to **read** a host-only file, it must be copied into the worktree in task context before provider launch.
2. If the agent needs to **write** a file that persists beyond the task, the write should happen in task context (using DB-stored output), not rely on the worktree (which is ephemeral).
3. Docker adds another layer: symlinks to host paths won't work. Copying into the worktree works for both native and Docker since the worktree is mounted as `/workspace`.
4. Provider home dirs (`~/.claude`, `~/.codex`) are a separate mechanism — mounted via Docker's `-v` flag, controlled by the provider's `config_dir` setting. Don't conflate with project-level `.gza/` files.
