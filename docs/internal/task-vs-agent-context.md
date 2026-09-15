# Task Context vs Agent Context

## Definitions

- **Task context**: The gza orchestration process running on the host. Handles DB writes, file setup, worktree creation, and post-task operations (learnings, status updates). Always runs on the host filesystem with access to `config.project_dir`.

- **Agent context**: The provider process (claude/codex/gemini) running inside a worktree, possibly inside Docker. Only sees files that exist in the worktree directory. Has no access to `config.project_dir` unless files are explicitly copied in.

## What lives where

| Resource | Location | Accessible in agent context? |
|---|---|---|
| `.gza/gza.db` | Worktree-local snapshot created by host runner at the scoped project root (`<worktree>/<project-scope>/.gza/gza.db`) | Yes — point-in-time snapshot for reads |
| `.claude/skills/` | Installed into the scoped project root in the worktree via `ensure_all_skills()` | Yes |
| `docs/internal/` | In git, checked out in worktree | Yes |
| `.gza/learnings.md` | Copied from `config.project_dir/.gza/` into worktree | Yes |
| Summary files | Worktree dir created in task context; agent writes there; read back after | Yes |
| `~/.codex`, `~/.claude` | Host home dir, mounted into Docker via `config_dir` | Yes (in Docker) |
| Git remotes / network (`origin/*` refs, fetch, push) | Host (task context) only | **No** — worktree exposes only local refs, and there is no network to fetch them |

## How existing resources flow between contexts

### Skills (`ensure_all_skills`)

Skills are **installed from the gza package** into the worktree's `.claude/skills/` directory before the provider launches. This is not a copy from `.gza/` — it's an install from bundled skill definitions. The staging happens in `src/gza/runner.py` via `_stage_worktree_agent_resources()`, which calls `ensure_all_skills()`.

### Summary files

The task context creates a summary **directory** in the worktree and tells the agent (via the prompt) to write its summary there. After the agent finishes, the summary is **read back from the worktree** into the project. The flow is worktree → project, not the other way around.

Even though those summaries and other host-injected worktree resources live under `.gza/` or `.claude/`, they are treated as gza-owned state, not task output. Restore, extraction-seed, and final staging paths explicitly exclude both repo-root and scoped-project forms of those directories (for example `.gza/...` and `tarantino-ui/.gza/...`) so `git apply`-based flows cannot commit them even when `.gitignore` would be bypassed in monorepo subdir projects.

### Provider config dirs (`~/.claude`, `~/.codex`)

Each provider has a `config_dir` setting (e.g., `".claude"`, `".codex"`) that controls whether its home directory is mounted into Docker. For example, Codex with OAuth sets `config_dir=".codex"`, which mounts `~/.codex` → `/home/gza/.codex` in the container. This gives the agent access to auth credentials and provider-level settings. This is the **provider's own config**, not the project's `.gza/` directory.

### Docker volume mapping

In Docker, the worktree is mounted as `/workspace`. So any file copied into the worktree in task context becomes visible at `/workspace/<path>` in the container. This means copying `.gza/learnings.md` into the worktree works for both native and Docker modes — no special Docker handling needed.

## Task DB snapshot model

Live shared task databases may auto-migrate only from the canonical project checkout on
the configured default branch. Runtime bootstrap proves the checkout is the repository's
primary worktree, the current branch is the default branch, and `HEAD` is the local
`refs/heads/<default>` tip before opening a shared store in a mode that could migrate it.
That proof is rechecked while the schema bootstrap lock is held before migration,
bootstrap, repair, or project-registration writes. Feature worktrees, linked worktrees,
detached checkouts, and stale proofs defer shared migrations before mutating the live DB;
on already-current shared DBs they also defer project registration and report that
initialization was deferred instead of claiming a completed `init`. Explicit
`uv run gza migrate` uses the same authority gate: run it from the primary
default-branch checkout after the migration code has landed, not from a task worktree or
detached evaluation checkout. Local project DBs and isolated snapshots keep their
ordinary private migration behavior.

Before provider launch, the host runner copies the live task DB into the worktree as `.gza/gza.db` under the scoped project root using SQLite's backup API. This gives agents a consistent point-in-time view of task state.

Provider child environments set `GZA_DB_PATH` to that staged snapshot, so nested `uv run gza ...` commands open the worktree-local copy rather than the host control-plane database. In Docker, `GZA_DB_PATH` is translated to the container-visible `/workspace/.../.gza/gza.db` path and passed into the container environment.

Standard task snapshots are then `chmod 0444`. Reads succeed from inside the worktree, while writes fail with SQLite read-only I/O errors (`attempt to write a readonly database` / similar). Rebase task workspaces are the narrow exception: their task DB snapshot stays writable so `/gza-rebase` flows can refresh local merge truth without tripping SQLite on the copied `gza.db`. The writable rebase snapshot is still isolated; writes are not copied back to the live task DB. Plain default-branch `uv run gza unmerged` therefore still requires a writable snapshot; live-target views such as `uv run gza unmerged --target BRANCH` remain read-only.

Verify subprocesses use a separate disposable writable task DB copy for each verify attempt. The copy is fresh per attempt, so a rerun never inherits SQLite mutations from an earlier failed verify command, and the host control plane records only explicit verify result evidence after the subprocess exits. Cross-project verify keeps the evaluated worktree as the command cwd, but it clones the DB from the affected project's canonical owning runtime; budget evidence uses that same owner mapping. If a sibling owner or owner DB is unavailable, the project verify is unavailable rather than using the evaluated worktree's `.gza` state as a fallback. Native verify snapshots stay owner-private. Docker verify snapshots are still disposable copies, but each attempt receives its own fresh host directory under `.gza/tmp/verify-db-*` and a unique container mount target under `/gza-verify-db-snapshots/<snapshot-dir>`. The verify subprocess exports internal routing metadata, and nested provider Docker launchers that receive that explicit runtime environment add the snapshot mount, translate `GZA_DB_PATH` to the container-visible database path, and pass the snapshot directory's group as a supplemental container group. Only the fresh snapshot directory and database file are made group-writable, which lets the non-root container user create SQLite WAL/SHM sidecars and commit to the copy. Pre-existing checkout ancestors such as the worktree, `.gza`, and `.gza/tmp` keep their original ownership and permissions. Overlapping Docker verify attempts use distinct mount targets, their writes cannot collide or appear in each other's snapshots, and snapshot files plus SQLite sidecars (`-wal`, `-shm`, and `-journal`) are discarded on success, nonzero exit, timeout, and exception before control returns to the host lifecycle.

After canonical main lands a schema bump and advances the shared DB, old watch,
advance, or worker processes may still have code that supports only the previous schema.
Those stale runtimes surface `schema-runtime-skew` as unavailable control-plane evidence,
not as a code-red verify verdict. The recovery path is operational: restart or update the
long-lived runtime so it loads the landed code, then rerun the blocked command from the
canonical default-branch checkout if an explicit migration is still required.

Any host-side DB updates that happen after snapshot creation are not reflected in the worktree snapshot for that run.

## Learnings flow

The host runner copies `.gza/learnings.md` from `config.project_dir/.gza/` into the worktree before provider launch (except internal/learn orchestration tasks). Agents read that copy; canonical learnings are still regenerated by gza-owned task-context flows after task completion.

## Rules of thumb

1. If the agent needs to **read** a host-only file, it must be copied into the worktree in task context before provider launch.
2. If the agent needs to **write** a file that persists beyond the task, the write should happen in task context (using DB-stored output), not rely on the worktree (which is ephemeral).
3. Docker adds another layer: symlinks to host paths won't work. Copying into the worktree works for both native and Docker since the worktree is mounted as `/workspace`.
4. Provider home dirs (`~/.claude`, `~/.codex`) are a separate mechanism — mounted via Docker's `-v` flag, controlled by the provider's `config_dir` setting. Don't conflate with project-level `.gza/` files.
5. **Remote git is host-only.** The agent context has no network and no `origin/*` remote-tracking refs. Any operation that must reach origin — fetch, push, `--force-with-lease`, rebase-onto-origin — runs in **task context** (advance/watch/`git_ops`), never dispatched to an agent or `rebase` task. A flow that hands an agent an `origin/<branch>` target silently fails: the ref isn't present and the agent cannot fetch it, so the rebase no-ops and surfaces as a git error. Reconcile against origin host-side; only hand the agent a content rebase against a **local** target.
