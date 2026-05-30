# Monorepo Project Boundary

## Problem

gza is increasingly used for a single project that lives inside a larger
repository (a monorepo, or a repo with unrelated sibling projects). Two distinct
things break, and they share a root cause: gza treats `project_dir` (the
directory containing `gza.yaml`, found by `discover_project_dir`,
config.py:283) as if it were the git repository root, which it is not.

### Problem 1 — the agent can commit into unrelated projects

`git worktree add` (git.py:331) always creates a worktree rooted at the git
repo's top level, so the worktree is a checkout of the **whole** repo. The
agent's working directory and the staging logic both operate at that root.
Nothing ties a task to its own project's subtree:

- The agent sees and can write anywhere in the monorepo.
- `_complete_code_task` stages whatever changed (the `files_to_stage` set,
  runner.py:3717–3721) regardless of which project the files belong to.

There is no boundary preventing a task scoped to project A from committing
changes into project B.

This same `project_dir == repo_root` assumption already produces latent bugs for
subdir configs: e.g. runner.py:4898 maps `report_path.relative_to(config.project_dir)`
into `/workspace`, which is wrong when `project_dir` is a subdirectory of the
worktree root. Making gza boundary-aware requires auditing every
`relative_to(config.project_dir)` site, not just adding a gate.

### Problem 2 — local path dependencies stop resolving at runtime

A project may declare local path dependencies (UV editable deps — e.g. `gza`
itself is a local dependency of every project that uses it). These are not
edited by the agent; they must be **resolvable at runtime** so `uv sync` and the
verify command work.

The runtime env is built by `docker_setup_command: 'uv sync'` with the worktree
mounted at `/workspace` and `.venv` on a throwaway tmpfs (base.py:334–337). At
sync time `uv` reads each local dep's source path, which must exist in the
execution environment. Because the checkout moved to `/tmp/gza-worktrees/...`
while the dependency did not move with it, paths that used to resolve no longer
do.

**Existing partial handling.** gza already addresses part of this:
`_create_local_dep_symlinks` (runner.py:3105) parses `[tool.uv.sources]` from the
project's `pyproject.toml`, and for **relative** `path` entries creates symlinks
in the worktree's ancestor directories so the relative paths resolve. Its
limits:

- It runs **only in native mode** — both call sites are guarded
  `if not config.use_docker` (runner.py:4565, 4916). `use_docker` defaults to
  **True** (config_schema.py:121), so for the default Docker runtime this does
  nothing, and a symlink to a host path outside `/workspace` would not resolve
  inside the container anyway.
- It parses only `[tool.uv.sources]` `path` entries — it misses other
  declaration forms and, crucially, **transitive** local deps (a local dep
  pulled in two levels down).
- It deliberately skips absolute paths (fine for native, where they already
  resolve; not fine for Docker, where they are unmounted).

So the residual gap is specifically: **Docker mode has no mechanism to make
out-of-worktree local deps available**, and the native resolver's dependency
enumeration is too narrow.

## Solution overview

Both problems are answered by knowing **the project's boundary**: which paths
belong to this project, and where its dependencies live. One
dependency-resolution pass feeds two consumers:

1. **A write-scope gate** — at the staging chokepoint, hard-fail commits to
   paths outside the project's boundary (Problem 1).
2. **Dependency availability** — extend the existing local-dep resolution to be
   complete (uv.lock) and to cover Docker via read-only mounts (Problem 2).

Unifying rule: **out-of-scope code is available to read and run against, never
to write or commit.** The scope gate enforces it for in-repo paths; a read-only
mount enforces it for out-of-repo deps.

## Shared front-end: scope root + dependency resolution

### Scope root (computed, not configured)

Compute the project's repo-relative path once at task setup:

```python
repo_root  = git("-C", project_dir, "rev-parse", "--show-toplevel")
scope_root = project_dir.relative_to(repo_root)   # e.g. "services/foo"
```

- If `scope_root == ""`, the config sits at the repo root; there is no inner
  subtree and the write-scope gate is a no-op.
- Otherwise `scope_root` is the default write boundary.

This work must also fix the existing `project_dir == repo_root` assumption: audit
all `relative_to(config.project_dir)` sites (confirmed: runner.py:4898) and the
agent's working directory, which under Docker should be
`/workspace/<scope_root>` rather than `/workspace`.

> Naming note: this is **not** the existing `project_prefix` config key, which is
> the ≤12-char task-ID/branch slug string (config.py:608, branch_naming.py) and
> is unrelated to filesystem paths. Do not overload that name.

### Dependency resolution (replace pyproject parsing with uv.lock)

Replace the enumeration inside `_create_local_dep_symlinks` (which parses
`[tool.uv.sources]` from `pyproject.toml`) with reading the **resolved
lockfile**, `uv.lock`. The lock records every package's resolved `source`,
transitively:

```toml
source = { registry = "https://pypi.org/simple" }   # external, ignore
source = { editable = "." }                          # the project itself, ignore
source = { editable = "/abs/path" }                  # local dep — handle
source = { directory = "../gza" }                    # local dep — handle
```

Algorithm:

1. Read `uv.lock` (documented TOML) from `project_dir`.
2. Keep `[[package]]` entries whose `source` is `editable` / `directory` / `path`
   and whose path is not `.`.
3. Resolve each path relative to the lockfile location (`project_dir`) to an
   absolute host path.
4. Classify:
   - **In-repo** (under `repo_root`): already in the worktree checkout; feeds the
     write-scope gate as a scope-widening path. No mount/symlink needed.
   - **Out-of-repo** (outside `repo_root`): not in the worktree; made available
     via the existing native symlink path and a new Docker read-only mount.

This both completes the resolver (transitivity, all source forms) and provides a
single resolved set shared by the gate and the mount/symlink logic.

**Assumptions / limits (v1):**

- `uv.lock` is assumed present and current. If stale, the path set is wrong. A
  hardened follow-up replaces lock-parsing with asking uv directly
  (`uv export` / inspect after `uv sync`).
- This resolver is **uv-specific**; poetry/npm/etc. are out of scope.

## Consumer 1: write-scope gate

The boundary for a task's writes:

```
allowed = { scope_root }  ∪  { in-repo dependency paths, repo-relative }
```

In-repo local deps (e.g. `services/foo` depending on `libs/shared`) are
legitimate write targets and **widen** the scope. A write into an unrelated
sibling project is the violation.

Enforce at the existing staging chokepoint in `_complete_code_task`
(runner.py:3717–3721), the only place `files_to_stage` is computed before commit:

```python
in_scope  = {p for p in files_to_stage if is_under(p, allowed)}
out_scope = files_to_stage - in_scope
```

**Violation policy: hard-fail.** If `out_scope` is non-empty and the task is not
cross-project, fail the task with a violation report listing the out-of-scope
paths. Do not commit a partial in-scope subset (that would produce a commit that
looks complete). The work is not lost — it remains uncommitted in the worktree,
so the operator can inspect it, or flip the task to cross-project and resume.
This matches the project's "errors over silent fallbacks" stance.

### Escape hatch: cross-project tasks (a reserved tag)

A reserved tag `cross-project` exempts a task from the gate (and widens the
agent's working directory back to the repo root). Using a tag — rather than a
new `Task` column and a new `--cross-project` flag — is deliberate and follows
the existing system:

- `Task.tags` is already first-class (db.py:378, backed by the `task_tags`
  table), so the gate just checks tag membership; no new column or migration.
- **It survives the edit restrictions.** `gza edit` only permits **tag
  mutations** on non-pending tasks (execution.py:1368–1382); every other edit
  flag is rejected for a non-`pending` task. A scope-violation failure leaves the
  task in `failed` (non-pending), so the only way to flip an exemption on it
  *without* relaxing that guard is via a tag.

The failure→fix→resume loop therefore uses existing commands unchanged:

- Set at creation: `gza add --tag cross-project ...`
- Flip on a failed task: `gza edit --add-tag cross-project <task>`, then
  `gza retry <task>` (or `gza resume <task>`).

`cross-project` is a *semantic* tag read by the gate, not an execution-trigger
tag, so it does not by itself cause `gza watch` to spawn work.

### Global switch: opt the whole behavior in/out

A project-level config flag `enforce_project_scope` (bool, default **true**)
governs the **write-scope gate only**:

- `true` (default): single-project enforcement is on.
- `false`: the gate is skipped entirely — "don't worry about this at all" — for
  projects/users who don't want the strictness.

Dependency availability (Consumer 2) is orthogonal correctness and is **not**
gated by this flag; deps must resolve regardless. (Out-of-repo deps remain
read-only either way, since editing them is out of scope — see below.)

## Consumer 2: dependency availability

The resolved out-of-repo deps (from uv.lock) are made available at runtime:

- **Native mode** (existing path, extended): keep `_create_local_dep_symlinks`'s
  symlink approach, now driven by the uv.lock-resolved set instead of the
  narrow pyproject parse.
- **Docker mode** (new — the actual gap): for each out-of-repo dep, auto-inject a
  **read-only** bind mount at its resolved host path, reusing the
  `docker_volumes` machinery (base.py:364–366):

  ```
  -v <resolved_host_path>:<resolved_host_path>:ro
  ```

  Mounting at the identical host path makes Docker behave like native — `uv`
  finds the dependency at the exact path the lockfile recorded. Read-only states
  the intended semantic (available, never edited/committed) and stays consistent
  with the write-scope gate. Relative-path deps are normalized to their resolved
  absolute path before mounting.

Alternatives considered and rejected for v1: snapshot-copy the dep into the
worktree and rewrite the source (uniform across modes but adds copy cost and
source rewriting); build a wheel and install non-editably (clean isolation but a
per-dep build step and uv-lock friction). Both remain fallbacks.

## Config & CLI additions

- `enforce_project_scope` (bool, default true) — new config key
  (config.py / config_schema.py). Governs the write-scope gate.
- `cross-project` — a reserved tag read by the gate. No new `Task` column, no
  migration, no new CLI flags: set via existing `gza add --tag` and
  `gza edit --add-tag`.
- No new key for scope root (computed) or for dependency mounts (derived from
  uv.lock).

## Acceptance criteria

1. In a repo where `gza.yaml` is in a subdirectory, a single-project task that
   modifies a file outside `scope_root` (and outside any in-repo dep) **fails**
   with a report naming the out-of-scope path(s); no commit is created.
2. The same task modifying only files under `scope_root` (or a declared in-repo
   dep) succeeds and commits normally.
3. After `gza edit --add-tag cross-project <task>` on the failed task (a
   non-pending edit, allowed because it is a tag mutation), re-running via
   `gza retry`/`gza resume` succeeds and commits the previously-out-of-scope
   files.
4. With `enforce_project_scope: false`, the out-of-scope write commits without a
   gate failure.
5. With `gza.yaml` at the repo root (`scope_root == ""`), behavior is unchanged
   from today (gate is a no-op).
6. Under Docker, a project with an out-of-repo local dep declared in `uv.lock`
   completes `uv sync` / the verify command successfully (dep resolves via the
   read-only mount); the dep mount is read-only.
7. A **transitive** out-of-repo local dep (present in uv.lock but not in the top
   project's `[tool.uv.sources]`) is resolved and made available — proving the
   uv.lock upgrade over pyproject parsing.
8. Existing native-mode symlink resolution continues to work for relative-path
   deps (no regression in `_create_local_dep_symlinks` behavior).

## Test strategy

- **Unit:** uv.lock parsing/classification (registry vs editable/directory/path;
  `.` excluded; relative→absolute resolution; in-repo vs out-of-repo split);
  scope-membership check (`is_under` against `scope_root` ∪ in-repo deps);
  `scope_root` computation including the `== ""` root case.
- **Unit:** docker mount-arg construction for an out-of-repo dep (correct
  `-v host:host:ro`).
- **Functional** (`@pytest.mark.functional`, shells out): a fixture monorepo
  with a subdir `gza.yaml` and a sibling project; assert hard-fail on
  out-of-scope write, success on in-scope, success after
  `gza edit --add-tag cross-project` + retry, and no-op when
  `enforce_project_scope: false`. Use `sys.executable -m gza` rather than
  `uv run gza`.
- **Regression:** native-mode local-dep symlink behavior; root-config (`scope_root
  == ""`) parity with current behavior.

## Open decisions (remaining)

1. **Native parity for absolute out-of-repo deps** beyond what symlinks cover:
   confirm the uv.lock-driven symlink set is sufficient, or whether absolute
   deps need explicit handling in native mode too.
2. **uv.lock currency**: ship v1 assuming the lock is current, or invest now in
   the `uv export` resolution path. (Leaning: assume-current for v1.)

## Out of scope

- **Editing out-of-repo dependencies.** Writing to code outside the worktree
  breaks task isolation (no branch/review/rollback, cross-task races) and needs
  its own isolation/unit-of-work story. This spec keeps out-of-repo deps
  read-only.
- **Non-uv ecosystems** (poetry, npm, …) — separate resolvers.
- **Hardened dependency resolution** via `uv export` instead of parsing
  `uv.lock` — noted as a follow-up.
