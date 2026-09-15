# `gza projects` and the project registry

Findings from a 2026-09-14 session. Written to hand off to a separate thread.

## What the command is

`gza projects` manages the `projects` table in the task DB. That table maps a
`project_id` to the checkout that owns it:

```sql
CREATE TABLE projects (
    id TEXT PRIMARY KEY,
    root_path TEXT NOT NULL DEFAULT '',
    config_path TEXT NOT NULL DEFAULT '',
    project_name TEXT NOT NULL DEFAULT '',
    project_prefix TEXT NOT NULL DEFAULT '',
    db_layout_version INTEGER NOT NULL DEFAULT 36,
    created_at TEXT NOT NULL DEFAULT '',
    last_seen_at TEXT NOT NULL DEFAULT ''
);
```

Three subcommands:

- `register` — point an id at a canonical root/config path
- `diagnose` — report invalid, missing, mismatched, and duplicate rows (read-only)
- `deactivate` — blank a row's canonical paths without deleting its tasks

Every row in `tasks`, `merge_units`, and `merge_unit_tasks` carries a
`project_id`; those tables are keyed `(project_id, id)`, not `id` alone.

## Why it exists

It came out of **gza-8889**, a plan filed manually on 2026-08-21:
*"Plan a multi-project `gza watch`: one watch process supervising several
projects at once, replacing the current one-watch-per-project arrangement."*

The stated motivation, from the plan prompt:

> Running a separate watch per project means hand-balancing batch sizes and poll
> intervals across processes that all ultimately draw on the same LLM provider
> capacity. The operator wants to size concurrency ONCE against their appetite
> for autonomous work, then let one process spread that budget across projects.

The immediate case was this repo — core `gza` at the root and `gzaserver` in
`server/`, after server's `db_path` was pinned to `../.gza/gza.db` so both share
one DB at `~/work/supreme/gza/.gza/gza.db`.

Key design point from the plan: the unit of watch selection is a
**(project, tag-filter) pair**, explicitly because a version tag like `v0.6`
means different things in different projects, so a single global `--tag` is
wrong. Default selection strategy is round-robin across projects, layered on top
of each project's own local queue order, designed against starvation.

So the registry's job is to answer *"which checkout does this `project_id` point
at"* when one watch process and one DB span several projects. Outside that
context the command is nearly invisible, which is why it went unnoticed.

`gza projects` landed via slice **gza-8911** on 2026-08-21, tagged
`cli, multiwatch, registry, system, v0.5.1`.

## Current registry state (2026-09-14)

`uv run gza projects diagnose` against `~/work/supreme/gza/.gza/gza.db` — 4 of 5
rows flagged:

| id | finding | tasks | detail |
|---|---|---|---|
| `alpha` | `missing_root` | 0 | root is `/private/var/folders/.../pytest-of-m3h/pytest-6124/popen-gw0/test_watch_project_runtime_rej1/alpha` — a pytest tmpdir that no longer exists |
| `default` | `empty_root_path` | 0 | empty `root_path` and `config_path`, but `last_seen_at` 2026-09-09 |
| `gzarepo01` | `project_id_mismatch` | 0 | root `~/work/supreme/gza`; its config loads `project_id: gza` |
| `gza` | `ok` | 10,667 | but "duplicate registry path pair also appears on another row" (that row is `gzarepo01`) |
| `gzaserver` | `linked_worktree` | 110 | root is `~/work/supreme/worktrees/gza-agent-sessions/server`, not a canonical checkout |

Task counts:

```
project_id  tasks   first        last
gza         10667   2026-01-12   2026-09-14
gzaserver     110   2026-08-17   2026-08-20
```

`alpha`, `default`, and `gzarepo01` own **zero** tasks. They are pure registry
cruft; nothing is at risk in removing them.

## Three open problems

### 1. `gzaserver` points into a temporary worktree

Its registered root is `worktrees/gza-agent-sessions/server`. Its 110 tasks are
real (2026-08-17 to 08-20). If that worktree is removed the registry row points
at nothing.

The canonical checkout is `~/work/supreme/gza/server` (confirmed to exist). This
wants a `gza projects register` against that path.

### 2. `default` is a silent identity fallback

`default` has zero tasks but a `last_seen_at` of 2026-09-09 — something resolves
to it and stamps the registry without ever filing work there.

Five sites collapse an unresolved project id to the literal string `"default"`:

- `src/gza/db.py:1819` — `project_id = str(row["project_id"]) if ... else "default"`
- `src/gza/db.py:6002`
- `src/gza/db.py:6028` — `self._project_id = project_id or "default"`
- `src/gza/config.py:2178`
- `src/gza/config.py:3566`

`config.py:2186` and `config.py:3572` both then special-case
`resolved_db != local_db_path and project_id == "default"`, which suggests the
fallback is already known to be dangerous in the shared-DB case.

In a single-project world a pathless fallback is harmless. In the multi-project
world this registry exists to serve, an unresolved `project_id` silently
adopting a shared `default` identity is an identity-resolution silent fallback —
the failure mode where two projects' work can merge into one bucket. It should
probably raise instead of defaulting.

Nothing has landed in `default` yet. That appears to be luck rather than design.

### 3. A test leaked into the production DB

`alpha`'s root is a pytest tmpdir from `test_watch_project_runtime_rej*`
(worker `popen-gw0`, so under xdist). A test run registered a project row in the
real `~/work/supreme/gza/.gza/gza.db`.

Worth establishing whether that test could also have written **tasks**, not just
a registry row, and what isolation gap let it reach the real DB at all. Compare
the note in `src/gza/skills/gza-task-fix/SKILL.md:103` about `SqliteTaskStore.from_config`
silently creating a fresh DB rather than erroring — the same class of problem in
the other direction.

## Is multi-project watch actually live?

Both `gza-8889` (the plan) and `gza-8911` (the slice that added the command) are
now **moot / redundant**. The plan fanned out to 22 implement slices,
`gza-8908` through `gza-8929`.

Not established in this session: whether multi-project watch actually shipped, or
whether `gza projects` is a surviving fragment of an abandoned effort. **This is
the first question to answer in the follow-up thread** — it decides whether
registry hygiene matters at all, or whether the right move is to remove the
surface.

## How this surfaced

Chasing a different question (reviews per merged unit), a query joined
`merge_units` and `merge_unit_tasks` on `id` alone. Because those tables are
keyed `(project_id, id)` and unit ids repeat across projects, merged-unit counts
inflated from the true **32** to **249** — roughly 8x. Cross-project id collision
is a live hazard in this DB, not a theoretical one. Any analysis query against
these tables must filter on `project_id`.

## Recommended next steps

1. Determine whether multi-project watch shipped (check the `gza-8908`..`gza-8929`
   slices and whether `gza watch` accepts a project set). Everything else depends
   on this.
2. Re-register `gzaserver` at `~/work/supreme/gza/server`.
3. Deactivate `alpha` and `gzarepo01` — zero tasks, no risk.
4. Decide whether `default` should raise instead of silently defaulting; likely
   its own task.
5. Investigate the test-isolation leak that created `alpha`.
