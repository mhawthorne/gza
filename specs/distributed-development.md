# Distributed Development — ID Generation

## Problem

Distributed development means multiple environments (laptops, cloud sandboxes) each run their own gza instance against their own `.gza/gza.db`, and we periodically sync state between them with a CRDT-style sqlite layer (cr-sqlite or similar — *not* Litestream, which is single-writer and would sidestep this entirely).

The current ID scheme guarantees collisions under multi-master sync:

- Task IDs come from a single monotonic counter in `project_sequences.next_seq`, allocated by `SqliteTaskStore._next_id` (`src/gza/db.py:2902`). It is atomic within one sqlite file, but uncoordinated across files.
- Merge unit IDs follow the same pattern (`{prefix}-mu-{seq}`).
- `task_comments.id` and `task_cycle_iterations.id` use sqlite `AUTOINCREMENT`.

Two envs both sitting at `next_seq=100` will both mint `gza-101`, `gza-102`, … On sync, each row's primary key collides. CRDT layers can merge *fields* of the same logical row; they cannot reconcile two different rows that happen to share a PK.

## Decisions

1. **Env-namespaced counter.** Each env gets a short `env_id`. New IDs embed it: `{prefix}-{env}-{seq}`. Each env's counter is independent; IDs are unique by construction; no cross-env coordination is needed at allocation time.
2. **In-place migration, dual-format parsers.** Existing `{prefix}-{seq}` IDs stay valid forever. New tasks in distributed mode use the new format. Parsers accept both.
3. **`env_id` is auto-generated, optionally overridden.** First run in distributed mode writes a random 4-char id to `.gza/env_id` (gitignored, per-env state). `gza.local.yaml` may override with a human-friendly value (`env_id: laptop`). Random default makes accidental collisions effectively impossible.
4. **Distributed mode is opt-in.** Absent config → no behavioral change; IDs remain `{prefix}-{seq}`.

## ID Formats

| Entity              | Legacy                | Distributed                  |
|---------------------|-----------------------|------------------------------|
| Task                | `gza-1234`            | `gza-a-1234`                 |
| Merge unit          | `gza-mu-12`           | `gza-a-mu-12`                |
| Slug (branch name)  | `20260526-gza-fix`    | `20260526-gza-a-fix`         |
| Task comment row PK | `id INTEGER`          | `(env_id TEXT, id INTEGER)`  |
| Cycle iteration PK  | `id INTEGER`          | `(env_id TEXT, id INTEGER)`  |

`env_id` constraints: 1–4 chars, `[a-z0-9]`, no hyphens (hyphen is the segment separator).

## Schema Changes

### `project_sequences`

Add `env_id` to the primary key so each env has its own counter row:

```sql
CREATE TABLE project_sequences (
    project_id TEXT NOT NULL,
    env_id     TEXT NOT NULL DEFAULT '',  -- '' = legacy single-env counter
    prefix     TEXT NOT NULL,
    next_seq   INTEGER NOT NULL DEFAULT 1,
    PRIMARY KEY(project_id, env_id)
);
```

Existing rows migrate as `env_id=''` — untouched, no rewrite. Legacy local-only mode continues to use the empty-string row. Distributed mode upserts against `(project_id, <local_env>)`.

### `task_comments`, `task_cycle_iterations`

Composite PK `(env_id, id)`. Each env's autoincrement is independent. Foreign-key references from `task_comments.task_id → tasks.id` are unaffected (the task ID is already globally unique under the new scheme).

`task_tags` PK is `(project_id, task_id, tag)` — already safe once `task_id` is unique.

### Migration shape

All additive — `ADD COLUMN env_id TEXT NOT NULL DEFAULT ''`, then rebuild PKs via the table-rename-and-copy idiom already used in the v25/v26 migrations. No `ManualMigrationRequired`; runs automatically on open.

## Code Changes

| File / symbol                                    | Change                                                                                   |
|--------------------------------------------------|------------------------------------------------------------------------------------------|
| `db.py:_FULL_TASK_ID_RE` (line 90)               | Extend regex to also match `{prefix}-{env}-{seq}`. Keep legacy alt.                      |
| `db.py:_MERGE_UNIT_ID_RE` (line 92)              | Same — accept optional env segment.                                                       |
| `db.py:task_id_numeric_key` (line 139)           | Return the trailing `seq`; tuple-ordering across envs uses `created_at` (already does).  |
| `db.py:_next_id` (line 2902)                     | Branch on `self._env_id`: when set, upsert against `(project_id, env_id)` and emit `{prefix}-{env}-{seq}`. |
| `db.py:resolve_task_id` (line 7632)              | Updated regex; error message mentions distributed form when env is configured.            |
| `db.py:get_by_seq` (line 3014)                   | When env configured, look up `{prefix}-{env}-{seq}`. Cross-env ordinal is meaningless.    |
| `db.py:next_task_after` (line 3025)              | Scope LIKE to current env's prefix segment in distributed mode.                           |
| `config.py`                                      | Add `env_id` field; load from `.gza/env_id` if present, allow `gza.local.yaml` override; auto-generate on first distributed-mode run. |
| `task_slug` builder                              | Insert env segment between prefix and slug body.                                          |

`task_id_numeric_key` callers (`failed_task_ordering`, `source_followup`, `query`, `task_query`, `advance_engine`, `runner`) all pair it with `created_at` as the primary sort key — no changes needed there.

## Open Questions

1. **CLI display.** Always show full `gza-a-1234` in distributed mode, or strip the env segment when it matches the local env (`gza-1234` local, `gza-b-1234` remote)? Stripping is friendlier; full is less ambiguous.
2. **Branch names.** Including `env` in slugs prevents two envs from creating the same branch name. But existing branches on remote don't have an env segment — coexistence is fine, just less consistent.
3. **cr-sqlite vs. alternative.** Doc assumes cr-sqlite-class CRDT semantics. If we pick something with different conflict rules (e.g. last-write-wins per-row), `task_comments.resolved_at` and similar fields may need attention separately. Out of scope here.
4. **Sync triggering.** When does the periodic export/import actually run? Manual `gza sync`, on every `gza work` boundary, on a timer? Not addressed by this doc — only ID generation is.

## Out of Scope

- The sync engine itself (cr-sqlite integration, schedule, transport).
- Conflict resolution for non-PK fields (e.g. two envs editing the same task's `prompt`).
- Cross-env worktree / branch coordination beyond unique naming.
