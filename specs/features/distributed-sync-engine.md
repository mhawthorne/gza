# Distributed Development — Sync Engine

> Companion to [Distributed Development — ID Generation](distributed-development.md). That doc makes
> IDs collision-free **so that** this engine can merge rows from multiple environments. This doc owns
> the other half it explicitly fences out (its Out of Scope #1 and Open Question 4): how DB state
> actually moves between environments.

## Problem

In distributed development, multiple environments (laptops, cloud sandboxes) each run their own gza
instance against their own `.gza/gza.db`. There is **no central database** — state must replicate
between environments asynchronously, over cheap durable storage, with no env required to be online
when another syncs.

The ID-generation doc removes primary-key collisions (`{prefix}-{env}-{seq}`, env-namespaced
counters). What remains:

- Each env's local writes (new tasks, status transitions, comments, edits) must reach every other env.
- Reconciliation must be **conflict-free for distinct rows** and **deterministic for concurrent edits
  to the same row** — without a coordinator.
- Replication is **periodic and pull-based**: an env publishes its changes to a shared store; peers
  fetch and apply on their own cadence.

## Approach

Two layered concerns, kept separate:

1. **Merge semantics — cr-sqlite (CRDT).** Tables are CRDT-ified (`crsql_as_crr`). cr-sqlite tracks
   per-column causal metadata and exposes changes since a given database version via the
   `crsql_changes` virtual table. This is the same assumption the ID-generation doc makes, and it is
   explicitly **not Litestream** — Litestream is single-writer streaming replication and would
   sidestep multi-master merge entirely.
2. **Transport — S3 (or pluggable blob store).** cr-sqlite produces changesets; it does not move
   them. Each env serializes its changeset-since-last-export to an object and uploads it; peers list,
   download, and apply. S3 is the async, durable, no-server transport. The blob store should be an
   interface (S3 / GCS / R2 / local NFS) so the transport is swappable, mirroring the
   `project_sequences` / storage-abstraction posture elsewhere.

The user-facing shape: *write the diffs since last sync to a file, push to S3, and every env pulls
peers' files down and applies them to its local DB.*

## The sync loop

**Watermarks (local state, per env, gitignored like `.gza/env_id`):**
- `own_exported_version` — the local cr-sqlite `db_version` at the last successful export.
- `peer_applied_version[env_id]` — the highest version applied from each peer.

**Export (publish local changes):**
1. Read `crsql_changes WHERE db_version > own_exported_version`.
2. Serialize to a changeset blob.
3. Upload under an env-scoped, version-ordered key:
   `s3://<bucket>/<project_id>/<env_id>/<db_version>.changeset`.
4. Advance `own_exported_version`.

**Import (apply peers' changes):**
1. List objects under each peer prefix with version greater than `peer_applied_version[peer]`.
2. Download and apply via cr-sqlite (changesets are idempotent and order-tolerant by design).
3. Advance `peer_applied_version[peer]`.

**Bootstrap:** a new env should not have to replay all history. Publish a periodic full **snapshot**
alongside incremental changesets; a fresh env loads the latest snapshot, then tails changesets after
it. (Snapshot cadence/retention — see Open Questions.)

## Cadence / triggering

This answers Open Question 4 of the ID-generation doc. Sync cadence should **align with the same
boundaries** the git-state sync work already targets, so DB rows and git refs refresh together:

- **Import before a lineage-owner implement task creates its branch** — pairs with `gza-3366`
  (fetch + fast-forward `main`). A new branch's DB context should be as fresh as its base commit.
- **Import before merge / advance** — pairs with `gza-3764` (pre-step fetch + reconcile of the
  task's own branch).
- **Import at the existing pre-retry/resume rebase point** — gza already rebases there; piggyback an
  import so a retry sees peers' state.
- **Export after state mutations** — task completion, status transitions, comment/edit writes.
- Plus a **manual `gza sync`** and an optional **timer** for idle envs.

This preserves a principle established for review/diff construction: **review stays
remote-agnostic.** Sync makes the local DB (and local git) fresh at well-defined boundaries; review
and diff logic just consume whatever local state exists at that point and never reach across to a
remote themselves.

## Conflict resolution (non-PK fields)

The ID-generation doc punts this (its Out of Scope #2). cr-sqlite's default is per-column
last-write-wins keyed by causal version. That is correct for most fields, but some need explicit
thought because two envs can act on the same logical row:

- **`tasks.status`** — two envs advancing the same task (e.g. both run review). LWW may silently drop
  one transition. May need a status lattice (terminal states win) rather than raw LWW.
- **`tasks.prompt` / report content** — concurrent edits; LWW loses one side. Acceptable for v1?
- **`task_comments.resolved_at` and resolution state** — resolving a comment on two envs.
- **Merge-unit / lineage fields** — must not let two envs both "own" a merge.

This doc **enumerates** the at-risk fields; a detailed resolution *policy* (and whether any field
needs a custom CRDT beyond per-column LWW) can be designed separately if v1's defaults prove unsafe.

## Relationship to other work

- **[ID generation](distributed-development.md)** — prerequisite; makes rows distinct so they can be
  merged rather than collided.
- **Git-state sync** — `gza-3366` (fetch+ff `main` before branch), `gza-3764` (pre-step fetch+reconcile
  of task branch), `gza-3765` (auto-fetch-rebase-retry on non-ff push). DB sync should fire at the
  same boundaries; together they keep an env's git refs and DB rows consistent.
- **Artifact archival** — `gza-3123` (explore) archives on-disk blobs (summaries/reviews/logs/plans)
  to S3 with "the DB is the index, S3 is the blob backend." Complementary: this engine syncs the
  *index rows*; that work syncs the *blobs*. They likely share a bucket/transport — worth unifying.
- **Content cache sync** — `gza-3124`→`gza-3125` keep edited report/plan/review content in step
  between disk and the DB cache on one machine. That must run **before export** so the changeset
  ships fresh content rather than a stale cache.
- **Supersedes** the "migrate to PostgreSQL for distributed agents" hand-waving in
  `shared-task-state.md`, which predates the cr-sqlite + S3 direction. (Reconciling/retiring that
  older doc is a follow-up, not part of this spec.)

## Open Questions

1. **cr-sqlite operational constraints.** Extension loading in every env + container; required
   CRDT-ification (`crsql_as_crr`) of each synced table; constraints cr-sqlite disallows (certain
   FKs, `AUTOINCREMENT`, CHECKs). Audit the current schema against cr-sqlite's requirements.
2. **Schema migrations across envs on different gza versions.** A changeset authored under schema vN
   applied on an env at vN-1 (or vice versa). Gate on a schema-version handshake?
3. **Snapshot vs. full replay for bootstrap**, and snapshot cadence/retention in S3.
4. **Transport specifics.** Bucket layout, credentials, encryption at rest, lifecycle/retention,
   and whether artifact archival (`gza-3123`) shares the same bucket and client.
5. **Which tables sync.** Almost certainly `tasks`, `task_tags`, `task_comments`,
   `task_cycle_iterations`, `merge_units`. Probably **not** `project_sequences` (env-local by
   design) or transient run/queue state. Enumerate explicitly.
6. **Failure / partial-apply semantics.** Atomicity of applying a multi-row changeset; recovery if an
   import is interrupted mid-batch.

## Out of Scope

- **ID generation** — owned by [`distributed-development.md`](distributed-development.md).
- **Git-state sync** — owned by `gza-3366` / `gza-3764` / `gza-3765`.
- **Deep conflict-resolution policy** beyond enumerating at-risk fields here.
- **Reconciling/retiring `shared-task-state.md`** — follow-up.
