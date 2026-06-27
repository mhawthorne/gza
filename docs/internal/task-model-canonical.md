# Canonical Task Model

`gza.db` is the single canonical task model and storage API.

- Use `src/gza/db.py` (`db.Task` and database-backed operations) for all task lifecycle behavior.
- Use `src/gza/task_query.py` (`TaskQuery`, `TaskQueryPresets`, `TaskQueryService`) for task reads that list, search, filter, group, or summarize tasks.
- Use `src/gza/lineage_query.py` for owner-keyed lineage reads that power `gza incomplete`, `gza advance`, and `gza watch --restart-failed`; `TaskQueryService` remains the public orchestration layer and delegates lineage rollups there.
- Treat direct `SqliteTaskStore` read methods such as `get_pending*()`, `get_history()`, `get_in_progress()`, and `get_all()` as query-engine internals for CLI/API presentation code.
- High-level surfaces should build a declarative `TaskQuery` and route through `TaskQueryService`, even when the service internally delegates to optimized store helpers for canonical ordering.
- Owner-keyed lineage rows are the canonical read model for unresolved branch ownership boundaries. Command surfaces should display or execute from the owner row and use `lifecycle_action_task` / `recovery_action_task` / `recovery_leaf_task` only as concrete execution details.
- Do not introduce parallel task model modules (for example, a second `Task` dataclass in another module).
- Task data now enters the system through the canonical CLI/config flows backed by `gza.db`; do not reintroduce retired importer-specific entry points.
- `Task.review_scope` is task metadata on the canonical model, not a second ask/task model. It records the gradeable review boundary for sliced implementation work while the linked plan or request remains the broader context source.
- Typed task comments are a second, task-attached data channel on the same canonical model. `feedback` comments are actionable improve input; `review_scope` comments are non-actionable scope metadata that can supply the next review boundary for a non-pending implementation without rewriting the task row.

## Read vs. Write Boundary

- Writes and lifecycle mutations stay on `SqliteTaskStore`.
- Reads should compose as a sequence of filters/sorts/projections in `TaskQuery`.
- Point lookups that are immediately followed by a mutation (`store.get(task_id)` before update/delete) are still fine outside the query layer.
- New CLI/API task-list features should add a query preset before adding another custom store read path.

## Merge-State Axes

gza tracks branch merge state across four different axes. They answer different questions and are not expected to agree in every case.

1. `DB lifecycle state` is the canonical answer to lifecycle questions such as "has this work landed?", "should this owner row still appear in `gza incomplete`?", and "should `gza advance` short-circuit?" `TaskQueryService._branch_merge_state()` reads that lifecycle state from the merge unit first (`src/gza/task_query.py:919`), and same-target merge units treat `merge_unit.state == "merged"` as authoritative in `src/gza/merge_state.py:69` and `src/gza/cli/git_ops.py:2465-2471`. Do not cross-check this against strict git ancestry.
2. `Ref existence` answers only whether a local or remote branch ref still points somewhere. A merged branch can still have a live ref, especially after a squash merge. Ref cleanup is intentionally independent from lifecycle state: `_reconcile_squash_merged_branch_with_origin()` rewrites surviving refs to the squash commit instead of deleting them (`src/gza/cli/git_ops.py:392`).
3. `Content equivalence` is the live git proof for "is the work already in the target regardless of how it got there?" `Git.is_merged()` uses `merge-tree` to compare result trees, so it handles squash merges, rebases, and divergent-but-equivalent branches (`src/gza/git.py:822`). `reconcile_branch_merge_truth()` uses that check when reconciling branch truth without persisting lifecycle state (`src/gza/sync_ops.py:317`), but a lagging remote proof target must not demote an already-recorded merged state unless the source ref itself is gone.
4. `Strict ancestry` answers only whether one commit is reachable from another in the commit graph. `Git.is_ancestor()` is a thin wrapper around `git merge-base --is-ancestor` (`src/gza/git.py:676`). After a squash merge, a landed branch commonly fails this test. In gza, `resolve_post_merge_rebase_state()` uses ancestry only as proof that a stale failed-rebase blocker can be cleared because the implementation branch already contains the target tip (positive-proof branch at `src/gza/advance_engine.py:374-384`); it is not a substitute for merge lifecycle state.

The expected squash-merge shape is therefore: `merge_unit.state == "merged"`, branch ref still exists, and `is_ancestor(branch, main) == false`. That combination is normal, not a corruption signal.

An `empty` merge-unit state is the other terminal lifecycle outcome for code-bearing branches. It is neither DB lifecycle `merged` nor active `unmerged`: the branch has no remaining net commits to land against its target, so lifecycle queries should treat it as moot/complete and exclude it from `needs_merge`, while merge provenance fields such as `merged_at` stay unset.

Merge units also have an **inactive manual tombstone axis**: `dropped` and `superseded`.
These states preserve historical membership and auditability, but they are not active
owner-row work and they are not landed/no-work dependency proof. Shared active-unit reads
must treat both `state in {"dropped", "superseded"}` and `superseded_by_unit_id != NULL`
as inactive. Historical direct lookup (`get_merge_unit(unit_id)`) still returns those rows.

That yields three distinct merge-unit buckets:

- Actionable active work: `unmerged`, `blocked`, `stale`
- Active terminal landed/no-work: `merged`, `empty`, `redundant`
- Inactive historical tombstones: `dropped`, `superseded`, plus any row hidden by `superseded_by_unit_id`

An `empty` prerequisite also has a distinct dependency-policy answer. It **does** satisfy a downstream merge-required `depends_on` edge, because the upstream merge unit is terminal and moot. That decision must stay routed through the single shared `empty_prereq_satisfies_dependency()` policy hook in `src/gza/dependency_preconditions.py`; its default return is `True`, and any future policy flip should only require changing that one hook instead of reworking multiple lifecycle call sites.

A prerequisite with authoritative merge-unit state `merged` is stronger still: once the dependency's work is already landed on target, downstream merge-required `depends_on` edges are satisfied even if the direct task row later ends in `failed`. That is a dependency-readiness rule, not a failed-task recovery suppression rule; the same failed task must still follow the shared recovery policy for its own operator-facing recovery row decisions.

Missing dependency rows are the opposite policy case: they remain a hard blocked pending state. Pickup, claim, runner preflight, and query projections must all treat a `depends_on` edge with no backing row as not runnable rather than as an unowned compatibility fallback.

When a dependency points at a failed original task whose work was later recovered, readiness still follows the dependency lineage's canonical active merge unit, not merely the completed descendant row that satisfied the retry chain. Resolve the active merge unit from the direct dependency lineage first; only fall back to legacy task-row `merge_status` when no merge unit exists anywhere in that lineage.

Guidance for future callers:

- Use DB lifecycle state for lifecycle and owner-row decisions.
- Use `Git.is_merged()` when you need live git proof that the content landed.
- Use `Git.is_ancestor()` only for ancestry-specific questions such as clearing stale post-rebase blockers.
- If a `branch_merge_state` consumer really wants ancestry semantics, it is probably asking the wrong helper.

## Task-Mode Guidance

When task-mode code needs to answer "did this work land?" or "should this lineage still block follow-up automation?", prefer the merge unit's DB lifecycle state over branch existence or strict ancestry. A surviving branch after a squash merge is normal, and ancestry-only checks are too strict for task-mode lifecycle decisions.
