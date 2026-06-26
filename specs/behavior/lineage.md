# Lineage — the task graph and its canonical operations

> **Status: Draft — initial, pending conformance pass against the code (2026-06-03).**
> The prescriptive model of *lineage*: the graph that links a project's tasks, and the
> canonical operations every other engine queries over it — dependency satisfaction,
> owner/merge-unit resolution, latest-node resolution, and recovery-target attachment.
> This is the substrate layer. [lifecycle-engine.md](lifecycle-engine.md) (transition
> rules), [watch-supervisor.md](watch-supervisor.md) (runtime), and the planned
> recovery spec all *consume* the operations defined here; none of them re-define them.
>
> *Implementation note (non-normative): today these operations live in `lineage_query.py`,
> `query.py`, `task_query.py`, `db.py`, and `recovery_engine.py`, plus the merge-unit
> rows in the task store. The statements below are the intended behavior; the code is the
> thing measured against them, not the source of them.*
>
> Out of scope here: the git-level mechanics of computing merge-ness for a branch (see
> [merge-status-tracking.md](../features/merge-status-tracking.md) and the
> distributed-sync engine), and which *action* the engine selects given a resolved unit
> (see [lifecycle-engine.md](lifecycle-engine.md)). This doc defines what the lineage
> graph *returns when queried*, not what is done with the answer.

## Why lineage is its own contract

A "unit of work" in gza is rarely a single task. A plan spawns an implement; the
implement is reviewed, improved, rebased; a failed attempt is retried or resumed; the
result is a merge unit. Across all of that, four questions recur, and every higher engine
asks them: *Is this dependency satisfied? Who owns this lineage? What is the latest thing
that happened to it? Where does a recovery attempt attach?* Those four operations are a
shared algebra over one graph. They are specified here so they have one definition, in
domain terms, that the system could be rebuilt from.

## The graph

Nodes are **tasks**. Edges are directed and there are exactly two kinds; a third field
records provenance on recovery edges.

| Edge / field | Meaning | Walked for |
|--------------|---------|-----------|
| `based_on` | **Context & provenance.** The new task derives from an existing one: branch/context inheritance (improve, rebase, fix derived from an implement) *and* recovery chains (a retry/resume derived from the attempt it replaces). | Owner resolution, recovery-chain walks, latest-node resolution. |
| `depends_on` | **Execution ordering only.** This task MUST NOT start until its dependency is satisfied (L1). | Scheduling/gating only. |
| `recovery_origin` | Provenance tag on a `based_on` edge: `retry`, `resume`, `manual`, or none. Distinguishes a recovery attempt from an ordinary derived task. | Classifying whether a `based_on` child continues a lineage vs. starts new work. |

- **E1 — Two edges, two purposes.** `based_on` and `depends_on` are distinct and MUST NOT
  be conflated. Recovery and ownership questions MUST be answered by walking `based_on`;
  `depends_on` MUST NOT be walked to decide that a dependency was satisfied by a retry
  (L1), nor to decide ownership (L2).
- **E2 — Recovery provenance is explicit.** A retry or resume task MUST record
  `based_on = <the attempt it replaces>` and a `recovery_origin` naming its role. The
  graph MUST be walkable to find a recovery chain from provenance alone, without
  re-deriving it from payload or branch heuristics.
  *Implementation note: `recovery_origin` is canonical as of schema v41; the payload/branch
  heuristic in `recovery_engine._classify_legacy_recovery_edge` exists only as a
  compatibility fallback for pre-v41 rows and is not the intended steady state (see OQ3).*

## Principles these operations must satisfy

- **P1 — One owner per unit.** Every unresolved work unit MUST resolve to exactly one
  owner node (L2). Operator-facing surfaces (e.g. `gza incomplete` "needs attention"
  rows) MUST show owners, not individual leaves of the lineage.
- **P2 — Recovery is transparent to dependents.** A dependency that *failed but was
  successfully retried* MUST be treated as satisfied (L1). A dependent MUST NOT be blocked
  forever because the literal task it named failed, when the work it needed actually
  completed and merged in the same lineage.
- **P3 — Abandonment is not completion.** A `dropped` node MUST NOT satisfy a dependency
  or stand in as a successful recovery. Dropping is a deliberate decision that the work
  will not happen; it MUST NOT silently unblock dependents.
- **P4 — Fail closed on unprovable state.** When merge-ness, completion, or chain
  membership cannot be established safely, the operation MUST report *unsatisfied /
  unresolved* and let the caller stop for a human, never guess in a direction that could
  run or merge work on a false premise. (Mirrors lifecycle P3/P4.)
- **P5 — Based-on, not depends-on, defines a lineage.** Membership in a lineage (and thus
  ownership and recovery-chain walks) follows `based_on`. `depends_on` connects *separate*
  lineages in execution order; it does not make two tasks the same unit.

## The canonical operations

### L1 — Dependency satisfaction

A task `T` with `depends_on = D` MUST NOT run until `D`'s work is **satisfied**.

- **Satisfied** means the merge unit of the satisfying task is in state `merged`, `empty`,
  or `redundant`. For a merge-required dependency, `status == "completed"` alone is **not**
  sufficient — the dependency's branch MUST actually be merged into the work unit's
  canonical local target (lifecycle P4), be provably `empty` (the task/branch carried no
  commits), or be provably `redundant` (the task carried commits already represented on
  the target). A completed held plan
  (`task_type == "plan"` with `auto_implement == false`) is a distinct exception:
  direct dependents MUST stay blocked until the hold is explicitly released.
  New `implement` tasks MUST NOT be created or rewired into that state through
  `--depends-on <plan-id>` or a `--based-on` lineage rooted at the held plan; the CLI
  MUST refuse those attempts and direct the operator to the explicit release commands.
- **Completed terminal no-work is dependency-satisfying.** A prerequisite with
  `status == "completed"` and authoritative merge-unit state `empty` or `redundant`
  MUST satisfy merge-required `depends_on` exactly like `merged`: the work unit is
  terminal and there is nothing left to merge.
- **Failed merged work is already satisfied.** A failed prerequisite whose authoritative
  merge-unit state is `merged` MUST satisfy downstream merge-required dependencies even
  when no completed retry descendant exists. Once the dependency's work is already on
  the target, dependents must not stay blocked on the failed task row.
- **Failed terminal no-work is not self-satisfying.** A failed or dropped prerequisite
  with `empty`/`redundant` merge evidence MUST remain blocked unless L1 resolves a valid
  completed representative through the `based_on` recovery chain. Recoverable failed
  empty work MUST NOT silently satisfy downstream merge-required dependencies on its own.
- **Retry-chain satisfaction.** If `D` itself is `failed` or `dropped`, satisfaction MUST
  follow the `based_on` recovery chain rooted at `D`: the **first `status == "completed"`
  descendant** (whose merge unit is then `merged`/`empty`/`redundant`) satisfies the dependency.
  `dropped` descendants MUST be skipped (P3). The walk MUST be over `based_on`, never
  `depends_on` (E1).
- **Fail closed.** If the satisfying task completed but its branch is not yet merged into
  the target, `T` MUST be blocked with a *prerequisite-unmerged* condition rather than
  run. This condition is **retryable**: it clears automatically once the dependency merges.

*Implementation note: `db.resolve_dependency_completion` → `_find_successful_retry_task`
(based_on walk, completed-not-dropped) and `dependency_preconditions.task_satisfies_merge_dependency`
(merge-state predicate). The retryable block surfaces as `PREREQUISITE_UNMERGED`.*

### L2 — Owner / merge-unit resolution

Every unresolved task MUST resolve to exactly one **owner** — the representative node of
its work unit (P1). Owner resolution is the basis for what operators see and for which
node lifecycle actions attach to.

The intended resolution order, first match wins:

1. **Canonical merge-unit owner.** If the task is attached to a merge unit, the unit's
   recorded owner is the owner.
2. **Shared-branch root.** Else, if the task is a descendant sharing the work unit's
   branch (an improve/rebase that did not fork a new branch), the branch root is the owner.
3. **Recovery-chain root.** Else, if the task is `failed`, the root of its recovery chain
   is the owner — so a string of failed attempts presents as one unit, not many.
4. **Same-type branch owner.** Else, walk `based_on` ancestors of the same task type until
   the branch changes; that ancestor is the owner.
5. **Self.** Else the task owns itself.

*Implementation note: `lineage_query.py` `_load_indexes` owner-resolution cascade;
`db.resolve_merge_unit_owner_task` (canonical `owner_task_id`). Rules 2 and 4 predate
first-class merge units and may be partly subsumed by rule 1 — see OQ1.*

### L3 — Latest-node resolution

For questions of the form "the current review for this implementation" or "the latest
attempt in this lineage", resolution MUST consider the **whole lineage / merge unit**, not
only the direct `based_on` children of one task.

- Reviews and other role-nodes attached anywhere on the owning lineage — including
  branchless nodes attached via the merge unit — MUST be considered candidates.
- "Latest" MUST order completed nodes ahead of incomplete ones, then by most-recent
  completion time. The current/applicable node is the first under that order.

*Implementation note: `query.get_reviews_for_root` (gathers direct + same-merge-unit +
slug-fallback) and `task_query._latest_review_verdict`. Consumed by lifecycle §6
(review/improve cycle).* 

### L4 — Recovery-target resolution

When a task is retried or resumed, the new attempt MUST attach to the lineage so that L1
and L2 can find it:

- The new task MUST record `based_on = <the failed/abandoned attempt it replaces>` and a
  `recovery_origin` of `retry` or `resume` (E2).
- It MUST attach to the **attempt it directly replaces**, preserving a walkable chain
  through every attempt; it MUST NOT skip intermediate attempts in a way that breaks the
  `based_on` walk used by L1/L2.

*Implementation note: `cli/_common.py` resume/retry creation sets `based_on=original_task.id`
plus `recovery_origin`.*

### L5 — Stale unmerged sweep

Operators MAY run a conservative stale-unmerged sweep to drop abandoned never-merged work
units that are still cluttering unresolved views. This is a maintenance operation over
the lineage graph; it does not change merge truth.

- **Eligible unit states.** The sweep MUST consider only active merge units still recorded
  as `unmerged`, `blocked`, or `stale`.
- **Fresh canonical merge proof.** Before reporting or dropping a candidate, the sweep MUST
  re-check that merge unit against the local canonical default target using the same
  non-network merge-truth semantics as plain default-target `gza unmerged`. A candidate
  proven `merged`, `empty`, or `redundant` there MUST be excluded. If that proof fails for
  any candidate, the command MUST fail before applying any drops rather than falling back
  to cached merge-unit state.
- **Terminal attached work only.** The sweep MUST skip any candidate with attached
  `pending` or `in_progress` tasks. It MAY drop only lineages whose attached tasks are
  otherwise terminal and old enough for the configured staleness threshold.
- **Live-edge safety rule.** The sweep MUST NOT drop a candidate when any external
  `depends_on` edge still points to or from a lineage that remains unresolved under the
  canonical lineage/lifecycle rules. In practice, pending work, in-progress work,
  unresolved never-merged work, and failed work still awaiting recovery all keep the edge
  live. An edge to a lineage already resolved under L1/L2 — for example one whose merge
  unit is `merged`, `empty`, or `redundant`, or whose recovery chain already completed —
  MUST NOT keep the stale candidate visible on its own.
- **Dry-run by default.** The maintenance command MUST default to reporting candidates
  without mutating task state. Mutation requires an explicit operator opt-in.
- **Mutation boundary.** When the operator explicitly executes the sweep, it MUST route
  task-state changes through the canonical manual drop path for the attached task rows. It
  MUST NOT delete branches or discard branch provenance as part of the sweep.

## Merge units & the ownership model

A **merge unit** is the durable record of "one body of work being merged into one place".
It is the object L1/L2 ultimately resolve against.

- A merge unit is identified by a **(source branch, target branch)** pair within a
  project and carries a **canonical owner task** for provenance.
- Its **state** is `merged`, `unmerged`, `empty`, or `redundant`. `empty` means the
  task/branch carried no commits of its own. `redundant` means the task carried commits,
  but branch inspection proved the target already contains equivalent work and no unique
  commits remain to land. Both `empty` and `redundant` are terminal for lifecycle and
  dependency-readiness policy: there is nothing left to merge, so they MUST NOT be
  re-reported as `unmerged`/blocking once proven. `empty`, `redundant`, and `merged` are
  **distinct**: a branch that is merely reachable from the target with no unique commits
  is never automatically `merged`/landed, and MUST NOT be treated as a landed
  representative for failed-task recovery suppression (see `recovery.md` R5 /
  `lifecycle-engine.md` §7). "Reachable from target" alone does not prove work landed —
  only contributed commits do.
- A unit MAY be **superseded** (e.g. on re-sync); resolution MUST consider only the active
  (non-superseded) unit.
- Tasks attach many-to-one to a merge unit. The unit, not any single task, is the "needs
  attention" row operators act on (P1).
- Proven-merged truth for any member of a merge unit, including a non-owner same-branch
  follow-up (`improve`/`fix`/`rebase`), MUST resolve the active unit to its terminal
  state. When that proof lands the unit as `merged`, merge provenance MUST stay
  attributed to the canonical owner task rather than the follow-up row that observed the
  merge.

*Implementation note: `MergeUnit` (db.py), `merge_unit_tasks` junction, `resolve_merge_unit_for_task`,
`list_active_merge_units`. The first-class `empty` state is being introduced by plan
gza-4065 (slices F-A/F-B); this section is the intended end-state and should be reconciled
with that work as it lands.*

## Open questions

- **OQ1 — How much of L2 is still load-bearing post-merge-units?** Owner rules 2
  (shared-branch root) and 4 (same-type branch owner) predate first-class merge units and
  may now be largely subsumed by the canonical merge-unit owner (rule 1). Decide which are
  intended steady-state semantics versus fallbacks retained only for lineages not yet
  attached to a unit. The doc currently lists all five as intent; this needs ratifying.
- **OQ2 — Legacy-DB fallbacks are explicitly compatibility debt, not contract.**
  `_legacy_merge_status_owner_for_unit` (owner without a resolvable `owner_task_id`) and
  `_classify_legacy_recovery_edge` (pre-v41 recovery detection by payload/branch) exist for
  old or externally damaged databases. Intended behavior is that every unit has a canonical
  owner and every recovery edge carries `recovery_origin`. These fallbacks should be named
  as debt with a removal condition, not specified as desired behavior.
- **OQ3 — Does `depends_on` ever participate in resolution?** This draft asserts strictly
  no (P5/E1): lineage membership, ownership, and recovery satisfaction follow `based_on`
  only, and `depends_on` is purely scheduling. Confirm no resolution path legitimately
  needs to walk `depends_on`.
- **OQ4 — Legacy compatibility boundary for no-work states.** The merge-unit
  `empty`/`redundant` dependency contract is explicit in L1; remaining open work is only
  about how long task-row-only compatibility fallbacks remain supported before all
  authoritative readiness decisions require merge-unit evidence.
