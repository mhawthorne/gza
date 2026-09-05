# Shared schema migration isolation

> **Status: Draft.** This document defines the safety boundary for durable task-database
> schema migrations when code from multiple lifecycle states can execute against the same
> project.

## What this owns

This document owns one question:

- When may running gza code advance the schema of a shared durable task database, and what
  database isolation is required when evaluated code runs?

It does **not** own:

- The concrete migration mechanism, table layout, or SQLite transaction shape.
- Concrete provider or verify subprocess environment mechanics such as host/container path
  mapping. This file owns the behavioral database-isolation requirements those mechanics
  must satisfy.
- Main-verify red remediation policy. That lives in
  [main-verify-self-heal.md](main-verify-self-heal.md).

## Terms

- **Shared durable database** — the authoritative task database used as control-plane
  state for a project or supervised project set. Its contents survive process exit and
  are used by future lifecycle, watch, recovery, and landing decisions.
- **Isolated private copy** — a database copy created for a task worktree, provider
  subprocess, verify subprocess, rebase exception, test fixture, or other disposable
  non-authoritative execution context. Its writes are not copied back into the shared
  durable database.
- **Evaluated code** — code that is being tested, verified, reviewed, rebased, or run
  from an unmerged worktree, candidate checkout, detached checkout, or otherwise
  non-canonical execution context.
- **Proven canonical landed code** — code executing from the canonical project checkout
  on the configured default branch, where the local checked-out commit is proven to be
  the current local tip of that default branch at the moment migration authority is used.
- **Migration authority** — explicit proof that the current process is allowed to mutate
  a shared durable database's schema. Absence of that proof is a denial, not an invitation
  to infer authority from branch names or task status.
- **Forward-migration deferral** — a fail-closed result where code that knows about a
  newer schema declines to advance a shared durable database because migration authority
  is missing.
- **Schema-runtime skew** — a fail-closed unavailable condition where the running gza
  runtime cannot safely interpret or use the database schema it is asked to open. This is
  an environment/control-plane compatibility problem, not proof that project code made
  the local target branch red.

## Contract

### SSI1 — Evaluated code MUST NOT advance shared durable schema

Evaluated code MUST NOT create, upgrade, repair, backfill, or otherwise mutate the schema
of a shared durable database.

This invariant applies even when the evaluated code is ahead of the shared database's
schema, even when the schema change is additive, and even when the evaluated code would
pass its own tests after applying that migration. A worktree being evaluated is not yet
trusted as the control plane for future lifecycle decisions.

### SSI2 — Shared schema migration requires proven canonical landed code

A process MAY advance a shared durable database's schema only when it has proven
migration authority for proven canonical landed code.

Migration authority MUST be established before any shared-database schema mutation,
current-version startup repair, current-version artifact repair, schema-dependent project
registration write, or migration backfill is attempted. It MUST be fresh enough to
prevent a checkout or branch-ref change from turning a previously valid proof into an
unsafe migration. A branch merely named like the default branch, a detached checkout at a
matching commit, an unmerged task worktree, or a caller that has not supplied proof MUST
NOT be treated as migration-authorized.

Explicit operator migration commands are subject to the same shared-database authority
rule. Manual confirmation MAY still be required by an individual migration, but manual
confirmation is not a substitute for migration authority.

### SSI3 — Missing authority MUST NOT perform schema/current-version mutation

When code that knows about a newer schema opens a shared durable database without shared
migration authority, it MUST NOT perform shared schema mutation, current-version repair,
schema-dependent project registration, or migration backfill. It MUST skip those writes
while migration is deferred.

Forward-migration deferral MUST be explicit and visible. If the attempted operation can
run correctly against the current shared schema, it MAY proceed without applying the
pending migration and MAY perform ordinary data writes that are valid for that current
schema. If the operation requires a deferred schema capability, automation MUST fail
closed at a named capability boundary before performing any write for that operation.

Compatibility is not assumed from the shape of a migration. Each forward migration MUST
declare whether code at the new version can operate against the immediately preceding
shared schema while migration is deferred. Missing, ambiguous, or failed capability proof
MUST stop before schema/current-version mutation and before any operation-specific write
that depends on the deferred capability.

### SSI4 — Isolated private copies MAY migrate independently

Isolated private copies MAY create or advance their own schema independently when doing
so cannot mutate the shared durable database and cannot be persisted back as shared
control-plane state.

This permission covers disposable task worktree databases, verify snapshots, rebase
exception copies, and tests. It does not grant authority to update the live shared
database after the isolated run finishes, and it does not make isolated writes durable
evidence unless the host control plane separately records explicit result evidence.

### SSI5 — Verify and provider execution MUST use isolated database state

Provider, agent, and verify subprocesses that execute evaluated code MUST be given an
isolated private copy of the relevant task database rather than the live shared durable
database.

Ordinary provider snapshots MUST be read-only. The rebase workflow is the narrow
exception: it MAY use an isolated writable copy, and its writes MUST NOT be copied back to
the shared durable database. Verify snapshots MAY be writable because repository verify
commands can run nested gza commands, but their writes MUST be discarded when the verify
attempt ends. Each verify attempt MUST receive a fresh isolated copy so a rerun cannot
inherit mutations from a prior failed attempt.

### SSI6 — Schema-runtime skew is unavailable, not code-red

When a runtime cannot use a database because the database schema is newer than the
runtime supports, or because a structured compatibility diagnostic reports
schema-runtime skew, lifecycle and watch automation MUST classify the result as an
unavailable environment/control-plane condition.

Schema-runtime skew MUST fail closed for any action that needs the unavailable evidence:
automation MUST NOT promote a green checkpoint, merge, or claim success based on it.
However, schema-runtime skew MUST NOT be converted into a deterministic red verdict,
MUST NOT create or extend red-main remediation identity, MUST NOT consume red-remediation
attempts, and MUST NOT be reported as proof that the canonical local target regressed.
The skew MUST route directly to unavailable attention for both ordinary lifecycle
`verify_fix` decisions and red-main `system-main-verify` decisions: automation MUST NOT
create, reuse, run, requeue, or consume attempts in either remediation lane based on
schema-runtime skew.

Operator-facing output SHOULD name the observed and supported schema versions when known
and SHOULD direct the operator toward restarting/updating the runtime or rerunning from
the canonical default-branch checkout after the migration code has landed.
