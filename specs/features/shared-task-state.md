# Shared Task State for Parallel Agents

> **Scope:** This documents the **current single-host model** — one machine, a shared SQLite DB
> mounted into task containers. For **multi-machine distributed development**, see
> [Distributed Development — ID Generation](distributed-development.md) and
> [Distributed Development — Sync Engine](distributed-sync-engine.md). Those supersede the
> "Future Considerations" below: the chosen direction **keeps SQLite** (via a cr-sqlite CRDT layer
> with changesets synced over S3), rather than migrating to PostgreSQL.

## Problem

When multiple agents run in parallel (in Docker containers), they need to observe each other's work to avoid conflicts and coordinate task execution.

## Decision

Store the SQLite database at `~/.gza/<project-name>.db` on the host, and mount it into containers via Docker volume mount.

## Why SQLite + Volume Mount

- **Atomic transactions**: One agent can atomically claim a task, and others see it immediately
- **Row-level visibility**: Agents can query what's currently in progress
- **File locking**: SQLite handles concurrent access on the same machine
- **Host visibility**: The DB is accessible from both host CLI and containers
- **Simplicity**: Just a directory mount, easy to reason about

## Docker Usage

```bash
docker run -v ~/.gza:/root/.gza ...
```

All containers and the host CLI share the same database.

## Task Store Interface

To support future migration to distributed systems (PostgreSQL, etc.), abstract storage behind an interface:

```python
class TaskStore(Protocol):
    def claim_next_task(self, agent_id: str) -> Task | None
    def complete_task(self, task_id: str, result: ...) -> None
    def list_in_progress(self) -> list[Task]
```

This keeps storage decoupled from application code — useful for testing and for the distributed
CRDT-backed store (see Future Considerations). Note the distributed direction keeps SQLite rather
than swapping in a `PostgresTaskStore`.

## Future Considerations

For multi-machine distributed development, the direction is **not** a PostgreSQL migration. SQLite is
retained and made multi-master via a CRDT layer (cr-sqlite), with changesets replicated between
environments over S3. See [Distributed Development — Sync Engine](distributed-sync-engine.md) for the
replication design and [Distributed Development — ID Generation](distributed-development.md) for the
collision-free ID scheme that makes cross-environment row merges possible.
