# Shared Task State for Parallel Agents

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

This allows swapping `SqliteTaskStore` for `PostgresTaskStore` later without changing application code.

## Future Considerations

For distributed agents across multiple machines, SQLite won't work (no network-based locking). At that point, migrate to PostgreSQL or another network-accessible database. The `TaskStore` abstraction ensures this is a straightforward swap.
