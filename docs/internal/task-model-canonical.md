# Canonical Task Model

`gza.db` is the single canonical task model and storage API.

- Use `src/gza/db.py` (`db.Task` and database-backed operations) for all task lifecycle behavior.
- Use `src/gza/task_query.py` (`TaskQuery`, `TaskQueryPresets`, `TaskQueryService`) for task reads that list, search, filter, group, or summarize tasks.
- Use `src/gza/lineage_query.py` for owner-keyed lineage reads that power `gza incomplete`, `gza advance`, and `gza watch --restart-failed`; `TaskQueryService` remains the public orchestration layer and delegates lineage rollups there.
- Treat direct `SqliteTaskStore` read methods such as `get_pending*()`, `get_history()`, `get_in_progress()`, and `get_all()` as query-engine internals for CLI/API presentation code.
- High-level surfaces should build a declarative `TaskQuery` and route through `TaskQueryService`, even when the service internally delegates to optimized store helpers for canonical ordering.
- Owner-keyed lineage rows are the canonical read model for unresolved branch ownership boundaries. Command surfaces should display or execute from the owner row and use `action_task` / `recovery_leaf_task` only as concrete execution details.
- Do not introduce parallel task model modules (for example, a second `Task` dataclass in another module).
- YAML-based task import remains supported via importer/config flows, but imported data is normalized into `gza.db`.

## Read vs. Write Boundary

- Writes and lifecycle mutations stay on `SqliteTaskStore`.
- Reads should compose as a sequence of filters/sorts/projections in `TaskQuery`.
- Point lookups that are immediately followed by a mutation (`store.get(task_id)` before update/delete) are still fine outside the query layer.
- New CLI/API task-list features should add a query preset before adding another custom store read path.
