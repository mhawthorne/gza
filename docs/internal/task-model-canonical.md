# Canonical Task Model

`gza.db` is the single canonical task model and storage API.

- Use `src/gza/db.py` (`db.Task` and database-backed operations) for all task lifecycle behavior.
- Do not introduce parallel task model modules (for example, a second `Task` dataclass in another module).
- YAML-based task import remains supported via importer/config flows, but imported data is normalized into `gza.db`.
