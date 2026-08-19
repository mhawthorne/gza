"""Tag mutation parsing and Gza-backed write helpers."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Literal

from gza.config import Config
from gza.db import SqliteTaskStore


@dataclass(frozen=True)
class TagMutation:
    """One bulk mutation matching ``gza retag``'s contract."""

    kind: Literal["add", "remove", "replace"]
    tag: str
    old_tag: str | None = None

    @property
    def summary(self) -> str:
        if self.kind == "add":
            return f"add tag '{self.tag}'"
        if self.kind == "remove":
            return f"remove tag '{self.tag}'"
        return f"replace tag '{self.old_tag}' with '{self.tag}'"


def normalize_tags(values: list[str]) -> tuple[str, ...]:
    """Apply Gza's public tag normalization semantics to request values."""
    normalized: set[str] = set()
    for value in values:
        tag = " ".join(str(value).split()).lower()
        if not tag:
            raise ValueError("tag must not be empty")
        normalized.add(tag)
    return tuple(sorted(normalized))


def parse_tag_mutation(payload: dict[str, Any]) -> TagMutation:
    """Parse exactly one add/remove/replace mutation from JSON or form data."""
    if "mutation" in payload:
        kind = str(payload.get("mutation", ""))
        if kind in {"add", "remove"}:
            candidates = [(kind, payload.get("mutation_tag"))]
        elif kind == "replace":
            candidates = [(kind, (payload.get("old_tag"), payload.get("new_tag")))]
        else:
            candidates = []
    else:
        candidates = [
            (kind, payload[kind])
            for kind in ("add", "remove", "replace")
            if payload.get(kind) is not None
        ]

    if len(candidates) != 1:
        raise ValueError("exactly one tag mutation is required")

    kind, raw_value = candidates[0]
    if kind in {"add", "remove"}:
        tag = normalize_tags([str(raw_value or "")])[0]
        return TagMutation(kind=kind, tag=tag)

    if not isinstance(raw_value, (list, tuple)) or len(raw_value) != 2:
        raise ValueError("replace requires exactly two tags: OLD and NEW")
    old_tag = normalize_tags([str(raw_value[0])])[0]
    new_tag = normalize_tags([str(raw_value[1])])[0]
    return TagMutation(kind="replace", old_tag=old_tag, tag=new_tag)


def writable_project_store(base_store: SqliteTaskStore, project_id: str) -> SqliteTaskStore:
    """Resolve a project-scoped writable store through Gza configuration APIs."""
    candidates = base_store.project_query_stores()
    candidate = next((store for store in candidates if store.project_id == project_id), None)
    if candidate is None:
        raise ValueError(f"Unknown project: {project_id}")
    if candidate.project_root is None:
        raise ValueError(f"Project {project_id} has no registered root")
    store = SqliteTaskStore.from_config(Config.load(candidate.project_root, discover=True))
    if store.project_id != project_id or store.db_path.resolve() != base_store.db_path.resolve():
        raise ValueError(f"Project {project_id} no longer resolves to this task database")
    return store


def edit_task_tags(
    store: SqliteTaskStore,
    task_id: str,
    *,
    add: list[str],
    remove: list[str],
) -> tuple[tuple[str, ...], bool]:
    """Add and remove tags from one task through Gza's tag edit API."""
    if not add and not remove:
        raise ValueError("at least one tag must be added or removed")
    additions = set(normalize_tags(add))
    removals = set(normalize_tags(remove))
    current = store.get_task_tags(task_id)
    final = tuple(sorted((set(current) - removals) | additions))
    if final == current:
        return current, False
    return store.replace_task_tags(task_id, final), True


def apply_bulk_tag_mutation(
    store_for_project: Callable[[str], SqliteTaskStore],
    rows: list[dict[str, object]],
    mutation: TagMutation,
) -> int:
    """Apply a frozen matched set through the same primitive as ``gza retag``."""
    ids_by_project: dict[str, list[str]] = {}
    for row in rows:
        ids_by_project.setdefault(str(row["project_id"]), []).append(str(row["id"]))

    changed_count = 0
    for project_id, task_ids in ids_by_project.items():
        store = store_for_project(project_id)
        changed = store.mutate_task_tags(
            task_ids,
            action=mutation.kind,
            tag=mutation.tag,
            old_tag=mutation.old_tag,
        )
        changed_count += sum(changed.values())
    return changed_count
