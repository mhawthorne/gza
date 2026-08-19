"""Tag mutation parsing and Gza-backed write helpers."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Literal

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


QualifiedTaskId = tuple[str, str]


@dataclass(frozen=True)
class BulkTagMutationResult:
    """Explicit outcome for a potentially multi-project bulk mutation."""

    changed: tuple[QualifiedTaskId, ...]
    unchanged: tuple[QualifiedTaskId, ...]
    skipped: tuple[QualifiedTaskId, ...]
    failed: tuple[QualifiedTaskId, ...]
    failures: tuple[tuple[str, str], ...]

    @property
    def changed_count(self) -> int:
        return len(self.changed)

    @property
    def skipped_count(self) -> int:
        return len(self.skipped)

    @property
    def unchanged_count(self) -> int:
        return len(self.unchanged)

    @property
    def failed_count(self) -> int:
        return len(self.failed)


def apply_bulk_tag_mutation(
    stores_by_project: Mapping[str, SqliteTaskStore],
    rows: list[dict[str, object]],
    mutation: TagMutation,
) -> BulkTagMutationResult:
    """Apply a frozen set and report every changed, missing, or failed target."""
    ids_by_project: dict[str, list[str]] = {}
    for row in rows:
        ids_by_project.setdefault(str(row["project_id"]), []).append(str(row["id"]))

    missing_stores = set(ids_by_project) - stores_by_project.keys()
    if missing_stores:
        missing = ", ".join(sorted(missing_stores))
        raise ValueError(f"Mutation stores were not preflighted for projects: {missing}")

    changed_ids: list[QualifiedTaskId] = []
    unchanged_ids: list[QualifiedTaskId] = []
    skipped_ids: list[QualifiedTaskId] = []
    failed_ids: list[QualifiedTaskId] = []
    failures: list[tuple[str, str]] = []
    for project_id, task_ids in ids_by_project.items():
        try:
            changed = stores_by_project[project_id].mutate_task_tags(
                task_ids,
                action=mutation.kind,
                tag=mutation.tag,
                old_tag=mutation.old_tag,
            )
        except Exception as exc:
            # Multi-project stores cannot share one transaction. Preserve an
            # honest partial-result contract if a preflighted store later fails.
            failed_ids.extend((project_id, task_id) for task_id in task_ids)
            failures.append((project_id, str(exc)))
            continue
        for task_id in task_ids:
            qualified_id = (project_id, task_id)
            if task_id not in changed:
                skipped_ids.append(qualified_id)
            elif changed[task_id]:
                changed_ids.append(qualified_id)
            else:
                unchanged_ids.append(qualified_id)
    return BulkTagMutationResult(
        changed=tuple(changed_ids),
        unchanged=tuple(unchanged_ids),
        skipped=tuple(skipped_ids),
        failed=tuple(failed_ids),
        failures=tuple(failures),
    )
