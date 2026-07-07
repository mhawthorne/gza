from datetime import UTC, datetime
import importlib.util
from pathlib import Path

from gza.db import SqliteTaskStore


def _load_task_iteration_summary_module():
    module_path = Path(__file__).resolve().parents[1] / "scripts" / "task_iteration_summary.py"
    spec = importlib.util.spec_from_file_location("task_iteration_summary", module_path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _set_created_at(store: SqliteTaskStore, task_id: str, created_at: datetime) -> None:
    task = store.get(task_id)
    assert task is not None
    task.created_at = created_at
    store.update(task)


def test_resolve_effective_impl_ignores_same_branch_forward_slices(tmp_path: Path) -> None:
    module = _load_task_iteration_summary_module()
    store = SqliteTaskStore(tmp_path / "test.db")

    root = store.add("Slice S9", task_type="implement")
    assert root.id is not None
    forward = store.add(
        "Slice S10",
        task_type="implement",
        based_on=root.id,
        same_branch=True,
    )
    assert forward.id is not None

    _set_created_at(store, root.id, datetime(2026, 7, 6, 10, 0, tzinfo=UTC))
    _set_created_at(store, forward.id, datetime(2026, 7, 6, 11, 0, tzinfo=UTC))

    resolved = module._resolve_effective_impl(store, root.id)

    assert resolved.id == root.id


def test_resolve_effective_impl_follows_recovery_children_only(tmp_path: Path) -> None:
    module = _load_task_iteration_summary_module()
    store = SqliteTaskStore(tmp_path / "test.db")

    root = store.add("Original impl", task_type="implement")
    assert root.id is not None
    ignored_forward = store.add(
        "Forward slice",
        task_type="implement",
        based_on=root.id,
        same_branch=True,
    )
    assert ignored_forward.id is not None
    retry = store.add(
        "Retry impl",
        task_type="implement",
        based_on=root.id,
        recovery_origin="retry",
    )
    assert retry.id is not None
    resumed_retry = store.add(
        "Manual follow-up",
        task_type="implement",
        based_on=retry.id,
        recovery_origin="manual",
    )
    assert resumed_retry.id is not None

    _set_created_at(store, root.id, datetime(2026, 7, 6, 10, 0, tzinfo=UTC))
    _set_created_at(store, ignored_forward.id, datetime(2026, 7, 6, 11, 0, tzinfo=UTC))
    _set_created_at(store, retry.id, datetime(2026, 7, 6, 12, 0, tzinfo=UTC))
    _set_created_at(store, resumed_retry.id, datetime(2026, 7, 6, 13, 0, tzinfo=UTC))

    resolved = module._resolve_effective_impl(store, root.id)

    assert resolved.id == resumed_retry.id
