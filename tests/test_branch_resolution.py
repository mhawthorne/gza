from datetime import UTC, datetime
from pathlib import Path

from gza.branch_resolution import resolve_rebase_base_branch
from gza.db import SqliteTaskStore


def test_resolve_rebase_base_branch_backfills_legacy_rebase_from_merge_unit(
    tmp_path: Path,
    monkeypatch,
) -> None:
    store = SqliteTaskStore(tmp_path / "test.db")
    monkeypatch.setattr(store, "default_merge_target", lambda *, strict=False: "trunk")

    impl = store.add("Implement feature", task_type="implement")
    assert impl.id is not None
    store.mark_completed(impl, has_commits=True, branch="feature/legacy-rebase")
    impl_unit = store.resolve_merge_unit_for_task(impl.id)
    assert impl_unit is not None
    assert impl_unit.target_branch == "trunk"

    rebase = store.add(
        "Legacy rebase feature",
        task_type="rebase",
        based_on=impl.id,
        same_branch=True,
    )
    assert rebase.id is not None
    rebase.branch = "feature/legacy-rebase"
    rebase.status = "failed"
    rebase.completed_at = datetime.now(UTC)
    store.update(rebase)

    assert resolve_rebase_base_branch(store, rebase) == "trunk"

    refreshed = store.get(rebase.id)
    assert refreshed is not None
    assert refreshed.base_branch == "trunk"


def test_resolve_rebase_base_branch_does_not_create_merge_unit_without_durable_target(
    tmp_path: Path,
    monkeypatch,
) -> None:
    store = SqliteTaskStore(tmp_path / "test.db")
    monkeypatch.setattr(store, "default_merge_target", lambda *, strict=False: "trunk")

    impl = store.add("Implement feature", task_type="implement")
    assert impl.id is not None
    impl.branch = "feature/no-merge-unit"
    store.update(impl)

    rebase = store.add(
        "Legacy rebase feature",
        task_type="rebase",
        based_on=impl.id,
        same_branch=True,
    )
    assert rebase.id is not None
    rebase.branch = "feature/no-merge-unit"
    store.update(rebase)

    assert resolve_rebase_base_branch(store, rebase) is None
    assert store.resolve_merge_unit_for_task(impl.id) is None
    assert store.resolve_merge_unit_for_task(rebase.id) is None

    refreshed = store.get(rebase.id)
    assert refreshed is not None
    assert refreshed.base_branch is None
