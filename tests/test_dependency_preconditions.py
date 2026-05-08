from pathlib import Path

from gza.dependency_preconditions import get_unmerged_dependency_precondition
from gza.db import SqliteTaskStore


def test_dependency_precondition_reads_merge_unit_state(tmp_path: Path) -> None:
    store = SqliteTaskStore(tmp_path / "test.db")

    dependency = store.add("Dependency", task_type="implement")
    store.mark_completed(dependency, has_commits=True, branch="feature/dependency")
    assert dependency.id is not None
    store.set_merge_status(dependency.id, "merged")

    refreshed_dependency = store.get(dependency.id)
    assert refreshed_dependency is not None
    refreshed_dependency.merge_status = None
    store.update(refreshed_dependency)

    downstream = store.add("Downstream", task_type="implement", depends_on=dependency.id)

    assert get_unmerged_dependency_precondition(store, downstream) is None


def test_dependency_precondition_blocks_when_unit_is_unmerged_but_legacy_row_says_merged(tmp_path: Path) -> None:
    store = SqliteTaskStore(tmp_path / "test.db")

    dependency = store.add("Dependency", task_type="implement")
    store.mark_completed(dependency, has_commits=True, branch="feature/dependency")
    assert dependency.id is not None

    refreshed_dependency = store.get(dependency.id)
    assert refreshed_dependency is not None
    refreshed_dependency.merge_status = "merged"
    store.update(refreshed_dependency)

    downstream = store.add("Downstream", task_type="implement", depends_on=dependency.id)

    assert get_unmerged_dependency_precondition(store, downstream).id == dependency.id


def test_dependency_precondition_blocks_when_unit_is_blocked_even_if_legacy_row_says_merged(tmp_path: Path) -> None:
    store = SqliteTaskStore(tmp_path / "test.db")

    dependency = store.add("Dependency", task_type="implement")
    store.mark_completed(dependency, has_commits=True, branch="feature/dependency")
    assert dependency.id is not None
    unit = store.resolve_merge_unit_for_task(dependency.id)
    assert unit is not None
    store.set_merge_unit_state(unit.id, "blocked")

    refreshed_dependency = store.get(dependency.id)
    assert refreshed_dependency is not None
    refreshed_dependency.merge_status = "merged"
    store.update(refreshed_dependency)

    downstream = store.add("Downstream", task_type="implement", depends_on=dependency.id)

    assert get_unmerged_dependency_precondition(store, downstream).id == dependency.id


def test_dependency_precondition_uses_requested_target_branch(tmp_path: Path) -> None:
    store = SqliteTaskStore(tmp_path / "test.db")

    dependency = store.add("Dependency", task_type="implement")
    store.mark_completed(dependency, has_commits=True, branch="feature/dependency")
    assert dependency.id is not None

    main_unit = store.get_or_create_merge_unit_for_task(dependency, "main")
    release_unit = store.get_or_create_merge_unit_for_task(dependency, "release")
    assert main_unit is not None
    assert release_unit is not None
    store.set_merge_unit_state(main_unit.id, "merged")
    store.set_merge_unit_state(release_unit.id, "unmerged")

    downstream = store.add("Downstream", task_type="implement", depends_on=dependency.id)

    assert get_unmerged_dependency_precondition(store, downstream, "main") is None
    assert get_unmerged_dependency_precondition(store, downstream, "release").id == dependency.id
