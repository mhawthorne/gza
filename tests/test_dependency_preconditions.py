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
