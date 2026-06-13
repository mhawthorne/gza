from gza.db import Task
from gza.derived_tags import resolve_derived_task_tags


def _task(*, tags: tuple[str, ...]) -> Task:
    return Task(id="gza-1", prompt="parent", task_type="implement", tags=tags)


def test_resolve_derived_task_tags_inherits_parent_tags_by_default() -> None:
    parent = _task(tags=("202606-recovery", "v0.5.0"))

    assert resolve_derived_task_tags(parent) == parent.tags


def test_resolve_derived_task_tags_explicit_override_wins_including_empty() -> None:
    parent = _task(tags=("202606-recovery", "v0.5.0"))

    assert resolve_derived_task_tags(parent, explicit_tags=("manual-override",)) == ("manual-override",)
    assert resolve_derived_task_tags(parent, explicit_tags=()) == ()
