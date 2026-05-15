"""Tests for task query presentation helpers."""

from datetime import UTC, datetime
from pathlib import Path

from gza.console import truncate
from gza.db import SqliteTaskStore
from gza.task_query import LineageRow, PresentationSpec, TaskQuery, TaskQueryResult


def _store(tmp_path: Path) -> SqliteTaskStore:
    return SqliteTaskStore(tmp_path / "test.db")


def _one_line_query() -> TaskQuery:
    return TaskQuery(
        scope="lineages",
        lifecycle_state=("incomplete",),
        presentation=PresentationSpec(mode="one_line"),
    )


def test_one_line_uses_headline_prompt_and_drops_context(tmp_path: Path) -> None:
    store = _store(tmp_path)
    root = store.add("Root context prompt that should stay hidden", task_type="plan")

    first_line = "Follow-up first line " + ("x" * 120)
    owner = store.add(f"\n\n{first_line}\nFull prompt body that should not render", task_type="implement")
    owner.status = "failed"
    owner.completed_at = datetime.now(UTC)
    owner.failure_reason = "PREREQUISITE_UNMERGED"
    store.update(owner)

    result = TaskQueryResult(
        query=_one_line_query(),
        rows=(
            LineageRow(
                owner_task=owner,
                members=(root, owner),
                tree=None,
                unresolved_tasks=(owner,),
                values={"next_action_reason": "Retry failed task (PREREQUISITE_UNMERGED)"},
            ),
        ),
        total_count=1,
    )

    rendered = result.render()

    assert rendered == (
        f"{owner.id}: Retry failed task (PREREQUISITE_UNMERGED) — {truncate(first_line, 100)}"
    )
    assert "Full prompt body that should not render" not in rendered
    assert root.prompt not in rendered
    assert "| context:" not in rendered
    assert "| unresolved:" not in rendered


def test_one_line_summarizes_multiple_unresolved_tasks_by_id_and_reason(tmp_path: Path) -> None:
    store = _store(tmp_path)
    owner = store.add("Owner prompt", task_type="implement")

    failed = store.add("Failed child full prompt body", task_type="implement")
    failed.status = "failed"
    failed.completed_at = datetime.now(UTC)
    failed.failure_reason = "TIMEOUT"
    store.update(failed)

    dropped = store.add("Dropped child full prompt body", task_type="implement")
    dropped.status = "dropped"
    dropped.completed_at = datetime.now(UTC)
    store.update(dropped)

    completed = store.add("Completed child full prompt body", task_type="implement")
    completed.status = "completed"
    completed.completed_at = datetime.now(UTC)
    completed.completion_reason = "FOLLOW_UP"
    completed.has_commits = True
    completed.merge_status = "unmerged"
    store.update(completed)

    result = TaskQueryResult(
        query=_one_line_query(),
        rows=(
            LineageRow(
                owner_task=owner,
                members=(owner, failed, dropped, completed),
                tree=None,
                unresolved_tasks=(failed, dropped, completed),
                values={"next_action_reason": "Needs attention"},
            ),
        ),
        total_count=1,
    )

    rendered = result.render()

    assert rendered == (
        f"{owner.id}: Needs attention — Owner prompt"
        f" | unresolved: {failed.id} (TIMEOUT); {dropped.id} (dropped); {completed.id} (FOLLOW_UP)"
    )
    assert failed.prompt not in rendered
    assert dropped.prompt not in rendered
    assert completed.prompt not in rendered


def test_one_line_renders_recommend_rebase_reason(tmp_path: Path) -> None:
    store = _store(tmp_path)
    owner = store.add("Owner prompt", task_type="implement")

    result = TaskQueryResult(
        query=_one_line_query(),
        rows=(
            LineageRow(
                owner_task=owner,
                members=(owner,),
                tree=None,
                unresolved_tasks=(owner,),
                values={"next_action_reason": "SKIP: branch is stale; branch is behind the target branch; rebase recommended"},
            ),
        ),
        total_count=1,
    )

    rendered = result.render()
    assert rendered == (
        f"{owner.id}: SKIP: branch is stale; branch is behind the target branch; rebase recommended — Owner prompt"
    )
