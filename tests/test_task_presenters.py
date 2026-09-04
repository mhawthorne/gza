"""Tests for task query presentation helpers."""

import ast
from datetime import UTC, datetime
from pathlib import Path

import gza.colors as _colors
from gza.console import truncate
from gza.db import SqliteTaskStore
from gza.task_query import LineageRow, PresentationSpec, TaskQuery, TaskQueryResult


def _colored_id(task_id: str) -> str:
    task_id_color = _colors.TASK_COLORS.task_id
    return f"[{task_id_color}]{task_id}[/{task_id_color}]"


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
        f"{_colored_id(owner.id)}: Retry failed task (PREREQUISITE_UNMERGED) — {truncate(first_line, 100)}"
    )
    assert "Full prompt body that should not render" not in rendered
    assert root.prompt not in rendered
    assert "| context:" not in rendered
    assert "| unresolved:" not in rendered


def test_one_line_omits_unresolved_task_list(tmp_path: Path) -> None:
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

    assert rendered == f"{_colored_id(owner.id)}: Needs attention — Owner prompt"
    assert failed.prompt not in rendered
    assert dropped.prompt not in rendered
    assert completed.prompt not in rendered
    assert "unresolved:" not in rendered


def test_one_line_renders_merge_reason(tmp_path: Path) -> None:
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
                values={"next_action_reason": "Merge branch into main"},
            ),
        ),
        total_count=1,
    )

    rendered = result.render()
    assert rendered == f"{_colored_id(owner.id)}: Merge branch into main — Owner prompt"


def test_task_presenters_has_no_unused_import_bindings() -> None:
    path = Path(__file__).resolve().parents[1] / "src/gza/task_presenters.py"
    module = ast.parse(path.read_text(), filename=str(path))
    imported: dict[str, int] = {}
    used_names: set[str] = set()

    for node in ast.walk(module):
        if isinstance(node, ast.Import):
            for alias in node.names:
                imported[alias.asname or alias.name.split(".")[0]] = node.lineno
        elif isinstance(node, ast.ImportFrom):
            if node.module == "__future__":
                continue
            for alias in node.names:
                if alias.name != "*":
                    imported[alias.asname or alias.name] = node.lineno
        elif isinstance(node, ast.Name) and isinstance(node.ctx, ast.Load):
            used_names.add(node.id)

    assert {name: line for name, line in imported.items() if name not in used_names} == {}
