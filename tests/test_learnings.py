"""Tests for learnings generation."""

import json
from pathlib import Path

from gza.config import Config
from gza.db import SqliteTaskStore
from gza.learnings import regenerate_learnings, maybe_auto_regenerate_learnings


def _new_store(tmp_path: Path) -> SqliteTaskStore:
    db_path = tmp_path / ".gza" / "gza.db"
    db_path.parent.mkdir(parents=True, exist_ok=True)
    return SqliteTaskStore(db_path)


def test_regenerate_learnings_writes_file_from_completed_tasks(tmp_path: Path):
    """Regeneration should write `.gza/learnings.md` from completed task outputs."""
    store = _new_store(tmp_path)
    config = Config(project_dir=tmp_path, project_name="test")

    done = store.add("Implement auth flow", task_type="implement")
    store.mark_completed(
        done,
        output_content="# Summary\n- Use pytest fixtures for auth database setup\n",
        has_commits=False,
    )

    pending = store.add("Pending task should not be used", task_type="implement")
    assert pending.status == "pending"

    result = regenerate_learnings(store, config, window=10)

    assert result.tasks_used == 1
    assert result.added_count >= 1
    assert result.removed_count == 0
    assert result.retained_count == 0
    assert result.path.exists()
    content = result.path.read_text()
    assert "# Project Learnings" in content
    assert "Use pytest fixtures for auth database setup" in content
    assert "Pending task should not be used" not in content


def test_regenerate_learnings_dedupes_items(tmp_path: Path):
    """Duplicate learnings across tasks should be deduplicated."""
    store = _new_store(tmp_path)
    config = Config(project_dir=tmp_path, project_name="test")

    t1 = store.add("Task one", task_type="implement")
    store.mark_completed(t1, output_content="- Use uv run pytest tests/ -v\n", has_commits=False)
    t2 = store.add("Task two", task_type="implement")
    store.mark_completed(t2, output_content="- use uv run pytest tests/ -v\n", has_commits=False)

    regenerate_learnings(store, config, window=10)
    content = (tmp_path / ".gza" / "learnings.md").read_text()
    assert content.lower().count("use uv run pytest tests/ -v") == 1


def test_auto_regenerate_only_on_interval(tmp_path: Path):
    """Auto-regeneration should run only when completed count hits interval."""
    store = _new_store(tmp_path)
    config = Config(project_dir=tmp_path, project_name="test")

    for i in range(4):
        task = store.add(f"Task {i}", task_type="implement")
        store.mark_completed(task, output_content=f"- Learn {i}\n", has_commits=False)

    assert maybe_auto_regenerate_learnings(store, config, interval=5, window=10) is None

    fifth = store.add("Task 5", task_type="implement")
    store.mark_completed(fifth, output_content="- Learn 5\n", has_commits=False)

    result = maybe_auto_regenerate_learnings(store, config, interval=5, window=10)
    assert result is not None
    assert (tmp_path / ".gza" / "learnings.md").exists()


def test_regenerate_learnings_reports_delta_counts(tmp_path: Path):
    """Delta metrics should reflect previous vs regenerated learnings."""
    store = _new_store(tmp_path)
    config = Config(project_dir=tmp_path, project_name="test")

    learnings_path = tmp_path / ".gza" / "learnings.md"
    learnings_path.parent.mkdir(parents=True, exist_ok=True)
    learnings_path.write_text(
        "# Project Learnings\n\n## Recent Patterns\n- Keep old pattern\n- Remove this\n"
    )

    task = store.add("Task", task_type="implement")
    store.mark_completed(
        task,
        output_content="- Keep old pattern\n- Add new pattern\n",
        has_commits=False,
    )

    result = regenerate_learnings(store, config, window=10)
    assert result.retained_count == 1
    assert result.added_count == 1
    assert result.removed_count == 1
    assert result.churn_percent == 100.0


def test_regenerate_learnings_writes_history_log(tmp_path: Path):
    """Regeneration should append metrics to .gza/learnings_history.jsonl."""
    store = _new_store(tmp_path)
    config = Config(project_dir=tmp_path, project_name="test")

    task = store.add("Task", task_type="implement")
    store.mark_completed(task, output_content="- Keep pattern\n", has_commits=False)

    regenerate_learnings(store, config, window=10)

    history_path = tmp_path / ".gza" / "learnings_history.jsonl"
    assert history_path.exists()
    lines = history_path.read_text().strip().splitlines()
    assert len(lines) == 1
    record = json.loads(lines[0])
    assert record["window"] == 10
    assert record["tasks_used"] == 1
    assert record["learnings_count"] >= 1
    assert "added_count" in record
    assert "removed_count" in record
    assert "retained_count" in record
    assert "churn_percent" in record
    assert record["learnings_file"] == ".gza/learnings.md"
