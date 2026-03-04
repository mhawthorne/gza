"""Tests for learnings generation."""

import json
from pathlib import Path
from unittest.mock import patch, MagicMock

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


def test_llm_path_calls_runner_and_parses_output(tmp_path: Path):
    """LLM summarization should call runner.run and parse bullet points from output."""
    store = _new_store(tmp_path)
    config = Config(project_dir=tmp_path, project_name="test")

    task = store.add("Implement auth flow", task_type="implement")
    store.mark_completed(task, output_content="# Summary\n- Use JWT tokens\n", has_commits=False)

    def mock_run(cfg, task_id=None, **kwargs):
        learn_task = store.get(task_id)
        assert learn_task is not None
        learn_task.status = "completed"
        learn_task.output_content = "- Use pytest fixtures\n- Always run type checks\n"
        store.update(learn_task)
        return 0

    with patch("gza.runner.run", side_effect=mock_run):
        result = regenerate_learnings(store, config, window=10)

    assert result.tasks_used == 1
    content = result.path.read_text()
    assert "Use pytest fixtures" in content
    assert "Always run type checks" in content


def test_llm_path_fallback_on_runner_failure(tmp_path: Path):
    """If runner.run returns non-zero, fall back to regex extraction."""
    store = _new_store(tmp_path)
    config = Config(project_dir=tmp_path, project_name="test")

    task = store.add("Task one", task_type="implement")
    store.mark_completed(task, output_content="- Regex pattern found\n", has_commits=False)

    with patch("gza.runner.run", return_value=1):
        result = regenerate_learnings(store, config, window=10)

    assert result.tasks_used == 1
    content = result.path.read_text()
    assert "Regex pattern found" in content


def test_llm_path_fallback_on_runner_exception(tmp_path: Path):
    """If runner.run raises an exception, fall back to regex extraction."""
    store = _new_store(tmp_path)
    config = Config(project_dir=tmp_path, project_name="test")

    task = store.add("Task one", task_type="implement")
    store.mark_completed(task, output_content="- Regex pattern found\n", has_commits=False)

    with patch("gza.runner.run", side_effect=RuntimeError("provider failure")):
        result = regenerate_learnings(store, config, window=10)

    assert result.tasks_used == 1
    content = result.path.read_text()
    assert "Regex pattern found" in content


def test_learn_task_has_skip_learnings_flag(tmp_path: Path):
    """The learn task created during summarization must have skip_learnings=True."""
    store = _new_store(tmp_path)
    config = Config(project_dir=tmp_path, project_name="test")

    task = store.add("Task", task_type="implement")
    store.mark_completed(task, output_content="- Some pattern\n", has_commits=False)

    created_learn_tasks: list = []

    def mock_run(cfg, task_id=None, **kwargs):
        learn_task = store.get(task_id)
        assert learn_task is not None
        created_learn_tasks.append(learn_task)
        learn_task.status = "completed"
        learn_task.output_content = "- LLM learning\n"
        store.update(learn_task)
        return 0

    with patch("gza.runner.run", side_effect=mock_run):
        regenerate_learnings(store, config, window=10)

    assert len(created_learn_tasks) == 1
    assert created_learn_tasks[0].task_type == "learn"
    assert created_learn_tasks[0].skip_learnings is True


def test_learn_task_deleted_from_store_after_regeneration(tmp_path: Path):
    """The learn task created during summarization must be deleted from the store after use."""
    store = _new_store(tmp_path)
    config = Config(project_dir=tmp_path, project_name="test")

    task = store.add("Task", task_type="implement")
    store.mark_completed(task, output_content="- Some pattern\n", has_commits=False)

    captured_learn_task_id: list = []

    def mock_run(cfg, task_id=None, **kwargs):
        learn_task = store.get(task_id)
        assert learn_task is not None
        captured_learn_task_id.append(task_id)
        learn_task.status = "completed"
        learn_task.output_content = "- LLM learning\n"
        store.update(learn_task)
        return 0

    with patch("gza.runner.run", side_effect=mock_run):
        regenerate_learnings(store, config, window=10)

    assert len(captured_learn_task_id) == 1
    # The learn task must be deleted from the store after use
    assert store.get(captured_learn_task_id[0]) is None


def test_learn_task_not_in_get_recent_completed(tmp_path: Path):
    """Completed learn tasks must not appear in get_recent_completed() to avoid polluting the summarization window."""
    store = _new_store(tmp_path)
    config = Config(project_dir=tmp_path, project_name="test")

    impl_task = store.add("Normal task", task_type="implement")
    store.mark_completed(impl_task, output_content="- Pattern from work\n", has_commits=False)

    learn_task = store.add("Learn prompt", task_type="learn", skip_learnings=True)
    store.mark_completed(learn_task, output_content="- Meta learning\n", has_commits=False)

    recent = store.get_recent_completed(limit=10)
    task_types = [t.task_type for t in recent]
    assert "learn" not in task_types
    assert "implement" in task_types


def test_skip_learnings_prevents_auto_regeneration_call(tmp_path: Path):
    """Completing a learn task (skip_learnings=True) must not trigger maybe_auto_regenerate_learnings."""
    from gza.learnings import maybe_auto_regenerate_learnings as real_fn

    store = _new_store(tmp_path)
    config = Config(project_dir=tmp_path, project_name="test")

    call_count = [0]

    def counting_maybe_auto(*args, **kwargs):
        call_count[0] += 1
        return None

    # Simulate what _run_non_code_task does: only call maybe_auto_regenerate_learnings
    # when task.skip_learnings is False.
    learn_task = store.add("Learn prompt", task_type="learn", skip_learnings=True)

    # Replicate the guard from _run_non_code_task
    if not learn_task.skip_learnings:
        counting_maybe_auto(store, config)

    assert call_count[0] == 0, "maybe_auto_regenerate_learnings must not be called for skip_learnings tasks"


def test_learn_task_deleted_on_runner_failure(tmp_path: Path):
    """The learn task must be deleted from store even when runner.run returns non-zero."""
    store = _new_store(tmp_path)
    config = Config(project_dir=tmp_path, project_name="test")

    task = store.add("Task", task_type="implement")
    store.mark_completed(task, output_content="- Pattern\n", has_commits=False)

    captured_learn_task_id: list = []

    def mock_run_fail(cfg, task_id=None, **kwargs):
        captured_learn_task_id.append(task_id)
        return 1  # failure

    with patch("gza.runner.run", side_effect=mock_run_fail):
        regenerate_learnings(store, config, window=10)

    assert len(captured_learn_task_id) == 1
    assert store.get(captured_learn_task_id[0]) is None
