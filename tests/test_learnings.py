"""Tests for learnings generation."""

import json
import subprocess
from pathlib import Path
from unittest.mock import patch, MagicMock

from gza.config import Config
from gza.db import SqliteTaskStore
from gza.learnings import LearningsResult, regenerate_learnings, maybe_auto_regenerate_learnings


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
    """Auto-regeneration should spawn background update only on interval."""
    store = _new_store(tmp_path)
    config = Config(project_dir=tmp_path, project_name="test")

    for i in range(4):
        task = store.add(f"Task {i}", task_type="implement")
        store.mark_completed(task, output_content=f"- Learn pattern {i}\n", has_commits=False)

    with patch("gza.learnings.subprocess.Popen") as mock_popen:
        assert maybe_auto_regenerate_learnings(store, config, interval=5, window=10) is None
    mock_popen.assert_not_called()

    fifth = store.add("Task 5", task_type="implement")
    store.mark_completed(fifth, output_content="- Learn pattern 5\n", has_commits=False)

    def _run_background_inline(*args, **kwargs):
        with patch("gza.runner.run", return_value=1):
            regenerate_learnings(store, config, window=10)
        return MagicMock(pid=99999)

    with patch("gza.learnings.subprocess.Popen", side_effect=_run_background_inline) as mock_popen:
        result = maybe_auto_regenerate_learnings(store, config, interval=5, window=10)

    assert result is None
    mock_popen.assert_called_once()
    args, kwargs = mock_popen.call_args
    cmd = args[0]
    assert cmd[:5] == ["uv", "run", "gza", "learnings", "update"]
    assert "--window" in cmd
    assert "--project" in cmd
    assert kwargs["start_new_session"] is True
    assert kwargs["stdin"] == subprocess.DEVNULL

    learnings_file = tmp_path / ".gza" / "learnings.md"
    assert learnings_file.exists()
    assert "Learn pattern 5" in learnings_file.read_text()


def test_auto_regenerate_async_falls_back_to_regex_when_llm_fails(tmp_path: Path):
    """Async path should still write learnings via regex fallback when LLM fails."""
    store = _new_store(tmp_path)
    config = Config(project_dir=tmp_path, project_name="test")

    for i in range(5):
        task = store.add(f"Task {i}", task_type="implement")
        store.mark_completed(task, output_content=f"- Regex fallback {i}\n", has_commits=False)

    def _run_background_inline(*args, **kwargs):
        with patch("gza.runner.run", side_effect=RuntimeError("provider failure")):
            regenerate_learnings(store, config, window=10)
        return MagicMock(pid=99998)

    with patch("gza.learnings.subprocess.Popen", side_effect=_run_background_inline):
        result = maybe_auto_regenerate_learnings(store, config, interval=5, window=10)

    assert result is None
    content = (tmp_path / ".gza" / "learnings.md").read_text()
    assert "Regex fallback 0" in content


def test_auto_regenerate_spawn_failure_runs_foreground_fallback(tmp_path: Path):
    """Spawn failure should run foreground fallback and avoid stranded internal pending tasks."""
    store = _new_store(tmp_path)
    config = Config(project_dir=tmp_path, project_name="test")

    for i in range(5):
        task = store.add(f"Task {i}", task_type="implement")
        store.mark_completed(task, output_content=f"- Learn {i}\n", has_commits=False)

    fallback_result = LearningsResult(
        path=tmp_path / ".gza" / "learnings.md",
        tasks_used=5,
        learnings_count=5,
        added_count=5,
        removed_count=0,
        retained_count=0,
        churn_percent=500.0,
    )

    with patch("gza.learnings.subprocess.Popen", side_effect=OSError("spawn exploded")), \
         patch("gza.learnings.regenerate_learnings", return_value=fallback_result) as mock_regen, \
         patch("gza.learnings.console.print") as mock_print:
        result = maybe_auto_regenerate_learnings(store, config, interval=5, window=10)

    assert result == fallback_result
    mock_regen.assert_called_once_with(store, config, window=10)
    assert any("foreground regeneration fallback" in str(call.args[0]) for call in mock_print.call_args_list)
    assert [t for t in store.get_pending() if t.task_type == "internal"] == []


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


def test_internal_task_kept_after_success(tmp_path: Path):
    """The internal task should remain in the store after successful summarization."""
    store = _new_store(tmp_path)
    config = Config(project_dir=tmp_path, project_name="test")

    task = store.add("Task", task_type="implement")
    store.mark_completed(task, output_content="- Some pattern\n", has_commits=False)

    captured_internal_task_id: list = []

    def mock_run(cfg, task_id=None, **kwargs):
        captured_internal_task_id.append(task_id)
        internal_task = store.get(task_id)
        internal_task.status = "completed"
        internal_task.output_content = "- LLM learning\n"
        store.update(internal_task)
        return 0

    with patch("gza.runner.run", side_effect=mock_run):
        regenerate_learnings(store, config, window=10)

    assert len(captured_internal_task_id) == 1
    assert store.get(captured_internal_task_id[0]) is not None


def test_internal_task_has_skip_learnings_flag(tmp_path: Path):
    """The internal task must have skip_learnings=True to prevent recursion."""
    store = _new_store(tmp_path)
    config = Config(project_dir=tmp_path, project_name="test")

    task = store.add("Task", task_type="implement")
    store.mark_completed(task, output_content="- Some pattern\n", has_commits=False)

    def mock_run(cfg, task_id=None, **kwargs):
        internal_task = store.get(task_id)
        assert internal_task.task_type == "internal"
        assert internal_task.skip_learnings is True
        internal_task.status = "completed"
        internal_task.output_content = "- LLM learning\n"
        store.update(internal_task)
        return 0

    with patch("gza.runner.run", side_effect=mock_run):
        regenerate_learnings(store, config, window=10)


def test_internal_task_kept_after_failure(tmp_path: Path):
    """The internal task should remain in the store even when the runner fails."""
    store = _new_store(tmp_path)
    config = Config(project_dir=tmp_path, project_name="test")

    task = store.add("Task", task_type="implement")
    store.mark_completed(task, output_content="- Pattern\n", has_commits=False)

    captured_internal_task_id: list = []

    def mock_run_fail(cfg, task_id=None, **kwargs):
        captured_internal_task_id.append(task_id)
        return 1  # failure

    with patch("gza.runner.run", side_effect=mock_run_fail):
        regenerate_learnings(store, config, window=10)

    assert len(captured_internal_task_id) == 1
    assert store.get(captured_internal_task_id[0]) is not None


def test_internal_task_not_in_get_recent_completed(tmp_path: Path):
    """Completed internal tasks must not appear in get_recent_completed() to avoid polluting summarization windows."""
    store = _new_store(tmp_path)

    impl_task = store.add("Normal task", task_type="implement")
    store.mark_completed(impl_task, output_content="- Pattern from work\n", has_commits=False)

    internal_task = store.add("Learn prompt", task_type="internal", skip_learnings=True)
    store.mark_completed(internal_task, output_content="- Meta learning\n", has_commits=False)

    recent = store.get_recent_completed(limit=10)
    task_types = [t.task_type for t in recent]
    assert "internal" not in task_types
    assert "implement" in task_types


def test_skip_learnings_prevents_auto_regeneration_call(tmp_path: Path):
    """Completing an internal task (skip_learnings=True) must not trigger maybe_auto_regenerate_learnings."""
    from unittest.mock import Mock
    from gza.runner import _run_non_code_task
    from gza.providers.base import RunResult

    store = _new_store(tmp_path)
    config = Mock(spec=Config)
    config.project_dir = tmp_path
    config.log_path = tmp_path / ".gza" / "logs"
    config.log_path.mkdir(parents=True, exist_ok=True)
    config.worktree_path = tmp_path / "worktrees"
    config.worktree_path.mkdir(parents=True, exist_ok=True)
    config.use_docker = False

    internal_task = store.add("Learn prompt", task_type="internal", skip_learnings=True)
    internal_task.task_id = "20260101-learn-prompt"
    store.update(internal_task)

    mock_provider = Mock()
    mock_provider.name = "MockProvider"
    mock_provider.run.return_value = RunResult(
        exit_code=0,
        duration_seconds=1.0,
        num_turns_reported=1,
        cost_usd=0.001,
        session_id="test-session",
        error_type=None,
    )

    mock_git = Mock()
    mock_git.default_branch.return_value = "main"
    mock_git._run.return_value = Mock(returncode=0)

    report_dir = tmp_path / "worktrees" / f"{internal_task.task_id}-internal" / ".gza" / "internal"
    report_dir.mkdir(parents=True, exist_ok=True)
    (report_dir / f"{internal_task.task_id}.md").write_text("# Internal report\n")

    with patch("gza.runner.console"), \
         patch("gza.runner.maybe_auto_regenerate_learnings") as mock_auto:
        _run_non_code_task(internal_task, config, store, mock_provider, mock_git)

    mock_auto.assert_not_called()
