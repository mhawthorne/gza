"""Tests for split log path helpers."""

from pathlib import Path

from gza.config import Config
from gza.db import SqliteTaskStore
from gza.log_paths import ops_log_path_for, resolve_task_log_paths


def test_resolve_task_log_paths_derives_split_and_startup_siblings(tmp_path: Path) -> None:
    config = Config(project_dir=tmp_path, project_name="test-project")
    store = SqliteTaskStore(tmp_path / "test.db")
    task = store.add("split logs")
    assert task.id is not None

    paths = resolve_task_log_paths(config, task)

    assert paths.conversation == config.log_path / f"{task.id}.startup.log"
    assert paths.ops == config.log_path / f"{task.id}.startup.ops.jsonl"
    assert paths.startup_conversation == paths.conversation
    assert paths.startup_ops == paths.ops


def test_ops_log_path_for_slugged_conversation_log() -> None:
    conversation = Path("/tmp/project/.gza/logs/20260508-split.log")
    assert ops_log_path_for(conversation) == Path("/tmp/project/.gza/logs/20260508-split.ops.jsonl")
