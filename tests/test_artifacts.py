"""Tests for durable task artifact helper functions."""

import hashlib
from datetime import datetime
from pathlib import Path

import pytest

from gza.artifacts import store_command_output_artifact
from gza.config import Config
from gza.db import SqliteTaskStore


@pytest.mark.timeout(4, method="signal")
def test_store_command_output_artifact_preserves_full_output_byte_for_byte(tmp_path: Path) -> None:
    config = Config(project_dir=tmp_path, project_name="demo")
    store = SqliteTaskStore(config.db_path, prefix="gza")
    task = store.add("persist verify output")

    output = (
        "test session starts\n"
        "...\n"
        "FAILED tests/test_example.py::test_case - AssertionError: boom\n"
        "=== short test summary info ===\n"
        "FAILED tests/test_example.py::test_case - AssertionError: boom\n"
    )
    stored = store_command_output_artifact(
        store,
        task,
        config,
        kind="verify_command_output",
        producer="review_verify",
        label="verify command",
        output=output,
        command="./bin/tests",
        status="failed",
        exit_status="1",
        head_sha="deadbeef",
    )

    artifact_path = tmp_path / stored.path
    assert artifact_path.read_bytes() == output.encode("utf-8")


@pytest.mark.timeout(4, method="signal")
def test_store_command_output_artifact_sanitizes_label_and_scope_for_paths(tmp_path: Path) -> None:
    config = Config(project_dir=tmp_path, project_name="demo")
    store = SqliteTaskStore(config.db_path, prefix="gza")
    task = store.add("sanitize artifact path")

    stored = store_command_output_artifact(
        store,
        task,
        config,
        kind="verify_command_output",
        producer="review_verify",
        label="../verify\\command",
        scope="../pkg/tests\\unit",
        output="failure output\n",
    )

    file_name = Path(stored.path).name
    assert ".." not in file_name
    assert "/" not in file_name
    assert "\\" not in file_name
    assert "verify-command" in file_name
    assert "pkg-tests-unit" in file_name


@pytest.mark.timeout(4, method="signal")
def test_store_command_output_artifact_records_sha256_and_byte_size(tmp_path: Path) -> None:
    config = Config(project_dir=tmp_path, project_name="demo")
    store = SqliteTaskStore(config.db_path, prefix="gza")
    task = store.add("record digest and size")

    output = "line one\nline two\n"
    stored = store_command_output_artifact(
        store,
        task,
        config,
        kind="verify_command_output",
        producer="review_verify",
        label="verify",
        output=output,
    )
    artifact = store.get_artifact(stored.id, task_id=task.id)

    assert artifact is not None
    assert stored.bytes == len(output.encode("utf-8"))
    assert stored.digest == hashlib.sha256(output.encode("utf-8")).hexdigest()
    assert artifact.byte_size == stored.bytes
    assert artifact.sha256 == stored.digest


@pytest.mark.timeout(4, method="signal")
def test_store_command_output_artifact_allows_metadata_only_rows_without_creating_file(tmp_path: Path) -> None:
    config = Config(project_dir=tmp_path, project_name="demo")
    store = SqliteTaskStore(config.db_path, prefix="gza")
    task = store.add("metadata only artifact")

    stored = store_command_output_artifact(
        store,
        task,
        config,
        kind="verify_command_output",
        producer="review_verify",
        label="verify",
        output="",
        status="unavailable",
        exit_status="launch failed",
        metadata={"scope": "pkg/api"},
    )
    artifact = store.get_artifact(stored.id, task_id=task.id)
    artifact_path = tmp_path / stored.path

    assert artifact is not None
    assert artifact.metadata == {"scope": "pkg/api"}
    assert stored.bytes == 0
    assert stored.digest == hashlib.sha256(b"").hexdigest()
    assert artifact_path.exists() is False


@pytest.mark.timeout(4, method="signal")
def test_store_command_output_artifact_uses_unique_paths_for_same_timestamp(tmp_path: Path) -> None:
    config = Config(project_dir=tmp_path, project_name="demo")
    store = SqliteTaskStore(config.db_path, prefix="gza")
    task = store.add("avoid overwriting artifact history")
    created_at = datetime.fromisoformat("2026-06-01T19:00:00+00:00")

    first = store_command_output_artifact(
        store,
        task,
        config,
        kind="verify_command_output",
        producer="review_verify",
        label="verify",
        scope="pkg/api",
        output="first output\n",
        created_at=created_at,
    )
    second = store_command_output_artifact(
        store,
        task,
        config,
        kind="verify_command_output",
        producer="review_verify",
        label="verify",
        scope="pkg/api",
        output="second output\n",
        created_at=created_at,
    )

    assert first.path != second.path
    assert (tmp_path / first.path).read_text(encoding="utf-8") == "first output\n"
    assert (tmp_path / second.path).read_text(encoding="utf-8") == "second output\n"

    first_row = store.get_artifact(first.id, task_id=task.id)
    second_row = store.get_artifact(second.id, task_id=task.id)
    assert first_row is not None
    assert second_row is not None
    assert first_row.sha256 == hashlib.sha256(b"first output\n").hexdigest()
    assert first_row.byte_size == len(b"first output\n")
    assert second_row.sha256 == hashlib.sha256(b"second output\n").hexdigest()
    assert second_row.byte_size == len(b"second output\n")
