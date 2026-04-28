"""Tests for extraction planning helpers."""

from datetime import UTC, datetime
from pathlib import Path

import pytest

from gza.db import SqliteTaskStore
from gza.extractions import (
    ExtractionError,
    copy_bundle_to_worktree,
    normalize_selected_paths,
    plan_extraction,
    resolve_source_selection,
    write_extraction_bundle,
)
from gza.git import Git


def _init_repo(tmp_path: Path) -> Git:
    git = Git(tmp_path)
    git._run("init", "-b", "main")
    git._run("config", "user.name", "Test User")
    git._run("config", "user.email", "test@example.com")
    (tmp_path / "base.txt").write_text("base\n")
    git._run("add", "base.txt")
    git._run("commit", "-m", "initial")
    return git


def test_normalize_selected_paths_rejects_escape() -> None:
    with pytest.raises(ExtractionError, match="within the repository root"):
        normalize_selected_paths(["../outside.py"])


def test_plan_extraction_and_bundle_roundtrip(tmp_path: Path) -> None:
    git = _init_repo(tmp_path)
    store = SqliteTaskStore(tmp_path / "test.db", prefix="gza")

    source_task = store.add("Source", task_type="implement")
    source_task.status = "completed"
    source_task.completed_at = datetime.now(UTC)
    source_task.branch = "feature/source"
    source_task.slug = "20260427-source-task"
    store.update(source_task)

    git._run("checkout", "-b", source_task.branch)
    (tmp_path / "src").mkdir(exist_ok=True)
    (tmp_path / "src" / "module.py").write_text("value = 1\n")
    git._run("add", "src/module.py")
    git._run("commit", "-m", "add module")
    git._run("checkout", "main")

    source = resolve_source_selection(
        store,
        git,
        source_task_id=source_task.id,
        source_branch=None,
        base_branch_override=None,
    )

    selected = normalize_selected_paths(["src/module.py"])
    draft = plan_extraction(
        git,
        source,
        selected,
        operator_prompt="Carry this change into a clean task.",
    )

    assert "diff --git" in draft.patch
    assert draft.touched_paths == ("src/module.py",)
    assert len(draft.file_summaries) == 1
    assert draft.file_summaries[0].additions == 1
    assert draft.file_summaries[0].deletions == 0
    assert draft.file_summaries[0].binary is False
    assert "Operator intent:" in draft.prompt

    target_task = store.add(draft.prompt, task_type="implement")
    target_task.slug = "20260427-target-task"
    store.update(target_task)

    bundle_dir = write_extraction_bundle(project_dir=tmp_path, task=target_task, draft=draft)
    assert (bundle_dir / "manifest.json").exists()
    assert (bundle_dir / "selected.patch").exists()
    assert (bundle_dir / "prompt.md").exists()

    worktree = tmp_path / "worktree"
    worktree.mkdir()
    copied = copy_bundle_to_worktree(bundle_dir, worktree)
    assert copied.exists()
    assert (copied / "manifest.json").exists()


def test_plan_extraction_supports_quoted_diff_headers_for_spaced_paths(tmp_path: Path) -> None:
    git = _init_repo(tmp_path)
    store = SqliteTaskStore(tmp_path / "test.db", prefix="gza")

    source_task = store.add("Source", task_type="implement")
    source_task.status = "completed"
    source_task.completed_at = datetime.now(UTC)
    source_task.branch = "feature/source"
    source_task.slug = "20260427-source-task-spaces"
    store.update(source_task)

    git._run("checkout", "-b", source_task.branch)
    (tmp_path / "src").mkdir(exist_ok=True)
    spaced_path = "src/file with space.py"
    (tmp_path / spaced_path).write_text("value = 1\n")
    git._run("add", spaced_path)
    git._run("commit", "-m", "add spaced path")
    git._run("checkout", "main")

    source = resolve_source_selection(
        store,
        git,
        source_task_id=source_task.id,
        source_branch=None,
        base_branch_override=None,
    )
    selected = normalize_selected_paths([spaced_path])
    draft = plan_extraction(
        git,
        source,
        selected,
        operator_prompt=None,
    )

    assert draft.touched_paths == (spaced_path,)


def test_plan_extraction_marks_binary_numstat_entries(tmp_path: Path) -> None:
    git = _init_repo(tmp_path)
    store = SqliteTaskStore(tmp_path / "test.db", prefix="gza")

    source_task = store.add("Source", task_type="implement")
    source_task.status = "completed"
    source_task.completed_at = datetime.now(UTC)
    source_task.branch = "feature/source"
    source_task.slug = "20260427-source-task-binary"
    store.update(source_task)

    git._run("checkout", "-b", source_task.branch)
    (tmp_path / "assets").mkdir(exist_ok=True)
    binary_path = "assets/image.bin"
    (tmp_path / binary_path).write_bytes(b"\x00\x01\x02\x03")
    git._run("add", binary_path)
    git._run("commit", "-m", "add binary file")
    git._run("checkout", "main")

    source = resolve_source_selection(
        store,
        git,
        source_task_id=source_task.id,
        source_branch=None,
        base_branch_override=None,
    )
    selected = normalize_selected_paths([binary_path])
    draft = plan_extraction(
        git,
        source,
        selected,
        operator_prompt=None,
    )

    assert len(draft.file_summaries) == 1
    summary = draft.file_summaries[0]
    assert summary.selected_path == binary_path
    assert summary.binary is True
    assert summary.additions is None
    assert summary.deletions is None
    assert f"- A: {binary_path} [binary]" in draft.prompt


def test_resolve_source_selection_rejects_non_code_task(tmp_path: Path) -> None:
    git = _init_repo(tmp_path)
    store = SqliteTaskStore(tmp_path / "test.db", prefix="gza")

    source_task = store.add("Plan source", task_type="plan")
    source_task.status = "completed"
    source_task.completed_at = datetime.now(UTC)
    source_task.branch = "feature/plan-source"
    store.update(source_task)

    git._run("checkout", "-b", source_task.branch)
    (tmp_path / "notes.md").write_text("plan\n")
    git._run("add", "notes.md")
    git._run("commit", "-m", "plan")
    git._run("checkout", "main")

    with pytest.raises(ExtractionError, match="must be a code task"):
        resolve_source_selection(
            store,
            git,
            source_task_id=source_task.id,
            source_branch=None,
            base_branch_override=None,
        )


def test_write_extraction_bundle_rejects_path_reuse_for_different_task(tmp_path: Path) -> None:
    git = _init_repo(tmp_path)
    store = SqliteTaskStore(tmp_path / "test.db", prefix="gza")

    source_task = store.add("Source", task_type="implement")
    source_task.status = "completed"
    source_task.completed_at = datetime.now(UTC)
    source_task.branch = "feature/source"
    source_task.slug = "20260427-source-task"
    store.update(source_task)

    git._run("checkout", "-b", source_task.branch)
    (tmp_path / "src").mkdir(exist_ok=True)
    (tmp_path / "src" / "module.py").write_text("value = 1\n")
    git._run("add", "src/module.py")
    git._run("commit", "-m", "add module")
    git._run("checkout", "main")

    source = resolve_source_selection(
        store,
        git,
        source_task_id=source_task.id,
        source_branch=None,
        base_branch_override=None,
    )
    selected = normalize_selected_paths(["src/module.py"])
    draft = plan_extraction(
        git,
        source,
        selected,
        operator_prompt=None,
    )

    first = store.add(draft.prompt, task_type="implement")
    first.slug = "20260427-shared-target"
    store.update(first)
    write_extraction_bundle(project_dir=tmp_path, task=first, draft=draft)

    second = store.add(draft.prompt, task_type="implement")
    second.slug = "20260427-shared-target"
    store.update(second)

    with pytest.raises(ExtractionError, match="already reserved for task"):
        write_extraction_bundle(project_dir=tmp_path, task=second, draft=draft)
