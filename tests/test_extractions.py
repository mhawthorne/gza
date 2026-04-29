"""Tests for extraction planning helpers."""

from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from gza.db import SqliteTaskStore
from gza.extractions import (
    ExtractionError,
    _parse_file_summaries,
    copy_bundle_to_worktree,
    infer_selected_paths,
    normalize_selected_paths,
    plan_extraction,
    resolve_source_selection,
    SourceSelection,
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


def test_infer_selected_paths_returns_full_source_diff(tmp_path: Path) -> None:
    git = _init_repo(tmp_path)
    store = SqliteTaskStore(tmp_path / "test.db", prefix="gza")

    source_task = store.add("Source", task_type="implement")
    source_task.status = "completed"
    source_task.completed_at = datetime.now(UTC)
    source_task.branch = "feature/source-full-diff"
    store.update(source_task)

    git._run("checkout", "-b", source_task.branch)
    (tmp_path / "src").mkdir(exist_ok=True)
    (tmp_path / "src" / "module.py").write_text("value = 1\n")
    (tmp_path / "src" / "second.py").write_text("value = 2\n")
    git._run("add", "src/module.py")
    git._run("add", "src/second.py")
    git._run("commit", "-m", "add files")
    git._run("checkout", "main")

    source = resolve_source_selection(
        store,
        git,
        source_task_id=source_task.id,
        source_branch=None,
        base_branch_override=None,
    )

    assert infer_selected_paths(git, source) == ("src/module.py", "src/second.py")


def test_infer_selected_paths_rejects_empty_source_diff(tmp_path: Path) -> None:
    git = _init_repo(tmp_path)
    store = SqliteTaskStore(tmp_path / "test.db", prefix="gza")

    source_task = store.add("Source", task_type="implement")
    source_task.status = "completed"
    source_task.completed_at = datetime.now(UTC)
    source_task.branch = "feature/source-empty-diff"
    store.update(source_task)

    git._run("checkout", "-b", source_task.branch)
    git._run("checkout", "main")

    source = resolve_source_selection(
        store,
        git,
        source_task_id=source_task.id,
        source_branch=None,
        base_branch_override=None,
    )

    with pytest.raises(ExtractionError, match="no extractable diff"):
        infer_selected_paths(git, source)


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


def test_plan_extraction_populates_text_diffstat_metadata(tmp_path: Path) -> None:
    git = _init_repo(tmp_path)
    store = SqliteTaskStore(tmp_path / "test.db", prefix="gza")

    source_task = store.add("Source", task_type="implement")
    source_task.status = "completed"
    source_task.completed_at = datetime.now(UTC)
    source_task.branch = "feature/source"
    source_task.slug = "20260427-source-task-diffstat"
    store.update(source_task)

    git._run("checkout", "-b", source_task.branch)
    target_path = tmp_path / "src" / "module.py"
    target_path.parent.mkdir(exist_ok=True)
    target_path.write_text("line1\nline2\n")
    git._run("add", "src/module.py")
    git._run("commit", "-m", "add module")
    target_path.write_text("line1\nline3\nline4\n")
    git._run("add", "src/module.py")
    git._run("commit", "-m", "update module")
    git._run("checkout", "main")

    source = resolve_source_selection(
        store,
        git,
        source_task_id=source_task.id,
        source_branch=None,
        base_branch_override=None,
    )
    selected = normalize_selected_paths(["src/module.py"])
    draft = plan_extraction(git, source, selected, operator_prompt=None)

    summary = draft.file_summaries[0]
    assert summary.selected_path == "src/module.py"
    assert summary.additions == 3
    assert summary.deletions == 0
    assert summary.binary is False


def test_plan_extraction_marks_binary_diffs_in_file_summaries(tmp_path: Path) -> None:
    git = _init_repo(tmp_path)
    store = SqliteTaskStore(tmp_path / "test.db", prefix="gza")

    source_task = store.add("Source", task_type="implement")
    source_task.status = "completed"
    source_task.completed_at = datetime.now(UTC)
    source_task.branch = "feature/source-binary"
    source_task.slug = "20260427-source-task-binary"
    store.update(source_task)

    git._run("checkout", "-b", source_task.branch)
    image_path = tmp_path / "assets" / "logo.bin"
    image_path.parent.mkdir(exist_ok=True)
    image_path.write_bytes(b"\x00\x01\x02\x03")
    git._run("add", "assets/logo.bin")
    git._run("commit", "-m", "add binary")
    image_path.write_bytes(b"\x00\x04\x05\x06")
    git._run("add", "assets/logo.bin")
    git._run("commit", "-m", "update binary")
    git._run("checkout", "main")

    source = resolve_source_selection(
        store,
        git,
        source_task_id=source_task.id,
        source_branch=None,
        base_branch_override=None,
    )
    selected = normalize_selected_paths(["assets/logo.bin"])
    draft = plan_extraction(git, source, selected, operator_prompt=None)

    summary = draft.file_summaries[0]
    assert summary.selected_path == "assets/logo.bin"
    assert summary.additions is None
    assert summary.deletions is None
    assert summary.binary is True


def test_plan_extraction_populates_renamed_text_diffstat_metadata(tmp_path: Path) -> None:
    git = _init_repo(tmp_path)
    store = SqliteTaskStore(tmp_path / "test.db", prefix="gza")

    source_task = store.add("Source", task_type="implement")
    source_task.status = "completed"
    source_task.completed_at = datetime.now(UTC)
    source_task.branch = "feature/source-rename-text"
    source_task.slug = "20260428-source-task-rename-text"
    store.update(source_task)

    before = tmp_path / "src" / "old_name.py"
    before.parent.mkdir(exist_ok=True)
    before.write_text("line1\nline2\n")
    git._run("add", "src/old_name.py")
    git._run("commit", "-m", "add old path on main")

    git._run("checkout", "-b", source_task.branch)
    git._run("mv", "src/old_name.py", "src/new_name.py")
    git._run("commit", "-m", "rename file")
    git._run("checkout", "main")

    source = resolve_source_selection(
        store,
        git,
        source_task_id=source_task.id,
        source_branch=None,
        base_branch_override=None,
    )
    selected = normalize_selected_paths(["src/old_name.py", "src/new_name.py"])
    draft = plan_extraction(git, source, selected, operator_prompt=None)

    summary = next(
        item
        for item in draft.file_summaries
        if item.old_path == "src/old_name.py" and item.new_path == "src/new_name.py"
    )
    assert summary.status == "R"
    assert summary.selected_path in {"src/old_name.py", "src/new_name.py"}
    assert summary.old_path == "src/old_name.py"
    assert summary.new_path == "src/new_name.py"
    assert summary.additions == 0
    assert summary.deletions == 0
    assert summary.binary is False


def test_plan_extraction_marks_renamed_binary_diffstat_metadata(tmp_path: Path) -> None:
    git = _init_repo(tmp_path)
    store = SqliteTaskStore(tmp_path / "test.db", prefix="gza")

    source_task = store.add("Source", task_type="implement")
    source_task.status = "completed"
    source_task.completed_at = datetime.now(UTC)
    source_task.branch = "feature/source-rename-binary"
    source_task.slug = "20260428-source-task-rename-binary"
    store.update(source_task)

    before = tmp_path / "assets" / "old_logo.bin"
    before.parent.mkdir(exist_ok=True)
    before.write_bytes(b"\x00\x01\x02\x03")
    git._run("add", "assets/old_logo.bin")
    git._run("commit", "-m", "add old binary on main")

    git._run("checkout", "-b", source_task.branch)
    git._run("mv", "assets/old_logo.bin", "assets/new_logo.bin")
    git._run("commit", "-m", "rename binary file")
    git._run("checkout", "main")

    source = resolve_source_selection(
        store,
        git,
        source_task_id=source_task.id,
        source_branch=None,
        base_branch_override=None,
    )
    selected = normalize_selected_paths(["assets/old_logo.bin", "assets/new_logo.bin"])
    draft = plan_extraction(git, source, selected, operator_prompt=None)

    summary = next(
        item
        for item in draft.file_summaries
        if item.old_path == "assets/old_logo.bin" and item.new_path == "assets/new_logo.bin"
    )
    assert summary.status == "R"
    assert summary.selected_path in {"assets/old_logo.bin", "assets/new_logo.bin"}
    assert summary.old_path == "assets/old_logo.bin"
    assert summary.new_path == "assets/new_logo.bin"
    assert summary.additions is None
    assert summary.deletions is None
    assert summary.binary is True


def test_plan_extraction_populates_copied_text_diffstat_metadata(tmp_path: Path) -> None:
    git = _init_repo(tmp_path)
    store = SqliteTaskStore(tmp_path / "test.db", prefix="gza")

    source_task = store.add("Source", task_type="implement")
    source_task.status = "completed"
    source_task.completed_at = datetime.now(UTC)
    source_task.branch = "feature/source-copy-text"
    source_task.slug = "20260428-source-task-copy-text"
    store.update(source_task)

    before = tmp_path / "src" / "original.py"
    before.parent.mkdir(exist_ok=True)
    before.write_text("line1\nline2\n")
    git._run("add", "src/original.py")
    git._run("commit", "-m", "add original file on main")

    git._run("checkout", "-b", source_task.branch)
    (tmp_path / "src" / "copied.py").write_text(before.read_text())
    git._run("add", "src/copied.py")
    git._run("commit", "-m", "copy file")
    git._run("checkout", "main")

    source = resolve_source_selection(
        store,
        git,
        source_task_id=source_task.id,
        source_branch=None,
        base_branch_override=None,
    )
    selected = normalize_selected_paths(["src/original.py", "src/copied.py"])
    draft = plan_extraction(git, source, selected, operator_prompt=None)

    summary = next(
        item
        for item in draft.file_summaries
        if item.old_path == "src/original.py" and item.new_path == "src/copied.py"
    )
    assert summary.status == "C"
    assert summary.selected_path in {"src/original.py", "src/copied.py"}
    assert summary.old_path == "src/original.py"
    assert summary.new_path == "src/copied.py"
    assert summary.additions == 0
    assert summary.deletions == 0
    assert summary.binary is False


def test_plan_extraction_marks_binary_numstat_entries(tmp_path: Path) -> None:
    git = _init_repo(tmp_path)
    store = SqliteTaskStore(tmp_path / "test.db", prefix="gza")

    source_task = store.add("Source", task_type="implement")
    source_task.status = "completed"
    source_task.completed_at = datetime.now(UTC)
    source_task.branch = "feature/source-binary-prompt"
    source_task.slug = "20260427-source-task-binary-prompt"
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
    draft = plan_extraction(git, source, selected, operator_prompt=None)

    assert len(draft.file_summaries) == 1
    summary = draft.file_summaries[0]
    assert summary.selected_path == binary_path
    assert summary.binary is True
    assert summary.additions is None
    assert summary.deletions is None
    assert f"- A: {binary_path} [binary]" in draft.prompt


def test_parse_file_summaries_braced_rename_numstat() -> None:
    name_status_text = "R100\tsrc/old_name.py\tsrc/new_name.py\n"
    numstat_text = "3\t2\tsrc/{old_name.py => new_name.py}\n"

    summaries = _parse_file_summaries(
        name_status_text=name_status_text,
        numstat_text=numstat_text,
        selected_paths=("src/old_name.py", "src/new_name.py"),
    )

    assert len(summaries) == 1
    summary = summaries[0]
    assert summary.status == "R"
    assert summary.old_path == "src/old_name.py"
    assert summary.new_path == "src/new_name.py"
    assert summary.additions == 3
    assert summary.deletions == 2
    assert summary.binary is False


def test_parse_file_summaries_braced_quoted_binary_copy_numstat() -> None:
    name_status_text = "C100\tsrc/old file.bin\tsrc/new file.bin\n"
    numstat_text = '-\t-\t"src/{old file.bin => new file.bin}"\n'

    summaries = _parse_file_summaries(
        name_status_text=name_status_text,
        numstat_text=numstat_text,
        selected_paths=("src/old file.bin", "src/new file.bin"),
    )

    assert len(summaries) == 1
    summary = summaries[0]
    assert summary.status == "C"
    assert summary.old_path == "src/old file.bin"
    assert summary.new_path == "src/new file.bin"
    assert summary.additions is None
    assert summary.deletions is None
    assert summary.binary is True


def test_plan_extraction_parses_numstat_rename_arrow() -> None:
    source = SourceSelection(
        source_task_id="gza-1",
        source_branch="feature/source",
        source_base_ref="main",
    )
    selected = ("src/old_name.py", "src/new_name.py")
    git = MagicMock(spec=Git)
    git.get_diff_name_status.return_value = "R100\tsrc/old_name.py\tsrc/new_name.py\n"
    git.get_diff_numstat.return_value = "4\t1\tsrc/old_name.py -> src/new_name.py\n"
    git.get_diff_patch_for_paths.return_value = (
        "diff --git a/src/old_name.py b/src/new_name.py\n"
        "similarity index 100%\n"
        "rename from src/old_name.py\n"
        "rename to src/new_name.py\n"
    )

    draft = plan_extraction(git, source, selected, operator_prompt=None)

    assert len(draft.file_summaries) == 1
    summary = draft.file_summaries[0]
    assert summary.status == "R"
    assert summary.old_path == "src/old_name.py"
    assert summary.new_path == "src/new_name.py"
    assert summary.additions == 4
    assert summary.deletions == 1
    assert summary.binary is False


def test_plan_extraction_parses_numstat_rename_brace() -> None:
    source = SourceSelection(
        source_task_id="gza-1",
        source_branch="feature/source",
        source_base_ref="main",
    )
    selected = ("src/old_name.py", "src/new_name.py")
    git = MagicMock(spec=Git)
    git.get_diff_name_status.return_value = "R100\tsrc/old_name.py\tsrc/new_name.py\n"
    git.get_diff_numstat.return_value = "3\t2\tsrc/{old_name.py => new_name.py}\n"
    git.get_diff_patch_for_paths.return_value = (
        "diff --git a/src/old_name.py b/src/new_name.py\n"
        "similarity index 100%\n"
        "rename from src/old_name.py\n"
        "rename to src/new_name.py\n"
    )

    draft = plan_extraction(git, source, selected, operator_prompt=None)

    assert len(draft.file_summaries) == 1
    summary = draft.file_summaries[0]
    assert summary.status == "R"
    assert summary.old_path == "src/old_name.py"
    assert summary.new_path == "src/new_name.py"
    assert summary.additions == 3
    assert summary.deletions == 2
    assert summary.binary is False


def test_plan_extraction_parses_numstat_copy() -> None:
    source = SourceSelection(
        source_task_id="gza-1",
        source_branch="feature/source",
        source_base_ref="main",
    )
    selected = ("src/original.py", "src/copied.py")
    git = MagicMock(spec=Git)
    git.get_diff_name_status.return_value = "C100\tsrc/original.py\tsrc/copied.py\n"
    git.get_diff_numstat.return_value = "7\t0\tsrc/original.py -> src/copied.py\n"
    git.get_diff_patch_for_paths.return_value = (
        "diff --git a/src/original.py b/src/copied.py\n"
        "similarity index 100%\n"
        "copy from src/original.py\n"
        "copy to src/copied.py\n"
    )

    draft = plan_extraction(git, source, selected, operator_prompt=None)

    assert len(draft.file_summaries) == 1
    summary = draft.file_summaries[0]
    assert summary.status == "C"
    assert summary.old_path == "src/original.py"
    assert summary.new_path == "src/copied.py"
    assert summary.additions == 7
    assert summary.deletions == 0
    assert summary.binary is False


def test_plan_extraction_marks_binary_rename_numstat() -> None:
    source = SourceSelection(
        source_task_id="gza-1",
        source_branch="feature/source",
        source_base_ref="main",
    )
    selected = ("assets/old.bin", "assets/new.bin")
    git = MagicMock(spec=Git)
    git.get_diff_name_status.return_value = "R100\tassets/old.bin\tassets/new.bin\n"
    git.get_diff_numstat.return_value = "-\t-\tassets/old.bin -> assets/new.bin\n"
    git.get_diff_patch_for_paths.return_value = (
        "diff --git a/assets/old.bin b/assets/new.bin\n"
        "similarity index 100%\n"
        "rename from assets/old.bin\n"
        "rename to assets/new.bin\n"
        "Binary files differ\n"
    )

    draft = plan_extraction(git, source, selected, operator_prompt=None)

    assert len(draft.file_summaries) == 1
    summary = draft.file_summaries[0]
    assert summary.status == "R"
    assert summary.old_path == "assets/old.bin"
    assert summary.new_path == "assets/new.bin"
    assert summary.additions is None
    assert summary.deletions is None
    assert summary.binary is True


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
