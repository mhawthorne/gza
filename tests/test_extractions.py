"""Fast unit tests for extraction planning helpers."""

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from gza.db import SqliteTaskStore
from gza.extractions import (
    ExtractionDraft,
    ExtractionError,
    FileDiffSummary,
    SourceSelection,
    _parse_file_summaries,
    copy_bundle_to_worktree,
    infer_selected_paths,
    load_manifest,
    normalize_selected_paths,
    plan_extraction,
    resolve_source_selection,
    write_extraction_bundle,
)
from gza.git import Git


def _sample_draft(*, prompt: str = "Carry over: Source\n") -> ExtractionDraft:
    source = SourceSelection(
        source_task_id="gza-1",
        source_branch="feature/source",
        source_base_ref="main",
        source_task_prompt="Source",
        source_task_slug="20260427-source",
    )
    summary = FileDiffSummary(
        status="A",
        selected_path="src/module.py",
        old_path=None,
        new_path="src/module.py",
        additions=1,
        deletions=0,
        binary=False,
    )
    return ExtractionDraft(
        source=source,
        selected_paths=("src/module.py",),
        touched_paths=("src/module.py",),
        file_summaries=(summary,),
        patch=(
            "diff --git a/src/module.py b/src/module.py\n"
            "index e69de29..8c7e5a6 100644\n"
            "--- a/src/module.py\n"
            "+++ b/src/module.py\n"
            "@@ -0,0 +1 @@\n"
            "+value = 1\n"
        ),
        prompt=prompt,
    )


def test_normalize_selected_paths_rejects_escape() -> None:
    with pytest.raises(ExtractionError, match="within the repository root"):
        normalize_selected_paths(["../outside.py"])


def test_infer_selected_paths_returns_full_source_diff() -> None:
    git = MagicMock(spec=Git)
    source = SourceSelection(
        source_task_id="gza-1",
        source_branch="feature/source",
        source_base_ref="main",
    )
    git.get_diff_patch_for_paths.return_value = (
        "diff --git a/src/module.py b/src/module.py\n"
        "diff --git a/src/second.py b/src/second.py\n"
    )

    assert infer_selected_paths(git, source) == ("src/module.py", "src/second.py")


def test_infer_selected_paths_rejects_empty_source_diff() -> None:
    git = MagicMock(spec=Git)
    source = SourceSelection(
        source_task_id="gza-1",
        source_branch="feature/source",
        source_base_ref="main",
    )
    git.get_diff_patch_for_paths.return_value = ""

    with pytest.raises(ExtractionError, match="no extractable diff"):
        infer_selected_paths(git, source)


def test_infer_selected_paths_returns_commit_scope_diff() -> None:
    git = MagicMock(spec=Git)
    source = SourceSelection(
        source_task_id=None,
        source_commits=("a" * 40, "b" * 40),
    )
    git.get_commit_name_status.return_value = ""
    git.get_commit_numstat.return_value = ""
    git.get_commit_patch_for_paths.side_effect = [
        "diff --git a/src/one.py b/src/one.py\n",
        "diff --git a/src/two.py b/src/two.py\n",
    ]

    assert infer_selected_paths(git, source) == ("src/one.py", "src/two.py")


def test_resolve_source_selection_uses_task_branch_and_base(tmp_path: Path) -> None:
    store = SqliteTaskStore(tmp_path / "resolve-source.db", prefix="gza")
    task = store.add("Source", task_type="implement")
    task.status = "completed"
    task.branch = "feature/source"
    task.base_branch = "release"
    task.slug = "20260427-source"
    store.update(task)

    git = MagicMock(spec=Git)
    git.ref_exists.side_effect = lambda ref: ref in {"feature/source", "release"}

    source = resolve_source_selection(
        store,
        git,
        source_task_id=task.id,
        source_branch=None,
        base_branch_override=None,
    )

    assert source == SourceSelection(
        source_task_id=task.id,
        source_branch="feature/source",
        source_base_ref="release",
        source_task_prompt="Source",
        source_task_slug="20260427-source",
    )


def test_resolve_source_selection_branch_source_uses_default_base(tmp_path: Path) -> None:
    store = SqliteTaskStore(tmp_path / "resolve-branch.db", prefix="gza")
    git = MagicMock(spec=Git)
    git.default_branch.return_value = "main"
    git.ref_exists.side_effect = lambda ref: ref in {"feature/auth-cleanup", "main"}

    source = resolve_source_selection(
        store,
        git,
        source_task_id=None,
        source_branch="feature/auth-cleanup",
        base_branch_override=None,
    )

    assert source == SourceSelection(
        source_task_id=None,
        source_branch="feature/auth-cleanup",
        source_base_ref="main",
    )


def test_resolve_source_selection_commit_source_resolves_in_order(tmp_path: Path) -> None:
    store = SqliteTaskStore(tmp_path / "resolve-commit.db", prefix="gza")
    git = MagicMock(spec=Git)
    git.ref_exists.return_value = True
    git.rev_parse.side_effect = ["a" * 40, "b" * 40]

    source = resolve_source_selection(
        store,
        git,
        source_task_id=None,
        source_branch=None,
        source_commits=("HEAD~2", "HEAD~1"),
        base_branch_override=None,
    )

    assert source == SourceSelection(
        source_task_id=None,
        source_commits=("a" * 40, "b" * 40),
    )


def test_resolve_source_selection_commit_source_rejects_duplicates_after_resolution(tmp_path: Path) -> None:
    store = SqliteTaskStore(tmp_path / "resolve-commit-dup.db", prefix="gza")
    git = MagicMock(spec=Git)
    git.ref_exists.return_value = True
    git.rev_parse.side_effect = ["a" * 40, "a" * 40]

    with pytest.raises(ExtractionError, match="Duplicate source commit selected"):
        resolve_source_selection(
            store,
            git,
            source_task_id=None,
            source_branch=None,
            source_commits=("HEAD", "HEAD^0"),
            base_branch_override=None,
        )


def test_resolve_source_selection_rejects_non_code_task(tmp_path: Path) -> None:
    store = SqliteTaskStore(tmp_path / "resolve-non-code.db", prefix="gza")
    task = store.add("Plan source", task_type="plan")
    task.status = "completed"
    task.branch = "feature/plan-source"
    store.update(task)

    git = MagicMock(spec=Git)

    with pytest.raises(ExtractionError, match="must be a code task"):
        resolve_source_selection(
            store,
            git,
            source_task_id=task.id,
            source_branch=None,
            base_branch_override=None,
        )


def test_write_extraction_bundle_and_bundle_roundtrip(tmp_path: Path) -> None:
    store = SqliteTaskStore(tmp_path / "test.db", prefix="gza")
    task = store.add("Carry over", task_type="implement")
    task.slug = "20260427-target-task"
    store.update(task)
    draft = _sample_draft()

    bundle_dir = write_extraction_bundle(project_dir=tmp_path, task=task, draft=draft)
    assert (bundle_dir / "manifest.json").exists()
    assert (bundle_dir / "selected.patch").exists()
    assert (bundle_dir / "prompt.md").exists()

    manifest = load_manifest(bundle_dir / "manifest.json")
    assert manifest["source_branch"] == "feature/source"
    assert manifest["source_base_ref"] == "main"
    assert manifest["source_commits"] == []
    assert manifest["selected_paths"] == ["src/module.py"]

    worktree = tmp_path / "worktree"
    worktree.mkdir()
    copied = copy_bundle_to_worktree(bundle_dir, worktree)
    assert copied.exists()
    assert (copied / "manifest.json").exists()


def test_plan_extraction_branch_source_uses_branch_name_in_prompt() -> None:
    git = MagicMock(spec=Git)
    source = SourceSelection(
        source_task_id=None,
        source_branch="feature/auth-cleanup",
        source_base_ref="main",
    )
    git.get_diff_name_status.return_value = "A\tsrc/module.py\n"
    git.get_diff_numstat.return_value = "1\t0\tsrc/module.py\n"
    git.get_diff_patch_for_paths.return_value = (
        "diff --git a/src/module.py b/src/module.py\n"
        "index e69de29..8c7e5a6 100644\n"
        "--- a/src/module.py\n"
        "+++ b/src/module.py\n"
        "@@ -0,0 +1 @@\n"
        "+value = 1\n"
    )

    draft = plan_extraction(git, source, ("src/module.py",), operator_prompt=None)

    assert draft.prompt.startswith("Carry over: auth cleanup\n")


def test_plan_extraction_commit_source_preserves_commit_order() -> None:
    git = MagicMock(spec=Git)
    source = SourceSelection(
        source_task_id=None,
        source_commits=("a" * 40, "b" * 40),
    )
    git.get_commit_name_status.side_effect = [
        "M\tsrc/first.py\n",
        "M\tsrc/second.py\n",
    ]
    git.get_commit_numstat.side_effect = [
        "1\t0\tsrc/first.py\n",
        "2\t1\tsrc/second.py\n",
    ]
    git.get_commit_patch_for_paths.side_effect = [
        "diff --git a/src/first.py b/src/first.py\n"
        "index 1111111..2222222 100644\n"
        "--- a/src/first.py\n"
        "+++ b/src/first.py\n"
        "@@ -0,0 +1 @@\n"
        "+first = True\n",
        "diff --git a/src/second.py b/src/second.py\n"
        "index 3333333..4444444 100644\n"
        "--- a/src/second.py\n"
        "+++ b/src/second.py\n"
        "@@ -0,0 +1 @@\n"
        "+second = True\n",
    ]

    draft = plan_extraction(git, source, ("src/first.py", "src/second.py"), operator_prompt=None)

    assert "commits in extraction order" in draft.prompt
    assert draft.patch.index("diff --git a/src/first.py b/src/first.py") < draft.patch.index(
        "diff --git a/src/second.py b/src/second.py"
    )
    assert draft.touched_paths == ("src/first.py", "src/second.py")


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


def test_write_extraction_bundle_rejects_path_reuse_for_different_task(tmp_path: Path) -> None:
    store = SqliteTaskStore(tmp_path / "test.db", prefix="gza")
    draft = _sample_draft()

    first = store.add(draft.prompt, task_type="implement")
    first.slug = "20260427-shared-target"
    store.update(first)
    write_extraction_bundle(project_dir=tmp_path, task=first, draft=draft)

    second = store.add(draft.prompt, task_type="implement")
    second.slug = "20260427-shared-target"
    store.update(second)

    with pytest.raises(ExtractionError, match="already reserved for task"):
        write_extraction_bundle(project_dir=tmp_path, task=second, draft=draft)
