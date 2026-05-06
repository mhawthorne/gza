"""Integration tests for extraction seeding against real git worktrees."""

import json
from pathlib import Path
from unittest.mock import Mock

import pytest

from gza.config import Config
from gza.db import SqliteTaskStore
from gza.git import Git
from gza.runner import (
    EXTRACTION_ALREADY_MERGED_COMPLETION_REASON,
    _seed_extraction_bundle_if_present,
)

pytestmark = pytest.mark.integration


def _init_repo(tmp_path: Path, *, initial_content: str) -> Git:
    git = Git(tmp_path)
    git._run("init", "-b", "main")
    git._run("config", "user.name", "Test User")
    git._run("config", "user.email", "test@example.com")
    target = tmp_path / "src" / "file.py"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(initial_content)
    git._run("add", "src/file.py")
    git._run("commit", "-m", "initial")
    return git


def _write_bundle(
    project_dir: Path,
    task_id: str,
    task_slug: str,
    *,
    patch_text: str,
    source_branch: str,
    source_base_ref: str = "main",
) -> None:
    bundle_dir = project_dir / ".gza" / "extractions" / task_slug
    bundle_dir.mkdir(parents=True, exist_ok=True)
    (bundle_dir / "manifest.json").write_text(
        json.dumps(
            {
                "version": 1,
                "source_branch": source_branch,
                "source_base_ref": source_base_ref,
                "target_task_id": task_id,
                "target_slug": task_slug,
                "selected_paths": ["src/file.py"],
                "touched_paths": ["src/file.py"],
                "patch_path": "selected.patch",
            }
        )
    )
    (bundle_dir / "selected.patch").write_text(patch_text)
    (bundle_dir / "prompt.md").write_text("seed prompt\n")


def _add_detached_worktree(git: Git, path: Path) -> Git:
    git._run("worktree", "add", "--detach", str(path), "main")
    return Git(path)


def test_seed_extraction_bundle_marks_already_merged_when_rederived_diff_is_empty(tmp_path: Path) -> None:
    git = _init_repo(tmp_path, initial_content="base\n")
    store = SqliteTaskStore(tmp_path / "test.db", prefix="testproject")
    task = store.add("Extracted task", task_type="implement")
    task.slug = "20260427-already-merged"
    store.update(task)

    git._run("checkout", "-b", "feature/source")
    target = tmp_path / "src" / "file.py"
    target.write_text("feature\n")
    git._run("add", "src/file.py")
    git._run("commit", "-m", "feature change")
    stored_patch = git.get_diff_patch_for_paths("main...feature/source", ("src/file.py",), binary=True)

    git._run("checkout", "main")
    git._run("merge", "--no-ff", "feature/source", "-m", "merge feature")

    _write_bundle(tmp_path, task.id, task.slug, patch_text=stored_patch, source_branch="feature/source")
    worktree_path = tmp_path / "worktree-already-merged"
    worktree_git = _add_detached_worktree(git, worktree_path)

    config = Mock(spec=Config)
    config.project_dir = tmp_path
    log_file = tmp_path / "logs" / "already-merged.log"
    log_file.parent.mkdir(parents=True, exist_ok=True)

    seeded = _seed_extraction_bundle_if_present(
        task,
        config,
        worktree_path,
        worktree_git,
        log_file,
        resume=False,
    )

    assert seeded.seeded_paths == frozenset()
    assert seeded.completion_reason == EXTRACTION_ALREADY_MERGED_COMPLETION_REASON
    assert (worktree_path / "src" / "file.py").read_text() == "feature\n"
    assert "re-derived hunks=0" in log_file.read_text()


def test_seed_extraction_bundle_marks_already_merged_when_selected_paths_are_equivalent_on_base(
    tmp_path: Path,
) -> None:
    git = _init_repo(tmp_path, initial_content="base\n")
    store = SqliteTaskStore(tmp_path / "test.db", prefix="testproject")
    task = store.add("Extracted task", task_type="implement")
    task.slug = "20260427-already-merged-cherry-pick"
    store.update(task)

    git._run("checkout", "-b", "feature/source")
    target = tmp_path / "src" / "file.py"
    target.write_text("feature\n")
    git._run("add", "src/file.py")
    git._run("commit", "-m", "feature change")
    stored_patch = git.get_diff_patch_for_paths("main...feature/source", ("src/file.py",), binary=True)

    git._run("checkout", "main")
    target.write_text("feature\n")
    git._run("add", "src/file.py")
    git._run("commit", "-m", "equivalent change on main")

    _write_bundle(tmp_path, task.id, task.slug, patch_text=stored_patch, source_branch="feature/source")
    worktree_path = tmp_path / "worktree-already-merged-equivalent"
    worktree_git = _add_detached_worktree(git, worktree_path)

    config = Mock(spec=Config)
    config.project_dir = tmp_path
    log_file = tmp_path / "logs" / "already-merged-equivalent.log"
    log_file.parent.mkdir(parents=True, exist_ok=True)

    seeded = _seed_extraction_bundle_if_present(
        task,
        config,
        worktree_path,
        worktree_git,
        log_file,
        resume=False,
    )

    assert seeded.seeded_paths == frozenset()
    assert seeded.completion_reason == EXTRACTION_ALREADY_MERGED_COMPLETION_REASON
    assert (worktree_path / "src" / "file.py").read_text() == "feature\n"
    assert "adds nothing to the current base for selected paths" in log_file.read_text()


def test_seed_extraction_bundle_rederives_patch_and_applies_with_context_drift(tmp_path: Path) -> None:
    git = _init_repo(tmp_path, initial_content="a\nb\nc\n")
    store = SqliteTaskStore(tmp_path / "test.db", prefix="testproject")
    task = store.add("Extracted task", task_type="implement")
    task.slug = "20260427-drift"
    store.update(task)

    git._run("checkout", "-b", "feature/source")
    target = tmp_path / "src" / "file.py"
    target.write_text("a\nbranch\nc\n")
    git._run("add", "src/file.py")
    git._run("commit", "-m", "feature change")
    stored_patch = git.get_diff_patch_for_paths("main...feature/source", ("src/file.py",), binary=True)

    git._run("checkout", "main")
    target.write_text("header\na\nb\nc\n")
    git._run("add", "src/file.py")
    git._run("commit", "-m", "main drift")

    _write_bundle(tmp_path, task.id, task.slug, patch_text=stored_patch, source_branch="feature/source")
    worktree_path = tmp_path / "worktree-drift"
    worktree_git = _add_detached_worktree(git, worktree_path)

    config = Mock(spec=Config)
    config.project_dir = tmp_path
    log_file = tmp_path / "logs" / "drift.log"
    log_file.parent.mkdir(parents=True, exist_ok=True)

    seeded = _seed_extraction_bundle_if_present(
        task,
        config,
        worktree_path,
        worktree_git,
        log_file,
        resume=False,
    )

    assert seeded.seeded_paths == frozenset({"src/file.py"})
    assert seeded.completion_reason is None
    assert (worktree_path / "src" / "file.py").read_text() == "header\na\nbranch\nc\n"
    assert "re-derived hunks=1" in log_file.read_text()


def test_seed_extraction_bundle_falls_back_to_stored_patch_when_source_branch_is_unreachable(
    tmp_path: Path,
) -> None:
    git = _init_repo(tmp_path, initial_content="base\n")
    store = SqliteTaskStore(tmp_path / "test.db", prefix="testproject")
    task = store.add("Extracted task", task_type="implement")
    task.slug = "20260427-stored-fallback"
    store.update(task)

    git._run("checkout", "-b", "feature/source")
    target = tmp_path / "src" / "file.py"
    target.write_text("feature\n")
    git._run("add", "src/file.py")
    git._run("commit", "-m", "feature change")
    stored_patch = git.get_diff_patch_for_paths("main...feature/source", ("src/file.py",), binary=True)

    git._run("checkout", "main")
    git._run("branch", "-D", "feature/source")

    _write_bundle(tmp_path, task.id, task.slug, patch_text=stored_patch, source_branch="feature/source")
    worktree_path = tmp_path / "worktree-stored-fallback"
    worktree_git = _add_detached_worktree(git, worktree_path)

    config = Mock(spec=Config)
    config.project_dir = tmp_path
    log_file = tmp_path / "logs" / "stored-fallback.log"
    log_file.parent.mkdir(parents=True, exist_ok=True)

    seeded = _seed_extraction_bundle_if_present(
        task,
        config,
        worktree_path,
        worktree_git,
        log_file,
        resume=False,
    )

    assert seeded.seeded_paths == frozenset({"src/file.py"})
    assert seeded.completion_reason is None
    assert (worktree_path / "src" / "file.py").read_text() == "feature\n"
    assert "source branch 'feature/source' unreachable" in log_file.read_text()
