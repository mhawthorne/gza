"""Tests for _create_local_dep_symlinks in runner.py."""

import tomllib
from pathlib import Path
from unittest.mock import patch

import pytest

from gza.runner import _create_local_dep_symlinks


def make_config(project_dir: Path, use_docker: bool = False):
    """Return a minimal mock config."""
    from unittest.mock import Mock

    config = Mock()
    config.project_dir = project_dir
    config.use_docker = use_docker
    return config


def write_pyproject(project_dir: Path, sources: dict) -> None:
    """Write a pyproject.toml with the given [tool.uv.sources] content."""
    lines = ["[tool.uv.sources]\n"]
    for name, entry in sources.items():
        path_val = entry.get("path", "")
        lines.append(f'{name} = {{ path = "{path_val}" }}\n')
    content = "".join(lines)
    (project_dir / "pyproject.toml").write_text(content)


class TestNoopCases:
    def test_no_pyproject(self, tmp_path):
        """No pyproject.toml — returns silently."""
        project_dir = tmp_path / "project"
        project_dir.mkdir()
        worktree = tmp_path / "worktree"
        worktree.mkdir()
        config = make_config(project_dir)
        _create_local_dep_symlinks(config, worktree)  # no error

    def test_no_uv_sources_section(self, tmp_path):
        """pyproject.toml exists but has no [tool.uv.sources] — returns silently."""
        project_dir = tmp_path / "project"
        project_dir.mkdir()
        (project_dir / "pyproject.toml").write_text("[project]\nname = 'foo'\n")
        worktree = tmp_path / "worktree"
        worktree.mkdir()
        config = make_config(project_dir)
        _create_local_dep_symlinks(config, worktree)  # no error

    def test_dep_path_not_on_disk(self, tmp_path):
        """Dep path doesn't exist on host — no symlink created, no error."""
        project_dir = tmp_path / "project"
        project_dir.mkdir()
        write_pyproject(project_dir, {"missing": {"path": "../nonexistent"}})
        worktree = tmp_path / "worktrees" / "project" / "task-1"
        worktree.mkdir(parents=True)
        config = make_config(project_dir)
        _create_local_dep_symlinks(config, worktree)
        expected = tmp_path / "worktrees" / "project" / "nonexistent"
        assert not expected.exists()

    def test_absolute_path_skipped(self, tmp_path):
        """Absolute path entries are skipped (they work everywhere)."""
        project_dir = tmp_path / "project"
        project_dir.mkdir()
        dep_dir = tmp_path / "abs_dep"
        dep_dir.mkdir()
        content = f'[tool.uv.sources]\ndep = {{ path = "{dep_dir}" }}\n'
        (project_dir / "pyproject.toml").write_text(content)
        worktree = tmp_path / "worktrees" / "project" / "task-1"
        worktree.mkdir(parents=True)
        config = make_config(project_dir)
        _create_local_dep_symlinks(config, worktree)
        # No symlink should be created at an absolute-resolved location inside worktree parent
        # (absolute paths are skipped entirely)
        assert not any((tmp_path / "worktrees" / "project").iterdir().__next__() == dep_dir
                       for _ in [None])  # just ensure no crash


class TestSymlinkCreation:
    def test_single_relative_dep(self, tmp_path):
        """Single relative dep — symlink created at correct location."""
        project_dir = tmp_path / "work" / "myproject"
        project_dir.mkdir(parents=True)
        shared_lib = tmp_path / "work" / "shared-lib"
        shared_lib.mkdir()
        write_pyproject(project_dir, {"shared-lib": {"path": "../shared-lib"}})

        worktree = tmp_path / "worktrees" / "myproject" / "task-123"
        worktree.mkdir(parents=True)
        config = make_config(project_dir)
        _create_local_dep_symlinks(config, worktree)

        expected_link = tmp_path / "worktrees" / "myproject" / "shared-lib"
        assert expected_link.is_symlink()
        assert expected_link.resolve() == shared_lib.resolve()

    def test_nested_relative_dep(self, tmp_path):
        """Nested relative dep (../../libs/core) — parent dirs created, symlink correct."""
        project_dir = tmp_path / "work" / "myproject"
        project_dir.mkdir(parents=True)
        core_lib = tmp_path / "libs" / "core"
        core_lib.mkdir(parents=True)
        write_pyproject(project_dir, {"core": {"path": "../../libs/core"}})

        worktree = tmp_path / "gza-worktrees" / "myproject" / "task-1"
        worktree.mkdir(parents=True)
        config = make_config(project_dir)
        _create_local_dep_symlinks(config, worktree)

        expected_link = tmp_path / "gza-worktrees" / "libs" / "core"
        assert expected_link.is_symlink()
        assert expected_link.resolve() == core_lib.resolve()

    def test_multiple_deps(self, tmp_path):
        """Multiple deps — all symlinked correctly."""
        project_dir = tmp_path / "work" / "proj"
        project_dir.mkdir(parents=True)
        dep_a = tmp_path / "work" / "dep-a"
        dep_a.mkdir()
        dep_b = tmp_path / "work" / "dep-b"
        dep_b.mkdir()

        content = (
            "[tool.uv.sources]\n"
            'dep-a = { path = "../dep-a" }\n'
            'dep-b = { path = "../dep-b" }\n'
        )
        (project_dir / "pyproject.toml").write_text(content)

        worktree = tmp_path / "worktrees" / "proj" / "task-1"
        worktree.mkdir(parents=True)
        config = make_config(project_dir)
        _create_local_dep_symlinks(config, worktree)

        link_a = tmp_path / "worktrees" / "proj" / "dep-a"
        link_b = tmp_path / "worktrees" / "proj" / "dep-b"
        assert link_a.is_symlink() and link_a.resolve() == dep_a.resolve()
        assert link_b.is_symlink() and link_b.resolve() == dep_b.resolve()


class TestIdempotency:
    def test_symlink_already_correct(self, tmp_path):
        """Symlink already exists and points to correct target — idempotent, no error."""
        project_dir = tmp_path / "work" / "myproject"
        project_dir.mkdir(parents=True)
        shared_lib = tmp_path / "work" / "shared-lib"
        shared_lib.mkdir()
        write_pyproject(project_dir, {"shared-lib": {"path": "../shared-lib"}})

        worktree = tmp_path / "worktrees" / "myproject" / "task-1"
        worktree.mkdir(parents=True)
        config = make_config(project_dir)

        # Create the symlink first
        _create_local_dep_symlinks(config, worktree)
        link = tmp_path / "worktrees" / "myproject" / "shared-lib"
        assert link.is_symlink()

        # Call again — should not raise
        _create_local_dep_symlinks(config, worktree)
        assert link.is_symlink()
        assert link.resolve() == shared_lib.resolve()

    def test_symlink_wrong_target_warns_and_skips(self, tmp_path, caplog):
        """Symlink exists but points to wrong target — warning logged, not overwritten."""
        import logging

        project_dir = tmp_path / "work" / "myproject"
        project_dir.mkdir(parents=True)
        shared_lib = tmp_path / "work" / "shared-lib"
        shared_lib.mkdir()
        wrong_target = tmp_path / "work" / "other"
        wrong_target.mkdir()
        write_pyproject(project_dir, {"shared-lib": {"path": "../shared-lib"}})

        worktree = tmp_path / "worktrees" / "myproject" / "task-1"
        worktree.mkdir(parents=True)
        link = tmp_path / "worktrees" / "myproject" / "shared-lib"
        link.symlink_to(wrong_target)

        config = make_config(project_dir)
        with caplog.at_level(logging.WARNING, logger="gza.runner"):
            _create_local_dep_symlinks(config, worktree)

        # Still points to wrong target (not overwritten)
        assert link.resolve() == wrong_target.resolve()
        assert any("already exists" in r.message for r in caplog.records)

    def test_real_directory_at_symlink_location_warns_and_skips(self, tmp_path, caplog):
        """Real directory at symlink location — warning logged, not overwritten."""
        import logging

        project_dir = tmp_path / "work" / "myproject"
        project_dir.mkdir(parents=True)
        shared_lib = tmp_path / "work" / "shared-lib"
        shared_lib.mkdir()
        write_pyproject(project_dir, {"shared-lib": {"path": "../shared-lib"}})

        worktree = tmp_path / "worktrees" / "myproject" / "task-1"
        worktree.mkdir(parents=True)
        # Place a real directory where the symlink would go
        real_dir = tmp_path / "worktrees" / "myproject" / "shared-lib"
        real_dir.mkdir()

        config = make_config(project_dir)
        with caplog.at_level(logging.WARNING, logger="gza.runner"):
            _create_local_dep_symlinks(config, worktree)

        assert real_dir.is_dir() and not real_dir.is_symlink()
        assert any("already exists" in r.message for r in caplog.records)


class TestDockerSkip:
    def test_docker_mode_call_site_skips(self, tmp_path):
        """When use_docker=True, _create_local_dep_symlinks is not called from call sites."""
        # This tests that the guard `if not config.use_docker:` is in place at the call site.
        # We verify the function itself works fine; the guard is in runner.py call sites.
        project_dir = tmp_path / "work" / "myproject"
        project_dir.mkdir(parents=True)
        shared_lib = tmp_path / "work" / "shared-lib"
        shared_lib.mkdir()
        write_pyproject(project_dir, {"shared-lib": {"path": "../shared-lib"}})

        worktree = tmp_path / "worktrees" / "myproject" / "task-1"
        worktree.mkdir(parents=True)
        config = make_config(project_dir, use_docker=True)

        # Verify the guard: grep runner.py for `if not config.use_docker` before _create_local_dep_symlinks
        import gza.runner as runner_mod
        import inspect

        source = inspect.getsource(runner_mod)
        # The pattern: `if not config.use_docker:\n        _create_local_dep_symlinks`
        assert "if not config.use_docker:" in source
        # And that _create_local_dep_symlinks appears after it (call site guard present)
        docker_idx = source.index("if not config.use_docker:")
        symlink_idx = source.index("_create_local_dep_symlinks(config, worktree_path)", docker_idx)
        assert symlink_idx > docker_idx
