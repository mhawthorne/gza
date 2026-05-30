"""Tests for uv.lock-driven local dependency symlink resolution."""

from pathlib import Path
from unittest.mock import Mock

from gza.runner import _create_local_dep_symlinks


def make_config(project_dir: Path, use_docker: bool = False):
    config = Mock()
    config.project_dir = project_dir
    config.use_docker = use_docker
    config.docker_volumes = []
    return config


def write_uv_lock(project_dir: Path, package_sources: list[str]) -> None:
    packages = "\n\n".join(
        f"[[package]]\nname = \"dep{i}\"\nversion = \"0.1.0\"\nsource = {{ {source} }}"
        for i, source in enumerate(package_sources, start=1)
    )
    (project_dir / "uv.lock").write_text(f"version = 1\n\n{packages}\n")


class TestNoopCases:
    def test_no_lockfile(self, tmp_path):
        project_dir = tmp_path / "project"
        project_dir.mkdir()
        worktree = tmp_path / "worktree"
        worktree.mkdir()
        _create_local_dep_symlinks(make_config(project_dir), worktree)

    def test_relative_dep_path_not_on_disk(self, tmp_path):
        repo_root = tmp_path / "repo"
        project_dir = repo_root / "project"
        project_dir.mkdir(parents=True)
        (repo_root / ".git").mkdir()
        write_uv_lock(project_dir, ['directory = "../missing"'])
        worktree = tmp_path / "worktrees" / "project" / "task-1"
        worktree.mkdir(parents=True)

        _create_local_dep_symlinks(make_config(project_dir), worktree)

        assert not (tmp_path / "worktrees" / "project" / "missing").exists()

    def test_absolute_path_skipped(self, tmp_path):
        repo_root = tmp_path / "repo"
        project_dir = repo_root / "project"
        dep_dir = tmp_path / "abs-dep"
        project_dir.mkdir(parents=True)
        dep_dir.mkdir()
        (repo_root / ".git").mkdir()
        write_uv_lock(project_dir, [f'editable = "{dep_dir}"'])
        worktree = tmp_path / "worktrees" / "project" / "task-1"
        worktree.mkdir(parents=True)

        _create_local_dep_symlinks(make_config(project_dir), worktree)

        assert list((tmp_path / "worktrees" / "project").iterdir()) == [worktree]


class TestSymlinkCreation:
    def test_single_relative_out_of_repo_dep(self, tmp_path):
        repo_root = tmp_path / "repo"
        project_dir = repo_root / "project"
        shared_lib = tmp_path / "shared-lib"
        project_dir.mkdir(parents=True)
        shared_lib.mkdir()
        (repo_root / ".git").mkdir()
        write_uv_lock(project_dir, ['directory = "../../shared-lib"'])

        worktree = tmp_path / "worktrees" / "project" / "task-123"
        worktree.mkdir(parents=True)
        _create_local_dep_symlinks(make_config(project_dir), worktree)

        expected_link = tmp_path / "worktrees" / "project" / "shared-lib"
        assert expected_link.is_symlink()
        assert expected_link.resolve() == shared_lib.resolve()

    def test_transitive_relative_out_of_repo_dep_from_uv_lock(self, tmp_path):
        repo_root = tmp_path / "repo"
        project_dir = repo_root / "project"
        transitive_dep = tmp_path / "vendor" / "transitive"
        project_dir.mkdir(parents=True)
        transitive_dep.mkdir(parents=True)
        (repo_root / ".git").mkdir()
        write_uv_lock(project_dir, ['path = "../../vendor/transitive"'])

        worktree = tmp_path / "gza-worktrees" / "project" / "task-1"
        worktree.mkdir(parents=True)
        _create_local_dep_symlinks(make_config(project_dir), worktree)

        expected_link = tmp_path / "gza-worktrees" / "project" / "vendor" / "transitive"
        assert expected_link.is_symlink()
        assert expected_link.resolve() == transitive_dep.resolve()

    def test_in_repo_dep_is_not_symlinked(self, tmp_path):
        repo_root = tmp_path / "repo"
        project_dir = repo_root / "services" / "api"
        shared_lib = repo_root / "libs" / "shared"
        project_dir.mkdir(parents=True)
        shared_lib.mkdir(parents=True)
        (repo_root / ".git").mkdir()
        write_uv_lock(project_dir, ['directory = "../../libs/shared"'])

        worktree = tmp_path / "worktrees" / "api" / "task-1"
        worktree.mkdir(parents=True)
        _create_local_dep_symlinks(make_config(project_dir), worktree)

        assert not (tmp_path / "worktrees" / "libs" / "shared").exists()


class TestIdempotency:
    def test_symlink_already_correct(self, tmp_path):
        repo_root = tmp_path / "repo"
        project_dir = repo_root / "project"
        shared_lib = tmp_path / "shared-lib"
        project_dir.mkdir(parents=True)
        shared_lib.mkdir()
        (repo_root / ".git").mkdir()
        write_uv_lock(project_dir, ['directory = "../../shared-lib"'])

        worktree = tmp_path / "worktrees" / "project" / "task-1"
        worktree.mkdir(parents=True)
        config = make_config(project_dir)

        _create_local_dep_symlinks(config, worktree)
        link = tmp_path / "worktrees" / "project" / "shared-lib"
        _create_local_dep_symlinks(config, worktree)

        assert link.is_symlink()
        assert link.resolve() == shared_lib.resolve()

    def test_symlink_wrong_target_warns_and_skips(self, tmp_path, caplog):
        import logging

        repo_root = tmp_path / "repo"
        project_dir = repo_root / "project"
        shared_lib = tmp_path / "shared-lib"
        wrong_target = tmp_path / "other"
        project_dir.mkdir(parents=True)
        shared_lib.mkdir()
        wrong_target.mkdir()
        (repo_root / ".git").mkdir()
        write_uv_lock(project_dir, ['directory = "../../shared-lib"'])

        worktree = tmp_path / "worktrees" / "project" / "task-1"
        worktree.mkdir(parents=True)
        link = tmp_path / "worktrees" / "project" / "shared-lib"
        link.symlink_to(wrong_target)

        with caplog.at_level(logging.WARNING, logger="gza.runner"):
            _create_local_dep_symlinks(make_config(project_dir), worktree)

        assert link.resolve() == wrong_target.resolve()
        assert any("already exists" in r.message for r in caplog.records)

    def test_real_directory_at_symlink_location_warns_and_skips(self, tmp_path, caplog):
        import logging

        repo_root = tmp_path / "repo"
        project_dir = repo_root / "project"
        shared_lib = tmp_path / "shared-lib"
        project_dir.mkdir(parents=True)
        shared_lib.mkdir()
        (repo_root / ".git").mkdir()
        write_uv_lock(project_dir, ['directory = "../../shared-lib"'])

        worktree = tmp_path / "worktrees" / "project" / "task-1"
        worktree.mkdir(parents=True)
        real_dir = tmp_path / "worktrees" / "project" / "shared-lib"
        real_dir.mkdir(parents=True)

        with caplog.at_level(logging.WARNING, logger="gza.runner"):
            _create_local_dep_symlinks(make_config(project_dir), worktree)

        assert real_dir.is_dir() and not real_dir.is_symlink()
        assert any("already exists" in r.message for r in caplog.records)
