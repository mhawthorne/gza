from pathlib import Path
from unittest.mock import Mock, patch

from gza.config import Config
from gza.db import Task
from gza.runner import (
    _is_path_in_allowed_scope,
    _resolve_project_boundary,
    _task_docker_volumes,
    _task_runtime_work_dir,
)


def test_resolve_project_boundary_subdir_scope_and_local_dependency_split(tmp_path: Path) -> None:
    repo_root = tmp_path / "repo"
    project_dir = repo_root / "services" / "api"
    shared_dir = repo_root / "libs" / "shared"
    external_dir = tmp_path / "external-lib"
    transitive_dir = tmp_path / "transitive-lib"

    project_dir.mkdir(parents=True)
    shared_dir.mkdir(parents=True)
    external_dir.mkdir()
    transitive_dir.mkdir()
    (repo_root / ".git").mkdir()

    (project_dir / "uv.lock").write_text(
        """
version = 1

[[package]]
name = "root-project"
version = "0.1.0"
source = { editable = "." }

[[package]]
name = "registry-dep"
version = "1.0.0"
source = { registry = "https://pypi.org/simple" }

[[package]]
name = "shared"
version = "0.1.0"
source = { directory = "../../libs/shared" }

[[package]]
name = "external"
version = "0.1.0"
source = { editable = "../../../external-lib" }

[[package]]
name = "transitive"
version = "0.1.0"
source = { path = "../../../transitive-lib" }
""".strip()
        + "\n"
    )

    config = Config(project_dir=project_dir, project_name="api", use_docker=False)
    boundary = _resolve_project_boundary(config)

    assert boundary.repo_root == repo_root.resolve()
    assert boundary.scope_root == Path("services/api")
    assert boundary.in_repo_dependency_paths == (Path("libs/shared"),)
    assert tuple(dep.resolved_path for dep in boundary.out_of_repo_dependencies) == (
        external_dir.resolve(),
        transitive_dir.resolve(),
    )


def test_resolve_project_boundary_root_scope_is_dot(tmp_path: Path) -> None:
    (tmp_path / ".git").mkdir()
    (tmp_path / "uv.lock").write_text("version = 1\n")

    config = Config(project_dir=tmp_path, project_name="root-project", use_docker=False)
    boundary = _resolve_project_boundary(config)

    assert boundary.scope_root == Path(".")


def test_is_path_in_allowed_scope_accepts_scope_root_and_in_repo_deps() -> None:
    allowed_roots = (Path("services/api"), Path("libs/shared"))

    assert _is_path_in_allowed_scope("services/api/app.py", allowed_roots) is True
    assert _is_path_in_allowed_scope("libs/shared/helpers.py", allowed_roots) is True
    assert _is_path_in_allowed_scope("services/other/app.py", allowed_roots) is False


def test_task_runtime_work_dir_uses_scope_root_unless_cross_project(tmp_path: Path) -> None:
    worktree_path = tmp_path / "worktree"
    worktree_path.mkdir()
    boundary = _resolve_project_boundary(Config(project_dir=tmp_path, project_name="root", use_docker=False))

    task = Task(id="gza-1", prompt="normal task", tags=())
    assert _task_runtime_work_dir(worktree_path, boundary, task) == worktree_path

    subdir_boundary = boundary.__class__(repo_root=tmp_path, scope_root=Path("services/api"))
    assert _task_runtime_work_dir(worktree_path, subdir_boundary, task) == worktree_path / "services/api"

    cross_project_task = Task(id="gza-2", prompt="cross project", tags=("cross-project",))
    assert _task_runtime_work_dir(worktree_path, subdir_boundary, cross_project_task) == worktree_path


def test_task_docker_volumes_adds_read_only_out_of_repo_mounts(tmp_path: Path) -> None:
    repo_root = tmp_path / "repo"
    project_dir = repo_root / "services" / "api"
    external_dir = tmp_path / "external-lib"

    project_dir.mkdir(parents=True)
    external_dir.mkdir()
    (repo_root / ".git").mkdir()
    (project_dir / "uv.lock").write_text(
        f"""
version = 1

[[package]]
name = "external"
version = "0.1.0"
source = {{ editable = "{external_dir}" }}
""".strip()
        + "\n"
    )

    config = Config(
        project_dir=project_dir,
        project_name="api",
        use_docker=True,
        docker_volumes=["/tmp/cache:/tmp/cache:rw"],
    )
    boundary = _resolve_project_boundary(config)

    assert _task_docker_volumes(config, boundary) == [
        "/tmp/cache:/tmp/cache:rw",
        f"{external_dir.resolve()}:{external_dir.resolve()}:ro",
    ]
