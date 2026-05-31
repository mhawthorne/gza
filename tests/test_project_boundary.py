from pathlib import Path

from gza.config import Config
from gza.runner import (
    LocalDependency,
    ProjectBoundary,
    _build_runtime_docker_volumes,
    _find_out_of_scope_paths,
    _project_boundary,
    _resolve_local_dependencies_from_uv_lock,
)


def test_resolve_local_dependencies_from_uv_lock_classifies_paths(tmp_path: Path) -> None:
    repo_root = tmp_path / "repo"
    project_dir = repo_root / "services" / "foo"
    in_repo_dep = repo_root / "libs" / "shared"
    out_of_repo_dep = tmp_path / "vendor" / "external"
    project_dir.mkdir(parents=True)
    in_repo_dep.mkdir(parents=True)
    out_of_repo_dep.mkdir(parents=True)
    (project_dir / "uv.lock").write_text(
        "[[package]]\n"
        'name = "registry"\n'
        'source = { registry = "https://pypi.org/simple" }\n'
        "[[package]]\n"
        'name = "self"\n'
        'source = { editable = "." }\n'
        "[[package]]\n"
        'name = "shared"\n'
        'source = { directory = "../../libs/shared" }\n'
        "[[package]]\n"
        'name = "external"\n'
        f'source = {{ editable = "{out_of_repo_dep}" }}\n'
        "[[package]]\n"
        'name = "transitive"\n'
        'source = { path = "../../libs/shared" }\n'
    )

    deps = _resolve_local_dependencies_from_uv_lock(project_dir, repo_root)

    assert deps == (
        LocalDependency(
            source_path=Path("../../libs/shared"),
            resolved_path=in_repo_dep.resolve(),
            repo_relative_path=Path("libs/shared"),
        ),
        LocalDependency(
            source_path=out_of_repo_dep.resolve(),
            resolved_path=out_of_repo_dep.resolve(),
            repo_relative_path=None,
        ),
    )


def test_project_boundary_computes_scope_root_for_subdir_project(tmp_path: Path, monkeypatch) -> None:
    repo_root = tmp_path / "repo"
    project_dir = repo_root / "services" / "foo"
    project_dir.mkdir(parents=True)
    config = Config(project_dir=project_dir, project_name="foo")

    monkeypatch.setattr("gza.runner._resolve_repo_root", lambda _project_dir: repo_root)

    boundary = _project_boundary(config)

    assert boundary.repo_root == repo_root
    assert boundary.scope_root == Path("services/foo")


def test_project_boundary_computes_root_scope_for_repo_root_config(tmp_path: Path, monkeypatch) -> None:
    repo_root = tmp_path / "repo"
    repo_root.mkdir()
    config = Config(project_dir=repo_root, project_name="repo")

    monkeypatch.setattr("gza.runner._resolve_repo_root", lambda _project_dir: repo_root)

    boundary = _project_boundary(config)

    assert boundary.scope_root == Path(".")


def test_find_out_of_scope_paths_allows_scope_root_and_in_repo_deps(tmp_path: Path) -> None:
    project_dir = tmp_path / "repo" / "services" / "foo"
    project_dir.mkdir(parents=True)
    config = Config(project_dir=project_dir, project_name="foo")
    setattr(
        config,
        "_project_boundary_cache",
        ProjectBoundary(
            repo_root=tmp_path / "repo",
            scope_root=Path("services/foo"),
            local_dependencies=(
                LocalDependency(
                    source_path=Path("../../libs/shared"),
                    resolved_path=(tmp_path / "repo" / "libs" / "shared").resolve(),
                    repo_relative_path=Path("libs/shared"),
                ),
            ),
        ),
    )

    violations = _find_out_of_scope_paths(
        config,
        {"services/foo/app.py", "libs/shared/util.py", "services/bar/other.py"},
    )

    assert violations == ["services/bar/other.py"]


def test_find_out_of_scope_paths_strict_scope_blocks_in_repo_deps(tmp_path: Path) -> None:
    project_dir = tmp_path / "repo" / "services" / "foo"
    project_dir.mkdir(parents=True)
    config = Config(project_dir=project_dir, project_name="foo")
    setattr(
        config,
        "_project_boundary_cache",
        ProjectBoundary(
            repo_root=tmp_path / "repo",
            scope_root=Path("services/foo"),
            local_dependencies=(
                LocalDependency(
                    source_path=Path("../../libs/shared"),
                    resolved_path=(tmp_path / "repo" / "libs" / "shared").resolve(),
                    repo_relative_path=Path("libs/shared"),
                ),
            ),
        ),
    )

    violations = _find_out_of_scope_paths(
        config,
        {"services/foo/app.py", "libs/shared/util.py"},
        strict_scope=True,
    )

    assert violations == ["libs/shared/util.py"]


def test_find_out_of_scope_paths_is_noop_for_root_scope(tmp_path: Path) -> None:
    config = Config(project_dir=tmp_path, project_name="repo")
    setattr(
        config,
        "_project_boundary_cache",
        ProjectBoundary(repo_root=tmp_path, scope_root=Path("."), local_dependencies=()),
    )

    violations = _find_out_of_scope_paths(config, {"elsewhere/file.py"})

    assert violations == []


def test_build_runtime_docker_volumes_adds_readonly_out_of_repo_mounts(tmp_path: Path) -> None:
    project_dir = tmp_path / "repo" / "services" / "foo"
    out_of_repo_dep = tmp_path / "vendor" / "external"
    project_dir.mkdir(parents=True)
    out_of_repo_dep.mkdir(parents=True)
    config = Config(
        project_dir=project_dir,
        project_name="foo",
        docker_volumes=["/host/data:/data:ro"],
    )
    setattr(
        config,
        "_project_boundary_cache",
        ProjectBoundary(
            repo_root=tmp_path / "repo",
            scope_root=Path("services/foo"),
            local_dependencies=(
                LocalDependency(
                    source_path=out_of_repo_dep.resolve(),
                    resolved_path=out_of_repo_dep.resolve(),
                    repo_relative_path=None,
                ),
            ),
        ),
    )

    volumes = _build_runtime_docker_volumes(config)

    assert volumes == [
        "/host/data:/data:ro",
        f"{out_of_repo_dep.resolve()}:{out_of_repo_dep.resolve()}:ro",
    ]
