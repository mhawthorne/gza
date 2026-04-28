from pathlib import Path

from gza.config import Config, discover_project_dir
from gza.db import SqliteTaskStore


def test_discover_project_dir_uses_nearest_ancestor(tmp_path: Path) -> None:
    project = tmp_path / "project"
    nested = project / "a" / "b" / "c"
    nested.mkdir(parents=True)
    (project / "gza.yaml").write_text("project_name: root\n")

    inner = project / "a" / "gza.yaml"
    inner.write_text("project_name: nested\n")

    assert discover_project_dir(nested) == project / "a"


def test_config_db_path_defaults_to_local_even_when_legacy_local_db_exists(tmp_path: Path) -> None:
    (tmp_path / "gza.yaml").write_text("project_name: demo\n")
    legacy_db = tmp_path / ".gza" / "gza.db"
    legacy_db.parent.mkdir(parents=True, exist_ok=True)
    legacy_db.write_text("")

    config = Config.load(tmp_path)
    assert config.db_path == tmp_path / ".gza" / "gza.db"


def test_config_db_path_defaults_to_local_when_no_local_db(tmp_path: Path) -> None:
    (tmp_path / "gza.yaml").write_text("project_name: demo\n")
    config = Config.load(tmp_path)
    assert config.db_path == tmp_path / ".gza" / "gza.db"


def test_config_db_path_respects_explicit_db_path(tmp_path: Path) -> None:
    (tmp_path / "gza.yaml").write_text("project_name: demo\ndb_path: custom.db\n")
    config = Config.load(tmp_path)
    assert config.db_path == tmp_path / "custom.db"


def test_config_load_discover_true_uses_nearest_project(tmp_path: Path) -> None:
    project = tmp_path / "project"
    nested = project / "a" / "b"
    nested.mkdir(parents=True)
    (project / "gza.yaml").write_text("project_name: project-root\n")

    config = Config.load(nested, discover=True)
    assert config.project_dir == project
    assert config.project_name == "project-root"


def test_store_default_uses_discovered_project_dir(tmp_path: Path) -> None:
    project = tmp_path / "project"
    nested = project / "a" / "b"
    nested.mkdir(parents=True)
    db_path = project / "shared.db"
    (project / "gza.yaml").write_text(f"project_name: demo\ndb_path: {db_path}\n")

    store = SqliteTaskStore.default(nested)
    created = store.add("hello")

    assert created.id == "demo-1"
    assert store.get(created.id) is not None


def test_shared_db_is_project_scoped_by_project_id(tmp_path: Path) -> None:
    db_path = tmp_path / "shared.db"

    store_a = SqliteTaskStore(db_path, prefix="gza", project_id="alpha")
    task_a = store_a.add("A")
    assert task_a.id == "gza-1"

    store_b = SqliteTaskStore(db_path, prefix="gza", project_id="beta")
    task_b = store_b.add("B")
    assert task_b.id == "gza-1"

    assert [t.prompt for t in store_a.get_all()] == ["A"]
    assert [t.prompt for t in store_b.get_all()] == ["B"]
