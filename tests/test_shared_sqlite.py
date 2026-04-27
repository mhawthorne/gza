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


def test_config_db_path_prefers_legacy_local_db_when_present(tmp_path: Path) -> None:
    (tmp_path / "gza.yaml").write_text("project_name: demo\n")
    legacy_db = tmp_path / ".gza" / "gza.db"
    legacy_db.parent.mkdir(parents=True, exist_ok=True)
    legacy_db.write_text("")

    config = Config.load(tmp_path)
    assert config.db_path == legacy_db


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
