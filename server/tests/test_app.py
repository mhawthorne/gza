from pathlib import Path

from fastapi.testclient import TestClient

from gza_server import __version__
from gza_server.app import create_app


class FakeStore:
    db_path = Path("/shared/gza.db")

    def get_all(self):
        return [object(), object()]


def test_health_reports_version_and_resolved_gza_data():
    app = create_app(store_factory=FakeStore)

    response = TestClient(app).get("/api/health")

    assert response.status_code == 200
    assert response.json() == {
        "status": "ok",
        "version": __version__,
        "db_path": "/shared/gza.db",
        "task_count": 2,
        "instance_id": None,
    }


def test_health_exposes_server_instance_identity():
    app = create_app(store_factory=FakeStore, instance_id="unique-server-instance")

    response = TestClient(app).get("/api/health")

    assert response.status_code == 200
    assert response.json()["instance_id"] == "unique-server-instance"


def test_health_resolves_database_through_gza_config_and_store(tmp_path):
    (tmp_path / "gza.yaml").write_text(
        "project_name: health-test\n"
        "project_id: healthtest\n"
        "db_path: .gza/gza.db\n",
        encoding="utf-8",
    )
    app = create_app(project_dir=tmp_path)

    response = TestClient(app).get("/api/health")

    assert response.status_code == 200
    assert response.json()["db_path"] == str(tmp_path / ".gza" / "gza.db")
    assert response.json()["task_count"] == 0


def test_index_renders_jinja_template():
    app = create_app(store_factory=FakeStore)

    response = TestClient(app).get("/")

    assert response.status_code == 200
    assert "<h1>gza server</h1>" in response.text
    assert f"Version {__version__}" in response.text
