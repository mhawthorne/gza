from pathlib import Path

from fastapi.testclient import TestClient

from gza.db import SqliteTaskStore
from gza.task_types import ALL_TASK_STATUSES

from gza_server import __version__
from gza_server.app import create_app

_STYLESHEET = Path(__file__).resolve().parents[1] / "src" / "gza_server" / "static" / "app.css"


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


def test_index_dashboard_reports_status_counts_and_recent_tasks(tmp_path: Path):
    store = SqliteTaskStore(tmp_path / "dash.db", prefix="srv", project_id="dash-test")
    pending = store.add("Dashboard pending task")
    failed = store.add("Dashboard failed task")
    failed.status = "failed"
    store.update(failed)

    response = TestClient(create_app(store_factory=lambda: store)).get("/")

    assert response.status_code == 200
    assert __version__ in response.text
    # Every status keeps a cell, including the ones with nothing in them.
    for status in ALL_TASK_STATUSES:
        assert f"status-cell-{status}" in response.text
    assert "2 tasks" in response.text
    assert pending.id in response.text
    assert failed.id in response.text


def test_index_dashboard_status_cells_link_to_filtered_task_lists(tmp_path: Path):
    store = SqliteTaskStore(tmp_path / "links.db", prefix="srv", project_id="dash-test")
    store.add("Dashboard pending task")

    response = TestClient(create_app(store_factory=lambda: store)).get("/")

    for status in ALL_TASK_STATUSES:
        assert f'href="/tasks?status={status}"' in response.text


def test_stylesheet_is_served() -> None:
    client = TestClient(create_app(store_factory=lambda: None))
    response = client.get("/static/app.css")

    assert response.status_code == 200
    assert "text/css" in response.headers["content-type"]


def test_script_is_served() -> None:
    client = TestClient(create_app(store_factory=lambda: None))
    response = client.get("/static/app.js")

    assert response.status_code == 200
    assert "javascript" in response.headers["content-type"]


def test_every_task_status_has_a_style_rule() -> None:
    """A status with no rule silently renders as the default grey pill."""
    stylesheet = _STYLESHEET.read_text()

    missing = [
        status for status in ALL_TASK_STATUSES if f".status-{status}" not in stylesheet
    ]
    assert not missing, f"statuses without a style rule: {missing}"
