from pathlib import Path

from fastapi.testclient import TestClient
from gza_server import __version__
from gza_server.app import create_app

from gza.db import SqliteTaskStore
from gza.task_types import ALL_TASK_STATUSES

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


def test_usage_card_is_cache_only_and_never_starts_a_provider(monkeypatch, tmp_path: Path):
    """A page render must never pay for a provider subprocess."""
    from gza_server import app as app_module

    seen: dict[str, object] = {}

    def _fake_get_primary_usage(store, config, *, refresh=True, now=None):
        seen["refresh"] = refresh
        return None

    monkeypatch.setattr("gza.usage_service.get_primary_usage", _fake_get_primary_usage)
    monkeypatch.setattr(
        "gza.config.Config.load",
        classmethod(lambda cls, *a, **k: _UsageConfig()),
    )
    monkeypatch.setattr(
        "gza.db.SqliteTaskStore.from_config", classmethod(lambda cls, *a, **k: FakeStore())
    )

    assert app_module.resolve_usage_card(tmp_path) is None
    assert seen["refresh"] is False


class _UsageConfig:
    usage = True
    usage_ttl_seconds = 900


def test_index_omits_the_usage_card_when_usage_is_unavailable(tmp_path: Path, monkeypatch):
    from gza_server import app as app_module

    monkeypatch.setattr(app_module, "resolve_usage_card", lambda *a, **k: None)
    db_path = tmp_path / "gza.db"
    store = SqliteTaskStore(db_path)
    app = create_app(store_factory=lambda: store)
    response = TestClient(app).get("/")
    assert response.status_code == 200
    assert "usage-board" not in response.text


def test_index_renders_the_usage_card_when_a_reading_is_cached(tmp_path: Path, monkeypatch):
    from gza_server import app as app_module

    card = app_module.UsageCard(
        provider="codex",
        used_percent=45.0,
        remaining_percent=55.0,
        duration_label="7d",
        resets_in="4d20h",
        age_label="4m",
        stale=False,
        warning="",
    )
    monkeypatch.setattr(app_module, "resolve_usage_card", lambda *a, **k: card)
    store = SqliteTaskStore(tmp_path / "gza.db")
    app = create_app(store_factory=lambda: store)
    response = TestClient(app).get("/")

    assert response.status_code == 200
    assert "usage-board" in response.text
    assert "45.0%" in response.text
    assert "resets in 4d20h" in response.text
    # Per-model buckets are captured but never surfaced on the homepage.
    assert "Spark" not in response.text
