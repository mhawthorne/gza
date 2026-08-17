"""FastAPI application for the local gza server."""

from __future__ import annotations

from collections.abc import Callable
import os
from pathlib import Path
from typing import Any, Protocol

from fastapi import FastAPI, Request
from fastapi.templating import Jinja2Templates
from gza.config import Config
from gza.db import SqliteTaskStore

from . import __version__


class TaskStore(Protocol):
    """The small portion of gza's task-store API needed in phase 1."""

    db_path: Path

    def get_all(self) -> list[Any]: ...


StoreFactory = Callable[[], TaskStore]
_TEMPLATES = Jinja2Templates(directory=Path(__file__).parent / "templates")


def resolve_store(project_dir: Path | None = None) -> SqliteTaskStore:
    """Resolve gza's configured database and open it through the public API."""
    config = Config.load(project_dir or Path.cwd(), discover=True)
    return SqliteTaskStore.from_config(config, open_mode="query_only")


def create_app(
    *,
    project_dir: Path | None = None,
    store_factory: StoreFactory | None = None,
    instance_id: str | None = None,
) -> FastAPI:
    """Build the server application.

    ``store_factory`` is an explicit testing seam; production requests always
    resolve the database with gza's normal config and task-store APIs.
    """
    make_store = store_factory or (lambda: resolve_store(project_dir))
    server_instance_id = instance_id or os.environ.get("GZA_SERVER_INSTANCE_ID")
    app = FastAPI(title="gza-server", version=__version__)

    @app.get("/api/health")
    def health() -> dict[str, object]:
        store = make_store()
        return {
            "status": "ok",
            "version": __version__,
            "db_path": str(store.db_path),
            "task_count": len(store.get_all()),
            "instance_id": server_instance_id,
        }

    @app.get("/")
    def index(request: Request):
        return _TEMPLATES.TemplateResponse(
            request=request,
            name="index.html",
            context={"version": __version__},
        )

    return app
