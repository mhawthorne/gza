"""FastAPI application for the local gza server."""

from __future__ import annotations

import os
from collections.abc import Callable
from pathlib import Path
from typing import Annotated, Any, Literal, Protocol, cast

from fastapi import Depends, FastAPI, HTTPException, Query, Request
from fastapi.templating import Jinja2Templates

from gza.config import Config
from gza.db import SqliteTaskStore
from gza.task_query import normalize_tag_filters

from . import __version__
from .task_detail import AmbiguousTaskIdError, TaskDetail, query_task_detail
from .task_list import (
    TASK_STATUSES,
    TASK_TYPES,
    TaskListFilters,
    query_task_list,
)


class TaskStore(Protocol):
    """The small portion of gza's task-store API needed in phase 1."""

    db_path: Path

    def get_all(self) -> list[Any]: ...

    def list_tags(self, *, all_projects: bool = False) -> tuple[str, ...]: ...


StoreFactory = Callable[[], TaskStore]
_TEMPLATES = Jinja2Templates(directory=Path(__file__).parent / "templates")


def _task_list_filters(
    q: Annotated[str, Query()] = "",
    tag: Annotated[list[str] | None, Query()] = None,
    status: Annotated[list[str] | None, Query()] = None,
    task_type: Annotated[list[str] | None, Query(alias="type")] = None,
    untagged: Annotated[bool, Query()] = False,
    sort: Annotated[Literal["created", "updated"], Query()] = "updated",
    direction: Annotated[Literal["asc", "desc"], Query()] = "desc",
) -> TaskListFilters:
    tags = normalize_tag_filters(tuple(tag or ())) or ()
    if tags and untagged:
        raise HTTPException(
            status_code=422,
            detail="tag and untagged filters cannot be combined",
        )
    return TaskListFilters(
        prompt=q,
        tags=tags,
        statuses=tuple(status or ()),
        task_types=tuple(task_type or ()),
        untagged=untagged,
        sort=sort,
        direction=direction,
    )


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

    @app.get("/tasks")
    def tasks_page(
        request: Request,
        filters: Annotated[TaskListFilters, Depends(_task_list_filters)],
    ):
        result = query_task_list(cast(SqliteTaskStore, make_store()), filters)
        return _TEMPLATES.TemplateResponse(
            request=request,
            name="tasks.html",
            context={
                "rows": result.rows,
                "known_tags": result.known_tags,
                "statuses": TASK_STATUSES,
                "task_types": TASK_TYPES,
                "filters": filters,
                "created_sort_url": filters.url(
                    "/tasks",
                    sort="created",
                    direction=("asc" if filters.sort != "created" or filters.direction == "desc" else "desc"),
                ),
                "updated_sort_url": filters.url(
                    "/tasks",
                    sort="updated",
                    direction=("asc" if filters.sort != "updated" or filters.direction == "desc" else "desc"),
                ),
            },
        )

    @app.get("/api/tasks")
    def tasks_api(
        filters: Annotated[TaskListFilters, Depends(_task_list_filters)],
    ) -> list[dict[str, object]]:
        return query_task_list(cast(SqliteTaskStore, make_store()), filters).rows

    def load_task_detail(task_id: str, project_id: str | None = None) -> TaskDetail | None:
        return query_task_detail(
            cast(SqliteTaskStore, make_store()),
            task_id,
            project_id=project_id,
        )

    def render_task_detail(request: Request, task_id: str, project_id: str | None = None):
        try:
            detail = load_task_detail(task_id, project_id)
        except AmbiguousTaskIdError as exc:
            return _TEMPLATES.TemplateResponse(
                request=request,
                name="409.html",
                context={"task_id": exc.task_id, "project_ids": exc.project_ids},
                status_code=409,
            )
        if detail is None:
            return _TEMPLATES.TemplateResponse(
                request=request,
                name="404.html",
                context={"task_id": task_id},
                status_code=404,
            )
        return _TEMPLATES.TemplateResponse(
            request=request,
            name="task_detail.html",
            context={"detail": detail, "task": detail.task},
        )

    def task_detail_record(task_id: str, project_id: str | None = None) -> dict[str, object]:
        try:
            detail = load_task_detail(task_id, project_id)
        except AmbiguousTaskIdError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        if detail is None:
            raise HTTPException(status_code=404, detail=f"Task {task_id} not found")
        return detail.json_record()

    @app.get("/tasks/{task_id}")
    def task_detail_page(request: Request, task_id: str):
        return render_task_detail(request, task_id)

    @app.get("/projects/{project_id}/tasks/{task_id}")
    def qualified_task_detail_page(request: Request, project_id: str, task_id: str):
        return render_task_detail(request, task_id, project_id)

    @app.get("/api/tasks/{task_id}")
    def task_detail_api(task_id: str) -> dict[str, object]:
        return task_detail_record(task_id)

    @app.get("/api/projects/{project_id}/tasks/{task_id}")
    def qualified_task_detail_api(project_id: str, task_id: str) -> dict[str, object]:
        return task_detail_record(task_id, project_id)

    return app
