"""FastAPI application for the local gza server."""

from __future__ import annotations

import os
import secrets
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from threading import Lock
from typing import Annotated, Any, Literal, Protocol, cast
from urllib.parse import parse_qs

from fastapi import Depends, FastAPI, HTTPException, Query, Request
from fastapi.responses import RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel, ConfigDict, Field, StrictBool, StrictStr, ValidationError

from gza.config import Config, ConfigError
from gza.db import SqliteTaskStore
from gza.task_query import normalize_tag_filters

from . import __version__
from .task_detail import AmbiguousTaskIdError, TaskDetail, query_task_detail
from .task_edit import (
    TaskEditConflict,
    edit_task_plan,
    edit_task_prompt,
    parse_content_edit,
)
from .task_list import (
    DEFAULT_PAGE_SIZE,
    PAGE_SIZES,
    TASK_STATUSES,
    TASK_TYPES,
    PageSpec,
    TaskListFilters,
    query_task_list,
)
from .task_tags import (
    BulkTagMutationResult,
    TagMutation,
    apply_bulk_tag_mutation,
    edit_task_tags,
    parse_selected_task_ids,
    parse_tag_mutation,
    parse_task_tag_edit,
    writable_project_store,
)


class TaskStore(Protocol):
    """The small portion of gza's task-store API needed in phase 1."""

    db_path: Path

    def get_all(self) -> list[Any]: ...

    def list_tags(self, *, all_projects: bool = False) -> tuple[str, ...]: ...


StoreFactory = Callable[[], TaskStore]
MutationStoreFactory = Callable[[str], SqliteTaskStore]
_TEMPLATES = Jinja2Templates(directory=Path(__file__).parent / "templates")
_STATIC_DIR = Path(__file__).parent / "static"
_DASHBOARD_RECENT_ROWS = 10


@dataclass(frozen=True)
class _BulkPreviewState:
    """Server-authenticated state for one confirmed bulk mutation."""

    filters: TaskListFilters
    mutation: TagMutation
    targets: tuple[tuple[str, str], ...]
    project_ids: tuple[str, ...]


class _JsonBulkRequest(BaseModel):
    """Strict durable-API schema for bulk tag requests."""

    model_config = ConfigDict(extra="forbid")

    q: StrictStr = ""
    tag: list[StrictStr] = Field(default_factory=list)
    status: list[StrictStr] = Field(default_factory=list)
    task_type: list[StrictStr] = Field(default_factory=list, alias="type")
    untagged: StrictBool = False
    confirmed: StrictBool = False
    target: list[StrictStr] = Field(default_factory=list)
    preview_token: StrictStr = ""
    mutation: StrictStr | None = None
    mutation_tag: StrictStr | None = None
    old_tag: StrictStr | None = None
    new_tag: StrictStr | None = None
    add: StrictStr | None = None
    remove: StrictStr | None = None
    replace: list[StrictStr] | None = None


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


def _payload_values(payload: dict[str, object], key: str) -> list[str]:
    value = payload.get(key, [])
    if value is None:
        return []
    if isinstance(value, list):
        return [str(item) for item in value if str(item).strip()]
    return [str(value)] if str(value).strip() else []


def _payload_bool(payload: dict[str, object], key: str) -> bool:
    value = payload.get(key, False)
    if isinstance(value, bool):
        return value
    if isinstance(value, list):
        value = value[-1] if value else False
    return str(value).lower() in {"1", "true", "yes", "on"}


async def _request_payload(request: Request) -> tuple[dict[str, object], bool]:
    """Read either the durable JSON API shape or a server-rendered form body."""
    is_json = request.headers.get("content-type", "").split(";", 1)[0] == "application/json"
    try:
        if is_json:
            value = await request.json()
            if not isinstance(value, dict):
                raise HTTPException(status_code=422, detail="request body must be an object")
            return cast(dict[str, object], value), True
        parsed = parse_qs(
            (await request.body()).decode("utf-8"),
            keep_blank_values=True,
            encoding="utf-8",
            errors="strict",
        )
    except (UnicodeError, ValueError) as exc:
        raise HTTPException(status_code=400, detail="malformed request body") from exc
    payload: dict[str, object] = {
        key: values if len(values) > 1 else values[0]
        for key, values in parsed.items()
    }
    return payload, False


def _validate_bulk_payload(
    payload: dict[str, object],
    *,
    is_json: bool,
) -> dict[str, object]:
    """Validate JSON strictly while preserving rendered-form coercion."""
    if not is_json:
        return payload
    try:
        validated = _JsonBulkRequest.model_validate(payload)
    except ValidationError as exc:
        raise HTTPException(
            status_code=422,
            detail=exc.errors(include_context=False, include_url=False),
        ) from exc
    return cast(
        dict[str, object],
        validated.model_dump(by_alias=True, exclude_unset=True),
    )


def _require_same_origin_form(request: Request, *, is_json: bool) -> None:
    """Reject browser-form writes that were not submitted by this server."""
    if is_json:
        return
    expected_origin = f"{request.url.scheme}://{request.url.netloc}".lower()
    supplied_origin = request.headers.get("origin", "").lower()
    if not supplied_origin or supplied_origin != expected_origin:
        raise HTTPException(status_code=403, detail="same-origin form submission required")


def _bulk_filters(payload: dict[str, object]) -> TaskListFilters:
    tags = normalize_tag_filters(tuple(_payload_values(payload, "tag"))) or ()
    untagged = _payload_bool(payload, "untagged")
    if tags and untagged:
        raise HTTPException(status_code=422, detail="tag and untagged filters cannot be combined")
    return TaskListFilters(
        prompt=str(payload.get("q", "")),
        tags=tags,
        statuses=tuple(_payload_values(payload, "status")),
        task_types=tuple(_payload_values(payload, "type")),
        untagged=untagged,
    )


def _bulk_targets(payload: dict[str, object]) -> list[dict[str, object]]:
    targets: list[dict[str, object]] = []
    for value in _payload_values(payload, "target"):
        project_id, separator, task_id = value.partition("|")
        if not separator or not project_id or not task_id:
            raise HTTPException(status_code=422, detail="invalid bulk retag target")
        targets.append({"project_id": project_id, "id": task_id})
    return targets


def _qualified_target_records(
    targets: tuple[tuple[str, str], ...],
) -> list[dict[str, str]]:
    return [
        {"project_id": project_id, "id": task_id}
        for project_id, task_id in targets
    ]


def _bulk_result_context(
    rows: list[dict[str, object]],
    mutation_summary: str,
    result: BulkTagMutationResult,
) -> dict[str, object]:
    return {
        "matched_tasks": rows,
        "mutation": mutation_summary,
        "changed_count": result.changed_count,
        "unchanged_count": result.unchanged_count,
        "skipped_count": result.skipped_count,
        "failed_count": result.failed_count,
        "changed_tasks": _qualified_target_records(result.changed),
        "unchanged_tasks": _qualified_target_records(result.unchanged),
        "skipped_tasks": _qualified_target_records(result.skipped),
        "failed_tasks": _qualified_target_records(result.failed),
        "failures": [
            {"project_id": project_id, "error": error}
            for project_id, error in result.failures
        ],
    }


def _selection_mutation_payload(payload: dict[str, Any]) -> dict[str, Any]:
    """Let one tag field serve all three mutations.

    The selection toolbar offers a single "Tag" box plus a "Replace this tag"
    box, so Replace arrives as old_tag plus mutation_tag. Fill in new_tag here
    rather than mirroring the value with a script, so the form keeps working
    with JavaScript disabled.
    """
    if payload.get("mutation") != "replace" or payload.get("new_tag"):
        return payload
    return {**payload, "new_tag": payload.get("mutation_tag", "")}


def _selection_return_url(
    payload: dict[str, Any],
    mutation: TagMutation,
    result: BulkTagMutationResult,
) -> str:
    """Rebuild the originating task-list URL and attach an outcome notice."""
    filters = _bulk_filters(payload)
    page = str(payload.get("page") or "1")
    per_page = str(payload.get("per_page") or DEFAULT_PAGE_SIZE)
    return filters.url(
        "/tasks",
        page=page,
        per_page=per_page,
        applied=mutation.summary,
        changed=str(result.changed_count),
        unchanged=str(result.unchanged_count),
        failed=str(result.failed_count + result.skipped_count),
    )


def create_app(
    *,
    project_dir: Path | None = None,
    store_factory: StoreFactory | None = None,
    mutation_store_factory: MutationStoreFactory | None = None,
    instance_id: str | None = None,
) -> FastAPI:
    """Build the server application.

    ``store_factory`` is an explicit testing seam; production requests always
    resolve the database with gza's normal config and task-store APIs.
    """
    make_store = store_factory or (lambda: resolve_store(project_dir))
    make_mutation_store = mutation_store_factory or (
        lambda project_id: writable_project_store(
            cast(SqliteTaskStore, make_store()),
            project_id,
        )
    )
    server_instance_id = instance_id or os.environ.get("GZA_SERVER_INSTANCE_ID")
    app = FastAPI(title="gza-server", version=__version__)
    app.mount("/static", StaticFiles(directory=_STATIC_DIR), name="static")
    bulk_previews: dict[str, _BulkPreviewState] = {}
    bulk_previews_lock = Lock()

    def save_bulk_preview(state: _BulkPreviewState) -> str:
        token = secrets.token_urlsafe(32)
        with bulk_previews_lock:
            # Bound abandoned confirmation pages without weakening nonce entropy.
            if len(bulk_previews) >= 256:
                bulk_previews.pop(next(iter(bulk_previews)))
            bulk_previews[token] = state
        return token

    def take_bulk_preview(token: str) -> _BulkPreviewState | None:
        with bulk_previews_lock:
            return bulk_previews.pop(token, None)

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
        store = cast(SqliteTaskStore, make_store())
        counts = store.get_status_counts(all_projects=True)
        # Show every status in its canonical order, including the empty ones:
        # a zero is information, and a disappearing cell moves its neighbours.
        status_counts = [
            {"status": status, "count": counts.get(status, 0)} for status in TASK_STATUSES
        ]
        recent = query_task_list(
            store,
            TaskListFilters(),
            page=PageSpec(page=1, per_page=_DASHBOARD_RECENT_ROWS),
        )
        return _TEMPLATES.TemplateResponse(
            request=request,
            name="index.html",
            context={
                "version": __version__,
                "status_counts": status_counts,
                "total_tasks": sum(counts.values()),
                "project_count": len(store.project_query_stores()),
                "recent": recent.rows,
            },
        )

    @app.get("/tasks")
    def tasks_page(
        request: Request,
        filters: Annotated[TaskListFilters, Depends(_task_list_filters)],
        page: Annotated[int, Query(ge=1)] = 1,
        per_page: Annotated[int, Query()] = DEFAULT_PAGE_SIZE,
        applied: Annotated[str | None, Query()] = None,
        changed: Annotated[int, Query(ge=0)] = 0,
        unchanged: Annotated[int, Query(ge=0)] = 0,
        failed: Annotated[int, Query(ge=0)] = 0,
    ):
        page_spec = PageSpec.normalized(page, per_page)
        result = query_task_list(
            cast(SqliteTaskStore, make_store()),
            filters,
            page=page_spec,
        )
        return _TEMPLATES.TemplateResponse(
            request=request,
            name="tasks.html",
            context={
                "rows": result.rows,
                "result": result,
                "notice": (
                    {
                        "applied": applied,
                        "changed": changed,
                        "unchanged": unchanged,
                        "failed": failed,
                    }
                    if applied
                    else None
                ),
                "page_sizes": PAGE_SIZES,
                "page_url": lambda number: filters.url(
                    "/tasks",
                    page=str(number),
                    per_page=str(result.page.per_page),
                ),
                "per_page_url": lambda size: filters.url(
                    "/tasks",
                    page="1",
                    per_page=str(size),
                ),
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

    @app.post("/api/tasks/tags/selected")
    async def selected_task_tags(request: Request):
        """Retag a hand-picked set of tasks.

        Deliberately has no preview step, unlike the filter-scoped bulk route.
        There the operator is trusting a filter to describe a set they cannot
        see, so the preview is the only chance to catch an over-broad match.
        Here they ticked each row themselves, and the confirmation would be
        asking them to re-read a list they just built.
        """
        payload, is_json = await _request_payload(request)
        _require_same_origin_form(request, is_json=is_json)
        try:
            targets = parse_selected_task_ids(payload)
            mutation = parse_tag_mutation(_selection_mutation_payload(payload))
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc

        rows = [
            {"project_id": project_id, "id": task_id}
            for project_id, task_id in targets
        ]
        stores: dict[str, SqliteTaskStore] = {}
        for project_id in dict.fromkeys(project_id for project_id, _ in targets):
            try:
                stores[project_id] = make_mutation_store(project_id)
            except (ConfigError, ValueError) as exc:
                raise HTTPException(
                    status_code=422,
                    detail=f"could not resolve mutation store for project {project_id}",
                ) from exc

        result = apply_bulk_tag_mutation(stores, rows, mutation)
        response_context = _bulk_result_context(rows, mutation.summary, result)
        if is_json:
            return response_context
        # Return to the list the selection was made from, so the operator can
        # keep working through the same filtered set.
        return RedirectResponse(
            _selection_return_url(payload, mutation, result),
            status_code=303,
        )

    @app.post("/api/tasks/tags/bulk")
    async def bulk_task_tags(request: Request):
        payload, is_json = await _request_payload(request)
        _require_same_origin_form(request, is_json=is_json)
        payload = _validate_bulk_payload(payload, is_json=is_json)
        confirmed = _payload_bool(payload, "confirmed")
        if confirmed:
            preview_token = str(payload.get("preview_token", ""))
            state = take_bulk_preview(preview_token)
            if state is None:
                raise HTTPException(
                    status_code=422,
                    detail="a valid, unused bulk preview is required",
                )
            try:
                submitted_filters = _bulk_filters(payload)
                submitted_mutation = parse_tag_mutation(payload)
                submitted_targets = tuple(
                    (str(row["project_id"]), str(row["id"]))
                    for row in _bulk_targets(payload)
                )
            except ValueError as exc:
                raise HTTPException(status_code=422, detail=str(exc)) from exc
            if (
                submitted_filters != state.filters
                or submitted_mutation != state.mutation
                or submitted_targets != state.targets
            ):
                raise HTTPException(
                    status_code=422,
                    detail="bulk apply does not match its authenticated preview",
                )

            rows = [
                {"project_id": project_id, "id": task_id}
                for project_id, task_id in state.targets
            ]
            stores: dict[str, SqliteTaskStore] = {}
            for project_id in state.project_ids:
                try:
                    stores[project_id] = make_mutation_store(project_id)
                except (ConfigError, ValueError) as exc:
                    raise HTTPException(
                        status_code=422,
                        detail=f"could not resolve mutation store for project {project_id}",
                    ) from exc
            result = apply_bulk_tag_mutation(stores, rows, state.mutation)
            response_context = _bulk_result_context(rows, state.mutation.summary, result)
            if is_json:
                return response_context
            return _TEMPLATES.TemplateResponse(
                request=request,
                name="bulk_tags_result.html",
                context=response_context,
            )

        filters = _bulk_filters(payload)
        if not filters.has_selection:
            raise HTTPException(
                status_code=422,
                detail="bulk retag requires at least one selection filter",
            )
        try:
            mutation = parse_tag_mutation(payload)
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc

        base_store = cast(SqliteTaskStore, make_store())
        rows = query_task_list(base_store, filters).rows
        matched = [
            {"id": str(row["id"]), "project_id": str(row["project_id"])}
            for row in rows
        ]
        targets = tuple(
            (str(row["project_id"]), str(row["id"]))
            for row in matched
        )
        project_ids = tuple(dict.fromkeys(project_id for project_id, _task_id in targets))
        preview_token = save_bulk_preview(
            _BulkPreviewState(
                filters=filters,
                mutation=mutation,
                targets=targets,
                project_ids=project_ids,
            )
        )
        if is_json:
            return {
                "matched_tasks": matched,
                "mutation": mutation.summary,
                "confirmed": False,
                "preview_token": preview_token,
            }
        return _TEMPLATES.TemplateResponse(
            request=request,
            name="bulk_tags_confirm.html",
            context={
                "matched_tasks": matched,
                "mutation": mutation,
                "filters": filters,
                "preview_token": preview_token,
            },
        )

    def load_task_detail(task_id: str, project_id: str | None = None) -> TaskDetail | None:
        return query_task_detail(
            cast(SqliteTaskStore, make_store()),
            task_id,
            project_id=project_id,
        )

    def render_task_detail(
        request: Request,
        task_id: str,
        project_id: str | None = None,
        *,
        edit_mode: str | None = None,
        edited_content: str | None = None,
        edit_error: str | None = None,
        status_code: int = 200,
    ):
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
        rejected_edit_mode: str | None = None
        prompt_eligible = detail.task.status == "pending"
        plan_eligible = detail.task.task_type == "plan" and detail.plan_content is not None
        if edit_mode == "prompt" and not prompt_eligible:
            if edit_error is not None and edited_content is not None:
                rejected_edit_mode = edit_mode
            edit_mode = None
        if edit_mode == "plan" and not plan_eligible:
            if edit_error is not None and edited_content is not None:
                rejected_edit_mode = edit_mode
            edit_mode = None
        return _TEMPLATES.TemplateResponse(
            request=request,
            name="task_detail.html",
            context={
                "detail": detail,
                "task": detail.task,
                "edit_mode": edit_mode,
                "edited_content": edited_content,
                "edit_error": edit_error,
                "rejected_edit_mode": rejected_edit_mode,
            },
            status_code=status_code,
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
    def task_detail_page(request: Request, task_id: str, edit: str | None = None):
        return render_task_detail(request, task_id, edit_mode=edit)

    @app.get("/projects/{project_id}/tasks/{task_id}")
    def qualified_task_detail_page(
        request: Request,
        project_id: str,
        task_id: str,
        edit: str | None = None,
    ):
        return render_task_detail(request, task_id, project_id, edit_mode=edit)

    @app.get("/api/tasks/{task_id}")
    def task_detail_api(task_id: str) -> dict[str, object]:
        return task_detail_record(task_id)

    @app.get("/api/projects/{project_id}/tasks/{task_id}")
    def qualified_task_detail_api(project_id: str, task_id: str) -> dict[str, object]:
        return task_detail_record(task_id, project_id)

    @app.post("/api/tasks/{task_id}/tags")
    async def task_tags_api(request: Request, task_id: str):
        payload, is_json = await _request_payload(request)
        _require_same_origin_form(request, is_json=is_json)
        try:
            edit = parse_task_tag_edit(payload, is_json=is_json)
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        try:
            detail = load_task_detail(task_id, edit.project_id)
        except AmbiguousTaskIdError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        if detail is None:
            raise HTTPException(status_code=404, detail=f"Task {task_id} not found")
        try:
            mutation_store = make_mutation_store(detail.project_id)
            tags, changed = edit_task_tags(
                mutation_store,
                task_id,
                add=edit.add,
                remove=edit.remove,
            )
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        if not is_json:
            return RedirectResponse(detail.detail_url, status_code=303)
        return {
            "id": task_id,
            "project_id": detail.project_id,
            "tags": list(tags),
            "changed": changed,
        }

    async def edit_task_content(request: Request, task_id: str, field: str):
        payload, is_json = await _request_payload(request)
        _require_same_origin_form(request, is_json=is_json)
        submitted = payload.get(field)
        try:
            edit = parse_content_edit(payload, field)
        except ValueError as exc:
            if is_json or not isinstance(submitted, str):
                raise HTTPException(status_code=422, detail=str(exc)) from exc
            return render_task_detail(
                request,
                task_id,
                edit_mode=field,
                edited_content=submitted,
                edit_error=str(exc),
                status_code=422,
            )

        try:
            detail = load_task_detail(task_id, edit.project_id)
        except AmbiguousTaskIdError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        if detail is None:
            raise HTTPException(status_code=404, detail=f"Task {task_id} not found")

        try:
            mutation_store = make_mutation_store(detail.project_id)
            if field == "prompt":
                updated = edit_task_prompt(mutation_store, task_id, edit.content)
                response_content = updated.prompt
            else:
                edit_task_plan(mutation_store, task_id, edit.content)
                response_content = edit.content
        except (ConfigError, TaskEditConflict, ValueError) as exc:
            status = 409 if isinstance(exc, TaskEditConflict) else 422
            if is_json:
                raise HTTPException(status_code=status, detail=str(exc)) from exc
            return render_task_detail(
                request,
                task_id,
                edit.project_id,
                edit_mode=field,
                edited_content=edit.content,
                edit_error=str(exc),
                status_code=status,
            )

        if not is_json:
            return RedirectResponse(detail.detail_url, status_code=303)
        return {
            "id": task_id,
            "project_id": detail.project_id,
            field if field == "prompt" else "plan_content": response_content,
        }

    @app.post("/api/tasks/{task_id}/prompt")
    async def task_prompt_api(request: Request, task_id: str):
        return await edit_task_content(request, task_id, "prompt")

    @app.post("/api/tasks/{task_id}/plan")
    async def task_plan_api(request: Request, task_id: str):
        return await edit_task_content(request, task_id, "plan")

    return app
