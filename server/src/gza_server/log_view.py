"""Assemble a task-log page or API payload from a resolved task detail."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from .log_entries import LogEvent, events_for
from .task_detail import TaskDetail
from .task_log import DEFAULT_MAX_BYTES, LogChunk, clamp_max_bytes, read_chunk, resolve_task_log

TERMINAL_STATUSES = frozenset({"completed", "failed", "cancelled", "canceled", "merged"})


@dataclass(frozen=True)
class LogView:
    """Everything the log page and its JSON twin need to render one window."""

    detail: TaskDetail
    events: tuple[LogEvent, ...]
    stream: str
    start_offset: int
    next_offset: int
    size: int
    eof: bool
    truncated_head: bool
    missing_message: str | None
    path: str | None

    @property
    def task(self):
        return self.detail.task

    @property
    def is_running(self) -> bool:
        """A running task's log can still grow, so the page may follow it."""
        return self.task.status not in TERMINAL_STATUSES

    @property
    def log_url(self) -> str:
        return f"{self.detail.detail_url}/log"

    @property
    def other_stream(self) -> str:
        return "conversation" if self.stream == "ops" else "ops"

    def url_at(self, offset: int) -> str:
        """A link back into this same window, for load-older / load-newer."""
        return f"{self.log_url}?stream={self.stream}&offset={offset}"

    def json_record(self) -> dict[str, object]:
        return {
            "id": self.task.id,
            "project_id": self.detail.project_id,
            "status": self.task.status,
            "stream": self.stream,
            "path": self.path,
            "missing": self.missing_message,
            "start_offset": self.start_offset,
            "next_offset": self.next_offset,
            "size": self.size,
            "eof": self.eof,
            "truncated_head": self.truncated_head,
            "is_running": self.is_running,
            "events": [
                {
                    "kind": event.kind,
                    "offset": event.offset,
                    "title": event.title,
                    "body": event.body,
                    "role": event.role,
                    "tool_name": event.tool_name,
                    "is_error": event.is_error,
                }
                for event in self.events
            ],
        }


def build_log_view(
    detail: TaskDetail,
    *,
    stream: str = "conversation",
    offset: int | None = None,
    max_bytes: int = DEFAULT_MAX_BYTES,
    tail: bool | None = None,
    fallback_root: Path | None = None,
) -> LogView:
    """Read one window of a task's log and map it to renderable events.

    With no explicit ``offset`` the window defaults to the tail: an operator
    opening a log is nearly always asking what happened most recently, and on a
    running task that is the only part still changing.
    """
    stream = "ops" if stream == "ops" else "conversation"
    # A store opened without a registered project root still has a serving
    # project dir; logs live under the project either way.
    location = resolve_task_log(detail.task, detail.project_root or fallback_root)

    if location is None:
        return _empty(detail, stream, "No log found: this task's project root is not readable here")

    path = location.ops if stream == "ops" else location.conversation
    if not path.is_file():
        return _empty(detail, stream, location.missing_message(ops=stream == "ops"), path=str(path))

    if tail is None:
        tail = offset is None
    chunk = read_chunk(path, offset=offset, max_bytes=clamp_max_bytes(max_bytes), tail=tail)
    return _from_chunk(detail, stream, chunk, path=str(path))


def _from_chunk(detail: TaskDetail, stream: str, chunk: LogChunk, *, path: str) -> LogView:
    events: list[LogEvent] = []
    for entry in chunk.entries:
        events.extend(events_for(entry.offset, entry.data, entry.raw))
    return LogView(
        detail=detail,
        events=tuple(events),
        stream=stream,
        start_offset=chunk.start_offset,
        next_offset=chunk.next_offset,
        size=chunk.size,
        eof=chunk.eof,
        truncated_head=chunk.truncated_head,
        missing_message=None,
        path=path,
    )


def _empty(detail: TaskDetail, stream: str, message: str, *, path: str | None = None) -> LogView:
    return LogView(
        detail=detail,
        events=(),
        stream=stream,
        start_offset=0,
        next_offset=0,
        size=0,
        eof=True,
        truncated_head=False,
        missing_message=message,
        path=path,
    )
