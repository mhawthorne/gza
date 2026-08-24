"""Server-sent events that follow a running task's log as it is written.

Paging and streaming solve different problems. Paging answers "show me a window
of a file that is too big to send at once"; streaming answers "this file is
still being written, tell me when there is more". They share a cursor -- the
byte offset handed out by :mod:`.task_log` -- so a client can page to the tail
and then follow from exactly where it stopped, with no gap and no repeats.

The stream ends on its own when the task reaches a terminal status, and is
bounded in both wall-clock time and bytes so a forgotten browser tab cannot
hold a connection open indefinitely.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator, Callable
from pathlib import Path

from .log_view import TERMINAL_STATUSES, build_log_view
from .task_detail import TaskDetail

DEFAULT_POLL_SECONDS = 1.0
DEFAULT_MAX_SECONDS = 30 * 60
DEFAULT_MAX_BYTES = 32 * 1024 * 1024
HEARTBEAT_SECONDS = 15.0


def sse(event: str, payload: dict[str, object]) -> str:
    """Encode one server-sent event."""
    return f"event: {event}\ndata: {json.dumps(payload)}\n\n"


async def stream_log(
    reload_detail: Callable[[], TaskDetail | None],
    *,
    stream: str = "conversation",
    offset: int = 0,
    fallback_root: Path | None = None,
    poll_seconds: float = DEFAULT_POLL_SECONDS,
    max_seconds: float = DEFAULT_MAX_SECONDS,
    max_bytes: int = DEFAULT_MAX_BYTES,
    sleep: Callable[[float], object] | None = None,
    clock: Callable[[], float] | None = None,
) -> AsyncIterator[str]:
    """Yield SSE frames for new log entries until the task finishes.

    ``reload_detail`` is re-read every poll rather than captured once: the task
    row is how we learn the run ended, and a stale copy would follow a finished
    task forever.

    After a terminal status is observed the log is drained once more before the
    stream closes, so the final entries a worker wrote on its way out are never
    lost to the race between the last write and the status update.
    """
    sleep = sleep or asyncio.sleep
    clock = clock or asyncio.get_event_loop().time
    started = clock()
    sent_bytes = 0
    last_emit = started
    draining = False

    while True:
        detail = reload_detail()
        if detail is None:
            yield sse("end", {"reason": "task_not_found", "offset": offset})
            return

        view = build_log_view(
            detail,
            stream=stream,
            offset=offset,
            tail=False,
            fallback_root=fallback_root,
        )

        if view.missing_message is not None and not draining:
            yield sse("waiting", {"message": view.missing_message, "offset": offset})
        elif view.events:
            payload = view.json_record()
            frame = sse("entries", payload)
            sent_bytes += len(frame)
            offset = view.next_offset
            last_emit = clock()
            yield frame

        if draining:
            yield sse("end", {"reason": "task_finished", "status": detail.task.status, "offset": offset})
            return

        now = clock()
        if detail.task.status in TERMINAL_STATUSES:
            # One more pass to pick up whatever landed after the final write.
            draining = True
            continue
        if sent_bytes >= max_bytes:
            yield sse("end", {"reason": "byte_limit", "offset": offset})
            return
        if now - started >= max_seconds:
            yield sse("end", {"reason": "time_limit", "offset": offset})
            return
        if now - last_emit >= HEARTBEAT_SECONDS:
            last_emit = now
            yield ": keepalive\n\n"

        await sleep(poll_seconds)
