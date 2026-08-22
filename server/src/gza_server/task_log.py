"""Byte-offset paging over a task's JSONL conversation and ops logs.

Task logs are append-only JSONL and routinely reach tens of megabytes, so every
read here is bounded by ``max_bytes`` and addressed by byte offset. Nothing in
this module walks the file from the start to reach a position: paging a large
log costs the same as paging a small one, and a live tail costs only the bytes
that were appended since the caller's last cursor.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

from gza.config import Config, ConfigError
from gza.db import Task
from gza.log_paths import resolve_conversation_log_path, resolve_ops_log_path

DEFAULT_MAX_BYTES = 256 * 1024
MAX_MAX_BYTES = 4 * 1024 * 1024


@dataclass(frozen=True)
class LogLocation:
    """Where a task's logs live, whether or not they are present."""

    conversation: Path
    ops: Path

    @property
    def conversation_exists(self) -> bool:
        return self.conversation.is_file()

    @property
    def ops_exists(self) -> bool:
        return self.ops.is_file()

    def missing_message(self, *, ops: bool = False) -> str:
        """Operator-facing empty state naming the path we actually looked at."""
        return f"No log found at {self.ops if ops else self.conversation}"


@dataclass(frozen=True)
class LogEntry:
    """One JSONL record, or the raw line when it does not parse."""

    offset: int
    data: dict | None
    raw: str

    @property
    def parsed(self) -> bool:
        return self.data is not None


@dataclass(frozen=True)
class LogChunk:
    """A bounded window of log entries plus the cursors to continue from."""

    entries: tuple[LogEntry, ...]
    start_offset: int
    next_offset: int
    size: int
    truncated_head: bool

    @property
    def eof(self) -> bool:
        """True when ``next_offset`` has reached the end of the complete lines."""
        return self.next_offset >= self.size


def resolve_task_log(task: Task, project_root: Path | None) -> LogLocation | None:
    """Resolve a task's log paths from its owning project's config.

    Returns ``None`` when no config can be loaded for the project, which is the
    honest answer for a task whose project root is not readable from here.
    """
    if project_root is None:
        return None
    try:
        config = Config.load(project_root, discover=True)
    except (ConfigError, OSError, ValueError):
        return None
    conversation = resolve_conversation_log_path(config, task)
    return LogLocation(conversation=conversation, ops=resolve_ops_log_path(config, conversation))


def clamp_max_bytes(value: int | None) -> int:
    """Bound a caller-supplied window so one request cannot read the whole file."""
    if value is None or value <= 0:
        return DEFAULT_MAX_BYTES
    return min(value, MAX_MAX_BYTES)


def read_chunk(
    path: Path,
    *,
    offset: int | None = None,
    max_bytes: int = DEFAULT_MAX_BYTES,
    tail: bool = False,
) -> LogChunk:
    """Read up to ``max_bytes`` of complete JSONL lines from ``path``.

    ``offset`` is a byte position that must sit on a line boundary -- always a
    ``start_offset`` or ``next_offset`` handed out by an earlier call. Pass
    ``tail=True`` with no offset to read the final window instead of the first;
    the partial line at the seek point is dropped and reported through
    ``truncated_head``.

    A trailing line without its newline is a record the writer has not finished
    flushing, so it is neither returned nor stepped over: ``next_offset`` stops
    in front of it and the next call picks it up once it is complete.
    """
    max_bytes = clamp_max_bytes(max_bytes)
    if not path.is_file():
        return LogChunk(entries=(), start_offset=0, next_offset=0, size=0, truncated_head=False)

    size = path.stat().st_size
    truncated_head = False

    if offset is None:
        if tail:
            start = max(0, size - max_bytes)
            truncated_head = start > 0
        else:
            start = 0
    else:
        # A cursor past a truncated or rotated file restarts rather than erroring.
        start = offset if 0 <= offset <= size else 0

    with path.open("rb") as handle:
        handle.seek(start)
        if truncated_head:
            # Drop the partial record we landed inside of.
            partial = handle.readline()
            start += len(partial)
        buffer = handle.read(max_bytes)
        if b"\n" not in buffer and start + len(buffer) < size:
            # One record longer than the window: take the whole line rather than
            # a fragment, so the cursor always lands on a record boundary.
            buffer += handle.readline(MAX_MAX_BYTES)

    complete, _, _remainder = buffer.rpartition(b"\n")
    if complete:
        complete += b"\n"

    entries: list[LogEntry] = []
    cursor = start
    for line in complete.splitlines(keepends=True):
        entries.append(_entry(cursor, line))
        cursor += len(line)

    return LogChunk(
        entries=tuple(entries),
        start_offset=start,
        next_offset=cursor,
        size=size,
        truncated_head=truncated_head,
    )


def _entry(offset: int, line: bytes) -> LogEntry:
    raw = line.decode("utf-8", errors="replace").rstrip("\n")
    try:
        data = json.loads(raw)
    except ValueError:
        return LogEntry(offset=offset, data=None, raw=raw)
    if not isinstance(data, dict):
        return LogEntry(offset=offset, data=None, raw=raw)
    return LogEntry(offset=offset, data=data, raw=raw)
