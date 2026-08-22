import json
from pathlib import Path

from gza_server.task_log import (
    LogLocation,
    clamp_max_bytes,
    read_chunk,
)


def _write_lines(path: Path, count: int, *, trailing_partial: bool = False) -> None:
    body = "".join(json.dumps({"n": index, "type": "assistant"}) + "\n" for index in range(count))
    if trailing_partial:
        body += '{"n": 999, "type": "assis'
    path.write_text(body)


def test_missing_log_reports_the_path_it_looked_at(tmp_path: Path) -> None:
    location = LogLocation(conversation=tmp_path / "abc.log", ops=tmp_path / "abc.ops.jsonl")

    assert location.conversation_exists is False
    assert location.missing_message() == f"No log found at {tmp_path / 'abc.log'}"
    assert location.missing_message(ops=True) == f"No log found at {tmp_path / 'abc.ops.jsonl'}"

    chunk = read_chunk(location.conversation)
    assert chunk.entries == ()
    assert chunk.eof is True


def test_paging_forward_covers_every_entry_exactly_once(tmp_path: Path) -> None:
    log = tmp_path / "task.log"
    _write_lines(log, 50)

    seen: list[int] = []
    offset: int | None = None
    for _ in range(50):
        chunk = read_chunk(log, offset=offset, max_bytes=64)
        seen.extend(entry.data["n"] for entry in chunk.entries)
        offset = chunk.next_offset
        if chunk.eof:
            break

    assert seen == list(range(50))


def test_tail_read_returns_the_last_entries_and_flags_truncated_head(tmp_path: Path) -> None:
    log = tmp_path / "task.log"
    _write_lines(log, 50)

    chunk = read_chunk(log, tail=True, max_bytes=100)

    assert chunk.truncated_head is True
    assert chunk.eof is True
    assert [entry.data["n"] for entry in chunk.entries][-1] == 49
    assert len(chunk.entries) < 50
    # The tail cursor is a real line boundary: reading from it yields the same tail.
    again = read_chunk(log, offset=chunk.start_offset, max_bytes=100)
    assert [entry.data["n"] for entry in again.entries] == [
        entry.data["n"] for entry in chunk.entries
    ]


def test_trailing_partial_line_is_withheld_until_complete(tmp_path: Path) -> None:
    log = tmp_path / "task.log"
    _write_lines(log, 3, trailing_partial=True)

    chunk = read_chunk(log)

    assert [entry.data["n"] for entry in chunk.entries] == [0, 1, 2]
    assert chunk.eof is False

    # The writer finishes the record; the next read from the same cursor sees it.
    with log.open("a") as handle:
        handle.write('tant"}\n')
    resumed = read_chunk(log, offset=chunk.next_offset)
    assert [entry.data["n"] for entry in resumed.entries] == [999]
    assert resumed.eof is True


def test_unparsable_line_is_returned_raw_rather_than_dropped(tmp_path: Path) -> None:
    log = tmp_path / "task.log"
    log.write_text('not json at all\n{"n": 1}\n["a list"]\n')

    chunk = read_chunk(log)

    assert [entry.parsed for entry in chunk.entries] == [False, True, False]
    assert chunk.entries[0].raw == "not json at all"
    assert chunk.entries[2].raw == '["a list"]'


def test_line_longer_than_the_window_still_advances(tmp_path: Path) -> None:
    log = tmp_path / "task.log"
    log.write_text(json.dumps({"n": 0, "pad": "x" * 500}) + "\n" + json.dumps({"n": 1}) + "\n")

    chunk = read_chunk(log, max_bytes=64)

    assert chunk.next_offset > 0
    assert chunk.eof is False
    rest = read_chunk(log, offset=chunk.next_offset, max_bytes=64)
    assert [entry.data["n"] for entry in rest.entries] == [1]


def test_cursor_beyond_a_truncated_file_restarts_instead_of_failing(tmp_path: Path) -> None:
    log = tmp_path / "task.log"
    _write_lines(log, 3)

    chunk = read_chunk(log, offset=10_000)

    assert [entry.data["n"] for entry in chunk.entries] == [0, 1, 2]


def test_max_bytes_is_clamped_to_a_bounded_window() -> None:
    assert clamp_max_bytes(None) > 0
    assert clamp_max_bytes(0) == clamp_max_bytes(None)
    assert clamp_max_bytes(-5) == clamp_max_bytes(None)
    assert clamp_max_bytes(10**12) < 10**12
