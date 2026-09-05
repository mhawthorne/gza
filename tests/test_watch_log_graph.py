"""Tests for scripts/watch_log_graph.py attention parsing."""

from __future__ import annotations

import importlib.util
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any

_SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "watch_log_graph.py"


def _load_module():
    spec = importlib.util.spec_from_file_location("watch_log_graph", _SCRIPT)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def _write_log(path: Path, lines: list[str]) -> None:
    path.write_text("\n".join(lines + [""]), encoding="utf-8")


def _wake_line(when: str, running: int) -> str:
    return (
        f"{when} WAKE      checking... "
        f"({running} running, pending=0 runnable, blocked=0, 4 slots)"
    )


def _merge_line(when: str, task_id: str = "gza-1") -> str:
    return f"{when} MERGE     {task_id} -> main"


def test_parse_log_infers_zero_attention_from_silent_cycles(tmp_path: Path) -> None:
    """A cycle with no attention line (once reporting has begun) means zero.

    The watch emits an attention line every cycle while the count is > 0 and
    nothing when it is zero, so the grapher must resolve silent cycles to 0
    rather than carrying the last non-zero value forward forever.
    """
    module = _load_module()
    log = tmp_path / "watch.log"
    log.write_text(
        "\n".join(
            [
                # Cycle 1: before any attention reporting -> unknown (None).
                "10:00:00 WAKE      checking... (0 running, 0 pending, 4 slots)",
                # Cycle 2: attention appears -> 2.
                "10:01:00 WAKE      checking... (0 running, 0 pending, 4 slots)",
                "10:01:05 INFO      Needs attention (2 tasks):",
                "                     gza-1 ...",
                "                     gza-2 ...",
                # Cycle 3: unchanged -> still 2.
                "10:02:00 WAKE      checking... (0 running, 0 pending, 4 slots)",
                "10:02:05 INFO      2 tasks still need attention (unchanged)",
                # Cycle 4: silent -> dropped to 0.
                "10:03:00 WAKE      checking... (0 running, 0 pending, 4 slots)",
                # Cycle 5: still silent -> stays 0.
                "10:04:00 WAKE      checking... (0 running, 0 pending, 4 slots)",
                "",
            ]
        ),
        encoding="utf-8",
    )

    points, _merges = module.parse_log(str(log), datetime(2026, 7, 8), legacy_attention=True)

    assert [p.parked for p in points] == [None, 2, 2, 0, 0]


def test_parse_log_accounting_line_overrides_all_series(tmp_path: Path) -> None:
    """A cycle accounting line is the source of truth for all six series."""
    module = _load_module()
    log = tmp_path / "watch.log"
    log.write_text(
        "\n".join(
            [
                # Legacy cycle: WAKE counts + attention -> parked; recovery/other None.
                "10:00:00 WAKE      checking... (1 running, pending=2 runnable, blocked=3, 4 slots)",
                "10:00:05 INFO      Needs attention (4 tasks):",
                "                     gza-1 ...",
                # Accounting cycle: all six series come from the accounting line,
                # not the (deliberately different) WAKE counts.
                "10:01:00 WAKE      checking... (9 running, pending=9 runnable, blocked=9, 0 slots)",
                "10:01:05 INFO      cycle accounting: running=1 pending=2 blocked=3 parked=4 recovery=5 other=6",
                # Accounting cycle with zero parked: parked is 0, not carried forward.
                "10:02:00 WAKE      checking... (0 running, pending=0 runnable, blocked=0, 4 slots)",
                "10:02:05 INFO      cycle accounting: running=0 pending=1 blocked=0 parked=0 recovery=1 other=0",
                "",
            ]
        ),
        encoding="utf-8",
    )

    points, _merges = module.parse_log(str(log), datetime(2026, 7, 8), legacy_attention=True)

    assert len(points) == 3
    legacy, acct, acct_zero = points
    assert (legacy.running, legacy.pending, legacy.blocked) == (1, 2, 3)
    assert legacy.parked == 4
    assert legacy.recovery is None
    assert legacy.other is None
    assert (acct.running, acct.pending, acct.blocked) == (1, 2, 3)
    assert (acct.parked, acct.recovery, acct.other) == (4, 5, 6)
    assert (acct_zero.parked, acct_zero.recovery, acct_zero.other) == (0, 1, 0)


def test_parse_log_leaves_legacy_parked_unknown_by_default(tmp_path: Path) -> None:
    """Without --legacy-attention, pre-accounting-line cycles report parked=None.

    The old "Needs attention" lines count a broader, differently-scoped set of
    conditions than the modern parked bucket; conflating them under one series
    name is misleading, so the default leaves them unknown rather than guessing.
    """
    module = _load_module()
    log = tmp_path / "watch.log"
    log.write_text(
        "\n".join(
            [
                "10:00:00 WAKE      checking... (1 running, pending=2 runnable, blocked=3, 4 slots)",
                "10:00:05 INFO      Needs attention (4 tasks):",
                "                     gza-1 ...",
                "10:01:00 WAKE      checking... (9 running, pending=9 runnable, blocked=9, 0 slots)",
                "10:01:05 INFO      cycle accounting: running=1 pending=2 blocked=3 parked=4 recovery=5 other=6",
                "",
            ]
        ),
        encoding="utf-8",
    )

    points, _merges = module.parse_log(str(log), datetime(2026, 7, 8))

    legacy, acct = points
    assert legacy.parked is None
    assert acct.parked == 4


def test_parse_log_attaches_unit_accounting_to_its_cycle(tmp_path: Path) -> None:
    """A ``unit accounting:`` line (emitted right after the task-level line)

    populates Point.unit with the merge-unit-level counts; cycles with no such
    line leave Point.unit as None rather than guessing or carrying forward.
    """
    module = _load_module()
    log = tmp_path / "watch.log"
    log.write_text(
        "\n".join(
            [
                "10:00:00 WAKE      checking... (1 running, pending=2 runnable, blocked=3, 4 slots)",
                "10:00:05 INFO      cycle accounting: running=1 pending=2 blocked=3 parked=4 recovery=5 other=6",
                "10:00:06 INFO      unit accounting: running=1 pending=1 blocked=1 parked=2 recovery=1 other=0",
                # No unit accounting line on this cycle - predates the feature.
                "10:01:00 WAKE      checking... (0 running, pending=0 runnable, blocked=0, 4 slots)",
                "10:01:05 INFO      cycle accounting: running=0 pending=0 blocked=0 parked=0 recovery=0 other=0",
                "",
            ]
        ),
        encoding="utf-8",
    )

    points, _merges = module.parse_log(str(log), datetime(2026, 7, 8))

    with_unit, without_unit = points
    assert with_unit.unit is not None
    assert (with_unit.unit.running, with_unit.unit.pending, with_unit.unit.blocked) == (1, 1, 1)
    assert (with_unit.unit.parked, with_unit.unit.recovery, with_unit.unit.other) == (2, 1, 0)
    assert without_unit.unit is None


def test_select_view_defaults_to_units_when_available(tmp_path: Path) -> None:
    module = _load_module()
    log = tmp_path / "watch.log"
    log.write_text(
        "\n".join(
            [
                "10:00:00 WAKE      checking... (1 running, pending=2 runnable, blocked=3, 4 slots)",
                "10:00:05 INFO      cycle accounting: running=1 pending=2 blocked=3 parked=4 recovery=5 other=6",
                "10:00:06 INFO      unit accounting: running=9 pending=8 blocked=7 parked=6 recovery=5 other=0",
                "",
            ]
        ),
        encoding="utf-8",
    )
    points, _merges = module.parse_log(str(log), datetime(2026, 7, 8))

    display, label = module._select_view(points, task_level=False)

    assert label == "merge units"
    assert (display[0].running, display[0].parked) == (9, 6)


def test_select_view_task_level_flag_shows_task_counts(tmp_path: Path) -> None:
    module = _load_module()
    log = tmp_path / "watch.log"
    log.write_text(
        "\n".join(
            [
                "10:00:00 WAKE      checking... (1 running, pending=2 runnable, blocked=3, 4 slots)",
                "10:00:05 INFO      cycle accounting: running=1 pending=2 blocked=3 parked=4 recovery=5 other=6",
                "10:00:06 INFO      unit accounting: running=9 pending=8 blocked=7 parked=6 recovery=5 other=0",
                "",
            ]
        ),
        encoding="utf-8",
    )
    points, _merges = module.parse_log(str(log), datetime(2026, 7, 8))

    display, label = module._select_view(points, task_level=True)

    assert label == "tasks"
    assert (display[0].running, display[0].parked) == (1, 4)


def test_select_view_falls_back_to_tasks_when_no_unit_data(tmp_path: Path) -> None:
    """A log from before unit accounting existed must not crash or go blank."""
    module = _load_module()
    log = tmp_path / "watch.log"
    log.write_text(
        "\n".join(
            [
                "10:00:00 WAKE      checking... (1 running, pending=2 runnable, blocked=3, 4 slots)",
                "10:00:05 INFO      cycle accounting: running=1 pending=2 blocked=3 parked=4 recovery=5 other=6",
                "",
            ]
        ),
        encoding="utf-8",
    )
    points, _merges = module.parse_log(str(log), datetime(2026, 7, 8))

    display, label = module._select_view(points, task_level=False)

    assert "tasks" in label
    assert (display[0].running, display[0].parked) == (1, 4)


def test_parse_watch_logs_all_spans_archives_and_live_chronologically(
    tmp_path: Path,
) -> None:
    module = _load_module()
    log_dir = tmp_path / ".gza"
    log_dir.mkdir()
    _write_log(log_dir / "watch.2026-08-25T10-00-00.log", [_wake_line("09:55:00", 1)])
    _write_log(log_dir / "watch.2026-08-25T11-00-00.log", [_wake_line("10:30:00", 2)])
    _write_log(log_dir / "watch.log", [_wake_line("12:00:00", 3)])

    points, _merges = module.parse_watch_logs(log_dir / "watch.log", datetime(2026, 8, 25))

    assert [p.when for p in points] == [
        datetime(2026, 8, 25, 9, 55),
        datetime(2026, 8, 25, 10, 30),
        datetime(2026, 8, 25, 12, 0),
    ]
    assert [p.running for p in points] == [1, 2, 3]
    assert len({p.when for p in points}) == len(points)


def test_parse_watch_logs_start_inside_second_archive_skips_first(
    tmp_path: Path,
) -> None:
    module = _load_module()
    log_dir = tmp_path / ".gza"
    log_dir.mkdir()
    first = log_dir / "watch.2026-08-25T10-00-00.log"
    second = log_dir / "watch.2026-08-25T11-00-00.log"
    live = log_dir / "watch.log"
    _write_log(first, [_wake_line("09:55:00", 1)])
    _write_log(second, [_wake_line("10:30:00", 2)])
    _write_log(live, [_wake_line("12:00:00", 3)])
    original_parse_log = module.parse_log
    parsed_paths: list[Path] = []

    def recording_parse_log(path: Path, base_date: Any, **kwargs: Any):
        parsed_paths.append(Path(path))
        return original_parse_log(path, base_date, **kwargs)

    module.parse_log = recording_parse_log

    points, _merges = module.parse_watch_logs(
        live,
        datetime(2026, 8, 25),
        lo=datetime(2026, 8, 25, 10, 15),
    )

    assert parsed_paths == [second, live]
    assert [p.running for p in points] == [2, 3]


def test_parse_watch_logs_dates_archive_from_filename_not_current_clock(
    tmp_path: Path,
) -> None:
    module = _load_module()
    log_dir = tmp_path / ".gza"
    log_dir.mkdir()
    _write_log(log_dir / "watch.2026-08-24T23-00-00.log", [_wake_line("22:59:00", 1)])
    _write_log(log_dir / "watch.log", [])

    points, _merges = module.parse_watch_logs(log_dir / "watch.log", datetime(2099, 1, 1))

    assert [p.when for p in points] == [datetime(2026, 8, 24, 22, 59)]


def test_parse_watch_logs_archive_anchor_keeps_hms_rows_before_archive_end(
    tmp_path: Path,
) -> None:
    module = _load_module()
    log_dir = tmp_path / ".gza"
    log_dir.mkdir()
    archive_end = datetime(2026, 8, 25, 0, 5)
    _write_log(
        log_dir / "watch.2026-08-25T00-05-00.log",
        [
            _wake_line("23:50:00", 1),
            _wake_line("00:10:00", 2),
            _wake_line("23:59:00", 3),
            _merge_line("23:59:30", "gza-1234"),
        ],
    )
    _write_log(log_dir / "watch.log", [])

    points, merges = module.parse_watch_logs(log_dir / "watch.log", datetime(2099, 1, 1))

    assert [p.when for p in points] == [
        datetime(2026, 8, 23, 23, 50),
        datetime(2026, 8, 24, 0, 10),
        datetime(2026, 8, 24, 23, 59),
    ]
    assert merges == [(datetime(2026, 8, 24, 23, 59, 30), "gza-1234")]
    assert points[-1].when <= archive_end
    assert merges[-1][0] <= archive_end


def test_parse_watch_logs_dry_run_target_reads_only_dry_run_family(
    tmp_path: Path,
) -> None:
    module = _load_module()
    log_dir = tmp_path / ".gza"
    log_dir.mkdir()
    _write_log(log_dir / "watch.2026-08-25T10-00-00.log", [_wake_line("09:55:00", 1)])
    _write_log(log_dir / "watch.log", [_wake_line("12:00:00", 2)])
    _write_log(log_dir / "watch.dry-run.2026-08-25T11-00-00.log", [_wake_line("10:30:00", 3)])
    _write_log(log_dir / "watch.dry-run.log", [_wake_line("13:00:00", 4)])

    points, _merges = module.parse_watch_logs(
        log_dir / "watch.dry-run.log", datetime(2026, 8, 25)
    )

    assert [p.running for p in points] == [3, 4]


def test_parse_watch_logs_attention_does_not_cross_file_boundary(tmp_path: Path) -> None:
    module = _load_module()
    log_dir = tmp_path / ".gza"
    log_dir.mkdir()
    _write_log(
        log_dir / "watch.2026-08-25T10-00-00.log",
        [
            _wake_line("09:55:00", 1),
            "09:55:01 INFO      Needs attention (2 tasks):",
        ],
    )
    _write_log(log_dir / "watch.log", [_wake_line("10:05:00", 2)])

    points, _merges = module.parse_watch_logs(
        log_dir / "watch.log", datetime(2026, 8, 25), legacy_attention=True
    )

    assert [p.parked for p in points] == [2, None]


def test_parse_watch_logs_default_hours_does_not_parse_old_archive_when_live_has_newest(
    tmp_path: Path,
) -> None:
    module = _load_module()
    log_dir = tmp_path / ".gza"
    log_dir.mkdir()
    old = log_dir / "watch.2026-08-23T00-00-00.log"
    recent = log_dir / "watch.2026-08-25T11-30-00.log"
    live = log_dir / "watch.log"
    _write_log(old, [_wake_line("23:55:00", 1)])
    _write_log(recent, [_wake_line("11:00:00", 2)])
    _write_log(live, [_wake_line("12:00:00", 3)])
    original_parse_log = module.parse_log
    parsed_paths: list[Path] = []

    def recording_parse_log(path: Path, base_date: Any, **kwargs: Any):
        parsed_paths.append(Path(path))
        return original_parse_log(path, base_date, **kwargs)

    module.parse_log = recording_parse_log

    points, _merges = module.parse_watch_logs(
        live,
        datetime(2026, 8, 25),
        hours=2,
    )

    assert old not in parsed_paths
    assert live in parsed_paths
    assert recent in parsed_paths
    assert [p.running for p in points] == [2, 3]


def test_parse_watch_logs_default_hours_walks_back_only_to_newest_archive_for_anchor(
    tmp_path: Path,
) -> None:
    module = _load_module()
    log_dir = tmp_path / ".gza"
    log_dir.mkdir()
    old = log_dir / "watch.2026-08-23T00-00-00.log"
    recent = log_dir / "watch.2026-08-25T11-30-00.log"
    live = log_dir / "watch.log"
    _write_log(old, [_wake_line("23:55:00", 1)])
    _write_log(recent, [_wake_line("11:00:00", 2)])
    _write_log(live, [])
    original_parse_log = module.parse_log
    parsed_paths: list[Path] = []

    def recording_parse_log(path: Path, base_date: Any, **kwargs: Any):
        parsed_paths.append(Path(path))
        return original_parse_log(path, base_date, **kwargs)

    module.parse_log = recording_parse_log

    points, _merges = module.parse_watch_logs(
        live,
        datetime(2026, 8, 25),
        hours=2,
    )

    assert old not in parsed_paths
    assert parsed_paths[:2] == [live, recent]
    assert [p.running for p in points] == [2]


def test_main_start_after_archive_reports_selected_window_not_family_empty(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    module = _load_module()
    log_dir = tmp_path / ".gza"
    log_dir.mkdir()
    _write_log(log_dir / "watch.2026-08-25T10-00-00.log", [_wake_line("09:55:00", 1)])
    _write_log(log_dir / "watch.log", [])

    result = module.main(
        [
            "--log",
            str(log_dir / "watch.log"),
            "--start",
            "2026-08-25 11:00",
            "--no-png",
        ]
    )

    captured = capsys.readouterr()
    assert result == 1
    assert "no cycles in the selected window" in captured.err
    assert "no WAKE cycles parsed" not in captured.err


def test_watch_loop_bounded_end_stops_after_family_advances_past_window(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    module = _load_module()
    log_dir = tmp_path / ".gza"
    log_dir.mkdir()
    end = datetime(2026, 8, 25, 11, 0)
    _write_log(log_dir / "watch.2026-08-25T11-30-00.log", [_wake_line("10:30:00", 1)])
    _write_log(log_dir / "watch.log", [_wake_line("12:00:00", 2)])
    printed_current: list[list[datetime]] = []

    def fail_sleep(_interval: int) -> None:
        raise AssertionError("bounded completed watch should not schedule another refresh")

    monkeypatch.setattr(
        module,
        "print_current",
        lambda points: printed_current.append([point.when for point in points]),
    )
    monkeypatch.setattr(module, "time", SimpleNamespace(sleep=fail_sleep))

    result = module._watch_loop(
        SimpleNamespace(
            aggregate=None,
            all=False,
            date=datetime(2026, 8, 25),
            end=end,
            hours=24.0,
            legacy_attention=False,
            task_level=False,
            markers=True,
            merge_band=module.MERGE_BAND_DEFAULT,
            merge_bucket="auto",
            merge_labels="auto",
            merges=False,
            no_png=True,
            out=tmp_path / "watch.png",
            resolution="raw",
            start=datetime(2026, 8, 25, 10, 0),
            table_rows=40,
            watch=60,
        ),
        log_dir / "watch.log",
    )

    assert result == 0
    assert printed_current == [[datetime(2026, 8, 25, 10, 30)]]
