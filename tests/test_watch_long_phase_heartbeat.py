"""Tests for verify phase-pass logging in the watch long-phase heartbeat."""

from __future__ import annotations

from pathlib import Path

from gza.cli.watch import _WatchLog, _WatchLongPhaseReporter


def _make_heartbeat(tmp_path: Path):
    log = _WatchLog(tmp_path / "watch.log", quiet=True)
    reporter = _WatchLongPhaseReporter(log=log, threshold_seconds=1, interval_seconds=1)
    heartbeat = reporter.start("verify:candidate", "gza-123")
    return log, heartbeat


def test_note_raw_output_logs_each_verify_phase_pass_with_running_total(tmp_path: Path) -> None:
    log, heartbeat = _make_heartbeat(tmp_path)

    heartbeat.note_raw_output(b"gza-verify phase=start name=ruff\n")
    heartbeat.note_raw_output(b"gza-verify phase=passed name=ruff duration_seconds=1.234567\n")
    heartbeat.note_raw_output(b"gza-verify phase=passed name=ty duration_seconds=3.5\n")

    lines = log.path.read_text(encoding="utf-8").splitlines()
    heartbeat_lines = [line for line in lines if " HEARTBEAT " in line]
    assert len(heartbeat_lines) == 2
    assert "gza-123 ruff passed (1.2s), 1/6 phases done" in heartbeat_lines[0]
    assert "gza-123 ty passed (3.5s), 2/6 phases done" in heartbeat_lines[1]


def test_note_raw_output_logs_failed_phase_without_incrementing_total(tmp_path: Path) -> None:
    log, heartbeat = _make_heartbeat(tmp_path)

    heartbeat.note_raw_output(b"gza-verify phase=passed name=ruff duration_seconds=1.0\n")
    heartbeat.note_raw_output(b"gza-verify phase=failed name=mypy duration_seconds=0.9\n")

    lines = log.path.read_text(encoding="utf-8").splitlines()
    heartbeat_lines = [line for line in lines if " HEARTBEAT " in line]
    assert "gza-123 mypy failed (0.9s), 1/6 phases done" in heartbeat_lines[-1]


def test_note_raw_output_handles_phase_lines_split_across_chunks(tmp_path: Path) -> None:
    log, heartbeat = _make_heartbeat(tmp_path)

    line = b"gza-verify phase=passed name=ruff duration_seconds=1.0\n"
    heartbeat.note_raw_output(line[:10])
    heartbeat.note_raw_output(line[10:])

    lines = log.path.read_text(encoding="utf-8").splitlines()
    heartbeat_lines = [line for line in lines if " HEARTBEAT " in line]
    assert len(heartbeat_lines) == 1
    assert "ruff passed" in heartbeat_lines[0]


def test_note_raw_output_ignores_unrelated_verify_output(tmp_path: Path) -> None:
    log, heartbeat = _make_heartbeat(tmp_path)

    heartbeat.note_raw_output(b"some unrelated test output\nmore output\n")

    lines = log.path.read_text(encoding="utf-8").splitlines()
    assert not [line for line in lines if " HEARTBEAT " in line]


def test_note_raw_output_is_noop_after_finish(tmp_path: Path) -> None:
    log, heartbeat = _make_heartbeat(tmp_path)
    heartbeat.finish()

    heartbeat.note_raw_output(b"gza-verify phase=passed name=ruff duration_seconds=1.0\n")

    lines = log.path.read_text(encoding="utf-8").splitlines()
    assert not [line for line in lines if " HEARTBEAT " in line]
