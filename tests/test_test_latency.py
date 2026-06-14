"""Tests for the unit-test latency reporting helpers."""

from __future__ import annotations

import json
import signal
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

import pytest

import gza.test_latency as test_latency
from gza.test_latency import MeasuredTest, build_report, render_json, render_markdown, render_summary


def test_markdown_lists_every_test_at_or_above_threshold() -> None:
    report = build_report(
        [
            MeasuredTest("tests/test_alpha.py::test_fast", 0.010),
            MeasuredTest("tests/test_alpha.py::test_mid_a", 0.200),
            MeasuredTest("tests/test_alpha.py::test_mid_b", 0.200),
            MeasuredTest("tests/test_alpha.py::test_mid_c", 0.200),
        ],
        total_wall_time_seconds=1.75,
        generated_at="2026-05-17T15:30:00Z",
    )

    markdown = render_markdown(report)

    assert "## Slow tests (≥p95)" in markdown
    assert "## Slow tests (≥p99)" in markdown
    assert report.p95_threshold_ms == 200
    assert len(report.slow_tests_p95) == 3
    assert [item.nodeid for item in report.slow_tests_p95] == [
        "tests/test_alpha.py::test_mid_a",
        "tests/test_alpha.py::test_mid_b",
        "tests/test_alpha.py::test_mid_c",
    ]
    assert "`tests/test_alpha.py::test_mid_a`" in markdown


def test_json_and_summary_include_expected_metrics() -> None:
    report = build_report(
        [
            MeasuredTest("tests/test_alpha.py::test_fast", 0.020),
            MeasuredTest("tests/test_alpha.py::test_slow", 0.120),
            MeasuredTest("tests/test_alpha.py::test_slowest", 0.500),
        ],
        total_wall_time_seconds=0.90,
        generated_at="2026-05-17T15:30:00Z",
    )

    payload = json.loads(render_json(report))
    summary = render_summary(report)

    assert payload["generated_at"] == "2026-05-17T15:30:00Z"
    assert payload["tests_run"] == 3
    assert payload["slow_tests_p95"]
    assert summary.startswith("latency: p50=")
    assert "p95=" in summary
    assert summary.endswith("n=3")


def test_sub_millisecond_percentiles_still_select_slow_tail_rows() -> None:
    report = build_report(
        [
            MeasuredTest("tests/test_alpha.py::test_fast", 0.0004),
            MeasuredTest("tests/test_alpha.py::test_mid", 0.0006),
            MeasuredTest("tests/test_alpha.py::test_slow", 0.0007),
        ],
        total_wall_time_seconds=0.02,
        generated_at="2026-05-17T15:30:00Z",
    )

    markdown = render_markdown(report)

    assert report.p95_threshold_ms == 1
    assert report.p99_threshold_ms == 1
    assert [item.nodeid for item in report.slow_tests_p95] == ["tests/test_alpha.py::test_slow"]
    assert [item.nodeid for item in report.slow_tests_p99] == ["tests/test_alpha.py::test_slow"]
    assert "## Slow tests (≥p95)" in markdown
    assert "1ms" in markdown
    assert "`tests/test_alpha.py::test_slow`" in markdown


def test_default_report_path_uses_tmp_under_repo_root(monkeypatch) -> None:
    """Default latency reports should go under tmp/, not the tracked repo root."""
    monkeypatch.setattr(test_latency, "_repo_root", lambda: Path("/repo"))
    monkeypatch.setattr(
        test_latency,
        "datetime",
        type("FakeDateTime", (), {"now": staticmethod(lambda: datetime(2026, 5, 30, 3, 31, 59))}),
    )

    markdown_path = test_latency._default_report_path(json_mode=False)
    json_path = test_latency._default_report_path(json_mode=True)

    assert markdown_path == "/repo/tmp/test-latency-20260530033159.md"
    assert json_path == "/repo/tmp/test-latency-20260530033159.json"


def test_build_partial_summary_marks_sigterm_output() -> None:
    plugin = test_latency._TimingPlugin()
    plugin.test_durations.extend(
        [
            MeasuredTest("tests/test_alpha.py::test_fast", 0.020),
            MeasuredTest("tests/test_alpha.py::test_slow", 0.120),
        ]
    )
    state = test_latency._SigtermSummaryState(
        plugin=plugin,
        started=10.0,
        previous_handler=signal.SIG_DFL,
    )

    summary = test_latency._build_partial_summary(state)

    assert summary.startswith("latency: p50=")
    assert "n=2" in summary
    assert "(partial before SIGTERM)" in summary


def test_install_sigterm_summary_handler_emits_summary_then_reemits(monkeypatch, capsys) -> None:
    plugin = test_latency._TimingPlugin()
    plugin.test_durations.append(MeasuredTest("tests/test_alpha.py::test_slow", 0.120))
    previous_handler = object()
    recorded_reemit: list[tuple[int, object]] = []

    monkeypatch.setattr(
        test_latency,
        "_reemit_sigterm",
        lambda signum, frame, previous: recorded_reemit.append((signum, previous)),
    )

    with patch("gza.test_latency.signal.getsignal", return_value=previous_handler), patch(
        "gza.test_latency.time.perf_counter",
        return_value=15.0,
    ), patch(
        "gza.test_latency.signal.signal",
        side_effect=lambda sig, handler: recorded_reemit.append((sig, handler)),
    ):
        test_latency._install_sigterm_summary_handler(plugin, started=10.0)
        handler = recorded_reemit[0][1]
        assert callable(handler)
        handler(signal.SIGTERM, None)

    captured = capsys.readouterr()
    assert "latency: p50=" in captured.err
    assert "(partial before SIGTERM)" in captured.err
    assert recorded_reemit[-1] == (signal.SIGTERM, previous_handler)


def test_reemit_sigterm_calls_previous_handler_then_terminates() -> None:
    calls: list[tuple[str, int]] = []

    def _previous_handler(signum: int, frame: object | None) -> None:
        del frame
        calls.append(("previous", signum))

    def _fake_signal(signum: int, handler: object) -> None:
        assert handler == signal.SIG_DFL
        calls.append(("signal", signum))

    def _fake_kill(pid: int, signum: int) -> None:
        assert pid == 12345
        calls.append(("kill", signum))
        raise SystemExit(128 + signum)

    with patch("gza.test_latency.signal.signal", side_effect=_fake_signal), patch(
        "gza.test_latency.os.getpid",
        return_value=12345,
    ), patch("gza.test_latency.os.kill", side_effect=_fake_kill), pytest.raises(
        SystemExit
    ) as excinfo:
        test_latency._reemit_sigterm(signal.SIGTERM, None, _previous_handler)

    assert excinfo.value.code == 128 + signal.SIGTERM
    assert calls == [
        ("previous", signal.SIGTERM),
        ("signal", signal.SIGTERM),
        ("kill", signal.SIGTERM),
    ]


def test_reemit_sigterm_still_restores_default_and_resends_when_previous_handler_raises() -> None:
    calls: list[tuple[str, int]] = []

    def _previous_handler(signum: int, frame: object | None) -> None:
        del frame
        calls.append(("previous", signum))
        raise RuntimeError("prior handler failed")

    def _fake_signal(signum: int, handler: object) -> None:
        assert handler == signal.SIG_DFL
        calls.append(("signal", signum))

    def _fake_kill(pid: int, signum: int) -> None:
        assert pid == 12345
        calls.append(("kill", signum))
        raise SystemExit(128 + signum)

    with patch("gza.test_latency.signal.signal", side_effect=_fake_signal), patch(
        "gza.test_latency.os.getpid",
        return_value=12345,
    ), patch("gza.test_latency.os.kill", side_effect=_fake_kill), pytest.raises(
        SystemExit
    ) as excinfo:
        test_latency._reemit_sigterm(signal.SIGTERM, None, _previous_handler)

    assert excinfo.value.code == 128 + signal.SIGTERM
    assert calls == [
        ("previous", signal.SIGTERM),
        ("signal", signal.SIGTERM),
        ("kill", signal.SIGTERM),
    ]
