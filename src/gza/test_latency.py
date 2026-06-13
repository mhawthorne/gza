"""Measure pytest test-call latency and render reports."""

from __future__ import annotations

import argparse
import json
import math
import os
import signal
import subprocess
import sys
import time
from collections.abc import Callable
from contextlib import redirect_stdout
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from types import FrameType
from typing import Any, TypeAlias, cast

import pytest


@dataclass(frozen=True)
class MeasuredTest:
    """One executed test call and its latency."""

    nodeid: str
    duration_seconds: float


@dataclass(frozen=True)
class BucketCount:
    """Count of tests within a fixed latency band."""

    label: str
    count: int
    suite_percent: float


@dataclass(frozen=True)
class LatencyReport:
    """Structured latency report data."""

    generated_at: str
    tests_run: int
    total_wall_time_seconds: float
    percentiles_ms: dict[str, int]
    buckets: list[BucketCount]
    p95_threshold_ms: int
    p99_threshold_ms: int
    slow_tests_p95: list[MeasuredTest]
    slow_tests_p99: list[MeasuredTest]


class _TimingPlugin:
    """Capture call-phase durations for each executed test."""

    def __init__(self) -> None:
        self.test_durations: list[MeasuredTest] = []

    def pytest_runtest_logreport(self, report: pytest.TestReport) -> None:
        if report.when != "call":
            return
        self.test_durations.append(MeasuredTest(nodeid=report.nodeid, duration_seconds=report.duration))


def _format_ms(duration_seconds: float) -> str:
    milliseconds = duration_seconds * 1000
    if milliseconds >= 1000:
        return f"{milliseconds / 1000:.1f}s"
    return f"{_round_ms(duration_seconds)}ms"


def _format_ms_int(duration_ms: int) -> str:
    if duration_ms >= 1000:
        return f"{duration_ms / 1000:.1f}s"
    return f"{duration_ms}ms"


def _format_wall_time(duration_seconds: float) -> str:
    return f"{duration_seconds:.1f}s"


def _percentile(sorted_values: list[float], percentile: int) -> int:
    raw_value = _percentile_value(sorted_values, percentile)
    return _round_ms(raw_value)


def _percentile_value(sorted_values: list[float], percentile: int) -> float:
    if not sorted_values:
        return 0.0
    if len(sorted_values) == 1:
        return sorted_values[0]
    position = (len(sorted_values) - 1) * (percentile / 100)
    lower_index = math.floor(position)
    upper_index = math.ceil(position)
    lower = sorted_values[lower_index]
    upper = sorted_values[upper_index]
    return lower + (upper - lower) * (position - lower_index)


def _round_ms(duration_seconds: float) -> int:
    if duration_seconds <= 0:
        return 0
    return max(1, int(round(duration_seconds * 1000)))


def build_report(test_durations: list[MeasuredTest], total_wall_time_seconds: float, generated_at: str) -> LatencyReport:
    """Build a structured report from raw test durations."""
    ordered = sorted(test_durations, key=lambda item: item.duration_seconds)
    values = [item.duration_seconds for item in ordered]
    percentiles_ms = {
        "p50": _percentile(values, 50),
        "p75": _percentile(values, 75),
        "p90": _percentile(values, 90),
        "p95": _percentile(values, 95),
        "p99": _percentile(values, 99),
        "max": _round_ms(values[-1]) if values else 0,
    }
    p95_threshold_seconds = _percentile_value(values, 95)
    p99_threshold_seconds = _percentile_value(values, 99)
    slow_tests_p95 = sorted(
        [item for item in test_durations if item.duration_seconds >= p95_threshold_seconds],
        key=lambda item: item.duration_seconds,
        reverse=True,
    )
    slow_tests_p99 = sorted(
        [item for item in test_durations if item.duration_seconds >= p99_threshold_seconds],
        key=lambda item: item.duration_seconds,
        reverse=True,
    )
    tests_run = len(test_durations)
    buckets = [
        BucketCount("≤50ms", sum(item.duration_seconds <= 0.05 for item in test_durations), 0.0),
        BucketCount("50-100ms", sum(0.05 < item.duration_seconds <= 0.1 for item in test_durations), 0.0),
        BucketCount("100-250ms", sum(0.1 < item.duration_seconds <= 0.25 for item in test_durations), 0.0),
        BucketCount("250-500ms", sum(0.25 < item.duration_seconds <= 0.5 for item in test_durations), 0.0),
        BucketCount("500ms-1s", sum(0.5 < item.duration_seconds <= 1.0 for item in test_durations), 0.0),
        BucketCount(">1s", sum(item.duration_seconds > 1.0 for item in test_durations), 0.0),
    ]
    normalized_buckets = [
        BucketCount(
            label=bucket.label,
            count=bucket.count,
            suite_percent=(bucket.count / tests_run * 100) if tests_run else 0.0,
        )
        for bucket in buckets
    ]
    return LatencyReport(
        generated_at=generated_at,
        tests_run=tests_run,
        total_wall_time_seconds=total_wall_time_seconds,
        percentiles_ms=percentiles_ms,
        buckets=normalized_buckets,
        p95_threshold_ms=percentiles_ms["p95"],
        p99_threshold_ms=percentiles_ms["p99"],
        slow_tests_p95=slow_tests_p95,
        slow_tests_p99=slow_tests_p99,
    )


def _render_table(headers: list[str], rows: list[list[str]], aligns: list[str]) -> list[str]:
    """Render a GFM table with cells padded so columns align in plain text.

    aligns: one of "l", "r", or "c" per column.
    """
    widths = [len(h) for h in headers]
    for row in rows:
        for i, cell in enumerate(row):
            if len(cell) > widths[i]:
                widths[i] = len(cell)

    def pad(cell: str, width: int, align: str) -> str:
        if align == "r":
            return cell.rjust(width)
        if align == "c":
            return cell.center(width)
        return cell.ljust(width)

    def separator(width: int, align: str) -> str:
        bar = "-" * max(width, 3)
        if align == "r":
            return bar[:-1] + ":"
        if align == "c":
            return ":" + bar[1:-1] + ":"
        return bar

    header_line = "| " + " | ".join(pad(h, widths[i], aligns[i]) for i, h in enumerate(headers)) + " |"
    sep_line = "| " + " | ".join(separator(widths[i], aligns[i]) for i in range(len(headers))) + " |"
    body_lines = [
        "| " + " | ".join(pad(cell, widths[i], aligns[i]) for i, cell in enumerate(row)) + " |" for row in rows
    ]
    return [header_line, sep_line, *body_lines]


def render_markdown(report: LatencyReport) -> str:
    """Render the report as GitHub-flavored markdown."""
    lines = [
        "# Unit Test Latency Report",
        "",
        f"Generated: {report.generated_at}",
        f"Tests run: {report.tests_run}",
        f"Total wall time: {_format_wall_time(report.total_wall_time_seconds)}",
        "",
        "## Summary",
        "",
    ]
    summary_rows = [[key, _format_ms_int(report.percentiles_ms[key])] for key in ("p50", "p75", "p90", "p95", "p99", "max")]
    lines.extend(_render_table(["Percentile", "Latency"], summary_rows, ["l", "r"]))

    lines.extend(["", "## Buckets", ""])
    bucket_rows = [[bucket.label, str(bucket.count), f"{bucket.suite_percent:.1f}%"] for bucket in report.buckets]
    lines.extend(_render_table(["Bucket", "Count", "% of suite"], bucket_rows, ["l", "r", "r"]))

    lines.extend(
        [
            "",
            "## Slow tests (≥p95)",
            "",
            (
                "Every test at or above the p95 threshold "
                f"({_format_ms_int(report.p95_threshold_ms)} in this run). "
                'Hand this list to an agent with "find a way to make these faster".'
            ),
            "",
        ]
    )
    p95_rows = [[_format_ms(item.duration_seconds), f"`{item.nodeid}`"] for item in report.slow_tests_p95]
    lines.extend(_render_table(["Duration", "Test"], p95_rows, ["r", "l"]))

    lines.extend(
        [
            "",
            "## Slow tests (≥p99)",
            "",
            (
                "Subset of the above, at or above the p99 threshold "
                f"({_format_ms_int(report.p99_threshold_ms)}). Highest priority."
            ),
            "",
        ]
    )
    p99_rows = [[_format_ms(item.duration_seconds), f"`{item.nodeid}`"] for item in report.slow_tests_p99]
    lines.extend(_render_table(["Duration", "Test"], p99_rows, ["r", "l"]))

    return "\n".join(lines) + "\n"


def render_json(report: LatencyReport) -> str:
    """Render the report as JSON."""
    payload = asdict(report)
    return json.dumps(payload, indent=2) + "\n"


def render_summary(report: LatencyReport) -> str:
    """Render the lightweight summary line used by bin/tests."""
    return (
        "latency: "
        f"p50={_format_ms_int(report.percentiles_ms['p50'])} "
        f"p95={_format_ms_int(report.percentiles_ms['p95'])} "
        f"p99={_format_ms_int(report.percentiles_ms['p99'])} "
        f"max={_format_ms_int(report.percentiles_ms['max'])} "
        f"n={report.tests_run}"
    )


def _current_timestamp() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


@dataclass
class _SigtermSummaryState:
    """Mutable state used to emit a partial summary if pytest is SIGTERM'd."""

    plugin: _TimingPlugin
    started: float
    previous_handler: SignalHandler


SignalHandler: TypeAlias = Callable[[int, FrameType | None], Any] | int | None


def _build_partial_summary(state: _SigtermSummaryState) -> str:
    report = build_report(
        state.plugin.test_durations,
        time.perf_counter() - state.started,
        _current_timestamp(),
    )
    return f"{render_summary(report)} (partial before SIGTERM)"


def _reemit_sigterm(signum: int, frame: FrameType | None, previous_handler: SignalHandler) -> None:
    if callable(previous_handler):
        cast(Callable[[int, FrameType | None], Any], previous_handler)(signum, frame)
        return
    signal.signal(signum, signal.SIG_DFL)
    os.kill(os.getpid(), signum)


def _install_sigterm_summary_handler(plugin: _TimingPlugin, started: float) -> SignalHandler:
    if not hasattr(signal, "SIGTERM"):
        return None
    previous_handler = signal.getsignal(signal.SIGTERM)
    state = _SigtermSummaryState(plugin=plugin, started=started, previous_handler=previous_handler)

    def _handle_sigterm(signum: int, frame: FrameType | None) -> None:
        sys.stderr.write(_build_partial_summary(state) + "\n")
        sys.stderr.flush()
        sys.stdout.flush()
        _reemit_sigterm(signum, frame, state.previous_handler)

    signal.signal(signal.SIGTERM, _handle_sigterm)
    return previous_handler


def run_pytest(pytest_args: list[str], *, emit_sigterm_summary: bool = False) -> tuple[int, list[MeasuredTest], float]:
    """Run pytest and capture call-phase durations."""
    plugin = _TimingPlugin()
    started = time.perf_counter()
    previous_sigterm_handler: SignalHandler = None
    if emit_sigterm_summary:
        previous_sigterm_handler = _install_sigterm_summary_handler(plugin, started)
    # Redirect pytest's stdout to stderr so progress is visible on the terminal
    # while keeping our rendered report (markdown/JSON) on stdout pipeable.
    try:
        with redirect_stdout(sys.stderr):
            exit_code = pytest.main(pytest_args, plugins=[plugin])
    finally:
        if emit_sigterm_summary and hasattr(signal, "SIGTERM") and previous_sigterm_handler is not None:
            signal.signal(signal.SIGTERM, previous_sigterm_handler)
    finished = time.perf_counter()
    return int(exit_code), plugin.test_durations, finished - started


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Measure pytest unit-test latency and report the distribution.")
    parser.add_argument("-o", "--output", help="Write the rendered report to PATH instead of stdout.")
    parser.add_argument("--json", action="store_true", help="Emit the structured latency report as JSON.")
    parser.add_argument(
        "--summary",
        action="store_true",
        help="Emit a one-line summary after the pytest run. Intended for bin/tests.",
    )
    parser.add_argument(
        "pytest_args",
        nargs=argparse.REMAINDER,
        help="Additional pytest args after '--'. Defaults to 'tests/ -q'.",
    )
    return parser.parse_args(argv)


def _default_pytest_args(extra_args: list[str]) -> list[str]:
    if extra_args and extra_args[0] == "--":
        extra_args = extra_args[1:]
    return extra_args or ["tests/", "-q"]


def _write_output(text: str, output_path: str | None) -> None:
    if output_path is None:
        sys.stdout.write(text)
        return
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as handle:
        handle.write(text)


def _repo_root() -> Path:
    """Return the git repository top-level for the current working directory."""
    result = subprocess.run(
        ["git", "rev-parse", "--show-toplevel"],
        capture_output=True,
        text=True,
        check=True,
    )
    return Path(result.stdout.strip())


def _default_report_path(json_mode: bool) -> str:
    """Build tmp/test-latency-<YYYYmmddHHMMSS>.{md,json} under the repo root."""
    tstamp = datetime.now().strftime("%Y%m%d%H%M%S")
    extension = "json" if json_mode else "md"
    return str(_repo_root() / "tmp" / f"test-latency-{tstamp}.{extension}")


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv or sys.argv[1:])
    pytest_args = _default_pytest_args(args.pytest_args)
    exit_code, test_durations, total_wall_time_seconds = run_pytest(
        pytest_args,
        emit_sigterm_summary=args.summary,
    )
    if exit_code != 0:
        return exit_code
    report = build_report(test_durations, total_wall_time_seconds, _current_timestamp())
    if args.summary:
        _write_output(render_summary(report) + "\n", args.output)
        return 0
    output_path = args.output if args.output is not None else _default_report_path(args.json)
    rendered = render_json(report) if args.json else render_markdown(report)
    _write_output(rendered, output_path)
    if args.output is None:
        print(f"wrote {output_path}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
