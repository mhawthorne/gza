"""Measure pytest test-call latency and render reports."""

from __future__ import annotations

import argparse
import io
import json
import math
import sys
import time
from contextlib import redirect_stderr, redirect_stdout
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from typing import TextIO

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


class _Tee(io.TextIOBase):
    """Write to an in-memory buffer and a live stream."""

    def __init__(self, sink: TextIO):
        self._sink = sink
        self._buffer = io.StringIO()

    def write(self, s: str) -> int:
        self._sink.write(s)
        self._buffer.write(s)
        return len(s)

    def flush(self) -> None:
        self._sink.flush()
        self._buffer.flush()

    def getvalue(self) -> str:
        return self._buffer.getvalue()


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
        "| Percentile | Latency |",
        "|---|---|",
    ]
    for key in ("p50", "p75", "p90", "p95", "p99", "max"):
        lines.append(f"| {key} | {_format_ms_int(report.percentiles_ms[key])} |")
    lines.extend(
        [
            "",
            "## Buckets",
            "",
            "| Bucket | Count | % of suite |",
            "|---|---|---|",
        ]
    )
    for bucket in report.buckets:
        lines.append(f"| {bucket.label} | {bucket.count} | {bucket.suite_percent:.1f}% |")
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
            "| Duration | Test |",
            "|---|---|",
        ]
    )
    for item in report.slow_tests_p95:
        lines.append(f"| {_format_ms(item.duration_seconds)} | `{item.nodeid}` |")
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
            "| Duration | Test |",
            "|---|---|",
        ]
    )
    for item in report.slow_tests_p99:
        lines.append(f"| {_format_ms(item.duration_seconds)} | `{item.nodeid}` |")
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


def run_pytest(pytest_args: list[str], *, echo_output: bool) -> tuple[int, list[MeasuredTest], float, str, str]:
    """Run pytest and capture call-phase durations."""
    plugin = _TimingPlugin()
    stdout_capture: io.StringIO | _Tee
    stderr_capture: io.StringIO | _Tee
    stdout_capture = _Tee(sys.stdout) if echo_output else io.StringIO()
    stderr_capture = _Tee(sys.stderr) if echo_output else io.StringIO()
    started = time.perf_counter()
    with redirect_stdout(stdout_capture), redirect_stderr(stderr_capture):
        exit_code = pytest.main(pytest_args, plugins=[plugin])
    finished = time.perf_counter()
    stdout_text = stdout_capture.getvalue()
    stderr_text = stderr_capture.getvalue()
    return int(exit_code), plugin.test_durations, finished - started, stdout_text, stderr_text


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
    with open(output_path, "w", encoding="utf-8") as handle:
        handle.write(text)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv or sys.argv[1:])
    pytest_args = _default_pytest_args(args.pytest_args)
    exit_code, test_durations, total_wall_time_seconds, stdout_text, stderr_text = run_pytest(
        pytest_args,
        echo_output=args.summary,
    )
    if exit_code != 0:
        if not args.summary:
            if stdout_text:
                sys.stdout.write(stdout_text)
            if stderr_text:
                sys.stderr.write(stderr_text)
        return exit_code
    report = build_report(
        test_durations,
        total_wall_time_seconds,
        datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
    )
    if args.summary:
        _write_output(render_summary(report) + "\n", args.output)
        return 0
    rendered = render_json(report) if args.json else render_markdown(report)
    _write_output(rendered, args.output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
