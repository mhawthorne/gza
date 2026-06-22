"""Guarded serial rerun bridge for the unit pytest lane."""

from __future__ import annotations

import argparse
import os
import sys
from dataclasses import dataclass, field
from datetime import UTC, datetime

import pytest

from gza.test_latency import build_report, render_summary, run_pytest

_DEFAULT_RERUN_CAP = 10


def _warn(message: str) -> None:
    print(f"test_serial_rerun: {message}", file=sys.stderr, flush=True)


def _log(message: str) -> None:
    print(f"unit-rerun: {message}", file=sys.stderr, flush=True)


def _current_timestamp() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


@dataclass
class _FailureCapturePlugin:
    """Collect rerunnable test failures and non-rerunnable pytest errors."""

    failed_nodeids: list[str] = field(default_factory=list)
    collection_errors: list[str] = field(default_factory=list)
    internal_errors: list[str] = field(default_factory=list)
    _seen_nodeids: set[str] = field(default_factory=set, init=False, repr=False)

    def pytest_runtest_logreport(self, report: pytest.TestReport) -> None:
        if not report.failed or report.when not in {"setup", "call", "teardown"}:
            return
        if report.nodeid in self._seen_nodeids:
            return
        self._seen_nodeids.add(report.nodeid)
        self.failed_nodeids.append(report.nodeid)

    def pytest_collectreport(self, report: pytest.CollectReport) -> None:
        if report.failed:
            self.collection_errors.append(report.nodeid)

    def pytest_internalerror(self, excrepr: object, excinfo: object | None = None) -> None:
        del excinfo
        self.internal_errors.append(str(excrepr))


@dataclass(frozen=True)
class _PytestPassResult:
    exit_code: int
    failed_nodeids: list[str]
    collection_errors: list[str]
    internal_errors: list[str]


def _parse_positive_int_env(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    try:
        value = int(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be a positive integer") from exc
    if value < 1:
        raise ValueError(f"{name} must be a positive integer")
    return value


def _parse_bool_env(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    if raw == "1":
        return True
    if raw == "0":
        return False
    raise ValueError(f"{name} must be 0 or 1")


def _override_options(pytest_args: list[str]) -> list[str]:
    options: list[str] = []
    index = 0
    while index < len(pytest_args):
        arg = pytest_args[index]
        if arg == "-o" and index + 1 < len(pytest_args):
            options.extend([arg, pytest_args[index + 1]])
            index += 2
            continue
        if arg.startswith("--override-ini="):
            options.append(arg)
        index += 1
    return options


def _run_pytest_pass(pytest_args: list[str], *, emit_sigterm_summary: bool) -> tuple[_PytestPassResult, str | None]:
    capture = _FailureCapturePlugin()
    exit_code, durations, total_wall_time_seconds = run_pytest(
        pytest_args,
        emit_sigterm_summary=emit_sigterm_summary,
        extra_plugins=[capture],
    )
    summary = None
    if durations:
        summary = render_summary(build_report(durations, total_wall_time_seconds, _current_timestamp()))
    return (
        _PytestPassResult(
            exit_code=exit_code,
            failed_nodeids=list(capture.failed_nodeids),
            collection_errors=list(capture.collection_errors),
            internal_errors=list(capture.internal_errors),
        ),
        summary,
    )


def _classify_parallel_failure(parallel: _PytestPassResult, cap: int) -> str | None:
    if parallel.collection_errors:
        details = ", ".join(parallel.collection_errors)
        return f"collection errors are not rerunnable: {details}"
    if parallel.internal_errors:
        return "internal pytest errors are not rerunnable"
    if parallel.exit_code != 0 and not parallel.failed_nodeids:
        return "parallel run exited non-zero without attributable per-test failures"
    if len(parallel.failed_nodeids) > cap:
        return f"over cap ({len(parallel.failed_nodeids)} > {cap})"
    return None


def run_unit_phase(pytest_args: list[str], *, cap: int, rerun_enabled: bool, emit_summary: bool) -> int:
    parallel_args = [*pytest_args, f"--maxfail={cap + 1}"]
    parallel, parallel_summary = _run_pytest_pass(parallel_args, emit_sigterm_summary=emit_summary)
    if parallel_summary is not None:
        sys.stdout.write(parallel_summary + "\n")
        sys.stdout.flush()
    if parallel.exit_code == 0:
        return 0
    if not rerun_enabled:
        return parallel.exit_code

    no_mask_reason = _classify_parallel_failure(parallel, cap)
    if no_mask_reason is not None:
        _log(f"NOT masking - {no_mask_reason}")
        return parallel.exit_code

    failed_nodeids = parallel.failed_nodeids
    _log(
        f"parallel pass failed; {len(failed_nodeids)} test(s) failed and are within cap {cap}; "
        f"re-running serially: {' '.join(failed_nodeids)}"
    )
    serial_args = [*failed_nodeids, "-n0", "-v", "--maxfail=0", *_override_options(pytest_args)]
    serial, _ = _run_pytest_pass(serial_args, emit_sigterm_summary=False)
    serial_failed = set(serial.failed_nodeids)
    for nodeid in failed_nodeids:
        if nodeid in serial_failed:
            _log(f"CONFIRMED FAILURE (failed serially too): {nodeid}")
        else:
            _log(f"PARALLEL-ONLY FAILURE (passed serially): {nodeid}")
    if serial.collection_errors:
        _log(f"serial rerun produced collection errors: {', '.join(serial.collection_errors)}")
    if serial.internal_errors:
        _log("serial rerun produced internal pytest errors")
    return serial.exit_code


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the unit pytest lane with a guarded serial rerun bridge.")
    parser.add_argument(
        "--summary",
        action="store_true",
        help="Emit the parallel-pass latency summary after the pytest run. Intended for bin/tests.",
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


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(sys.argv[1:] if argv is None else argv)
    try:
        cap = _parse_positive_int_env("GZA_UNIT_RERUN_CAP", _DEFAULT_RERUN_CAP)
        rerun_enabled = _parse_bool_env("GZA_UNIT_SERIAL_RERUN", True)
    except ValueError as exc:
        _warn(str(exc))
        return 2
    return run_unit_phase(
        _default_pytest_args(args.pytest_args),
        cap=cap,
        rerun_enabled=rerun_enabled,
        emit_summary=args.summary,
    )


if __name__ == "__main__":
    raise SystemExit(main())
