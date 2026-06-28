"""Guarded serial rerun bridge for the unit pytest lane."""

from __future__ import annotations

import argparse
import sys
from typing import TypeAlias

from gza import pytest_serial_rerun

_DEFAULT_RERUN_CAP = 10
_FailureCapturePlugin: TypeAlias = pytest_serial_rerun.FailureCapturePlugin
_PytestPassResult: TypeAlias = pytest_serial_rerun.PytestPassResult
_run_pytest_pass = pytest_serial_rerun.run_pytest_pass


def run_unit_phase(pytest_args: list[str], *, cap: int, rerun_enabled: bool, emit_summary: bool) -> int:
    return pytest_serial_rerun.run_pytest_phase(
        pytest_args,
        cap=cap,
        rerun_enabled=rerun_enabled,
        emit_summary=emit_summary,
        phase_label="unit",
    )


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
        cap = pytest_serial_rerun.parse_positive_int_env("GZA_UNIT_RERUN_CAP", _DEFAULT_RERUN_CAP)
        rerun_enabled = pytest_serial_rerun.parse_bool_env("GZA_UNIT_SERIAL_RERUN", True)
    except ValueError as exc:
        pytest_serial_rerun.warn(str(exc))
        return 2
    return run_unit_phase(
        _default_pytest_args(args.pytest_args),
        cap=cap,
        rerun_enabled=rerun_enabled,
        emit_summary=args.summary,
    )


if __name__ == "__main__":
    raise SystemExit(main())
