"""Guarded serial rerun bridge for the functional pytest lane."""

from __future__ import annotations

import argparse
import sys

from gza.test_serial_rerun import (
    _DEFAULT_RERUN_CAP,
    _default_lane_pytest_args,
    _parse_bool_env,
    _parse_cli_args,
    _parse_positive_int_env,
    _warn,
    run_functional_phase,
)


def _parse_args(argv: list[str]) -> argparse.Namespace:
    return _parse_cli_args(argv, lane_name="functional", default_target="tests_functional/")


def _default_pytest_args(extra_args: list[str]) -> list[str]:
    return _default_lane_pytest_args(extra_args, default_target="tests_functional/")


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(sys.argv[1:] if argv is None else argv)
    try:
        cap = _parse_positive_int_env("GZA_FUNCTIONAL_RERUN_CAP", _DEFAULT_RERUN_CAP)
        rerun_enabled = _parse_bool_env("GZA_FUNCTIONAL_SERIAL_RERUN", True)
    except ValueError as exc:
        _warn(str(exc), warn_prefix="test_functional_rerun")
        return 2
    return run_functional_phase(
        _default_pytest_args(args.pytest_args),
        cap=cap,
        rerun_enabled=rerun_enabled,
        emit_summary=args.summary,
    )


if __name__ == "__main__":
    raise SystemExit(main())
