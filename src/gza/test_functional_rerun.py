"""Guarded serial rerun bridge entrypoint for the functional pytest lane."""

from __future__ import annotations

from .test_serial_rerun import _FUNCTIONAL_LANE, _main_for_lane, run_functional_phase


def main(argv: list[str] | None = None) -> int:
    return _main_for_lane(argv, lane=_FUNCTIONAL_LANE, run_phase=run_functional_phase)


if __name__ == "__main__":
    raise SystemExit(main())
