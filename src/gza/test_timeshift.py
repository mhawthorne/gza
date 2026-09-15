"""Find tests that will fail on a future date but pass today.

A test that stamps fixture data with a literal timestamp, then lets production
compare it against real ``datetime.now()`` through a staleness window, passes
only while that window still contains the literal date. Afterwards it fails
permanently, and the failure looks like anything but a calendar problem --
three such tests in this repo were mistaken in turn for test pollution, an
ordering dependency, an xdist parity issue, and a product bug.

Run as a pytest plugin, this shifts ``datetime.now`` forward inside gza's own
modules only. Test modules keep the real ``datetime``, so their literal fixture
timestamps stay exactly as written: the widening gap between a fixed fixture and
a moving clock is precisely what the probe exposes.

    bin/test-timebombs                    # two-arm audit, reports what breaks
    bin/test-timebombs --days 30          # probe a nearer horizon
    pytest -p gza.test_timeshift ...      # single run, with GZA_TIMESHIFT_DAYS set

A shift only reveals windows shorter than the offset, so one probe is evidence
rather than proof; probe more than one horizon before calling a suite clean.
"""

from __future__ import annotations

import datetime as _dt
import importlib
import os
import pkgutil
import sys

TIMESHIFT_ENV = "GZA_TIMESHIFT_DAYS"
_real_datetime = _dt.datetime


def shift_days() -> int:
    try:
        return int(os.environ.get(TIMESHIFT_ENV, "0"))
    except ValueError:
        return 0


class ShiftedDateTime(_real_datetime):
    """``datetime`` whose notion of "now" is in the future.

    Construction is untouched, so fixtures that build explicit datetimes are
    unaffected; only the clock reads move.
    """

    _offset = _dt.timedelta(0)

    @classmethod
    def now(cls, tz=None):  # noqa: ANN001, ANN206
        return _real_datetime.now(tz) + cls._offset

    @classmethod
    def utcnow(cls):  # noqa: ANN206
        return _real_datetime.now(_dt.UTC).replace(tzinfo=None) + cls._offset

    @classmethod
    def today(cls):  # noqa: ANN206
        return _real_datetime.today() + cls._offset


def _import_all_gza() -> None:
    """Import every gza submodule so none escapes patching by being imported later."""
    import gza

    for info in pkgutil.walk_packages(gza.__path__, prefix="gza."):
        try:
            importlib.import_module(info.name)
        except Exception:  # noqa: BLE001 - a module that will not import cannot read a clock
            pass


def apply_shift(days: int) -> int:
    """Point gza's modules at a shifted clock. Returns how many were patched."""
    ShiftedDateTime._offset = _dt.timedelta(days=days)
    _import_all_gza()
    patched = 0
    for name, module in list(sys.modules.items()):
        if not name.startswith("gza"):
            continue
        if getattr(module, "datetime", None) is _real_datetime:
            # Rebinding a module attribute mypy cannot see statically; the guard
            # above is the real check that this module imported `datetime`.
            setattr(module, "datetime", ShiftedDateTime)  # noqa: B010
            patched += 1
    return patched


def pytest_configure(config):  # noqa: ANN001, ANN201 - pytest hook
    days = shift_days()
    if days:
        count = apply_shift(days)
        config.stash  # noqa: B018 - touch stash so pytest keeps config alive
        print(f"\ntimeshift: +{days}d applied to {count} gza modules", flush=True)
