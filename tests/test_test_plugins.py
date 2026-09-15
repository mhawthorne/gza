from __future__ import annotations

import datetime as _dt

from gza import test_cull, test_timeshift


def test_cull_runtest_call_is_registered_as_hookwrapper() -> None:
    impl = test_cull.pytest_runtest_call.pytest_impl

    assert impl["hookwrapper"] is True


def test_shifted_datetime_utcnow_returns_naive_shifted_utc(monkeypatch) -> None:
    offset = _dt.timedelta(days=3)
    monkeypatch.setattr(test_timeshift.ShiftedDateTime, "_offset", offset)

    before = test_timeshift._real_datetime.now(_dt.UTC).replace(tzinfo=None) + offset
    shifted = test_timeshift.ShiftedDateTime.utcnow()
    after = test_timeshift._real_datetime.now(_dt.UTC).replace(tzinfo=None) + offset

    assert shifted.tzinfo is None
    assert before <= shifted <= after
