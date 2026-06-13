"""Tests for shared pytest timeout diagnostics helpers."""

from __future__ import annotations

import signal

from gza.pytest_timeout_diagnostics import register_sigterm_faulthandler


def test_register_sigterm_faulthandler_uses_sigterm_with_chaining(monkeypatch) -> None:
    calls: list[tuple[int, bool]] = []

    def fake_register(sig: int, *, chain: bool) -> None:
        calls.append((sig, chain))

    monkeypatch.setattr("gza.pytest_timeout_diagnostics.faulthandler.register", fake_register)

    assert register_sigterm_faulthandler() is True
    assert calls == [(signal.SIGTERM, True)]


def test_register_sigterm_faulthandler_returns_false_when_registration_fails(monkeypatch) -> None:
    def fake_register(_sig: int, *, chain: bool) -> None:
        raise RuntimeError("no tty")

    monkeypatch.setattr("gza.pytest_timeout_diagnostics.faulthandler.register", fake_register)

    assert register_sigterm_faulthandler() is False
