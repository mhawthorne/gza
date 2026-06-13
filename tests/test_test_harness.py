from __future__ import annotations

from types import SimpleNamespace

from gza.test_harness import register_sigterm_faulthandler


def test_register_sigterm_faulthandler_registers_when_supported() -> None:
    calls: list[tuple[int, bool]] = []

    signal_module = SimpleNamespace(SIGTERM=15)
    faulthandler_module = SimpleNamespace(register=lambda signum, *, chain: calls.append((signum, chain)))

    assert (
        register_sigterm_faulthandler(
            faulthandler_module=faulthandler_module,
            signal_module=signal_module,
        )
        is True
    )
    assert calls == [(15, True)]


def test_register_sigterm_faulthandler_skips_when_sigterm_is_unavailable() -> None:
    signal_module = SimpleNamespace()
    faulthandler_module = SimpleNamespace(register=lambda *_args, **_kwargs: None)

    assert (
        register_sigterm_faulthandler(
            faulthandler_module=faulthandler_module,
            signal_module=signal_module,
        )
        is False
    )


def test_register_sigterm_faulthandler_skips_when_registration_fails() -> None:
    signal_module = SimpleNamespace(SIGTERM=15)

    def _raise_runtime_error(_signum: int, *, chain: bool) -> None:
        raise RuntimeError("unsupported")

    faulthandler_module = SimpleNamespace(register=_raise_runtime_error)

    assert (
        register_sigterm_faulthandler(
            faulthandler_module=faulthandler_module,
            signal_module=signal_module,
        )
        is False
    )
