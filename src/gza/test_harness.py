"""Helpers shared by the project's own test harness."""

from __future__ import annotations

import faulthandler
import os
import signal
from typing import Any


def positive_int_env(name: str, default: int) -> int:
    """Return a validated positive integer env override."""
    raw_value = os.environ.get(name)
    if raw_value is None:
        return default
    try:
        value = int(raw_value)
    except ValueError as exc:
        raise ValueError(f"{name} must be a positive integer, got {raw_value!r}") from exc
    if value <= 0:
        raise ValueError(f"{name} must be a positive integer, got {raw_value!r}")
    return value


def register_sigterm_faulthandler(
    *,
    faulthandler_module: Any = faulthandler,
    signal_module: Any = signal,
) -> bool:
    """Best-effort SIGTERM stack dumps for harness-driven shutdowns."""
    register = getattr(faulthandler_module, "register", None)
    sigterm = getattr(signal_module, "SIGTERM", None)
    if register is None or sigterm is None:
        return False
    try:
        register(sigterm, chain=True)
    except (AttributeError, OSError, RuntimeError, ValueError):
        return False
    return True
