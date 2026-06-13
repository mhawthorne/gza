"""Helpers shared by the project's own test harness."""

from __future__ import annotations

import faulthandler
import signal
from typing import Any


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
