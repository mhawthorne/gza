"""Shared SIGTERM diagnostics for pytest-based verification harnesses."""

from __future__ import annotations

import faulthandler
import signal


def register_sigterm_faulthandler() -> bool:
    """Best-effort SIGTERM faulthandler registration for pytest entrypoints."""
    if not hasattr(signal, "SIGTERM"):
        return False
    try:
        faulthandler.register(signal.SIGTERM, chain=True)
    except (AttributeError, OSError, RuntimeError, ValueError):
        return False
    return True
