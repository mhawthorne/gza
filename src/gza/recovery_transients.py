"""Helpers for transient recovery cooldown behavior."""

from __future__ import annotations

from .config import Config

_TRANSIENT_RECOVERY_BACKOFF_FACTORS: tuple[int, ...] = (1, 2, 5, 10)


def compute_transient_recovery_backoff_seconds(config: Config, streak: int) -> int:
    """Return the bounded cooldown for one transient recovery streak.

    The schedule is anchored to ``watch.failure_backoff_initial`` and follows
    ``1x, 2x, 5x, 10x``, then doubles from there until capped by
    ``watch.transient_recovery_backoff_max``.
    """
    if streak <= 0:
        return 0
    initial = config.watch.failure_backoff_initial
    maximum = config.watch.transient_recovery_backoff_max
    if streak <= len(_TRANSIENT_RECOVERY_BACKOFF_FACTORS):
        return min(initial * _TRANSIENT_RECOVERY_BACKOFF_FACTORS[streak - 1], maximum)
    base = initial * _TRANSIENT_RECOVERY_BACKOFF_FACTORS[-1]
    growth = 2 ** (streak - len(_TRANSIENT_RECOVERY_BACKOFF_FACTORS))
    return min(base * growth, maximum)
