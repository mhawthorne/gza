"""Provider usage/quota stats: normalized models, parsing, and rendering.

The provider-specific query lives on each provider (see
``CodexProvider.read_usage``). This module owns everything that is not
provider-specific: the normalized result shape, the wire-format parser, the
failure taxonomy, and the single formatter every surface shares.

See specs/features/usage-stats.md.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

# Windows are labelled by duration, never by their primary/secondary position:
# a real Codex account returns the 168h weekly window as `primary` on its
# account-wide bucket, with no secondary at all.
_WINDOW_KEYS: tuple[str, ...] = ("primary", "secondary")


class UsageError(Exception):
    """Base class for usage query failures."""

    #: Short machine-readable reason, surfaced to operators.
    reason = "usage_error"
    #: Actionable hint rendered next to the failure, when one applies.
    hint = ""


class UsageUnsupported(UsageError):
    """The provider cannot report usage at all."""

    reason = "unsupported"


class UsageCliMissing(UsageError):
    """The provider CLI is not installed or does not support the query."""

    reason = "cli_missing"


class UsageUnauthenticated(UsageError):
    """The provider CLI is installed but not logged in."""

    reason = "unauthenticated"


class UsageTimeout(UsageError):
    """The query exceeded its wall-clock budget."""

    reason = "timeout"


class UsageProtocolError(UsageError):
    """Malformed output, early EOF, or a JSON-RPC error response."""

    reason = "protocol_error"


@dataclass(frozen=True)
class UsageWindow:
    """One quota window of one limit bucket."""

    limit_id: str
    limit_name: str | None
    window: str  # "primary" | "secondary"
    used_percent: float
    window_duration_minutes: int
    resets_at: datetime

    @property
    def remaining_percent(self) -> float:
        return max(0.0, 100.0 - self.used_percent)

    @property
    def is_account_wide(self) -> bool:
        """True for the account-wide bucket, which carries no limit name.

        Per-model buckets ("GPT-5.3-Codex-Spark") are named; the account-wide
        one is not. Matching on the absent name is more durable than matching
        on a limit ID, which is not guaranteed stable.
        """
        return not self.limit_name

    @property
    def duration_label(self) -> str:
        """Human label derived from the duration, e.g. "5h" or "168h"."""
        minutes = self.window_duration_minutes
        if minutes <= 0:
            return "?"
        if minutes % 1440 == 0:
            return f"{minutes // 1440}d"
        if minutes % 60 == 0:
            return f"{minutes // 60}h"
        return f"{minutes}m"


@dataclass(frozen=True)
class ProviderUsage:
    """One complete usage fetch for one provider."""

    provider: str
    fetched_at: datetime
    windows: tuple[UsageWindow, ...]
    plan_type: str | None = None
    spend_control_reached: bool | None = None
    rate_limit_reached_type: str | None = None
    credits_has: bool | None = None
    credits_unlimited: bool | None = None
    credits_balance: str | None = None
    reset_credits_available: int = 0
    raw_json: str = ""

    @property
    def primary_window(self) -> UsageWindow | None:
        """The one window every default surface renders.

        The account-wide bucket's most-consumed window. Falls back to the
        most-consumed window overall when no bucket is account-wide, so a shape
        we have not observed degrades to something rather than nothing.
        """
        if not self.windows:
            return None
        account_wide = [w for w in self.windows if w.is_account_wide]
        pool = account_wide or list(self.windows)
        return max(pool, key=lambda w: w.used_percent)


def _coerce_bool(value: Any) -> bool | None:
    return bool(value) if isinstance(value, bool) else None


def _parse_windows(limit_id: str, bucket: dict[str, Any]) -> list[UsageWindow]:
    limit_name = bucket.get("limitName") or None
    windows: list[UsageWindow] = []
    for key in _WINDOW_KEYS:
        raw = bucket.get(key)
        if not isinstance(raw, dict):
            continue
        used = raw.get("usedPercent")
        resets_at = raw.get("resetsAt")
        duration = raw.get("windowDurationMins")
        if used is None or resets_at is None or duration is None:
            continue
        windows.append(
            UsageWindow(
                limit_id=limit_id,
                limit_name=limit_name,
                window=key,
                used_percent=float(used),
                window_duration_minutes=int(duration),
                resets_at=datetime.fromtimestamp(int(resets_at), UTC),
            )
        )
    return windows


def parse_codex_rate_limits(
    result: dict[str, Any],
    *,
    fetched_at: datetime,
    raw_json: str = "",
) -> ProviderUsage:
    """Normalize a Codex ``account/rateLimits/read`` result.

    ``rateLimitsByLimitId`` is preferred and the legacy ``rateLimits`` field is
    ignored when it is present: the two overlap verbatim on real responses, and
    reading both double-counts the account-wide window.
    """
    by_limit_id = result.get("rateLimitsByLimitId")
    buckets: dict[str, Any] = {}
    if isinstance(by_limit_id, dict) and by_limit_id:
        for limit_id, bucket in by_limit_id.items():
            if isinstance(bucket, dict):
                buckets[str(limit_id)] = bucket
    else:
        legacy = result.get("rateLimits")
        if isinstance(legacy, dict):
            buckets[str(legacy.get("limitId") or "codex")] = legacy

    windows: list[UsageWindow] = []
    for limit_id, bucket in sorted(buckets.items()):
        windows.extend(_parse_windows(limit_id, bucket))

    # Fetch-level fields live on the account-wide bucket when present.
    account_bucket: dict[str, Any] = {}
    for bucket in buckets.values():
        if not bucket.get("limitName"):
            account_bucket = bucket
            break
    credits = account_bucket.get("credits")
    credits = credits if isinstance(credits, dict) else {}

    reset_credits = result.get("rateLimitResetCredits")
    available = 0
    if isinstance(reset_credits, dict):
        entries = reset_credits.get("credits")
        if isinstance(entries, list):
            available = sum(1 for e in entries if isinstance(e, dict) and e.get("status") == "available")

    return ProviderUsage(
        provider="codex",
        fetched_at=fetched_at,
        windows=tuple(windows),
        plan_type=account_bucket.get("planType") or None,
        spend_control_reached=_coerce_bool(account_bucket.get("spendControlReached")),
        rate_limit_reached_type=account_bucket.get("rateLimitReachedType") or None,
        credits_has=_coerce_bool(credits.get("hasCredits")),
        credits_unlimited=_coerce_bool(credits.get("unlimited")),
        credits_balance=(str(credits["balance"]) if credits.get("balance") is not None else None),
        reset_credits_available=available,
        raw_json=raw_json,
    )


@dataclass(frozen=True)
class UsageSnapshot:
    """What consumers read: a usage result plus its provenance."""

    provider: str
    usage: ProviderUsage | None
    source: str  # "cache" | "fetch" | "unavailable"
    age: timedelta | None = None
    stale: bool = False
    error: str | None = None
    error_reason: str | None = None

    @property
    def available(self) -> bool:
        return self.usage is not None and self.usage.primary_window is not None

    @property
    def primary_window(self) -> UsageWindow | None:
        return self.usage.primary_window if self.usage is not None else None


def format_duration_short(delta: timedelta) -> str:
    """Compact duration: "4d21h", "3h12m", "41m", "now"."""
    seconds = int(delta.total_seconds())
    if seconds <= 0:
        return "now"
    days, seconds = divmod(seconds, 86400)
    hours, seconds = divmod(seconds, 3600)
    minutes = seconds // 60
    if days:
        return f"{days}d{hours}h" if hours else f"{days}d"
    if hours:
        return f"{hours}h{minutes}m" if minutes else f"{hours}h"
    return f"{minutes}m"


def format_usage_line(snapshot: UsageSnapshot, *, now: datetime | None = None) -> str:
    """The one-line form shared by the watch header and `gza usage`.

    Renders only the primary limit. Per-model buckets are captured but not
    displayed unless explicitly requested.
    """
    now = now or datetime.now(UTC)
    provider = snapshot.provider

    window = snapshot.primary_window
    if window is None:
        detail = snapshot.error or "no usage data"
        hint = f" — {detail}" if detail else ""
        return f"{provider} unavailable{hint}"

    resets_in = format_duration_short(window.resets_at - now)
    line = f"{provider} {window.used_percent:g}% used · {window.duration_label} window · resets in {resets_in}"

    usage = snapshot.usage
    warnings: list[str] = []
    if usage is not None:
        if usage.rate_limit_reached_type:
            warnings.append(f"rate limited: {usage.rate_limit_reached_type}")
        if usage.spend_control_reached:
            warnings.append("spend control reached")
    if warnings:
        line = f"{line}  [{'; '.join(warnings)}]"

    if snapshot.stale:
        age = f"stale {format_duration_short(snapshot.age)}" if snapshot.age else "stale"
        suffix = f"{age}, last fetch failed" if snapshot.error else age
    elif snapshot.source == "cache" and snapshot.age is not None:
        suffix = f"cached {format_duration_short(snapshot.age)} ago"
    else:
        suffix = "fresh"
    return f"{line}   ({suffix})"
