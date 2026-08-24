"""TTL-cached read path for provider usage stats.

Every consumer (watch, `gza usage`, the server homepage) reads through
``get_usage``; none call a provider directly. See specs/features/usage-stats.md.
"""

from __future__ import annotations

import logging
import os
import sqlite3
import time
from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING

from .usage import (
    ProviderUsage,
    UsageError,
    UsageSnapshot,
    UsageUnsupported,
)

if TYPE_CHECKING:
    from .config import Config
    from .db import SqliteTaskStore as TaskStore

logger = logging.getLogger(__name__)

# A refresh that cannot take the lease returns cache instead of queueing: a
# duplicate `codex app-server` spawn is worse than a slightly stale number.
_LEASE_STALE_SECONDS = 120.0


def _lease_path(db_path: Path, provider: str) -> Path:
    return db_path.parent / f"usage-refresh-{provider}.lock"


def _acquire_refresh_lease(db_path: Path, provider: str) -> Path | None:
    """Best-effort cross-process lease around a provider refresh."""
    path = _lease_path(db_path, provider)
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        if path.exists() and time.time() - path.stat().st_mtime > _LEASE_STALE_SECONDS:
            # A crashed refresh must not wedge usage forever.
            path.unlink(missing_ok=True)
        fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
    except FileExistsError:
        return None
    except OSError:
        # Lease is an optimization, never a hard gate on reading usage.
        return None
    os.close(fd)
    return path


def _release_refresh_lease(path: Path | None) -> None:
    if path is not None:
        try:
            path.unlink(missing_ok=True)
        except OSError:
            pass


def usage_providers(config: Config) -> list[str]:
    """Providers that are actually routed to *and* can report usage.

    Derived, never configured: if Codex is not routed to, nothing spawns
    `codex app-server`.
    """
    from .providers import get_provider_by_name

    names: list[str] = []
    candidates = [config.provider, *config.task_providers.values(), *config.providers.keys()]
    for raw in candidates:
        name = (raw or "").strip().lower()
        if not name or name in names:
            continue
        try:
            provider = get_provider_by_name(name)
        except Exception:  # Unknown provider names are a config problem, not ours.
            continue
        if provider.supports_usage():
            names.append(name)
    return names


def _snapshot_from_cache(
    store: TaskStore,
    provider: str,
    *,
    now: datetime,
    max_age: timedelta,
    error: str | None = None,
    error_reason: str | None = None,
) -> UsageSnapshot:
    cached = store.get_latest_provider_usage(provider)
    if cached is None:
        return UsageSnapshot(
            provider=provider,
            usage=None,
            source="unavailable",
            error=error,
            error_reason=error_reason,
        )
    age = now - cached.fetched_at
    return UsageSnapshot(
        provider=provider,
        usage=cached,
        source="cache",
        age=age,
        stale=age > max_age or error is not None,
        error=error,
        error_reason=error_reason,
    )


def get_usage(
    store: TaskStore,
    provider: str,
    *,
    max_age: timedelta,
    refresh: bool = True,
    timeout_seconds: float = 10.0,
    retention_days: int = 30,
    now: datetime | None = None,
    env: Mapping[str, str] | None = None,
    cwd: Path | None = None,
) -> UsageSnapshot:
    """Read usage for one provider, fetching only when the cache is stale.

    Never raises: usage is decoration, so every failure degrades to the last
    good sample marked stale, or to an ``unavailable`` snapshot.
    """
    now = now or datetime.now(UTC)

    try:
        supported = store.supports_provider_usage()
    except (sqlite3.Error, RuntimeError):
        supported = False
    if not supported:
        return UsageSnapshot(
            provider=provider,
            usage=None,
            source="unavailable",
            error="usage storage unavailable (run `gza migrate`)",
            error_reason="no_storage",
        )

    last_failure = store.get_provider_usage_failure(provider)
    cached = store.get_latest_provider_usage(provider)
    if cached is not None and now - cached.fetched_at <= max_age:
        return UsageSnapshot(provider=provider, usage=cached, source="cache", age=now - cached.fetched_at)

    if not refresh:
        error, reason = last_failure or (None, None)
        return _snapshot_from_cache(store, provider, now=now, max_age=max_age, error=error, error_reason=reason)

    lease = _acquire_refresh_lease(store.db_path, provider)
    if lease is None:
        # Someone else is already refreshing; their result lands shortly.
        error, reason = last_failure or (None, None)
        return _snapshot_from_cache(store, provider, now=now, max_age=max_age, error=error, error_reason=reason)

    try:
        usage = _fetch_usage(provider, timeout_seconds=timeout_seconds, env=env, cwd=cwd)
    except UsageError as exc:
        logger.debug("usage fetch failed for %s: %s", provider, exc)
        store.record_provider_usage_failure(provider, error=str(exc), reason=exc.reason)
        return _snapshot_from_cache(store, provider, now=now, max_age=max_age, error=str(exc), error_reason=exc.reason)
    except Exception as exc:  # A usage bug must never break a caller.
        logger.debug("unexpected usage failure for %s: %s", provider, exc)
        store.record_provider_usage_failure(provider, error=str(exc), reason="unexpected")
        return _snapshot_from_cache(
            store, provider, now=now, max_age=max_age, error=str(exc), error_reason="unexpected"
        )
    finally:
        _release_refresh_lease(lease)

    try:
        store.record_provider_usage(usage, retention_days=retention_days)
    except (sqlite3.Error, RuntimeError) as exc:
        # Serving the value we just fetched beats failing because we could not
        # write history for it.
        logger.debug("could not record usage for %s: %s", provider, exc)
    return UsageSnapshot(provider=provider, usage=usage, source="fetch", age=timedelta(0))


def _fetch_usage(
    provider: str,
    *,
    timeout_seconds: float,
    env: Mapping[str, str] | None = None,
    cwd: Path | None = None,
) -> ProviderUsage:
    from .providers import get_provider_by_name

    impl = get_provider_by_name(provider)
    if not impl.supports_usage():
        raise UsageUnsupported(f"{provider} does not report usage")
    return impl.read_usage(timeout_seconds=timeout_seconds, env=env, cwd=cwd)


def get_primary_usage(
    store: TaskStore,
    config: Config,
    *,
    refresh: bool = True,
    now: datetime | None = None,
    env: Mapping[str, str] | None = None,
    cwd: Path | None = None,
) -> UsageSnapshot | None:
    """Snapshot for the first usage-capable routed provider, or None."""
    providers = usage_providers(config)
    if not providers:
        return None
    return get_usage(
        store,
        providers[0],
        max_age=timedelta(seconds=config.usage_ttl_seconds),
        refresh=refresh,
        timeout_seconds=config.usage_timeout_seconds,
        retention_days=config.usage_retention_days,
        now=now,
        env=env,
        cwd=cwd,
    )
