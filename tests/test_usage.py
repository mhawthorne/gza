"""Usage stats: parsing, storage, TTL cache, and rendering.

The payload fixture is a real `account/rateLimits/read` response captured from
codex-cli 0.145.0, so the tests exercise the shape we actually receive rather
than the illustrative one in the protocol doc.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from io import StringIO
from pathlib import Path
from unittest.mock import patch

import pytest

from gza.db import SqliteTaskStore
from gza.usage import (
    ProviderUsage,
    UsageSnapshot,
    UsageTimeout,
    UsageWindow,
    format_duration_short,
    format_usage_line,
    parse_codex_rate_limits,
)
from gza.usage_service import get_usage

# Captured live; `rateLimits` deliberately duplicates the `codex` bucket, and
# the account-wide bucket's *primary* is the 168h weekly window.
CODEX_PAYLOAD: dict = {
    "rateLimits": {
        "limitId": "codex",
        "limitName": None,
        "primary": {"usedPercent": 45, "windowDurationMins": 10080, "resetsAt": 1787845741},
        "secondary": None,
        "credits": {"hasCredits": False, "unlimited": False, "balance": "0"},
        "individualLimit": None,
        "spendControlReached": False,
        "planType": "pro",
        "rateLimitReachedType": None,
    },
    "rateLimitsByLimitId": {
        "codex_bengalfox": {
            "limitId": "codex_bengalfox",
            "limitName": "GPT-5.3-Codex-Spark",
            "primary": {"usedPercent": 0, "windowDurationMins": 300, "resetsAt": 1787442669},
            "secondary": {"usedPercent": 0, "windowDurationMins": 10080, "resetsAt": 1788029469},
            "credits": None,
            "individualLimit": None,
            "spendControlReached": None,
            "planType": "pro",
            "rateLimitReachedType": None,
        },
        "codex": {
            "limitId": "codex",
            "limitName": None,
            "primary": {"usedPercent": 45, "windowDurationMins": 10080, "resetsAt": 1787845741},
            "secondary": None,
            "credits": {"hasCredits": False, "unlimited": False, "balance": "0"},
            "individualLimit": None,
            "spendControlReached": False,
            "planType": "pro",
            "rateLimitReachedType": None,
        },
    },
    "rateLimitResetCredits": {
        "availableCount": 1,
        "credits": [{"id": "x", "resetType": "codexRateLimits", "status": "available"}],
    },
}

FETCHED_AT = datetime(2026, 8, 22, 12, 0, tzinfo=UTC)


def _parse() -> ProviderUsage:
    return parse_codex_rate_limits(CODEX_PAYLOAD, fetched_at=FETCHED_AT, raw_json=json.dumps(CODEX_PAYLOAD))


def test_parses_every_bucket_and_window() -> None:
    usage = _parse()
    assert len(usage.windows) == 3
    assert {(w.limit_id, w.window) for w in usage.windows} == {
        ("codex", "primary"),
        ("codex_bengalfox", "primary"),
        ("codex_bengalfox", "secondary"),
    }


def test_legacy_rate_limits_field_does_not_duplicate_a_window() -> None:
    """`rateLimits` repeats the `codex` bucket; reading both would double-count."""
    usage = _parse()
    codex_windows = [w for w in usage.windows if w.limit_id == "codex"]
    assert len(codex_windows) == 1


def test_falls_back_to_legacy_field_when_by_limit_id_is_absent() -> None:
    payload = {"rateLimits": CODEX_PAYLOAD["rateLimits"]}
    usage = parse_codex_rate_limits(payload, fetched_at=FETCHED_AT)
    assert len(usage.windows) == 1
    assert usage.windows[0].limit_id == "codex"


def test_primary_window_is_the_account_wide_bucket_not_the_first_position() -> None:
    """The account-wide bucket is the one with no limit name, whatever its ID."""
    usage = _parse()
    primary = usage.primary_window
    assert primary is not None
    assert primary.limit_id == "codex"
    assert primary.used_percent == 45
    # Position says "primary" but the duration says weekly: label from duration.
    assert primary.window_duration_minutes == 10080


def test_primary_falls_back_to_most_consumed_when_no_bucket_is_account_wide() -> None:
    usage = ProviderUsage(
        provider="codex",
        fetched_at=FETCHED_AT,
        windows=(
            UsageWindow("a", "Named A", "primary", 10.0, 300, FETCHED_AT),
            UsageWindow("b", "Named B", "primary", 80.0, 300, FETCHED_AT),
        ),
    )
    primary = usage.primary_window
    assert primary is not None and primary.limit_id == "b"


def test_captures_fetch_level_fields() -> None:
    usage = _parse()
    assert usage.plan_type == "pro"
    assert usage.spend_control_reached is False
    assert usage.credits_has is False
    assert usage.credits_balance == "0"  # string on the wire; kept verbatim
    assert usage.reset_credits_available == 1


def test_remaining_percent_and_duration_label() -> None:
    usage = _parse()
    primary = usage.primary_window
    assert primary is not None
    assert primary.remaining_percent == 55
    assert primary.duration_label == "7d"
    spark_5h = next(w for w in usage.windows if w.window_duration_minutes == 300)
    assert spark_5h.duration_label == "5h"


@pytest.mark.parametrize(
    ("delta", "expected"),
    [
        (timedelta(days=4, hours=20), "4d20h"),
        (timedelta(hours=3, minutes=12), "3h12m"),
        (timedelta(minutes=41), "41m"),
        (timedelta(seconds=-5), "now"),
    ],
)
def test_format_duration_short(delta: timedelta, expected: str) -> None:
    assert format_duration_short(delta) == expected


def test_store_round_trips_all_windows(tmp_path: Path) -> None:
    store = SqliteTaskStore(tmp_path / "test.db")
    usage = _parse()
    store.record_provider_usage(usage)

    loaded = store.get_latest_provider_usage("codex")
    assert loaded is not None
    assert len(loaded.windows) == 3
    assert loaded.plan_type == "pro"
    assert loaded.reset_credits_available == 1
    assert loaded.raw_json
    primary = loaded.primary_window
    assert primary is not None and primary.used_percent == 45


def test_store_returns_none_for_unknown_provider(tmp_path: Path) -> None:
    store = SqliteTaskStore(tmp_path / "test.db")
    assert store.get_latest_provider_usage("codex") is None


def test_recording_success_clears_a_standing_failure(tmp_path: Path) -> None:
    store = SqliteTaskStore(tmp_path / "test.db")
    store.record_provider_usage_failure("codex", error="boom", reason="timeout")
    assert store.get_provider_usage_failure("codex") == ("boom", "timeout")
    store.record_provider_usage(_parse())
    assert store.get_provider_usage_failure("codex") is None


def test_retention_prunes_old_fetches(tmp_path: Path) -> None:
    store = SqliteTaskStore(tmp_path / "test.db")
    old = ProviderUsage(
        provider="codex",
        fetched_at=datetime.now(UTC) - timedelta(days=90),
        windows=(UsageWindow("codex", None, "primary", 10.0, 10080, FETCHED_AT),),
    )
    store.record_provider_usage(old, retention_days=30)
    fresh = ProviderUsage(
        provider="codex",
        fetched_at=datetime.now(UTC),
        windows=(UsageWindow("codex", None, "primary", 20.0, 10080, FETCHED_AT),),
    )
    store.record_provider_usage(fresh, retention_days=30)

    with store._connect() as conn:  # noqa: SLF001 - asserting on retention
        rows = conn.execute("SELECT COUNT(*) AS n FROM provider_usage_fetches").fetchone()
        samples = conn.execute("SELECT COUNT(*) AS n FROM provider_usage_samples").fetchone()
    assert rows["n"] == 1
    assert samples["n"] == 1  # samples cascade with their parent fetch


def test_get_usage_serves_cache_without_fetching(tmp_path: Path, monkeypatch) -> None:
    store = SqliteTaskStore(tmp_path / "test.db")
    store.record_provider_usage(
        ProviderUsage(
            provider="codex",
            fetched_at=datetime.now(UTC),
            windows=(UsageWindow("codex", None, "primary", 45.0, 10080, FETCHED_AT),),
        )
    )

    def _explode(*args, **kwargs):
        raise AssertionError("a fresh cache must not spawn a provider process")

    monkeypatch.setattr("gza.usage_service._fetch_usage", _explode)
    snapshot = get_usage(store, "codex", max_age=timedelta(minutes=15))
    assert snapshot.source == "cache"
    assert snapshot.stale is False


def test_get_usage_refreshes_when_stale(tmp_path: Path, monkeypatch) -> None:
    store = SqliteTaskStore(tmp_path / "test.db")
    store.record_provider_usage(
        ProviderUsage(
            provider="codex",
            fetched_at=datetime.now(UTC) - timedelta(hours=2),
            windows=(UsageWindow("codex", None, "primary", 10.0, 10080, FETCHED_AT),),
        )
    )
    monkeypatch.setattr("gza.usage_service._fetch_usage", lambda *a, **k: _parse())

    snapshot = get_usage(store, "codex", max_age=timedelta(minutes=15))
    assert snapshot.source == "fetch"
    assert snapshot.primary_window is not None
    assert snapshot.primary_window.used_percent == 45


def test_get_usage_fetch_uses_selected_runtime_context_and_store(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store_a = SqliteTaskStore(tmp_path / "runtime-a" / "test.db")
    store_b = SqliteTaskStore(tmp_path / "runtime-b" / "test.db")
    env_a = {
        "HOME": str(tmp_path / "runtime-a" / "home"),
        "PATH": "/runtime-a/bin",
        "CODEX_HOME": str(tmp_path / "runtime-a" / "codex-home"),
    }
    env_b = {
        "HOME": str(tmp_path / "runtime-b" / "home"),
        "PATH": "/runtime-b/bin",
        "CODEX_HOME": str(tmp_path / "runtime-b" / "codex-home"),
    }
    cwd_a = tmp_path / "runtime-a" / "project"
    cwd_b = tmp_path / "runtime-b" / "project"
    monkeypatch.setenv("HOME", str(tmp_path / "ambient-home"))
    monkeypatch.setenv("PATH", "/ambient/bin")
    monkeypatch.setenv("CODEX_HOME", str(tmp_path / "ambient-codex"))
    seen: list[tuple[dict[str, str] | None, Path | None]] = []

    def fetch_spy(_provider: str, *, timeout_seconds: float, env=None, cwd=None) -> ProviderUsage:
        seen.append((dict(env) if env is not None else None, cwd))
        return _parse()

    monkeypatch.setattr("gza.usage_service._fetch_usage", fetch_spy)
    snapshot_a = get_usage(store_a, "codex", max_age=timedelta(0), env=env_a, cwd=cwd_a)
    snapshot_b = get_usage(store_b, "codex", max_age=timedelta(0), env=env_b, cwd=cwd_b)

    assert snapshot_a.source == "fetch"
    assert snapshot_b.source == "fetch"
    assert seen == [(env_a, cwd_a), (env_b, cwd_b)]
    assert store_a.get_latest_provider_usage("codex") is not None
    assert store_b.get_latest_provider_usage("codex") is not None
    assert store_a.db_path != store_b.db_path


def test_failed_fetch_serves_the_last_good_value_as_stale(tmp_path: Path, monkeypatch) -> None:
    """A failure must never read as zero usage."""
    store = SqliteTaskStore(tmp_path / "test.db")
    store.record_provider_usage(
        ProviderUsage(
            provider="codex",
            fetched_at=datetime.now(UTC) - timedelta(hours=2),
            windows=(UsageWindow("codex", None, "primary", 45.0, 10080, FETCHED_AT),),
        )
    )

    def _fail(*args, **kwargs):
        raise UsageTimeout("codex app-server did not respond within 10s")

    monkeypatch.setattr("gza.usage_service._fetch_usage", _fail)
    snapshot = get_usage(store, "codex", max_age=timedelta(minutes=15))
    assert snapshot.stale is True
    assert snapshot.error_reason == "timeout"
    assert snapshot.primary_window is not None
    assert snapshot.primary_window.used_percent == 45
    assert store.get_provider_usage_failure("codex") is not None


def test_no_refresh_never_fetches(tmp_path: Path, monkeypatch) -> None:
    store = SqliteTaskStore(tmp_path / "test.db")

    def _explode(*args, **kwargs):
        raise AssertionError("refresh=False must not spawn a provider process")

    monkeypatch.setattr("gza.usage_service._fetch_usage", _explode)
    snapshot = get_usage(store, "codex", max_age=timedelta(minutes=15), refresh=False)
    assert snapshot.source == "unavailable"


def test_format_usage_line_renders_only_the_primary_limit() -> None:
    now = datetime(2026, 8, 22, 12, 0, tzinfo=UTC)
    usage = parse_codex_rate_limits(CODEX_PAYLOAD, fetched_at=now)
    line = format_usage_line(UsageSnapshot(provider="codex", usage=usage, source="fetch", age=timedelta(0)), now=now)
    assert "codex 45% used" in line
    assert "7d window" in line
    assert "Spark" not in line  # per-model buckets are captured, not displayed


def test_format_usage_line_marks_stale_and_unavailable() -> None:
    now = datetime(2026, 8, 22, 12, 0, tzinfo=UTC)
    usage = parse_codex_rate_limits(CODEX_PAYLOAD, fetched_at=now)
    stale = format_usage_line(
        UsageSnapshot(
            provider="codex",
            usage=usage,
            source="cache",
            age=timedelta(minutes=41),
            stale=True,
            error="timeout",
        ),
        now=now,
    )
    assert "stale 41m, last fetch failed" in stale

    missing = format_usage_line(
        UsageSnapshot(provider="codex", usage=None, source="unavailable", error="run `codex login`"),
        now=now,
    )
    assert "unavailable" in missing and "codex login" in missing


def test_format_usage_line_surfaces_rate_limit_reached() -> None:
    now = datetime(2026, 8, 22, 12, 0, tzinfo=UTC)
    usage = parse_codex_rate_limits(CODEX_PAYLOAD, fetched_at=now)
    limited = ProviderUsage(
        provider=usage.provider,
        fetched_at=usage.fetched_at,
        windows=usage.windows,
        rate_limit_reached_type="primary",
    )
    line = format_usage_line(UsageSnapshot(provider="codex", usage=limited, source="fetch", age=timedelta(0)), now=now)
    assert "rate limited: primary" in line


def test_rpc_error_classification_is_actionable() -> None:
    """Distinct failures must stay distinguishable, not collapse to one error."""
    from gza.providers.codex import _classify_usage_rpc_error
    from gza.usage import UsageCliMissing, UsageProtocolError, UsageUnauthenticated

    unauth = _classify_usage_rpc_error({"message": "Unauthorized: please log in"})
    assert isinstance(unauth, UsageUnauthenticated)
    assert "codex login" in str(unauth)

    unsupported = _classify_usage_rpc_error({"message": "Method not found"})
    assert isinstance(unsupported, UsageCliMissing)

    other = _classify_usage_rpc_error({"message": "internal failure"})
    assert isinstance(other, UsageProtocolError)


def test_codex_usage_launch_uses_runtime_path_cwd_and_environment(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from gza.providers.codex import _read_codex_usage

    runtime_cwd = tmp_path / "runtime-project"
    runtime_cwd.mkdir()
    env = {
        "HOME": str(tmp_path / "runtime-home"),
        "PATH": "/runtime/bin",
        "CODEX_HOME": str(tmp_path / "runtime-codex"),
        "GZA_DB_PATH": str(tmp_path / "runtime.db"),
    }
    monkeypatch.setenv("HOME", str(tmp_path / "ambient-home"))
    monkeypatch.setenv("PATH", "/ambient/bin")
    monkeypatch.setenv("CODEX_HOME", str(tmp_path / "ambient-codex"))
    popen_calls: list[dict[str, object]] = []

    class FakeProcess:
        def __init__(self, *args, **kwargs) -> None:
            popen_calls.append({"args": args, "kwargs": kwargs})
            self.stdin = StringIO()
            self.stdout = StringIO(json.dumps({"id": 1, "result": CODEX_PAYLOAD}) + "\n")
            self.stderr = StringIO("")
            self.returncode = 0

        def poll(self):
            return 0

        def terminate(self) -> None:
            raise AssertionError("completed usage process should not be terminated")

    with (
        patch("gza.providers.codex.shutil.which", lambda name, path=None: "/runtime/bin/codex"),
        patch("gza.providers.codex.subprocess.Popen", FakeProcess),
    ):
        usage = _read_codex_usage(timeout_seconds=10.0, gza_version="test", env=env, cwd=runtime_cwd)

    assert usage.provider == "codex"
    assert len(popen_calls) == 1
    kwargs = popen_calls[0]["kwargs"]
    assert popen_calls[0]["args"] == (["/runtime/bin/codex", "app-server"],)
    assert kwargs["cwd"] == runtime_cwd
    assert kwargs["env"]["PATH"] == "/runtime/bin"
    assert kwargs["env"]["PWD"] == str(runtime_cwd.resolve())
    assert kwargs["env"]["CODEX_HOME"] == str(tmp_path / "runtime-codex")


def test_read_usage_is_unsupported_by_default() -> None:
    """Providers without the capability raise rather than reporting zero."""
    from gza.providers import get_provider_by_name
    from gza.usage import UsageUnsupported

    claude = get_provider_by_name("claude")
    assert claude.supports_usage() is False
    with pytest.raises(UsageUnsupported):
        claude.read_usage()

    assert get_provider_by_name("codex").supports_usage() is True
