# Provider Usage Stats

> **Status: Proposed**
> Builds on [codex-usage-query.md](codex-usage-query.md), which defines the Codex
> `app-server` protocol. This document defines the *mechanism* around it: a
> provider-agnostic interface, a cached read path, and three consumers
> (`gza usage`, the watch loop, the server homepage).

## Observed Response (verified 2026-08-22)

A live probe against `codex-cli 0.145.0` on a Pro account returned **three
windows across two limit IDs** in 0.72s total (process start to response).
The real shape differs from the illustrative sample in the protocol spec:

| limit_id | limit_name | window | used% | duration | resets |
|---|---|---|---|---|---|
| `codex` | *(null)* | primary | 45 | 10080m (168h) | 2026-08-27 |
| `codex_bengalfox` | GPT-5.3-Codex-Spark | primary | 0 | 300m (5h) | 2026-08-22 |
| `codex_bengalfox` | GPT-5.3-Codex-Spark | secondary | 0 | 10080m (168h) | 2026-08-29 |

Findings that drive the design:

- **Position does not imply duration.** The `codex` bucket's *primary* is the
  168-hour weekly window, and it has no secondary at all. Code that labels
  primary "5-hour" and secondary "weekly" is wrong on this very account.
  Always read `windowDurationMins`.
- **Per-model buckets exist.** `codex_bengalfox` / "GPT-5.3-Codex-Spark" is a
  model-scoped limit alongside the account-wide `codex` one. The bucket set is
  not fixed and must not be enumerated in code.
- **`rateLimits` duplicates the `codex` entry** of `rateLimitsByLimitId`
  verbatim. Prefer `rateLimitsByLimitId` and ignore the legacy field when it is
  present, or the same window is counted twice.
- **Undocumented fields are present**: `credits`
  (`hasCredits`/`unlimited`/`balance`), `planType`, `individualLimit`,
  `spendControlReached`, `rateLimitReachedType`, and a top-level
  `rateLimitResetCredits` list of one-off reset grants.
- **Cost is low.** 0.72s wall clock, ~0.09s of it handshake. The 10s timeout is
  generous and a per-cycle refresh would be affordable even with no cache — the
  cache is for the homepage, not for watch.

## Problem

Fetching Codex usage is expensive relative to how fast it changes: it spawns a
`codex app-server` child process, does a JSON-RPC handshake, and can take
seconds. No consumer should pay that cost on demand. Usage percentages move on
the order of minutes, so a cache with a modest TTL is indistinguishable from a
live read for every consumer we have.

## Shape

Three layers, each independently testable:

1. **Provider query** — per-provider, uncached, bounded. One call, one result or
   one typed failure.
2. **Usage store** — SQLite-backed append-only history plus a TTL read.
3. **Consumers** — `gza usage`, watch cycle header, server homepage. All read
   through the store; none call a provider directly.

### 1. Provider query

Extend the provider interface with an optional capability rather than making it
mandatory:

```python
class UsageQuery(Protocol):
    def read_usage(self, *, timeout_seconds: float) -> ProviderUsage: ...
```

A provider that cannot report usage simply does not implement it; the store
treats "unsupported" as a first-class outcome distinct from "failed". Claude and
Gemini start unsupported. Codex implements it per the protocol spec.

Normalized result, provider-agnostic:

```python
@dataclass(frozen=True)
class UsageWindow:
    limit_id: str            # "codex"
    limit_name: str | None
    window: str              # "primary" | "secondary"
    used_percent: float
    window_duration_minutes: int
    resets_at: datetime

@dataclass(frozen=True)
class ProviderUsage:
    provider: str
    fetched_at: datetime
    windows: tuple[UsageWindow, ...]
```

`remaining_percent` stays a derived property, never stored.

### 2. Usage store

Append-only, two tables: fetch-level facts do not belong on a per-window row.

```sql
CREATE TABLE provider_usage_fetches (
    id INTEGER PRIMARY KEY,
    provider TEXT NOT NULL,
    fetched_at TEXT NOT NULL,             -- ISO8601 UTC
    plan_type TEXT,                       -- "pro"
    spend_control_reached INTEGER,        -- 0/1/NULL
    rate_limit_reached_type TEXT,         -- NULL when not limited
    credits_has INTEGER,                  -- credits.hasCredits
    credits_unlimited INTEGER,            -- credits.unlimited
    credits_balance TEXT,                 -- string on the wire; keep verbatim
    reset_credits_available INTEGER,      -- count of available rateLimitResetCredits
    raw_json TEXT NOT NULL                -- full response, for unmodelled fields
);

CREATE TABLE provider_usage_samples (
    id INTEGER PRIMARY KEY,
    fetch_id INTEGER NOT NULL REFERENCES provider_usage_fetches(id) ON DELETE CASCADE,
    limit_id TEXT NOT NULL,
    limit_name TEXT,
    window TEXT NOT NULL,                 -- primary | secondary
    used_percent REAL NOT NULL,
    window_duration_minutes INTEGER NOT NULL,
    resets_at TEXT NOT NULL
);
CREATE INDEX idx_provider_usage_fetch_latest ON provider_usage_fetches (provider, fetched_at DESC);
CREATE INDEX idx_provider_usage_sample_fetch ON provider_usage_samples (fetch_id);
```

`raw_json` is the hedge. The response already carries fields the published
protocol does not document, and it will grow more; storing the raw payload makes
a future field a backfill query rather than a lost month of history. At ~1.5KB
per fetch and a 15-minute TTL that is roughly 4MB/month, pruned on the same
retention schedule.

`credits_balance` stays TEXT because the wire format sends `"0"`, not `0`; we
should not guess its numeric type or units.

Deliberately not modelled as columns: `individualLimit` (null on every observed
bucket, semantics unknown) and the per-grant contents of `rateLimitResetCredits`
beyond the available count. Both stay recoverable from `raw_json`.

Append-only costs nothing extra now and gives us burn-rate analysis later
(percent consumed per hour, projected exhaustion before reset) without a second
migration. A retention prune (default 30 days) runs opportunistically on write.

Failures are *not* stored as fetches. A failed fetch leaves the last good one in
place and records a separate lightweight failure marker (provider, `last_error`,
`last_error_at`) so consumers can render "stale, last error: …" instead of
silently showing a week-old number as current.

Read API:

```python
def get_usage(provider: str, *, max_age: timedelta, refresh: bool = True) -> UsageSnapshot
```

- Fresh sample within `max_age` → return it, `source="cache"`.
- Stale or absent and `refresh=True` → fetch, store, return `source="fetch"`.
- Fetch fails → return the stale sample marked `stale=True` with the error, or
  `unavailable` if there is nothing cached.
- `refresh=False` → never spawn a process; return whatever is cached (used by
  the web request path, see below).

Concurrency: refreshes take a short-lived SQLite advisory lock keyed on the
provider (same pattern as `project_leases`). A second caller that cannot take
the lock returns the cached value rather than queueing — a duplicate spawn is
worse than a slightly stale number.

### 3. Refresh trigger

The store is refreshed opportunistically by whatever process happens to notice
staleness, so nothing depends on a dedicated daemon:

- **Watch cycle** — at the top of each cycle, `get_usage(..., refresh=True)`.
  At a 5-minute loop and a 15-minute TTL this refreshes every third cycle, and
  the header line always prints the cached value regardless.
- **`gza usage`** — `refresh=True`, plus `--no-refresh` to read cache only and
  `--refresh` to force past the TTL.
- **Server** — `refresh=False` on the request path, always. A background
  refresh task inside `gza-server` (same supervisor that already manages the
  server lifecycle) ticks at the TTL and refreshes out of band. That way the
  homepage never blocks on a subprocess, and usage stays warm even when watch
  is not running.

## Config

```
usage.enabled: bool = true
usage.ttl_seconds: int = 900        # 15 minutes
usage.timeout_seconds: float = 10
usage.retention_days: int = 30
```

Registered in `config_schema.py` like every other key. Per the project's
no-backcompat rule these are named outright; no aliases.

Which providers get queried is derived, not configured: the set of providers
actually routed to (`provider`, `task_providers.*`, `providers.*`) intersected
with the set that implements `UsageQuery`. If Codex is not routed to, we never
spawn `codex app-server`.

## Rendering

One shared formatter so all three surfaces agree.

Compact (watch header, one line). Windows are labelled by their **duration**,
never by primary/secondary position, and named by `limitName` when present:

```
usage  codex 45% 168h (resets 4d21h) · Spark 0% 5h · Spark 0% 168h   (cached 4m ago)
```

Selection rule for the compact line: show every window with `used_percent > 0`,
plus the shortest-duration window always (it is the one that bites mid-session),
capped at three entries with a `+N more` suffix beyond that. If every window is
at 0%, show only the shortest. On the observed account that renders the 45%
weekly and the 5h Spark window, which is exactly the pair worth watching.

Stale or failed:

```
usage  codex 45% 168h (resets 4d21h)   (stale 41m, last fetch failed)
usage  codex unavailable — run `codex login`
```

When `rateLimitReachedType` is non-null or `spendControlReached` is true, the
line is rendered in the warning style with the reason appended.

`gza usage` prints every window in a table (limit, window, used, remaining,
duration, resets at, age) and supports `--json`.

Homepage: a small card in the board row next to "Tasks by status" — one meter per
window, percent used, reset countdown, and a muted "updated Nm ago". Absent or
unsupported → the card is omitted entirely rather than showing an empty box.

## Failure Policy

Usage is decoration, never control flow. A failed or unavailable usage read must
never fail a watch cycle, block a page render, or change scheduling. It is
logged at debug, surfaced in the rendered line, and otherwise ignored.

Explicit non-goal for v1: throttling or scheduling decisions based on usage.
The history table exists so that becomes possible later, on real data.

## Acceptance Criteria

- Codex usage can be read via `gza usage`, and reads are correct against a
  faked `codex app-server`.
- A second consumer within the TTL spawns no child process.
- The watch header prints usage every cycle without a per-cycle fetch.
- The homepage never spawns a provider process on a request.
- A provider that fails still renders the last good value marked stale.
- All three windows observed on a multi-bucket account round-trip intact; the
  legacy `rateLimits` field does not produce a duplicate fourth window.
- Windows are labelled from `windowDurationMins`; a bucket whose primary is the
  weekly window renders as weekly.
- Samples accumulate in `provider_usage_samples` with `raw_json` retained on the
  parent fetch, oldest pruned by retention.
- Nothing in watch, the server, or the CLI fails because usage is unavailable.
