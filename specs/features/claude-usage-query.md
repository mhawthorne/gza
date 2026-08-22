# Claude Usage Query

> **Status: Exploration**
> Companion to [codex-usage-query.md](codex-usage-query.md) and
> [usage-stats.md](usage-stats.md). This document reports what is actually
> obtainable for Claude, under both authentication modes, and what it costs to
> get. It does not propose an implementation until the shape question below is
> settled.

## The headline finding

**Claude has no equivalent of Codex's `account/rateLimits/read`.** There is no
supported call that returns "you have used N% of your quota window."

Codex is the easy case: one local process, one JSON-RPC method, a percentage.
Claude splits into two authentication modes that expose *completely different*
data through *completely different* channels, and neither returns the shape
`usage-stats.md` was designed around.

| | API key (Console org) | OAuth subscription (Pro/Max) |
|---|---|---|
| Quota model | Spend cap + per-minute rate limits | 5-hour + weekly seat windows |
| "% of window used" | Not exposed | **Not exposed by any documented API** |
| Historical spend | Admin API (usage + cost reports) | claude.ai UI only |
| Real-time headroom | Rate-limit response headers | Not exposed |
| Requires | A separate **Admin** key | Nothing usable |

This machine currently has neither `ANTHROPIC_API_KEY` nor
`ANTHROPIC_AUTH_TOKEN` set, and `~/.claude` holds live session data, so gza's
Claude traffic here runs on **OAuth subscription** — the column with no
documented answer.

## Option A — Admin Usage & Cost API (API-key orgs only)

Two documented endpoints, both requiring an Admin API key
(`sk-ant-admin01-...`), which is a *different credential* from the API key used
for inference and must be created separately in the Console:

```
GET https://api.anthropic.com/v1/organizations/usage_report/messages
      ?starting_at=…&ending_at=…&bucket_width=1m|1h|1d
      &group_by[]=model&group_by[]=api_key_id&…
GET https://api.anthropic.com/v1/organizations/cost_report
      ?starting_at=…&ending_at=…&group_by[]=workspace_id|description
```

Both take `x-api-key` + `anthropic-version: 2023-06-01`, paginate via
`has_more`/`next_page`, and lag real usage by ~5 minutes. Polling is supported
at roughly once per minute. Bucket limits: `1m` up to 1,440 buckets, `1h` up to
168, `1d` up to 31.

What it gives: exact token counts (uncached input, cache creation, cache read,
output) and USD cost, sliceable by model, workspace, API key, and service tier.

What it does **not** give: any notion of remaining capacity. It is an accounting
feed, not a quota gauge. To render "45% used" you would have to know the
organization's spend cap and divide — and the cap lives in the Console, not in
these responses.

**Blockers for us:** unavailable for individual accounts (org required), needs a
second privileged credential, and Claude Enterprise orgs need a different key
type (Analytics API) entirely.

## Option B — Rate-limit response headers (API key)

Every Messages API response carries current headroom:

```
anthropic-ratelimit-requests-limit / -remaining / -reset
anthropic-ratelimit-input-tokens-limit / -remaining / -reset
anthropic-ratelimit-output-tokens-limit / -remaining / -reset
```

`-reset` is RFC 3339. This is the **closest true analogue to Codex's
`usedPercent`**: `1 - remaining/limit` is a real utilization figure, and the
reset timestamp maps onto `resets_at`.

Two catches. First, these are per-minute token-bucket limits, so the "window"
is a minute, not five hours — the number is a throughput gauge, not a budget
burn-down, and it will read ~100% remaining whenever gza is idle. Second, headers
only arrive as a side effect of a real request; there is no status endpoint. A
dedicated probe would mean spending tokens (a 1-token `max_tokens` call, or
`count_tokens`) purely to read headers.

Not available at all under OAuth subscription auth.

## Option C — OAuth subscription

Claude Code's `/usage` does render plan usage bars with 5-hour and weekly
windows, so the data exists. But:

- The endpoint behind it is **undocumented**, and reaching it would require
  lifting the OAuth token out of the keychain or `~/.claude/.credentials.json`.
  That is exactly what
  [codex-usage-query.md](codex-usage-query.md) § Security Boundary forbids, and
  the same rule should hold here. **This option is closed on principle, not on
  difficulty.**
- Anthropic has open feature requests for a `claude usage` subcommand and a
  subscription usage endpoint; neither has shipped.
- The plan-usage breakdown in `/usage` is itself computed from *local session
  history on this machine*, and excludes usage from other devices and claude.ai.

Two boundary-respecting substitutes exist:

**C1 — OpenTelemetry export.** Claude Code emits
`claude_code.token.usage` (attribute `type` ∈ input/output/cacheRead/cacheCreation)
and `claude_code.cost.usage` (attributes `model`, `query_source`, `speed`,
`effort`) when `CLAUDE_CODE_ENABLE_TELEMETRY=1` and an OTLP exporter is
configured. Works under **both** auth modes. This is the only supported path to
per-session Claude numbers on a subscription — but it is a push pipeline needing
a collector, not a query, and it reports consumption, never remaining quota.

**C2 — gza's own accounting (recommended first step).** gza *already* parses
`total_cost_usd` and per-turn `input_tokens` / `output_tokens` /
`cache_creation_input_tokens` / `cache_read_input_tokens` out of every Claude
run (`src/gza/providers/claude.py`). We are already holding the raw material for
"what has gza spent on Claude", per task and per model, with zero new
credentials, zero new endpoints, and no boundary problem. It answers a genuinely
useful question the Codex integration cannot answer at all: *which tasks cost
the most.* It cannot answer "how close am I to my limit."

## The shape problem

`UsageWindow` assumes `used_percent` + `resets_at` + `window_duration_minutes`.
That fits Codex exactly and fits nothing on the Claude side:

- Admin API → cumulative tokens and dollars over a chosen range; no window, no
  percentage.
- Rate-limit headers → a percentage, but of a per-minute throughput bucket.
- gza's own accounting → dollars and tokens spent; no denominator at all.

Forcing these into `UsageWindow` would produce a homepage card that reads
"claude 3% used · 1m window" — technically true, operationally meaningless next
to "codex 45% used · 7d window."

The honest modelling is a second result kind alongside the quota window — a
*consumption* record (tokens, cost, period) with no denominator — and a renderer
that shows a meter only when a real denominator exists and a bare figure
otherwise.

## Recommendation

1. **Ship C2 first.** Surface gza's already-captured Claude spend (cost and
   tokens, by model, over a rolling window) through the same store and the same
   `gza usage` command. No credentials, no new failure modes, immediately useful.
2. **Add Option A behind config** if and when a Console org with an Admin key is
   in play — `usage_admin_key` or similar, absent by default, feature-off when
   unset. It is the only path to authoritative billing numbers.
3. **Do not pursue Option C's undocumented endpoint.** Revisit if Anthropic
   ships the requested public subscription usage API.
4. **Treat Option B as optional telemetry**, not a headline number: worth
   capturing opportunistically if we ever call the Messages API directly, never
   worth a synthetic request.

## Open question for the operator

Codex answers "how much of my quota is left." For Claude on a subscription, that
question has no supported answer today. Is the useful substitute *"what has gza
spent on Claude"* (C2 — buildable now), or should the Claude card simply be
absent until a real quota API exists?
