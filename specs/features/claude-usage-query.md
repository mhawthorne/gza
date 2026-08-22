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

**C2 — gza's own token accounting.** gza already parses per-turn
`input_tokens` / `output_tokens` / `cache_creation_input_tokens` /
`cache_read_input_tokens` and `total_cost_usd` out of every run, and persists
`input_tokens`, `output_tokens`, `cost_usd`, `provider`, and `model` on the
`tasks` row. This is provider-agnostic — Codex has the same accounting
(`get_pricing_for_model` / `calculate_cost` in `providers/codex.py`) — so cost
and token totals are already answerable for *both* providers today, and are
visible in `gza stats`. Nothing new is needed to report spend.

## The shape problem

`UsageWindow` assumes `used_percent` + `resets_at` + `window_duration_minutes`.
That fits Codex exactly and fits nothing Claude reports:

- Admin API → cumulative tokens and dollars over a chosen range; no window, no
  percentage.
- Rate-limit headers → a percentage, but of a per-minute throughput bucket.

Cost and token totals we already compute for every provider are consumption
figures with no denominator, so they cannot fill a quota meter either. The next
section supplies the missing denominator by configuration.

## Derived quota: a configured budget as the denominator

Cost is already solved. The unanswered question is **"how much of my quota is
left"**, and for a subscription harness that publishes no quota, the only way to
answer it is to supply the denominator ourselves: configure a token budget per
window, sum our own consumption over that window, and divide.

```yaml
usage_budgets:
  claude:
    window: 7d           # rolling
    input_tokens: 2_000_000_000
```

`used_percent = tokens_in_window / budget`, `resets_at` = end of the rolling
window. That produces a genuine `UsageWindow` — same shape, same renderer, same
homepage meter as Codex — with `source: "derived"` instead of `"provider"`.

The query is already available from the data we keep:

```sql
SELECT SUM(COALESCE(input_tokens,0)), SUM(COALESCE(output_tokens,0))
FROM tasks
WHERE provider = ?
  AND COALESCE(completed_at, updated_at, created_at) >= :window_start
```

Measured on the live DB, a 7-day window returns 4.7B input tokens across 894
tasks, 795 of which carry token counts — the signal is dense enough to meter.

### Four things that decide whether the number means anything

1. **Cache reads are conflated.** gza sums `input_tokens`,
   `cache_creation_input_tokens`, and `cache_read_input_tokens` into one
   `input_tokens` column. Anthropic's own limits exclude cache reads from ITPM,
   and cache reads dominate an agentic workload — they are most of that 4.7B. A
   budget denominated in "input tokens" therefore measures mostly cache traffic
   and will not track a real subscription window. **Fix first:** persist the
   breakdown in separate columns so the budget can be denominated in
   non-cached input tokens. Without this the meter is decorative.

2. **It only sees gza's traffic.** Interactive Claude Code, claude.ai, and other
   machines share the same subscription window and are invisible here. On a
   shared subscription the derived number is a floor, not a measure.

3. **Rolling ≠ the real window.** Subscription windows reset on a fixed schedule
   we cannot observe. A rolling 7-day sum is a defensible approximation and
   arguably more useful for pacing, but it will not agree with what `/usage`
   shows.

4. **The budget has to be calibrated, not guessed.** A number pulled from the
   air produces a confident-looking meter with no meaning. The honest path is to
   let the operator set it and refine it — and, better, to record the timestamp
   whenever a run fails on a rate/usage limit, so observed limit events
   calibrate the budget against reality over time.

### Labelling

A derived window must never render identically to a provider-reported one.
Same meter, explicit provenance:

```
usage  codex  45% used · 7d window · resets in 4d20h        (cached 4m ago)
usage  claude 62% used · 7d rolling · budget 2.0B tok       (derived, gza traffic only)
```

## Recommendation

1. **Split the cached-token columns first.** Everything else is unreliable
   until the denominator can exclude cache reads.
2. **Then implement derived windows** as a `source: "derived"` variant of
   `UsageWindow`, config-driven, off unless a budget is set. It is
   provider-agnostic by construction — it would work for any harness that
   reports tokens but not quota.
3. **Record limit-hit events** so the configured budget can be calibrated
   against observed reality rather than left at a guess.
4. **Add the Admin API (Option A) only if a Console org with an Admin key
   appears.** Not applicable to this machine's subscription auth.
5. **Do not pursue the undocumented `/usage` endpoint.**
