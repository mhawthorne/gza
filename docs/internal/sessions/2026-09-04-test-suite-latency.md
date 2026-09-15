# Test suite latency: where the time goes, and what did not work

Investigation into unit-suite runtime against a target of **60 seconds on a
single core**. Records what was measured, what landed, and — at least as
usefully — which promising ideas were tried and failed, so they are not
attempted again from scratch.

## Where the time actually goes

Profiling the slowest decile (219 tests, 52% of suite time) by self time:

| bucket | self time | share |
| --- | --- | --- |
| SQLite | 59.5s | **85.5%** |
| gza source | 2.75s | 3.9% |
| stdlib | 2.56s | 3.7% |
| pytest/plugins | 1.27s | 1.8% |
| test code itself | 0.06s | **0.1%** |

Test bodies are a rounding error. The cost is database work the tests drive,
and within that, the connection lifecycle — `connect` + first `execute` +
`close` — is 82%. `close` alone is 42%, larger than `execute`.

The mechanism: SQLite defers opening the file and parsing the schema until a
connection's **first** statement. Against this schema (76 objects) that costs
~626us, while the second statement on the same connection costs ~12us. Every
store operation opens a fresh connection, so every operation pays it.

```
store.get(task_id)          802us
  the SQL it runs            10.7us   (1.3%)
  connect                    50us
  close                     127us
  3 executes on a fresh conn 595us
```

### Cost per operation is not constant

Test duration correlates only moderately with database operation *count*
(pearson 0.485). The slowest decile does 31% of the connection opens but takes
52% of the time, because a connection's cost grows with how much the test has
written. Counting operations is the wrong proxy; profile instead.

## What landed

| change | effect |
| --- | --- |
| store construction (SCHEMA completeness, duplicate repair passes, batched PRAGMA probes) | store open 13.45ms -> 5.43ms fresh, 11.84ms -> 4.36ms reopen |
| fixed a red functional test whose failure triggered a serial rerun | verify ~290s -> ~220s |
| `synchronous=NORMAL` now applied to every connection, not just the store's first | ~6% on writes |
| one read session across `walk_based_on_descendants` | connection opens for that call 604 -> 212 |
| xdist workers clamped down to core count on single-core hosts | correctness, not speed |

## What did not work

**Thread-local connection reuse.** Measured 802us -> 48us per `store.get`
(16.6x), then failed two tests for a legitimate reason: holding a connection
open blocks operations needing exclusive access. `PRAGMA journal_mode=DELETE`
raises `database is locked`; whole-file replacement breaks too. The same class
covers `VACUUM`. Any reuse scheme needs an explicit release point at those
boundaries rather than holding connections for process lifetime.

**Permission-based read-only detection.** Pursued on the theory that read-only
detection worked by attempting a write, and that this was what blocked reuse.
It is not: `open_mode="query_only"` is chosen explicitly by callers, the
`_is_readonly_*` helpers are error *recovery* during schema repair rather than
mode *detection*, and the query-only warning path uses schema introspection.
Nothing here would unblock reuse.

**Shared-cache in-memory databases.** The most promising idea by
microbenchmark, and it lost decisively end-to-end:

| | on-disk (WAL) | in-memory (shared cache) |
| --- | --- | --- |
| first query on a connection | 343.4us | 5.8us |
| close | 20.8us | 4.1us |
| build schema | 16.52ms | 1.81ms |
| **`tests/cli/test_watch.py` end to end** | **96.5s** | **141.5-146.0s** |

Three runs, consistently ~48% slower, plus 49 failures. Ruled out as causes:
schema building (2.7s of 146s), connection count (unchanged at ~81k), and path
resolution in the harness. The leading unverified suspect is shared-cache mode
itself — `cache=shared` serialises access with table-level locks, which the
microbenchmark never exercised. **Verify that before revisiting.**

The 49 failures are a real design obstacle independent of speed: the store uses
`db_path.exists()` as the signal for "database is initialised", and a
`watch_lease_acquisition` store opens no connection in `__init__`.

**Template-database copying.** Sharing a prebuilt schema file measured slower
than replaying the DDL (7.45ms vs 5.43ms).

**More xdist workers.** Rejected on requirements, not performance: workers
contend with `watch`, interactive sessions, and manual runs, and the floor is a
single-core container. `-n 8` also surfaced genuine test pollution in
`tests/cli/test_watch.py`.

## Ideas that sound right and are not

* **Parametrising tests does not speed anything up.** Collapsing N methods into
  one method x N cases yields the same N test items, N setups, and identical
  coverage. It is a source-duplication fix.
* **Re-scoping fixtures does not help either.** Measured setup vs call on
  `tests/cli/test_watch.py`: setup ~0.0 CPU-s against call 88.5 CPU-s. The
  staging is inline in test bodies, not in fixtures.
* **Deleting cheap tests saves nothing.** 13% of tests perform zero database
  operations and account for ~0% of runtime. Removing string-assertion tests is
  a maintenance argument, never a latency one.

## The cull

The only lever sized to the 60-second target. A greedy set cover keeps ~3,272
tests that together reproduce every branch arc the full suite covers; the
remainder costs about half the runtime.

Generate the ranked list with `bin/test-cull-candidates` (see
`docs/internal/profiling.md`). Candidates as of this investigation are in
`2026-09-04-test-cull-candidates.tsv`, ordered most expensive first, with the
saving front-loaded: the first 100 removals recover ~15% of runtime.

Two classes are held back automatically, because arc coverage cannot see them:

* **parametrised variants whose siblings survive** — `[case-a]` and `[case-b]`
  walk the same lines with different expected outcomes
* **tests carrying a `specs/behavior/` term no surviving test carries** —
  e.g. `same_head` had 30 candidate tests and no survivor naming it, against
  8+ MUST statements in `lifecycle-engine.md`; `recovery_origin` is defined in
  `lineage.md` as a canonical schema-v41 provenance tag

**This remains a human review list.** Arc coverage is not behaviour coverage:
two tests walking one path with different data are indistinguishable, and the
cover keeps only one. The spec-term filter matches on test *names*, so it flags
candidates for review rather than proving loss, and it cannot see a behaviour
no test name mentions. Verify deletions against `specs/behavior/`, not just a
green suite.

## Measuring on a busy machine

Wall-clock comparisons were repeatedly unusable here: concurrent `gza` verify
gates ran at ~96% CPU, and identical suite runs drifted 186s / 216s / 258s /
337s with no code change. Prefer counters that do not move with load —
connection opens, SQL statement counts — and treat any timing A/B taken under
load as provisional.

## Expiring tests (added 2026-09-14)

Three failures in `tests/test_runner.py` cost most of a session to diagnose and
turned out to be a calendar problem. The tests stamp fixture data with a literal
`datetime(2026, 8, 28, ...)`, and production compares it against real
`datetime.now()` through a staleness window (`autonomous_verify_observation_max_age_hours`,
default 168h). They passed until 2026-09-04 and failed permanently after.

The failure mimics almost every other kind of flake, and I worked through the
wrong ones in order: test pollution, an enabling file, a poisoning file that
normally sorts later, in-file pollution, an xdist parity issue, and a product
bug in preflight. All were disproved. The giveaway was a single trace line:

```
REJECTED: captured_at 2026-08-28 12:00+00:00 < cutoff 2026-09-01 ...
```

Preflight was correct throughout -- it declined to trust an 11-day-old runtime
observation, which is its contract.

**Comparing runs across days is what made this confusing.** Runs from Sept 3-4
passed and runs from Sept 5+ failed, so any dimension that happened to differ
between them -- parallel vs serial, coverage on or off, plugin loaded or not --
looked causal. Two of the three tests then recovered by themselves on Sept 14,
which is the same effect from the other side.

### Finding them

`bin/test-timebombs` runs each file twice, once normally and once with gza's
clock shifted forward, and reports tests that only fail in the shifted arm.

```
bin/test-timebombs --days 365          # whole suite
bin/test-timebombs --days 30 tests/cli/test_query.py
```

At +365d it found **58** such tests, listed in
`2026-09-14-expiring-tests.txt`. They cluster exactly where you would expect:
retention (`clean_deletes_old_backups`), reporting windows
(`history_lookback_days`, `merged_json_filters_by_source_and_last_days`),
staleness (`stale_unmerged_dry_run_reports_only_old_abandoned_units`), and
recency (`reconciliation_skips_recent_live_task`, `queue_shows_quiet_lane`).

A shift only reveals windows shorter than the offset, so one probe is evidence
and not proof -- probe more than one horizon before calling the suite clean.

### Fixing them

Make fixture timestamps relative to now (`datetime.now(UTC) - timedelta(hours=1)`)
so "recent" stays recent, or inject a fixed clock so the test controls both sides
of the comparison. Bumping the literal date only re-arms the bomb.

Two traps cost two 25-minute runs while building the audit, both worth knowing
for any suite-wide scripting here: pytest's short summary reaches **stderr** in
this configuration, and the agent shell exports `FORCE_COLOR`, so summary lines
arrive wrapped in ANSI codes and an anchored `^FAILED` silently matches nothing.
Prove a measurement pipeline on one file before spending a long run on it.
