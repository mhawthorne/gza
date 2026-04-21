# gza stats subcommands

> **Status: Implemented** — Describes current behavior as of 2026-04-12.

## Overview

`gza stats` has two subcommands: `reviews` and `iterations`. Running `gza stats` without a subcommand prints help text.

## gza stats reviews

Ports the functionality of the former `bin/review-cycle-stats.py` script into the CLI. Shows per-implementation-task review iteration stats, weekly groupings, iteration-count distribution, and per-model issue counts.

```bash
gza stats reviews                            # Last 14 days (default)
gza stats reviews --days N                   # Last N days
gza stats reviews --start-date YYYY-MM-DD    # From a specific start date
gza stats reviews --end-date YYYY-MM-DD      # Up to a specific end date
gza stats reviews --issues                   # Show per-model must-fix/suggestion counts
```

### Date range logic

The three-way priority for determining the start date:
1. `--start-date` takes highest priority if supplied.
2. `--days N` is used if supplied without `--start-date`.
3. Default: last 14 days from end date.

`--end-date` defaults to today if not supplied.

### Output

- Summary header: implement task count, total reviews, reviewed fraction.
- Weekly table: week range, impl count, review count, review %, median/P90/max number of review iterations per impl.
- Reviews-per-implementation distribution: histogram of how many impls had 1, 2, 3, … review iterations.
- Per-model section: when `--issues` is passed, must-fix and suggestion counts parsed from review content.

### Notes

- `bin/review-cycle-stats.py` was removed in the same PR this subcommand was added. Use `gza stats reviews` instead.
- `gza stats cycles` was removed as of 2026-04-09. Use `gza stats reviews` instead.
- Output uses `print()` (plain text) rather than Rich markup, consistent with the original script and suitable for piping.

## gza stats iterations

Shows per-implementation operational rollups for review/improve loops, including completed review/improve counts, latest completed review verdict, and total cost.

```bash
gza stats iterations                              # Last 14 days (default)
gza stats iterations --last N                     # Limit to N most-recent implementation rows
gza stats iterations --hours N                    # Activity in the last N hours
gza stats iterations --days N                     # Last N days
gza stats iterations --start-date YYYY-MM-DD      # From a specific start date
gza stats iterations --end-date YYYY-MM-DD        # Up to a specific end date
gza stats iterations --all                         # All-time view
```

### Activity window semantics

- Inclusion uses activity timestamps:
  - Completed tasks use `completed_at`.
  - Incomplete tasks fall back to `created_at`.
- A row is included when any of these are in-window:
  - The implementation task activity timestamp.
  - Any child review task activity timestamp.
  - Any child improve task activity timestamp.
- Latest verdict comes from the most recently completed review task.

### Row filtering

Only impls with at least one completed review appear in the table. Failed, in-progress/queued, and no-review impls are excluded so they don't skew the iteration-count distribution (failed impls would otherwise inject 0s into every percentile). Excluded impls are summarized on a single `Excluded:` line below the totals, with a per-verdict breakdown and aggregated cost.

Row task labels display the stored slug body after removing only the date prefix (`YYYYMMDD-`).
This avoids ambiguous prefix stripping when a semantic slug itself starts with the
project prefix token (for example, `YYYYMMDD-gza-rollout`).

### Flag compatibility

- `--hours` cannot be combined with `--days`, `--start-date`, or `--end-date`.
- `--all` cannot be combined with any date-window flags (`--hours`, `--days`, `--start-date`, `--end-date`).
