# gza stats subcommands

> **Status: Implemented** — Describes current behavior as of 2026-04-09.

## Overview

`gza stats` has one subcommand: `reviews`. Running `gza stats` without a subcommand prints help text.

## gza stats reviews

Ports the functionality of the former `bin/review-cycle-stats.py` script into the CLI. Shows per-implementation-task review cycle stats, weekly groupings, cycle distribution, and per-model issue counts.

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
- Weekly table: week range, impl count, review count, review %, median/P90/max cycles.
- Cycle distribution: histogram of how many impls had 1, 2, 3, … review cycles.
- Per-model section: when `--issues` is passed, must-fix and suggestion counts parsed from review content.

### Notes

- `bin/review-cycle-stats.py` was removed in the same PR this subcommand was added. Use `gza stats reviews` instead.
- `gza stats cycles` was removed as of 2026-04-09. Use `gza stats reviews` instead.
- Output uses `print()` (plain text) rather than Rich markup, consistent with the original script and suitable for piping.
