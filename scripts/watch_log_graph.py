#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = ["matplotlib"]
# ///
"""Graph gza ``watch`` queue depth over time from ``.gza/watch.log``.

Parses the watch log and plots four series — tasks **running**, **pending**
(runnable), **blocked**, and **need attention** — against time, saving a PNG and
printing a text table of the same numbers.

The watch log records only ``HH:MM:SS`` (no date), so dates are inferred: we walk
the log in order and bump a day counter every time the clock jumps backwards
(a midnight wrap), then anchor the *latest* line to ``--date`` (default today) so
the log "ends now". If a future watch log grows a full date-bearing timestamp,
that is used directly and the wrap inference is skipped.

Run it with uv (deps are declared inline above)::

    uv run scripts/watch_log_graph.py
    uv run scripts/watch_log_graph.py --log /path/to/.gza/watch.log --out q.png
    uv run scripts/watch_log_graph.py --watch 60      # live: refresh table + PNG every 60s

Log line shapes it understands::

    HH:MM:SS WAKE      checking... (4 running, pending=2 runnable, blocked=14, 0 slots)
    HH:MM:SS WAKE      checking... (5 running, 0 slots)          # older, running only
    HH:MM:SS INFO      12 tasks still need attention (unchanged)
    HH:MM:SS INFO      Needs attention (13 tasks):
"""

from __future__ import annotations

import argparse
import re
import sys
import time
from datetime import date as date_cls
from datetime import datetime, timedelta
from pathlib import Path

# --- line patterns ---------------------------------------------------------

# Optional full date-bearing timestamp at the very start of a line (forward-compat
# for when the watch log gains real dates). Matches e.g. "2026-07-01 19:15:58" or
# "2026-07-01T19:15:58". If present we trust it and skip midnight-wrap inference.
_FULL_TS_RE = re.compile(r"^(\d{4}-\d{2}-\d{2})[ T](\d{2}:\d{2}:\d{2})")
_HMS_RE = re.compile(r"^(\d{2}):(\d{2}):(\d{2})\b")

# Newest format: running / runnable-pending / blocked all broken out.
_WAKE_FULL_RE = re.compile(
    r"WAKE\s+checking\.\.\. \((\d+) running, pending=(\d+) runnable, blocked=(\d+),"
)
# Middle-era format (most common): running + pending, no blocked breakdown.
_WAKE_MID_RE = re.compile(r"WAKE\s+checking\.\.\. \((\d+) running, (\d+) pending, (\d+) slots\)")
# Oldest format: running + slots only.
_WAKE_OLD_RE = re.compile(r"WAKE\s+checking\.\.\. \((\d+) running, (\d+) slots\)")
_ATTN_UNCHANGED_RE = re.compile(r"(\d+) tasks? still need attention")
_ATTN_HEADER_RE = re.compile(r"Needs attention \((\d+) tasks?\)")


class Point:
    """One WAKE cycle's counts, plus the carried-forward attention count."""

    __slots__ = ("when", "running", "pending", "blocked", "attention")

    def __init__(self, when, running, pending, blocked, attention):
        self.when = when
        self.running = running  # int
        self.pending = pending  # int | None (None for old-format WAKE)
        self.blocked = blocked  # int | None
        self.attention = attention  # int | None


def _parse_clock(line):
    """Return (full_datetime | None, hms_tuple | None) for a log line, or (None, None)."""
    m = _FULL_TS_RE.match(line)
    if m:
        dt = datetime.strptime(f"{m.group(1)} {m.group(2)}", "%Y-%m-%d %H:%M:%S")
        return dt, None
    m = _HMS_RE.match(line)
    if m:
        return None, (int(m.group(1)), int(m.group(2)), int(m.group(3)))
    return None, None


def parse_log(path, base_date):
    """Parse ``path`` into a chronological list of Point.

    ``base_date`` anchors the newest line (HMS-only logs). Attention is carried
    forward across cycles because the log only re-emits it when it changes.
    """
    raw = []  # (full_dt | None, hms | None, kind, values...)
    attention = None  # carried forward as we scan

    with open(path, encoding="utf-8", errors="replace") as fh:
        for line in fh:
            full_dt, hms = _parse_clock(line)
            if full_dt is None and hms is None:
                continue  # continuation / blank / undated line

            m = _WAKE_FULL_RE.search(line)
            if m:
                raw.append(
                    (full_dt, hms, "wake",
                     int(m.group(1)), int(m.group(2)), int(m.group(3)))
                )
                continue
            m = _WAKE_MID_RE.search(line)
            if m:
                # running + pending, no blocked breakdown in this era.
                raw.append((full_dt, hms, "wake", int(m.group(1)), int(m.group(2)), None))
                continue
            m = _WAKE_OLD_RE.search(line)
            if m:
                raw.append((full_dt, hms, "wake", int(m.group(1)), None, None))
                continue
            m = _ATTN_UNCHANGED_RE.search(line) or _ATTN_HEADER_RE.search(line)
            if m:
                attention = int(m.group(1))
                # attach to a marker so ordering with WAKE lines is preserved
                raw.append((full_dt, hms, "attn", attention))

    if not raw:
        return []

    # Assign real datetimes. If any line carried a full timestamp we trust those and
    # forward-fill HMS-only lines from the last known date; otherwise infer via wraps.
    have_full = any(r[0] is not None for r in raw)
    times = _assign_datetimes(raw, base_date, have_full)

    # Second pass: build Points from WAKE rows, carrying attention forward.
    points = []
    attention = None
    for (row, when) in zip(raw, times):
        kind = row[2]
        if kind == "attn":
            attention = row[3]
            continue
        _, _, _, running, pending, blocked = row
        points.append(Point(when, running, pending, blocked, attention))
    return points


def _assign_datetimes(raw, base_date, have_full):
    """Map each raw row to a datetime, inferring dates from midnight wraps."""
    if have_full:
        # Forward-fill: carry the most recent full date across HMS-only lines.
        out = []
        last_date = base_date
        for full_dt, hms, *_ in raw:
            if full_dt is not None:
                last_date = full_dt.date()
                out.append(full_dt)
            else:
                h, m, s = hms
                out.append(datetime.combine(last_date, datetime.min.time())
                           .replace(hour=h, minute=m, second=s))
        return out

    # HMS-only: day counter increments on each backwards clock jump.
    day = 0
    prev = None
    day_offsets = []
    for _, hms, *_ in raw:
        secs = hms[0] * 3600 + hms[1] * 60 + hms[2]
        if prev is not None and secs < prev:
            day += 1
        day_offsets.append(day)
        prev = secs
    max_day = day_offsets[-1] if day_offsets else 0
    # Anchor so the last line lands on base_date; earlier days count backwards.
    out = []
    for (_, hms, *_), d in zip(raw, day_offsets):
        h, m, s = hms
        the_date = base_date - timedelta(days=(max_day - d))
        out.append(datetime.combine(the_date, datetime.min.time())
                   .replace(hour=h, minute=m, second=s))
    return out


# --- output ---------------------------------------------------------------

_SERIES = [
    ("running", "running"),
    ("pending", "pending"),
    ("blocked", "blocked"),
    ("attention", "need attention"),
]


def make_figure():
    """Create a reusable (fig, ax) with the Agg backend. Import matplotlib once."""
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    return plt.subplots(figsize=(14, 7))


def render_png(points, out_path, log_path, fig_ax=None):
    """Render the 4-series plot to ``out_path``.

    Pass ``fig_ax`` (from :func:`make_figure`) to reuse a single figure across
    ticks in ``--watch`` mode; otherwise a throwaway figure is created and closed.
    """
    import matplotlib.dates as mdates

    if fig_ax is None:
        fig, ax = make_figure()
        own = True
    else:
        fig, ax = fig_ax
        ax.clear()
        own = False

    xs = [p.when for p in points]
    for attr, label in _SERIES:
        ys = [float("nan") if getattr(p, attr) is None else getattr(p, attr) for p in points]
        ax.plot(xs, ys, label=label, linewidth=1.2)

    ax.set_ylabel("task count")
    ax.set_xlabel("time")
    ax.set_title(f"gza watch queue depth — {log_path}  ({len(points)} cycles)")
    ax.legend(loc="upper left")
    ax.grid(True, alpha=0.3)
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%m-%d %H:%M"))
    fig.autofmt_xdate()
    fig.tight_layout()
    fig.savefig(out_path, dpi=120)
    if own:
        import matplotlib.pyplot as plt

        plt.close(fig)


def _fmt(v):
    return "" if v is None else str(v)


def print_table(points, max_rows, tail=False):
    """Print an aligned text table.

    Default: evenly sample at most ``max_rows`` rows across the full range.
    ``tail=True`` (watch mode): show the most recent ``max_rows`` cycles instead.
    """
    if tail:
        sampled = points[-max_rows:] if max_rows and max_rows > 0 else points
        note = f"(latest {len(sampled)} of {len(points)} cycles)"
    elif max_rows and max_rows > 0 and len(points) > max_rows:
        step = len(points) / max_rows
        idxs = sorted({int(i * step) for i in range(max_rows)})
        idxs = [i for i in idxs if i < len(points)]
        if idxs[-1] != len(points) - 1:
            idxs.append(len(points) - 1)  # always include the newest cycle
        sampled = [points[i] for i in idxs]
        note = f"(sampled {len(sampled)} of {len(points)} cycles; --table-rows 0 for all)"
    else:
        sampled = points
        note = f"({len(points)} cycles)"

    header = ("datetime", "running", "pending", "blocked", "attention")
    rows = [
        (p.when.strftime("%Y-%m-%d %H:%M:%S"),
         _fmt(p.running), _fmt(p.pending), _fmt(p.blocked), _fmt(p.attention))
        for p in sampled
    ]
    widths = [max(len(header[c]), *(len(r[c]) for r in rows)) for c in range(len(header))]
    line = "  ".join(header[c].ljust(widths[c]) for c in range(len(header)))
    print(line)
    print("  ".join("-" * widths[c] for c in range(len(header))))
    for r in rows:
        print("  ".join(r[c].ljust(widths[c]) for c in range(len(header))))
    print(note)


def print_current(points):
    """One-line live status: refresh wall-clock + the newest cycle's counts."""
    p = points[-1]
    now = datetime.now().strftime("%H:%M:%S")
    print(f"[{now}]  latest cycle {p.when:%Y-%m-%d %H:%M:%S}  |  "
          f"running={_fmt(p.running)}  pending={_fmt(p.pending)}  "
          f"blocked={_fmt(p.blocked)}  attention={_fmt(p.attention)}")


def _series_range(points, attr):
    vals = [getattr(p, attr) for p in points if getattr(p, attr) is not None]
    if not vals:
        return "n/a"
    return f"{min(vals)}–{max(vals)}"


def print_summary(points, out_path):
    first, last = points[0].when, points[-1].when
    print()
    print(f"range     : {first:%Y-%m-%d %H:%M:%S} → {last:%Y-%m-%d %H:%M:%S}")
    print(f"cycles    : {len(points)}")
    for attr, label in _SERIES:
        print(f"{label:<14}: {_series_range(points, attr)}")
    print(f"png       : {out_path}")


# --- discovery / cli -------------------------------------------------------


def default_log():
    """Current project's .gza/watch.log, else newest watch.log under the supreme tree."""
    here = Path.cwd()
    for base in (here, *here.parents):
        cand = base / ".gza" / "watch.log"
        if cand.is_file():
            return cand
    root = Path.home() / "work" / "supreme"
    if root.is_dir():
        logs = list(root.rglob(".gza/watch.log"))
        if logs:
            return max(logs, key=lambda p: p.stat().st_mtime)
    return None


def parse_date(s):
    return datetime.strptime(s, "%Y-%m-%d").date()


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--log", type=Path, default=None,
                    help="path to a .gza/watch.log (default: auto-discover)")
    ap.add_argument("--out", type=Path, default=Path("watch_status.png"),
                    help="output PNG path (default: watch_status.png)")
    ap.add_argument("--date", type=parse_date, default=None,
                    help="base date to assume for the newest line (default: today)")
    ap.add_argument("--table-rows", type=int, default=40,
                    help="max rows in the printed table, evenly sampled (0 = all)")
    ap.add_argument("--no-png", action="store_true", help="skip PNG, table only")
    ap.add_argument("--watch", nargs="?", type=int, const=60, default=None, metavar="N",
                    help="live mode: refresh table + PNG every N seconds (default 60). "
                         "Table shows the most recent --table-rows cycles.")
    args = ap.parse_args(argv)

    log_path = args.log or default_log()
    if not log_path or not Path(log_path).is_file():
        ap.error(f"watch.log not found (looked for {log_path or '.gza/watch.log'}); pass --log")

    if args.watch is not None:
        return _watch_loop(args, log_path)

    base_date = args.date or date_cls.today()
    points = parse_log(log_path, base_date)
    if not points:
        print(f"no WAKE cycles parsed from {log_path}", file=sys.stderr)
        return 1

    print_table(points, args.table_rows)
    if not args.no_png:
        render_png(points, args.out, log_path)
    print_summary(points, "(skipped)" if args.no_png else args.out)
    return 0


_CLEAR = "\033[2J\033[3J\033[H"  # clear screen + scrollback, cursor home


def _watch_loop(args, log_path):
    """Refresh the table (and PNG) every ``args.watch`` seconds until Ctrl-C."""
    interval = args.watch
    fig_ax = None if args.no_png else make_figure()
    try:
        while True:
            try:
                points = parse_log(log_path, args.date or date_cls.today())
            except OSError as exc:  # log momentarily unreadable — keep looping
                print(f"read error: {exc}; retrying in {interval}s...", file=sys.stderr)
                time.sleep(interval)
                continue

            print(_CLEAR, end="")
            if not points:
                print(f"no WAKE cycles yet in {log_path}")
            else:
                print_current(points)
                print()
                print_table(points, args.table_rows, tail=True)
                if not args.no_png:
                    render_png(points, args.out, log_path, fig_ax=fig_ax)
                    print(f"\npng refreshed: {args.out}")
            print(f"\nrefreshing every {interval}s — Ctrl-C to stop", flush=True)
            time.sleep(interval)
    except KeyboardInterrupt:
        print("\nstopped.")
    finally:
        if fig_ax is not None:
            import matplotlib.pyplot as plt

            plt.close(fig_ax[0])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
