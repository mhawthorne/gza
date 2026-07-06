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

    uv run scripts/watch_log_graph.py                      # last 24h (default)
    uv run scripts/watch_log_graph.py --hours 72           # last 3 days
    uv run scripts/watch_log_graph.py --all                # full log history
    uv run scripts/watch_log_graph.py --start 2026-06-28   # from an absolute date
    uv run scripts/watch_log_graph.py --all --resolution day --aggregate p90  # daily p90
    uv run scripts/watch_log_graph.py --resolution hour --agg max        # hourly peaks
    uv run scripts/watch_log_graph.py --no-merges          # hide merge dots (on by default)
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
# Real merge events: "MERGE     gza-7957 -> main" (dry-run variants excluded below).
_MERGE_RE = re.compile(r"MERGE\s+(gza-\d+)\s*->")


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
    """Parse ``path`` into ``(points, merges)``.

    ``points`` is a chronological list of Point; ``merges`` is a list of
    ``(datetime, task_id)`` for each real ``MERGE gza-NNNN -> main`` event.
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
                continue
            m = _MERGE_RE.search(line)
            if m and "[dry-run]" not in line:
                raw.append((full_dt, hms, "merge", m.group(1)))

    if not raw:
        return [], []

    # Assign real datetimes. If any line carried a full timestamp we trust those and
    # forward-fill HMS-only lines from the last known date; otherwise infer via wraps.
    have_full = any(r[0] is not None for r in raw)
    times = _assign_datetimes(raw, base_date, have_full)

    # Second pass: build Points from WAKE rows, carrying attention forward.
    points = []
    merges = []  # (datetime, task_id)
    attention = None
    for (row, when) in zip(raw, times):
        kind = row[2]
        if kind == "attn":
            attention = row[3]
            continue
        if kind == "merge":
            merges.append((when, row[3]))
            continue
        _, _, _, running, pending, blocked = row
        points.append(Point(when, running, pending, blocked, attention))
    return points, merges


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

# Row-unit word per resolution, for table/plot/summary captions.
_UNIT = {"raw": "cycles", "hour": "hours", "day": "days"}


def make_figure():
    """Create a reusable (fig, ax) with the Agg backend. Import matplotlib once."""
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    return plt.subplots(figsize=(14, 7))


def _draw_merges(ax, merges):
    """Mark merges: exact-time dots on the baseline + quadrant boxes of task ids.

    Merges are bucketed by the hour (one box per hour listing that hour's task
    ids), and the boxes are stacked in a reserved band of "fake negative" space
    **below** the baseline. Because every data series is non-negative, hanging the
    boxes below zero keeps them clear of the lines and makes the leaders short.
    Boxes are lane-packed (each takes the lowest lane whose previous box has ended)
    to avoid horizontal overlap, then the lanes are distributed evenly inside a band
    whose depth is **hard-capped** at roughly twice the data height. Busy windows
    pack the lanes tighter rather than letting the band balloon downward.
    """
    if not merges:
        return
    import matplotlib.dates as mdates

    xs = [when for when, _ in merges]
    ax.scatter(xs, [0] * len(xs), marker="o", s=16, color="tab:purple",
               zorder=6, label=f"merge ({len(merges)})")

    # Bucket by the hour: one box per hour listing that hour's merges.
    groups = {}  # hour-start -> [task_id, ...] in time order
    for when, tid in merges:
        start = when.replace(minute=0, second=0, microsecond=0)
        groups.setdefault(start, []).append(tid)
    ordered = sorted(groups.items())

    # Pixel geometry for width/height estimates (approximate; conservative is fine).
    fig = ax.figure
    fontsize = 9
    x0, x1 = ax.get_xlim()
    dmin, dmax = ax.get_ylim()
    pos = ax.get_position()
    px_per_datex = (pos.width * fig.get_figwidth() * fig.dpi) / (x1 - x0)
    axheight_px = pos.height * fig.get_figheight() * fig.dpi
    char_px = fontsize * 0.62 * fig.dpi / 72.0
    line_px = fontsize * 1.5 * fig.dpi / 72.0

    # Lane-pack in display-x: first lane whose last box ends before this one starts.
    lane_right = []          # last right-edge (px) per lane, index == lane
    placed = []              # (center_dt, ids, lane)
    for start, ids in ordered:
        center = start + timedelta(minutes=30)
        cx = (mdates.date2num(center) - x0) * px_per_datex
        half = (max(len(t) for t in ids) * char_px + 12) / 2
        left, right = cx - half, cx + half
        lane = next((i for i, r in enumerate(lane_right) if left >= r + 6), None)
        if lane is None:
            lane = len(lane_right)
            lane_right.append(right)
        else:
            lane_right[lane] = right
        placed.append((center, ids, lane))

    # Fit the lanes into a "fake negative" band whose depth is hard-capped at ~2x
    # the data height. Lanes are spread evenly across the band, so more lanes pack
    # tighter instead of extending the band downward without bound.
    n = len(lane_right)
    span = max(dmax, 1.0)
    gap = span * 0.05                    # small gap below the baseline dots
    cap = max(2.0 * span, 12.0)          # hard floor on how deep the band may go
    band_top = -gap
    band_bottom = -cap
    step_y = (band_top - band_bottom) / n
    ax.set_ylim(band_bottom - gap, dmax)

    for center, ids, lane in placed:
        y = band_top - (lane + 0.5) * step_y
        ax.plot([center, center], [0, y], color="0.8", linewidth=0.6, alpha=0.5, zorder=0)
        ax.annotate(
            "\n".join(ids), (center, y), ha="center", va="center",
            fontsize=fontsize, color="tab:purple", zorder=7,
            bbox=dict(boxstyle="round", fc="white", ec="tab:purple", alpha=0.9),
        )


def render_png(points, out_path, log_path, fig_ax=None, resolution="raw", agg_label="",
               merges=None):
    """Render the 4-series plot to ``out_path``.

    Pass ``fig_ax`` (from :func:`make_figure`) to reuse a single figure across
    ticks in ``--watch`` mode; otherwise a throwaway figure is created and closed.
    ``resolution``/``agg_label`` only affect the axis formatter and title text.
    ``merges`` (list of ``(datetime, task_id)``) is drawn as labeled dots.
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

    _draw_merges(ax, merges)

    unit = _UNIT.get(resolution, "cycles")
    generated = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    ax.set_ylabel("task count")
    ax.set_xlabel("time")
    ax.set_title(
        f"gza watch queue depth — {log_path}\n"
        f"{len(points)} {unit}{agg_label} · generated {generated}"
    )
    ax.legend(loc="upper left")
    ax.grid(True, alpha=0.3)
    date_fmt = "%m-%d" if resolution == "day" else "%m-%d %H:%M"
    ax.xaxis.set_major_formatter(mdates.DateFormatter(date_fmt))
    fig.autofmt_xdate()
    fig.tight_layout()
    Path(out_path).parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_path, dpi=120)
    if own:
        import matplotlib.pyplot as plt

        plt.close(fig)


def _fmt(v):
    return "" if v is None else str(v)


def print_table(points, max_rows, tail=False, unit="cycles"):
    """Print an aligned text table.

    Default: evenly sample at most ``max_rows`` rows across the full range.
    ``tail=True`` (watch mode): show the most recent ``max_rows`` rows instead.
    ``unit`` is the row-noun (cycles / hours / days) shown in the footer.
    """
    if tail:
        sampled = points[-max_rows:] if max_rows and max_rows > 0 else points
        note = f"(latest {len(sampled)} of {len(points)} {unit})"
    elif max_rows and max_rows > 0 and len(points) > max_rows:
        step = len(points) / max_rows
        idxs = sorted({int(i * step) for i in range(max_rows)})
        idxs = [i for i in idxs if i < len(points)]
        if idxs[-1] != len(points) - 1:
            idxs.append(len(points) - 1)  # always include the newest row
        sampled = [points[i] for i in idxs]
        note = f"(sampled {len(sampled)} of {len(points)} {unit}; --table-rows 0 for all)"
    else:
        sampled = points
        note = f"({len(points)} {unit})"

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


def print_merges(merges, cap=50):
    """List merge events (timestamp + task id) in the window; tail to ``cap``."""
    print()
    if not merges:
        print("merges    : none in window")
        return
    shown = merges[-cap:] if cap and len(merges) > cap else merges
    extra = f" (showing last {len(shown)})" if len(shown) < len(merges) else ""
    print(f"merges ({len(merges)}){extra}:")
    for when, tid in shown:
        print(f"  {when:%Y-%m-%d %H:%M:%S}  {tid}")


def print_summary(points, out_path, unit="cycles", agg_label=""):
    first, last = points[0].when, points[-1].when
    print()
    print(f"range     : {first:%Y-%m-%d %H:%M:%S} → {last:%Y-%m-%d %H:%M:%S}")
    print(f"{unit:<10}: {len(points)}{agg_label}")
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


def compute_cutoff(points, args):
    """Resolve the start-of-window datetime from args (precedence: all > start > hours).

    Default is a rolling window of ``args.hours`` before the newest cycle, so the
    graph shows recent activity rather than the whole log. Returns None for "no cut".
    """
    if getattr(args, "all", False):
        return None
    if args.start is not None:
        return datetime.combine(args.start, datetime.min.time())
    if points:
        return points[-1].when - timedelta(hours=args.hours)
    return None


def filter_since(points, cutoff):
    """Keep only cycles at/after ``cutoff`` (a datetime), or all if ``cutoff`` is None."""
    if cutoff is None:
        return points
    return [p for p in points if p.when >= cutoff]


def filter_since_events(events, cutoff):
    """Keep only ``(datetime, id)`` events at/after ``cutoff``."""
    if cutoff is None:
        return events
    return [(when, tid) for when, tid in events if when >= cutoff]


# --- rollup / aggregation --------------------------------------------------

_AGG_ALIASES = {"min": 0.0, "max": 100.0, "median": 50.0, "med": 50.0}


def parse_aggregate(s):
    """Normalise an aggregate spec to ('mean', None) or ('pct', q) with 0<=q<=100.

    Accepts: mean/avg, min (=p0), max (=p100), median (=p50), or any ``pN``
    percentile such as ``p50``, ``p90``, ``p99``, ``p99.9``.
    """
    s = s.strip().lower()
    if s in ("mean", "avg", "average"):
        return ("mean", None)
    if s in _AGG_ALIASES:
        return ("pct", _AGG_ALIASES[s])
    if s.startswith("p"):
        try:
            q = float(s[1:])
        except ValueError:
            raise argparse.ArgumentTypeError(f"bad percentile: {s!r}")
        if not 0.0 <= q <= 100.0:
            raise argparse.ArgumentTypeError(f"percentile out of range 0..100: {s!r}")
        return ("pct", q)
    raise argparse.ArgumentTypeError(
        f"unknown aggregate {s!r} (use mean, min, max, median, or pN e.g. p90)"
    )


def _aggregate(vals, agg):
    """Collapse a bucket's values (Nones dropped) via ``agg`` = parse_aggregate()."""
    vals = [v for v in vals if v is not None]
    if not vals:
        return None
    kind, q = agg
    if kind == "mean":
        m = sum(vals) / len(vals)
        return int(m) if m == int(m) else round(m, 1)
    s = sorted(vals)  # nearest-rank percentile keeps integer task counts
    idx = int(round(q / 100.0 * (len(s) - 1)))
    return s[idx]


def _bucket_key(dt, resolution):
    if resolution == "hour":
        return dt.replace(minute=0, second=0, microsecond=0)
    return dt.replace(hour=0, minute=0, second=0, microsecond=0)  # day


def rollup(points, resolution, agg):
    """Bucket ``points`` to hour/day and aggregate each series within a bucket."""
    if resolution == "raw":
        return points
    buckets = {}
    order = []
    for p in points:
        key = _bucket_key(p.when, resolution)
        if key not in buckets:
            buckets[key] = {attr: [] for attr, _ in _SERIES}
            order.append(key)
        for attr, _ in _SERIES:
            buckets[key][attr].append(getattr(p, attr))
    out = []
    for key in order:
        col = buckets[key]
        out.append(Point(
            key,
            _aggregate(col["running"], agg),
            _aggregate(col["pending"], agg),
            _aggregate(col["blocked"], agg),
            _aggregate(col["attention"], agg),
        ))
    return out


def agg_display(agg):
    """Human name for a parsed aggregate: min/max/median/mean/pN."""
    kind, q = agg
    if kind == "mean":
        return "mean"
    return {0.0: "min", 100.0: "max", 50.0: "median"}.get(q, f"p{q:g}")


def rollup_config(args):
    """Resolve (agg, unit, agg_label) from args; default aggregate is max."""
    agg = args.aggregate or ("pct", 100.0)  # default: max (peak per bucket)
    unit = _UNIT[args.resolution]
    if args.resolution == "raw":
        return agg, unit, ""
    return agg, unit, f", {agg_display(agg)}/{args.resolution}"


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--log", type=Path, default=None,
                    help="path to a .gza/watch.log (default: auto-discover)")
    ap.add_argument("--out", type=Path, default=Path("tmp/watch_status.png"),
                    help="output PNG path (default: tmp/watch_status.png under the cwd)")
    ap.add_argument("--date", type=parse_date, default=None,
                    help="base date to assume for the newest line (default: today)")
    ap.add_argument("--hours", type=float, default=24.0, metavar="H",
                    help="rolling window: show only the last H hours of activity "
                         "before the newest cycle (default 24)")
    ap.add_argument("--start", type=parse_date, default=None, metavar="YYYY-MM-DD",
                    help="absolute start date (overrides --hours)")
    ap.add_argument("--all", action="store_true",
                    help="show the full log history (disable the default 24h window)")
    ap.add_argument("--resolution", "--rollup", choices=("raw", "hour", "day"), default="raw",
                    help="bucket cycles by hour or day before plotting (default: raw)")
    ap.add_argument("--aggregate", "--agg", type=parse_aggregate, default=None, metavar="AGG",
                    help="how to collapse each bucket: mean, min, max, median, or pN "
                         "(e.g. p90, p99); default max when --resolution is set")
    ap.add_argument("--table-rows", type=int, default=40,
                    help="max rows in the printed table, evenly sampled (0 = all)")
    ap.add_argument("--no-png", action="store_true", help="skip PNG, table only")
    ap.add_argument("--merges", action=argparse.BooleanOptionalAction, default=True,
                    help="mark each merge on the graph as a task-id-labeled dot "
                         "(default: on; use --no-merges to hide, --start to declutter)")
    ap.add_argument("--watch", nargs="?", type=int, const=60, default=None, metavar="N",
                    help="live mode: refresh table + PNG every N seconds (default 60). "
                         "Table shows the most recent --table-rows cycles.")
    args = ap.parse_args(argv)

    log_path = args.log or default_log()
    if not log_path or not Path(log_path).is_file():
        ap.error(f"watch.log not found (looked for {log_path or '.gza/watch.log'}); pass --log")
    if args.aggregate is not None and args.resolution == "raw":
        ap.error("--aggregate requires --resolution hour|day")

    if args.watch is not None:
        return _watch_loop(args, log_path)

    agg, unit, agg_label = rollup_config(args)
    base_date = args.date or date_cls.today()
    points, merges = parse_log(log_path, base_date)
    if not points:
        print(f"no WAKE cycles parsed from {log_path}", file=sys.stderr)
        return 1
    cutoff = compute_cutoff(points, args)
    points = filter_since(points, cutoff)
    if not points:
        print("no cycles in the selected window", file=sys.stderr)
        return 1
    points = rollup(points, args.resolution, agg)
    merges = filter_since_events(merges, cutoff) if args.merges else None

    print_table(points, args.table_rows, unit=unit)
    if merges is not None:
        print_merges(merges)
    if not args.no_png:
        render_png(points, args.out, log_path, resolution=args.resolution,
                   agg_label=agg_label, merges=merges)
    print_summary(points, "(skipped)" if args.no_png else args.out,
                  unit=unit, agg_label=agg_label)
    return 0


_CLEAR = "\033[2J\033[3J\033[H"  # clear screen + scrollback, cursor home


def _watch_loop(args, log_path):
    """Refresh the table (and PNG) every ``args.watch`` seconds until Ctrl-C."""
    interval = args.watch
    agg, unit, agg_label = rollup_config(args)
    fig_ax = None if args.no_png else make_figure()
    try:
        while True:
            try:
                points, merges = parse_log(log_path, args.date or date_cls.today())
            except OSError as exc:  # log momentarily unreadable — keep looping
                print(f"read error: {exc}; retrying in {interval}s...", file=sys.stderr)
                time.sleep(interval)
                continue
            cutoff = compute_cutoff(points, args)
            points = filter_since(points, cutoff)
            points = rollup(points, args.resolution, agg)
            merges = filter_since_events(merges, cutoff) if args.merges else None

            print(_CLEAR, end="")
            if not points:
                print(f"no WAKE cycles yet in {log_path} for the selected window")
            else:
                print_current(points)
                print()
                print_table(points, args.table_rows, tail=True, unit=unit)
                if merges is not None:
                    print_merges(merges)
                if not args.no_png:
                    render_png(points, args.out, log_path, fig_ax=fig_ax,
                               resolution=args.resolution, agg_label=agg_label,
                               merges=merges)
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
