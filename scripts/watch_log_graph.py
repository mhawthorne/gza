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
    uv run scripts/watch_log_graph.py --start "2026-07-01 09:00" --end "2026-07-01 17:00"  # hour range
    uv run scripts/watch_log_graph.py --all --resolution day --aggregate p90  # daily p90
    uv run scripts/watch_log_graph.py --resolution hour --agg max        # hourly peaks
    uv run scripts/watch_log_graph.py --no-merges          # hide merge dots (on by default)
    uv run scripts/watch_log_graph.py --start 2026-06-01 --merge-bucket day   # daily merge boxes
    uv run scripts/watch_log_graph.py --merge-labels count --merge-band 0.2    # compact merge band
    uv run scripts/watch_log_graph.py --watch 60      # live: refresh table + PNG every 60s

Log line shapes it understands::

    HH:MM:SS WAKE      checking... (4 running, pending=2 runnable, blocked=14, 0 slots)
    HH:MM:SS WAKE      checking... (5 running, 0 slots)          # older, running only
    HH:MM:SS INFO      12 tasks still need attention (unchanged)
    HH:MM:SS INFO      Needs attention (13 tasks):
"""

from __future__ import annotations

import argparse
import math
import re
import sys
import time
from datetime import date as date_cls
from datetime import datetime, timedelta
from pathlib import Path

# Make sibling scripts/ modules (e.g. palette) importable whether this file is run
# directly or loaded by path (tests load it via importlib without scripts/ on sys.path).
sys.path.insert(0, str(Path(__file__).resolve().parent))

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

    # Second pass: build Points from WAKE rows, resolving each cycle's attention.
    #
    # While the count is > 0 the watch emits exactly one attention line per cycle
    # (a "Needs attention (N)" header or an "N still need attention (unchanged)"
    # line); when the count is zero it emits nothing. So we can't just carry the
    # last value forward — that would pin the curve at the last non-zero count
    # forever. Instead, for each WAKE we look ahead within its own cycle (up to
    # the next WAKE) for an attention line: found -> that count; absent -> zero.
    # We only infer zero once the log has started reporting attention at all, so
    # older logs that never emit attention lines keep attention=None as before.
    first_attn_idx = next((i for i, r in enumerate(raw) if r[2] == "attn"), None)
    points = []
    merges = []  # (datetime, task_id)
    attention = None
    for i, (row, when) in enumerate(zip(raw, times)):
        kind = row[2]
        if kind == "attn":
            continue
        if kind == "merge":
            merges.append((when, row[3]))
            continue
        # WAKE: find this cycle's attention line, if any, before the next WAKE.
        cycle_attn = None
        for j in range(i + 1, len(raw)):
            nxt = raw[j][2]
            if nxt == "wake":
                break
            if nxt == "attn":
                cycle_attn = raw[j][3]
                break
        if cycle_attn is not None:
            attention = cycle_attn
        elif first_attn_idx is not None and i > first_attn_idx:
            attention = 0  # attention reporting is active but this cycle is silent
        # else: before any attention reporting -> leave as None (unknown)
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

# Semantic line color per series (name of a color constant in scripts/palette.py).
_SERIES_COLORS = {
    "running": "GREEN",
    "pending": "BLUE",
    "blocked": "ORANGE",
    "attention": "RED",
}

# Row-unit word per resolution, for table/plot/summary captions.
_UNIT = {"raw": "cycles", "hour": "hours", "day": "days"}

# The merge id boxes hang in negative space below the baseline. Cap how much of the
# vertical axis that band may consume so the data lines always stay readable.
MERGE_BAND_DEFAULT = 0.25
MERGE_BAND_MAX = 0.5


def make_figure():
    """Create a reusable (fig, ax) with the Agg backend, themed dark/neon."""
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import palette

    palette.apply()  # dark background + neon color cycle, before the figure is created
    fig, ax = plt.subplots(figsize=(14, 7))
    palette.style(fig, ax)
    return fig, ax


def _spread(centers, halves, lo, hi, gap):
    """Nudge box centers (px) apart so none overlap, staying within ``[lo, hi]``.

    A forward pass pushes each box right until it clears its left neighbour; if the
    row then overruns ``hi`` a backward pass compresses it leftward, and a final
    forward pass re-clamps the left edge. Boxes end up as close to their desired x
    as the no-overlap constraint allows.
    """
    pos = list(centers)
    n = len(pos)
    for i in range(1, n):
        pos[i] = max(pos[i], pos[i - 1] + halves[i - 1] + gap + halves[i])
    if pos and pos[-1] + halves[-1] > hi:
        pos[-1] = hi - halves[-1]
        for i in range(n - 2, -1, -1):
            pos[i] = min(pos[i], pos[i + 1] - halves[i + 1] - gap - halves[i])
    if pos and pos[0] - halves[0] < lo:
        pos[0] = lo + halves[0]
        for i in range(1, n):
            pos[i] = max(pos[i], pos[i - 1] + halves[i - 1] + gap + halves[i])
    return pos


def _draw_merges(ax, merges, bucket="hour", labels="auto", band=MERGE_BAND_DEFAULT):
    """Mark merges: exact-time dots on the baseline + task-id/count boxes below it.

    Merges are bucketed by ``bucket`` (``"hour"`` or ``"day"``; ``"off"`` draws dots
    only) — one box per bucket — and drawn in a reserved band of "fake negative" space
    **below** the baseline. Because every data series is non-negative, hanging the
    boxes below zero keeps them clear of the lines.

    The band is **hard-capped** at ``band`` (a fraction of the axis height, default
    0.25) so the data lines always keep the rest of the vertical space — this is the
    whole point: over long windows the boxes must never crush the lines into a sliver.

    ``labels`` controls box content: ``"ids"`` lists the bucket's task ids (one per
    line), ``"count"`` shows just how many merged, ``"off"`` draws no boxes, and
    ``"auto"`` uses ids when they fit the capped band and falls back to counts when
    they don't. If even the chosen labels can't fit the capped band we clamp to the
    rows that fit and print a note rather than stealing space from the lines.

    A box's horizontal position is **decoupled from its timestamp**: boxes are spread
    evenly along the axis (in time order) and a thin leader connects each to its real
    merge time on the baseline — keeping them legible when merges cluster in a short
    real-time span instead of stacking into an unreadable diagonal cascade.
    """
    if not merges:
        return
    import matplotlib.dates as mdates
    import palette

    xs = [when for when, _ in merges]
    ax.scatter(xs, [0] * len(xs), marker="o", s=16, color=palette.PINK,
               zorder=6, label=f"merge ({len(merges)})")

    if bucket == "off" or labels == "off":
        return

    # Bucket by the chosen window: one box per bucket listing its merges (time order).
    day_bucket = bucket == "day"
    groups = {}
    for when, tid in merges:
        if day_bucket:
            start = when.replace(hour=0, minute=0, second=0, microsecond=0)
        else:
            start = when.replace(minute=0, second=0, microsecond=0)
        groups.setdefault(start, []).append(tid)
    ordered = sorted(groups.items())

    # Pixel geometry for width/height estimates (approximate; conservative is fine).
    fig = ax.figure
    fontsize = 9
    xlo, xhi = ax.get_xlim()             # matplotlib date numbers
    top = max(ax.get_ylim()[1], 1.0)
    pos = ax.get_position()
    usable_px = pos.width * fig.get_figwidth() * fig.dpi
    axheight_px = pos.height * fig.get_figheight() * fig.dpi
    px_per_dx = usable_px / (xhi - xlo)
    char_px = fontsize * 0.62 * fig.dpi / 72.0
    line_px = fontsize * 1.5 * fig.dpi / 72.0
    gap_px = 8.0
    band = min(max(band, 0.05), MERGE_BAND_MAX)
    band_budget_px = band * axheight_px
    box_offset = timedelta(hours=12) if day_bucket else timedelta(minutes=30)

    def build(mode):
        """Boxes for a label mode: (center_dt, desired_px, half_px, text, nlines)."""
        out = []
        for start, ids in ordered:
            center = start + box_offset
            desired = (mdates.date2num(center) - xlo) * px_per_dx
            if mode == "count":
                text, width_chars, nlines = str(len(ids)), len(str(len(ids))), 1
            else:
                text, width_chars, nlines = "\n".join(ids), max(len(t) for t in ids), len(ids)
            half = (width_chars * char_px + 14) / 2
            out.append((center, desired, half, text, nlines))
        return out

    def layout(bxs):
        """(rows_needed for horizontal fit, uniform row height px) for a box set."""
        total_px = sum(2 * b[2] for b in bxs) + gap_px * (len(bxs) - 1)
        rows_needed = max(1, math.ceil(total_px / usable_px)) if usable_px else 1
        row_h = max((b[4] for b in bxs), default=1) * line_px + 12
        return rows_needed, row_h

    def vpx(rows_needed, row_h):
        """Vertical pixels a layout wants: rows_needed rows, each row_h tall."""
        return rows_needed * (row_h + gap_px)

    # Resolve auto label mode: prefer ids only if the id boxes genuinely fit inside the
    # capped band (a single day's box can be dozens of ids tall — taller than the whole
    # band — in which case we must fall back to counts, not silently overflow).
    mode = labels
    if mode == "auto":
        rn, rh = layout(build("ids"))
        mode = "ids" if band_budget_px and vpx(rn, rh) <= band_budget_px else "count"

    boxes = build(mode)
    rows_needed, row_h = layout(boxes)
    # `rows_needed` is how many rows the boxes need to sit side-by-side without overlap;
    # `fit_rows` is how many rows the capped band can hold. When the boxes need more rows
    # than fit (a wide window with more merges than 25% of the axis can legibly hold), we
    # keep only as many boxes as those rows can show — evenly sampled across the window —
    # and note the drop. This bounds the layout so boxes never overrun the axis. Auto-mode
    # counts are narrow enough that this rarely triggers; it mostly guards forced ids.
    fit_rows = int(band_budget_px // (row_h + gap_px)) if band_budget_px else 1
    fit_rows = max(1, fit_rows)
    if rows_needed > fit_rows and len(boxes) > 1:
        keep = max(1, int(len(boxes) * fit_rows / rows_needed))
        step = len(boxes) / keep
        idxs = sorted({int(i * step) for i in range(keep)})
        idxs = [i for i in idxs if i < len(boxes)]
        dropped = len(boxes) - len(idxs)
        boxes = [boxes[i] for i in idxs]
        rows_needed, row_h = layout(boxes)
        print(f"note: showing {len(boxes)} of {len(boxes) + dropped} merge boxes — the "
              f"{band:.0%} band can't hold them all; use --merge-labels count, "
              f"--merge-bucket day, or a shorter window (--start/--hours)", file=sys.stderr)
    rows = max(1, min(rows_needed, fit_rows))

    # Assign boxes to rows round-robin (in time order) so each row stays sparse,
    # then spread each row so its boxes don't overlap.
    row_boxes = [[] for _ in range(rows)]
    for i, b in enumerate(boxes):
        row_boxes[i % rows].append(b)
    row_x = {}               # id(box) -> adjusted center px
    for rb in row_boxes:
        adj = _spread([b[1] for b in rb], [b[2] for b in rb], 0.0, usable_px, gap_px)
        for b, x in zip(rb, adj):
            row_x[id(b)] = x

    # Vertical: stack the rows below zero, each tall enough for its tallest box. Solve
    # the new axis bottom so the band occupies at most ``band`` of the axis (no g gap
    # in data space keeps the negative fraction exactly k <= band).
    row_h_px = [max((b[4] for b in rb), default=1) * line_px + 12 for rb in row_boxes]
    band_px = sum(row_h_px) + gap_px * (rows + 1)
    k = min(band_px / axheight_px, band) if axheight_px else band
    newmin = -(k * top) / (1 - k)
    ax.set_ylim(newmin, top)
    # Map the band's pixel height onto the reserved data depth so every row lands inside
    # [newmin, 0] even when the requested boxes are taller than the cap allows (then they
    # compress). Scaling by band_px (not the full axis height) is what keeps them on-screen.
    depth = -newmin
    dy_per_px = depth / band_px if band_px else 0.0

    # Row center y (data units), walking down from just below the baseline.
    cursor = gap_px
    row_center_y = []
    for h in row_h_px:
        row_center_y.append(-((cursor + h / 2) * dy_per_px))
        cursor += h + gap_px

    for r, rb in enumerate(row_boxes):
        y = row_center_y[r]
        for b in rb:
            center, _, _, text, _ = b
            # Work in matplotlib date numbers, not datetimes: num2date returns a
            # timezone-aware value, and feeding that back to plot/annotate makes matplotlib
            # warn about "no timezone representation for np.datetime64". Numbers sidestep it.
            center_num = mdates.date2num(center)
            bx_num = xlo + row_x[id(b)] / px_per_dx
            ax.plot([center_num, bx_num], [0, y], color=palette.MUTED, linewidth=0.6,
                    alpha=0.4, zorder=0)
            ax.annotate(
                text, (bx_num, y), ha="center", va="center",
                fontsize=fontsize, color=palette.PINK, zorder=7,
                bbox=dict(boxstyle="round", fc=palette.PANEL, ec=palette.PINK, alpha=0.92),
            )

    # Pin x to the data range: the boxes are placed in axis pixels and their annotations
    # must never autoscale the x-axis past where the lines actually end.
    ax.set_xlim(xlo, xhi)


def render_png(points, out_path, log_path, fig_ax=None, resolution="raw", agg_label="",
               merges=None, merge_bucket="auto", merge_labels="auto",
               merge_band=MERGE_BAND_DEFAULT, xmin=None, xmax=None, markers=True):
    """Render the 4-series plot to ``out_path``.

    Pass ``fig_ax`` (from :func:`make_figure`) to reuse a single figure across
    ticks in ``--watch`` mode; otherwise a throwaway figure is created and closed.
    ``resolution``/``agg_label`` only affect the axis formatter and title text.
    ``merges`` (list of ``(datetime, task_id)``) is drawn as labeled dots plus a
    capped band of id/count boxes; ``merge_bucket``/``merge_labels``/``merge_band``
    control that band (see :func:`_draw_merges`). ``merge_bucket="auto"`` resolves to
    hourly boxes for short windows and daily boxes for windows spanning over ~2 days.
    ``xmin``/``xmax`` (datetimes) pin the x-axis to an explicit window so the plot spans
    exactly the requested range even when the data stops short of it (a gap in cycles
    before ``--end`` would otherwise autoscale the axis in and look like it "ends early").
    """
    import matplotlib.dates as mdates
    import palette

    if fig_ax is None:
        fig, ax = make_figure()
        own = True
    else:
        fig, ax = fig_ax
        ax.clear()  # resets the axes facecolor — repaint it dark
        palette.style(fig, ax)
        own = False

    # Dot each actual cycle so sparse stretches (e.g. an overnight gap with only a couple
    # of cycles) read as discrete points instead of a line that looks like it "cuts off".
    # Thin the markers on dense windows so they don't smear into the line, but show every
    # point once a window is sparse — which is exactly when the markers matter most.
    marker = "o" if markers else None
    markevery = max(1, len(points) // 200) if markers else None

    xs = [p.when for p in points]
    for attr, label in _SERIES:
        ys = [float("nan") if getattr(p, attr) is None else getattr(p, attr) for p in points]
        ax.plot(xs, ys, label=label, linewidth=1.2,
                color=getattr(palette, _SERIES_COLORS[attr]),
                marker=marker, markersize=3.0, markevery=markevery)

    # Pin the x-axis to any explicitly requested bound before drawing merges (which read
    # and re-pin the limits), so the plot honors --start/--end exactly. An unspecified
    # side keeps its autoscaled edge (right edge tracks "latest" when there's no --end).
    if xmin is not None or xmax is not None:
        cur_lo, cur_hi = ax.get_xlim()
        left = mdates.date2num(xmin) if xmin is not None else cur_lo
        right = mdates.date2num(xmax) if xmax is not None else cur_hi
        ax.set_xlim(left, right)

    # Resolve an "auto" merge bucket from the plotted span: daily boxes for windows
    # spanning more than ~2 days (keeps the box count small), hourly for short ones.
    bucket = merge_bucket
    if bucket == "auto":
        span = (points[-1].when - points[0].when) if points else timedelta(0)
        bucket = "day" if span > timedelta(hours=48) else "hour"
    _draw_merges(ax, merges, bucket=bucket, labels=merge_labels, band=merge_band)

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


# Accepted --start/--end shapes, tried in order. A date with no time yields midnight.
_DATE_ONLY_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_WHEN_FORMATS = (
    "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%d %H:%M", "%Y-%m-%dT%H:%M",
    "%Y-%m-%d %H", "%Y-%m-%dT%H",
    "%Y-%m-%d",
)


def parse_when(s):
    """Parse a date or date+time into a datetime (date-only -> 00:00:00)."""
    s = s.strip()
    for fmt in _WHEN_FORMATS:
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            continue
    raise argparse.ArgumentTypeError(
        f"bad date/time {s!r} (use YYYY-MM-DD or 'YYYY-MM-DD HH:MM')")


def parse_end(s):
    """Like :func:`parse_when`, but a date-only end covers that whole day (inclusive)."""
    dt = parse_when(s)
    if _DATE_ONLY_RE.match(s.strip()):
        dt = dt.replace(hour=23, minute=59, second=59)
    return dt


def compute_window(points, args):
    """Resolve the ``(lo, hi)`` datetime bounds of the plotted window (None = open).

    ``lo`` precedence: ``--all`` (None) > ``--start`` > rolling ``--hours`` before the
    anchor. ``hi`` is ``--end`` if given, else None. When the rolling ``--hours`` window
    is used it is measured back from ``--end`` (if set) rather than the newest cycle.
    """
    hi = getattr(args, "end", None)
    if getattr(args, "all", False):
        return None, hi
    if args.start is not None:
        return args.start, hi
    if points:
        anchor = hi if hi is not None else points[-1].when
        return anchor - timedelta(hours=args.hours), hi
    return None, hi


def filter_window(points, lo, hi):
    """Keep cycles within ``[lo, hi]`` (either bound None = open on that side)."""
    return [p for p in points
            if (lo is None or p.when >= lo) and (hi is None or p.when <= hi)]


def filter_window_events(events, lo, hi):
    """Keep ``(datetime, id)`` events within ``[lo, hi]`` (None bound = open)."""
    return [(when, tid) for when, tid in events
            if (lo is None or when >= lo) and (hi is None or when <= hi)]


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
                         "before the newest cycle, or before --end if set (default 24)")
    ap.add_argument("--start", type=parse_when, default=None, metavar="WHEN",
                    help="absolute start, date or date+time e.g. 2026-07-01 or "
                         "'2026-07-01 14:00' (overrides --hours)")
    ap.add_argument("--end", type=parse_end, default=None, metavar="WHEN",
                    help="absolute end, date or date+time e.g. '2026-07-05 18:00'; a "
                         "date-only end covers that whole day")
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
    ap.add_argument("--markers", action=argparse.BooleanOptionalAction, default=True,
                    help="dot each actual cycle on the series lines so sparse stretches "
                         "(e.g. an overnight gap) read as discrete points, not a line that "
                         "looks cut off (default: on; use --no-markers for clean lines)")
    ap.add_argument("--merges", action=argparse.BooleanOptionalAction, default=True,
                    help="mark each merge on the graph as a task-id-labeled dot "
                         "(default: on; use --no-merges to hide, --start to declutter)")
    ap.add_argument("--merge-bucket", choices=("auto", "hour", "day", "off"), default="auto",
                    help="aggregate merge id boxes by hour or day, one box per bucket "
                         "(default auto: hourly for short windows, daily for long ones); "
                         "off = baseline dots only")
    ap.add_argument("--merge-labels", choices=("auto", "ids", "count", "off"), default="auto",
                    help="merge box content: task ids, a merge count, or off; auto shows ids "
                         "when they fit the capped band, else counts (default auto)")
    ap.add_argument("--merge-band", type=float, default=MERGE_BAND_DEFAULT, metavar="FRAC",
                    help="max share of vertical space the merge id band may use, so the data "
                         f"lines stay readable (default {MERGE_BAND_DEFAULT}; clamped to "
                         f"0.05–{MERGE_BAND_MAX})")
    ap.add_argument("--watch", nargs="?", type=int, const=60, default=None, metavar="N",
                    help="live mode: refresh table + PNG every N seconds (default 60). "
                         "Table shows the most recent --table-rows cycles.")
    args = ap.parse_args(argv)

    log_path = args.log or default_log()
    if not log_path or not Path(log_path).is_file():
        ap.error(f"watch.log not found (looked for {log_path or '.gza/watch.log'}); pass --log")
    if args.aggregate is not None and args.resolution == "raw":
        ap.error("--aggregate requires --resolution hour|day")
    if args.start is not None and args.end is not None and args.start > args.end:
        ap.error(f"--start ({args.start}) is after --end ({args.end})")

    if args.watch is not None:
        return _watch_loop(args, log_path)

    agg, unit, agg_label = rollup_config(args)
    base_date = args.date or date_cls.today()
    points, merges = parse_log(log_path, base_date)
    if not points:
        print(f"no WAKE cycles parsed from {log_path}", file=sys.stderr)
        return 1
    lo, hi = compute_window(points, args)
    points = filter_window(points, lo, hi)
    if not points:
        print("no cycles in the selected window", file=sys.stderr)
        return 1
    points = rollup(points, args.resolution, agg)
    merges = filter_window_events(merges, lo, hi) if args.merges else None

    print_table(points, args.table_rows, unit=unit)
    if merges is not None:
        print_merges(merges)
    if not args.no_png:
        render_png(points, args.out, log_path, resolution=args.resolution,
                   agg_label=agg_label, merges=merges, merge_bucket=args.merge_bucket,
                   merge_labels=args.merge_labels, merge_band=args.merge_band,
                   xmin=args.start, xmax=args.end, markers=args.markers)
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
            latest = points[-1].when if points else None
            lo, hi = compute_window(points, args)
            points = filter_window(points, lo, hi)
            points = rollup(points, args.resolution, agg)
            merges = filter_window_events(merges, lo, hi) if args.merges else None

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
                               merges=merges, merge_bucket=args.merge_bucket,
                               merge_labels=args.merge_labels, merge_band=args.merge_band,
                               xmin=args.start, xmax=args.end, markers=args.markers)
                    print(f"\npng refreshed: {args.out}")
            # A bounded --end window is static once the log has advanced past it, so there
            # is nothing new to show — render once and stop rather than re-drawing forever.
            # An open-ended window (no --end) always tracks "latest" and keeps refreshing.
            if hi is not None and latest is not None and latest >= hi:
                print(f"\nwindow ends {hi:%Y-%m-%d %H:%M:%S} and the log has passed it — "
                      f"view is complete, not refreshing.", flush=True)
                break
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
