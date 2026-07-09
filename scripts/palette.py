#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = ["matplotlib"]
# ///
"""Shared dark / neon plotting palette for ``scripts/`` (colors from supreme-hacks/basquiat).

Neon-ish series colors on a near-black background. Any sibling script under ``scripts/``
can use it — add the scripts dir to ``sys.path`` (a one-liner most of these scripts already
do so they can be run directly) and::

    import palette

    fig, ax = plt.subplots()
    palette.apply()             # set the dark+neon rcParams — call once before plotting
    palette.style(fig, ax)      # paint this fig/ax dark (also re-call after ax.clear())
    ax.plot(xs, ys)             # lines pick neon colors from the cycle automatically
    ax.scatter(xs, ys, color=palette.PURPLE)

The matplotlib import is deferred into :func:`apply`/:func:`style` so importing this module
is cheap and dependency-free (only the color math runs at import time).

Preview the palette::

    uv run scripts/palette.py                    # writes tmp/palette_preview.png
    uv run scripts/palette.py --out /tmp/p.png
"""

from __future__ import annotations

from colorsys import hls_to_rgb

# --- surfaces / text -------------------------------------------------------
BACKGROUND = "#111111"   # figure + axes background (near-black)
PANEL = "#1b1b1b"        # slightly lifted fill for boxes / legend
FOREGROUND = "#f3f3f3"   # primary text
MUTED = "#bbbbbb"        # axis labels, ticks, subtext
GRID = "#333333"         # faint gridlines

# --- neon-ish base palette (copied from basquiat BASE_COLORS) --------------
BASE_COLORS = {
    "red": "#ff6b6b",
    "orange": "#ffb74d",
    "yellow": "#ffe066",
    "green": "#69f0ae",
    "cyan": "#4dd0e1",
    "blue": "#64b5f6",
    "purple": "#ce93d8",
}
RED = BASE_COLORS["red"]
ORANGE = BASE_COLORS["orange"]
YELLOW = BASE_COLORS["yellow"]
GREEN = BASE_COLORS["green"]
CYAN = BASE_COLORS["cyan"]
BLUE = BASE_COLORS["blue"]
PURPLE = BASE_COLORS["purple"]
# Extra bright-neon accent (not part of the base cycle) — e.g. merge markers/labels.
PINK = "#ffaaff"

_VALUES = list(BASE_COLORS.values())
_DISTINCT_STEP = 3  # stride the base colors so adjacent series stay well-separated
_SIMILAR_DEFAULT_HUE = 0.58

DISTINCT_MODE = "distinct"
SIMILAR_MODE = "similar"


def _rgb_to_hex(red: float, green: float, blue: float) -> str:
    return "#{:02x}{:02x}{:02x}".format(
        int(round(red * 255)), int(round(green * 255)), int(round(blue * 255))
    )


def _distinct(count: int) -> list[str]:
    ordered = [_VALUES[(i * _DISTINCT_STEP) % len(_VALUES)] for i in range(len(_VALUES))]
    return [ordered[i % len(ordered)] for i in range(count)]


def _similar(count: int, hue: float = _SIMILAR_DEFAULT_HUE) -> list[str]:
    if count == 1:
        return [_rgb_to_hex(*hls_to_rgb(hue, 0.62, 0.75))]
    min_l, max_l = 0.38, 0.78
    out = []
    for i in range(count):
        lightness = min_l + (max_l - min_l) * (i / (count - 1))
        saturation = 0.55 + 0.2 * (1 - abs(0.5 - (i / (count - 1))) * 2)
        out.append(_rgb_to_hex(*hls_to_rgb(hue, lightness, saturation)))
    return out


def get_colors(count: int, mode: str = DISTINCT_MODE) -> list[str]:
    """Return ``count`` deterministic neon colors.

    ``mode="distinct"`` gives well-separated hues (for unrelated series); ``"similar"``
    gives a cohesive lightness ramp within one hue family.
    """
    if count < 1:
        raise ValueError(f"count must be >= 1, got {count}")
    if mode == DISTINCT_MODE:
        return _distinct(count)
    if mode == SIMILAR_MODE:
        return _similar(count)
    raise ValueError(f"unsupported color mode: {mode!r}")


def apply(n_cycle: int = 7) -> None:
    """Set global dark/neon matplotlib rcParams. Call once before creating figures.

    Sets the default color cycle to the neon palette so ``ax.plot`` picks it up, and
    themes backgrounds, text, ticks, grid and legend for a dark canvas.
    """
    import matplotlib as mpl
    import matplotlib.pyplot as plt

    plt.style.use("dark_background")
    mpl.rcParams.update({
        "figure.facecolor": BACKGROUND,
        "axes.facecolor": BACKGROUND,
        "savefig.facecolor": BACKGROUND,
        "savefig.edgecolor": BACKGROUND,
        "axes.edgecolor": MUTED,
        "axes.labelcolor": FOREGROUND,
        "axes.titlecolor": FOREGROUND,
        "text.color": FOREGROUND,
        "xtick.color": MUTED,
        "ytick.color": MUTED,
        "grid.color": GRID,
        "grid.alpha": 0.35,
        "legend.facecolor": PANEL,
        "legend.edgecolor": MUTED,
        "legend.labelcolor": FOREGROUND,
        "axes.prop_cycle": mpl.cycler(color=get_colors(n_cycle)),
    })


def style(fig=None, ax=None) -> None:
    """Paint an existing ``fig``/``ax`` with the dark background.

    Handy after :meth:`Axes.clear`, which resets facecolors, and as a belt-and-suspenders
    for figures created before :func:`apply` ran.
    """
    if fig is not None:
        fig.patch.set_facecolor(BACKGROUND)
    if ax is not None:
        ax.set_facecolor(BACKGROUND)


def _preview(out_path: str) -> None:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from pathlib import Path

    apply()
    fig, ax = plt.subplots(figsize=(9, 5))
    style(fig, ax)
    names = list(BASE_COLORS)
    for i, (name, value) in enumerate(BASE_COLORS.items()):
        y = len(names) - 1 - i
        ax.barh(y, 1.0, color=value, edgecolor=MUTED, linewidth=0.5)
        ax.text(0.02, y, f"{name}  {value}", va="center", ha="left",
                color=BACKGROUND, fontsize=11, weight="bold")
    ax.set_xlim(0, 1)
    ax.set_ylim(-0.6, len(names) - 0.4)
    ax.set_yticks([])
    ax.set_xticks([])
    ax.set_title("scripts palette — neon on dark (from basquiat)")
    fig.tight_layout()
    Path(out_path).parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_path, dpi=120)
    print(f"wrote {out_path}")


if __name__ == "__main__":
    import argparse

    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--out", default="tmp/palette_preview.png", help="preview PNG path")
    _preview(ap.parse_args().out)
