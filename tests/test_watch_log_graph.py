"""Tests for scripts/watch_log_graph.py attention parsing."""

from __future__ import annotations

import importlib.util
from datetime import datetime
from pathlib import Path

_SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "watch_log_graph.py"


def _load_module():
    spec = importlib.util.spec_from_file_location("watch_log_graph", _SCRIPT)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def test_parse_log_infers_zero_attention_from_silent_cycles(tmp_path: Path) -> None:
    """A cycle with no attention line (once reporting has begun) means zero.

    The watch emits an attention line every cycle while the count is > 0 and
    nothing when it is zero, so the grapher must resolve silent cycles to 0
    rather than carrying the last non-zero value forward forever.
    """
    module = _load_module()
    log = tmp_path / "watch.log"
    log.write_text(
        "\n".join(
            [
                # Cycle 1: before any attention reporting -> unknown (None).
                "10:00:00 WAKE      checking... (0 running, 0 pending, 4 slots)",
                # Cycle 2: attention appears -> 2.
                "10:01:00 WAKE      checking... (0 running, 0 pending, 4 slots)",
                "10:01:05 INFO      Needs attention (2 tasks):",
                "                     gza-1 ...",
                "                     gza-2 ...",
                # Cycle 3: unchanged -> still 2.
                "10:02:00 WAKE      checking... (0 running, 0 pending, 4 slots)",
                "10:02:05 INFO      2 tasks still need attention (unchanged)",
                # Cycle 4: silent -> dropped to 0.
                "10:03:00 WAKE      checking... (0 running, 0 pending, 4 slots)",
                # Cycle 5: still silent -> stays 0.
                "10:04:00 WAKE      checking... (0 running, 0 pending, 4 slots)",
                "",
            ]
        ),
        encoding="utf-8",
    )

    points, _merges = module.parse_log(str(log), datetime(2026, 7, 8))

    assert [p.attention for p in points] == [None, 2, 2, 0, 0]
