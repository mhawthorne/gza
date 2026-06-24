"""Tests for transient recovery cooldown helpers."""

from pathlib import Path

from gza.config import Config
from gza.recovery_transients import compute_transient_recovery_backoff_seconds


def test_compute_transient_recovery_backoff_seconds_follows_bounded_schedule(tmp_path: Path) -> None:
    (tmp_path / "gza.yaml").write_text("project_name: demo\n", encoding="utf-8")
    config = Config.load(tmp_path)

    assert compute_transient_recovery_backoff_seconds(config, 0) == 0
    assert compute_transient_recovery_backoff_seconds(config, 1) == 60
    assert compute_transient_recovery_backoff_seconds(config, 2) == 120
    assert compute_transient_recovery_backoff_seconds(config, 3) == 300
    assert compute_transient_recovery_backoff_seconds(config, 4) == 600
    assert compute_transient_recovery_backoff_seconds(config, 5) == 1200
    assert compute_transient_recovery_backoff_seconds(config, 6) == 1800
    assert compute_transient_recovery_backoff_seconds(config, 7) == 1800


def test_compute_transient_recovery_backoff_seconds_scales_from_initial_and_caps_at_max(tmp_path: Path) -> None:
    (tmp_path / "gza.yaml").write_text(
        "project_name: demo\n"
        "watch:\n"
        "  failure_backoff_initial: 30\n"
        "  transient_recovery_backoff_max: 500\n",
        encoding="utf-8",
    )
    config = Config.load(tmp_path)

    assert compute_transient_recovery_backoff_seconds(config, 1) == 30
    assert compute_transient_recovery_backoff_seconds(config, 2) == 60
    assert compute_transient_recovery_backoff_seconds(config, 3) == 150
    assert compute_transient_recovery_backoff_seconds(config, 4) == 300
    assert compute_transient_recovery_backoff_seconds(config, 5) == 500
