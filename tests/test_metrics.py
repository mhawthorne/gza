"""Tests for the internal aggregate metrics facade."""

from __future__ import annotations

import importlib
from unittest.mock import patch

from pytest import approx


def _reload_metrics():
    import gza.metrics as metrics_module

    return importlib.reload(metrics_module)


def test_metrics_disabled_are_safe_noops(monkeypatch) -> None:
    monkeypatch.delenv("GZA_PROFILE", raising=False)
    metrics = _reload_metrics()

    metrics.incr("gza_test_counter", labels={"task_id": "gza-123"})
    metrics.observe_latency("gza_test_latency_seconds", 1.25, labels={"sql": "select * from tasks"})
    with metrics.timer("gza_test_timer_seconds", labels={"path": "/tmp/secret"}):
        pass

    snapshot = metrics.snapshot()

    assert metrics.enabled() is False
    assert snapshot.counters == {}
    assert snapshot.latencies == {}


def test_metrics_enabled_aggregate_in_memory(monkeypatch) -> None:
    monkeypatch.setenv("GZA_PROFILE", "1")
    metrics = _reload_metrics()

    metrics.incr("gza_test_counter", labels={"kind": "read"})
    metrics.incr("gza_test_counter", labels={"kind": "read"}, value=2)
    metrics.observe_latency("gza_test_latency_seconds", 0.5, labels={"kind": "read"})

    perf_counter_values = iter((10.0, 10.125))
    with patch.object(metrics.time, "perf_counter", side_effect=lambda: next(perf_counter_values)):
        with metrics.timer("gza_test_latency_seconds", labels={"kind": "read"}):
            pass

    snapshot = metrics.snapshot()

    assert metrics.enabled() is True
    assert snapshot.counters == {
        metrics.MetricKey(name="gza_test_counter", labels=(("kind", "read"),)): 3,
    }
    aggregate = snapshot.latencies[
        metrics.MetricKey(name="gza_test_latency_seconds", labels=(("kind", "read"),))
    ]
    assert aggregate.count == 2
    assert aggregate.total_seconds == approx(0.625)


def test_render_cli_summary_is_one_line_and_omits_labels(monkeypatch) -> None:
    monkeypatch.setenv("GZA_PROFILE", "1")
    metrics = _reload_metrics()

    metrics.incr("gza_test_counter", labels={"task_id": "gza-123"})
    metrics.observe_latency(
        "gza_test_latency_seconds",
        1.25,
        labels={"sql": "select * from tasks", "task_id": "gza-123"},
    )

    summary = metrics.render_cli_summary(metrics.snapshot(), total_seconds=2.0)

    assert summary == "profile: gza_test_latency_seconds 1 calls 1.250s | gza_test_counter 1 | total 2.000s"
    assert "\n" not in summary
    assert "gza-123" not in summary
    assert "select * from tasks" not in summary
