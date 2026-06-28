"""Tests for the internal aggregate metrics facade."""

from __future__ import annotations

import importlib
import inspect
from pathlib import Path
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


def test_instrument_public_methods_preserves_metadata_and_descriptors(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("GZA_PROFILE", "1")
    metrics = _reload_metrics()

    @metrics.instrument_public_methods("gza_dummy_method_latency_seconds")
    class DummyStore:
        def __init__(self, value: int) -> None:
            self.value = value

        def public(self, increment: int = 1) -> int:
            return self.value + increment

        def _private(self) -> int:
            return self.value

        @classmethod
        def build(cls, value: int) -> "DummyStore":
            return cls(value)

        @staticmethod
        def describe(value: int) -> str:
            return f"value={value}"

    class ChildDummyStore(DummyStore):
        pass

    dummy = DummyStore(4)
    assert dummy.public(3) == 7
    built = ChildDummyStore.build(9)
    assert isinstance(built, ChildDummyStore)
    assert built.value == 9
    assert DummyStore.describe(5) == "value=5"
    assert dummy._private() == 4

    snapshot = metrics.snapshot()

    assert hasattr(DummyStore.public, "__wrapped__")
    assert inspect.signature(DummyStore.public) == inspect.signature(DummyStore.public.__wrapped__)
    assert DummyStore.public.__wrapped__(dummy, 6) == 10
    assert DummyStore.build.__wrapped__.__name__ == "build"
    assert DummyStore.describe.__wrapped__.__name__ == "describe"
    assert not hasattr(DummyStore._private, "__gza_latency_instrumented__")

    assert snapshot.latencies[metrics.MetricKey("gza_dummy_method_latency_seconds", (("method", "public"),))].count == 1
    assert snapshot.latencies[metrics.MetricKey("gza_dummy_method_latency_seconds", (("method", "build"),))].count == 1
    assert (
        snapshot.latencies[
            metrics.MetricKey("gza_dummy_method_latency_seconds", (("method", "describe"),))
        ].count
        == 1
    )
    assert metrics.MetricKey("gza_dummy_method_latency_seconds", (("method", "_private"),)) not in snapshot.latencies


def test_sqlite_task_store_public_methods_are_wrapped_without_private_helpers(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("GZA_PROFILE", "1")
    metrics = _reload_metrics()

    db_module = importlib.import_module("gza.db")
    store = db_module.SqliteTaskStore(tmp_path / "test.db", prefix="gza")

    before = metrics.snapshot()
    store.startup_warnings()
    after = metrics.snapshot()

    key = metrics.MetricKey("gza_db_method_latency_seconds", (("method", "startup_warnings"),))
    before_count = before.latencies.get(key, metrics.LatencyAggregate()).count
    assert after.latencies[key].count == before_count + 1

    assert hasattr(db_module.SqliteTaskStore.startup_warnings, "__wrapped__")
    assert db_module.SqliteTaskStore.startup_warnings.__wrapped__(store) == ()
    assert not hasattr(db_module.SqliteTaskStore._row_to_task, "__gza_latency_instrumented__")
