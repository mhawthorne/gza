"""Internal aggregate metrics facade for optional CLI profiling."""

from __future__ import annotations

import functools
import inspect
import os
import threading
import time
from collections import defaultdict
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass

LabelMap = Mapping[str, str]


@dataclass(frozen=True)
class MetricKey:
    """Canonical metric identity used for in-memory aggregation."""

    name: str
    labels: tuple[tuple[str, str], ...] = ()


@dataclass(frozen=True)
class LatencyAggregate:
    """Aggregated count and total wall time for one metric key."""

    count: int = 0
    total_seconds: float = 0.0


@dataclass(frozen=True)
class MetricsSnapshot:
    """Immutable snapshot of the process-local aggregate metrics."""

    counters: dict[MetricKey, int]
    latencies: dict[MetricKey, LatencyAggregate]


class _MetricsState:
    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.counters: dict[MetricKey, int] = defaultdict(int)
        self.latencies: dict[MetricKey, LatencyAggregate] = {}


_STATE = _MetricsState()


def enabled() -> bool:
    """Return whether aggregate profiling is enabled for this process."""
    return os.environ.get("GZA_PROFILE") == "1"


def _metric_key(name: str, labels: LabelMap | None) -> MetricKey:
    if not labels:
        return MetricKey(name=name)
    return MetricKey(name=name, labels=tuple(sorted(labels.items())))


def _wrap_latency_callable(
    func,
    *,
    metric_name: str,
    labels: LabelMap,
):
    if getattr(func, "__gza_latency_instrumented__", False):
        return func

    @functools.wraps(func)
    def wrapped(*args, **kwargs):
        if not enabled():
            return func(*args, **kwargs)
        started = time.perf_counter()
        try:
            return func(*args, **kwargs)
        finally:
            observe_latency(metric_name, time.perf_counter() - started, labels=labels)

    setattr(wrapped, "__gza_latency_instrumented__", True)
    return wrapped


def instrument_public_methods(metric_name: str, *, label_key: str = "method"):
    """Class decorator that times public methods defined directly on a class."""

    def decorate(cls):
        for name, value in tuple(vars(cls).items()):
            if name.startswith("_"):
                continue

            labels = {label_key: name}
            if isinstance(value, classmethod):
                setattr(
                    cls,
                    name,
                    classmethod(_wrap_latency_callable(value.__func__, metric_name=metric_name, labels=labels)),
                )
                continue
            if isinstance(value, staticmethod):
                setattr(
                    cls,
                    name,
                    staticmethod(_wrap_latency_callable(value.__func__, metric_name=metric_name, labels=labels)),
                )
                continue
            if isinstance(value, property) or not callable(value):
                continue
            setattr(cls, name, _wrap_latency_callable(value, metric_name=metric_name, labels=labels))
        return cls

    return decorate


def instrument_module_functions(
    module_globals: dict[str, object],
    *,
    metric_name: str,
    module_name: str,
    module_label_key: str = "module",
    function_label_key: str = "function",
) -> None:
    """Wrap public functions defined directly in a module."""

    for name, value in tuple(module_globals.items()):
        if name.startswith("_") or not inspect.isfunction(value):
            continue
        if value.__module__ != module_name:
            continue
        module_globals[name] = _wrap_latency_callable(
            value,
            metric_name=metric_name,
            labels={module_label_key: module_name, function_label_key: name},
        )


def incr(name: str, *, labels: LabelMap | None = None, value: int = 1) -> None:
    """Increment a named counter when profiling is enabled."""
    if not enabled():
        return
    key = _metric_key(name, labels)
    with _STATE.lock:
        _STATE.counters[key] += value


def observe_latency(name: str, seconds: float, *, labels: LabelMap | None = None) -> None:
    """Record one latency observation when profiling is enabled."""
    if not enabled():
        return
    key = _metric_key(name, labels)
    with _STATE.lock:
        current = _STATE.latencies.get(key, LatencyAggregate())
        _STATE.latencies[key] = LatencyAggregate(
            count=current.count + 1,
            total_seconds=current.total_seconds + seconds,
        )


@contextmanager
def timer(name: str, *, labels: LabelMap | None = None) -> Iterator[None]:
    """Measure wall time for a block when profiling is enabled."""
    if not enabled():
        yield
        return
    started = time.perf_counter()
    try:
        yield
    finally:
        observe_latency(name, time.perf_counter() - started, labels=labels)


def snapshot() -> MetricsSnapshot:
    """Return a copy of the current aggregate metrics."""
    if not enabled():
        return MetricsSnapshot(counters={}, latencies={})
    with _STATE.lock:
        return MetricsSnapshot(
            counters=dict(_STATE.counters),
            latencies=dict(_STATE.latencies),
        )


def render_cli_summary(metrics_snapshot: MetricsSnapshot, *, total_seconds: float) -> str:
    """Render a compact one-line stderr summary for CLI profiling."""
    parts: list[str] = []

    latency_by_name: dict[str, LatencyAggregate] = {}
    for key, aggregate in metrics_snapshot.latencies.items():
        current = latency_by_name.get(key.name, LatencyAggregate())
        latency_by_name[key.name] = LatencyAggregate(
            count=current.count + aggregate.count,
            total_seconds=current.total_seconds + aggregate.total_seconds,
        )

    for name, aggregate in sorted(
        latency_by_name.items(),
        key=lambda item: (-item[1].total_seconds, item[0]),
    ):
        parts.append(f"{name} {aggregate.count} calls {aggregate.total_seconds:.3f}s")

    counter_by_name: dict[str, int] = defaultdict(int)
    for key, value in metrics_snapshot.counters.items():
        counter_by_name[key.name] += value

    for name, value in sorted(counter_by_name.items()):
        if name in latency_by_name:
            continue
        parts.append(f"{name} {value}")

    if not parts:
        parts.append("no metrics")

    return f"profile: {' | '.join(parts)} | total {total_seconds:.3f}s"
