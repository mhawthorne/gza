"""Tests for the guard that keeps a dead worker from silently holding the port."""

from __future__ import annotations

import signal

import pytest
from gza_server import supervisor


class FakeChild:
    """A stand-in for the uvicorn process, scripted per test."""

    def __init__(self, exit_status: int | None = None) -> None:
        # A pid no process group can match, so signalling falls back to the
        # child methods this fake records.
        self.pid = -1
        self.exit_status = exit_status
        self.terminated = False
        self.killed = False
        self.waited = False

    def poll(self) -> int | None:
        return self.exit_status

    def terminate(self) -> None:
        self.terminated = True
        self.exit_status = -signal.SIGTERM

    def kill(self) -> None:
        self.killed = True
        self.exit_status = -signal.SIGKILL

    def wait(self, timeout: float | None = None) -> int:
        self.waited = True
        return self.exit_status or 0


@pytest.fixture
def instant_sleep(monkeypatch: pytest.MonkeyPatch) -> None:
    """Remove real waiting so the loop runs at test speed."""
    monkeypatch.setattr(supervisor.time, "sleep", lambda _seconds: None)


@pytest.fixture
def no_grace(monkeypatch: pytest.MonkeyPatch) -> None:
    """Health results count immediately instead of after a startup window."""
    monkeypatch.setattr(supervisor, "STARTUP_GRACE_SECONDS", 0.0)


def _run(
    monkeypatch: pytest.MonkeyPatch,
    *,
    healthy: list[bool],
    children: list[FakeChild],
) -> tuple[int, list[list[str]]]:
    """Run the guard over scripted health results and return its spawn calls."""
    spawned: list[list[str]] = []
    pending = list(children)

    def fake_spawn(command: list[str]) -> FakeChild:
        spawned.append(command)
        return pending.pop(0) if pending else FakeChild()

    health_results = list(healthy)

    def fake_healthy(url: str, instance_id: str) -> bool:
        if not health_results:
            # Nothing left to script: stop the loop rather than spin.
            raise StopIteration
        return health_results.pop(0)

    monkeypatch.setattr(supervisor, "_spawn", fake_spawn)
    monkeypatch.setattr(supervisor, "_is_healthy", fake_healthy)
    try:
        status = supervisor.main(["uvicorn"])
    except StopIteration:
        status = 0
    return status, spawned


def test_healthy_server_is_never_recycled(
    monkeypatch: pytest.MonkeyPatch, instant_sleep: None, no_grace: None
) -> None:
    _, spawned = _run(monkeypatch, healthy=[True, True, True], children=[FakeChild()])
    assert len(spawned) == 1


def test_unhealthy_worker_is_recycled_after_repeated_failures(
    monkeypatch: pytest.MonkeyPatch, instant_sleep: None, no_grace: None
) -> None:
    """The reported hang: the process lives while nothing answers the port."""
    live_but_dead_inside = FakeChild(exit_status=None)
    _, spawned = _run(
        monkeypatch,
        healthy=[False] * supervisor.UNHEALTHY_POLLS_BEFORE_RESTART,
        children=[live_but_dead_inside, FakeChild()],
    )
    assert live_but_dead_inside.terminated
    assert len(spawned) == 2


def test_single_failed_check_does_not_recycle(
    monkeypatch: pytest.MonkeyPatch, instant_sleep: None, no_grace: None
) -> None:
    """A blip must not bounce the server."""
    child = FakeChild()
    _, spawned = _run(monkeypatch, healthy=[False, True, True], children=[child])
    # Only a recycle spawns a replacement; the guard still stops its child when
    # the loop itself ends.
    assert len(spawned) == 1


def test_exited_child_is_restarted(
    monkeypatch: pytest.MonkeyPatch, instant_sleep: None, no_grace: None
) -> None:
    _, spawned = _run(
        monkeypatch,
        healthy=[True],
        children=[FakeChild(exit_status=1), FakeChild()],
    )
    assert len(spawned) == 2


def test_repeated_crashes_give_up_so_the_port_is_released(
    monkeypatch: pytest.MonkeyPatch, instant_sleep: None, no_grace: None
) -> None:
    """A crash that reproduces every boot must surface, not restart forever."""
    children = [FakeChild(exit_status=1) for _ in range(supervisor.MAX_RESTARTS + 2)]
    status, spawned = _run(monkeypatch, healthy=[], children=children)
    assert status == 1
    assert len(spawned) == supervisor.MAX_RESTARTS + 1


def test_stop_request_shuts_down_without_restarting(
    monkeypatch: pytest.MonkeyPatch, instant_sleep: None, no_grace: None
) -> None:
    """An explicit stop must never be fought by the restart guard."""
    child = FakeChild(exit_status=None)
    spawned: list[list[str]] = []
    shutdowns: list[supervisor._Shutdown] = []

    original_shutdown = supervisor._Shutdown

    class RecordingShutdown(original_shutdown):  # type: ignore[misc, valid-type]
        def __init__(self) -> None:
            super().__init__()
            shutdowns.append(self)

    def fake_spawn(command: list[str]) -> FakeChild:
        spawned.append(command)
        return child

    def fake_healthy(url: str, instance_id: str) -> bool:
        # Stop arrives while the server is failing its checks, which is exactly
        # when a restart would otherwise be triggered.
        shutdowns[0].request(signal.SIGTERM, None)
        return False

    monkeypatch.setattr(supervisor, "_Shutdown", RecordingShutdown)
    monkeypatch.setattr(supervisor, "_spawn", fake_spawn)
    monkeypatch.setattr(supervisor, "_is_healthy", fake_healthy)

    assert supervisor.main(["uvicorn"]) == 0
    assert len(spawned) == 1
    assert child.terminated


def test_health_requires_a_matching_instance(monkeypatch: pytest.MonkeyPatch) -> None:
    """Another server on the same port must not read as this one being healthy."""

    class FakeResponse:
        status = 200

        def read(self) -> bytes:
            return b'{"instance_id": "other"}'

        def __enter__(self) -> FakeResponse:
            return self

        def __exit__(self, *args: object) -> None:
            return None

    monkeypatch.setattr(supervisor, "urlopen", lambda *a, **k: FakeResponse())
    assert not supervisor._is_healthy("http://127.0.0.1:1/api/health", "mine")
    assert supervisor._is_healthy("http://127.0.0.1:1/api/health", "other")


def test_unreachable_endpoint_is_unhealthy(monkeypatch: pytest.MonkeyPatch) -> None:
    def refuse(*args: object, **kwargs: object) -> None:
        raise OSError("connection refused")

    monkeypatch.setattr(supervisor, "urlopen", refuse)
    assert not supervisor._is_healthy("http://127.0.0.1:1/api/health", "mine")
