from __future__ import annotations

import inspect
from dataclasses import dataclass

import pytest

import gza.watch_strategies as watch_strategies
from gza.watch_strategies import create_watch_dispatch_strategy


@dataclass(frozen=True)
class SyntheticCandidate:
    project_key: str
    local_index: int


class PoisonCandidate:
    @property
    def queue_position(self) -> int:
        raise AssertionError("cross-project strategy must not inspect queue_position")

    def __lt__(self, other: object) -> bool:
        raise AssertionError("cross-project strategy must not compare candidates")


def _heads(queues: dict[str, list[SyntheticCandidate]]) -> dict[str, SyntheticCandidate | None]:
    return {project_key: candidates[0] if candidates else None for project_key, candidates in queues.items()}


def _take(
    strategy_name: str,
    project_order: tuple[str, ...],
    queues: dict[str, list[SyntheticCandidate]],
    *,
    count: int,
    weights: dict[str, int] | None = None,
) -> list[SyntheticCandidate]:
    strategy = create_watch_dispatch_strategy(
        strategy_name,
        project_order=project_order,
        weights=weights,
    )
    selected: list[SyntheticCandidate] = []
    for _ in range(count):
        choice = strategy.select_next(_heads(queues))
        if choice is None:
            break
        selected.append(choice.candidate)
        queues[choice.project_key].pop(0)
    return selected


def test_round_robin_preserves_local_candidate_order() -> None:
    queues = {
        "core": [SyntheticCandidate("core", 1), SyntheticCandidate("core", 2)],
        "server": [SyntheticCandidate("server", 1), SyntheticCandidate("server", 2)],
    }

    selected = _take("round-robin", ("core", "server"), queues, count=4)

    assert selected == [
        SyntheticCandidate("core", 1),
        SyntheticCandidate("server", 1),
        SyntheticCandidate("core", 2),
        SyntheticCandidate("server", 2),
    ]


@pytest.mark.parametrize(
    ("strategy_name", "weights"),
    [
        ("round-robin", None),
        ("weighted-round-robin", {"beta": 1, "alpha": 1}),
        ("project-priority", None),
    ],
)
def test_declared_project_order_is_the_initial_tie_break(strategy_name: str, weights: dict[str, int] | None) -> None:
    strategy = create_watch_dispatch_strategy(
        strategy_name,
        project_order=("beta", "alpha"),
        weights=weights,
    )

    choice = strategy.select_next({"alpha": "alpha-head", "beta": "beta-head"})

    assert choice is not None
    assert choice.project_key == "beta"
    assert choice.candidate == "beta-head"


def test_round_robin_skips_projects_without_eligible_heads() -> None:
    strategy = create_watch_dispatch_strategy("round-robin", project_order=("held", "ready", "empty"))

    first = strategy.select_next({"held": None, "ready": "ready-1", "empty": None})
    second = strategy.select_next({"held": None, "ready": "ready-2", "empty": None})

    assert first is not None
    assert first.project_key == "ready"
    assert first.candidate == "ready-1"
    assert second is not None
    assert second.project_key == "ready"
    assert second.candidate == "ready-2"


def test_round_robin_cursor_rotates_fairly_when_batch_is_smaller_than_project_count() -> None:
    strategy = create_watch_dispatch_strategy("round-robin", project_order=("a", "b", "c"))

    selected: list[str] = []
    for _ in range(5):
        choice = strategy.select_next({"a": "a", "b": "b", "c": "c"})
        assert choice is not None
        selected.append(choice.project_key)

    assert selected == ["a", "b", "c", "a", "b"]


def test_round_robin_state_serializes_for_reexec_reconstruction() -> None:
    strategy = create_watch_dispatch_strategy("round-robin", project_order=("a", "b", "c"))
    first = strategy.select_next({"a": "a", "b": "b", "c": "c"})
    restored = create_watch_dispatch_strategy(
        "round-robin",
        project_order=("a", "b", "c"),
        state=strategy.serialize_state(),
    )
    second = restored.select_next({"a": "a", "b": "b", "c": "c"})

    assert first is not None
    assert first.project_key == "a"
    assert second is not None
    assert second.project_key == "b"


def test_weighted_round_robin_uses_positive_weights_proportionally() -> None:
    strategy = create_watch_dispatch_strategy(
        "weighted-round-robin",
        project_order=("a", "b", "c"),
        weights={"a": 2, "b": 1, "c": 3},
    )

    selected: list[str] = []
    for _ in range(8):
        choice = strategy.select_next({"a": "a", "b": "b", "c": "c"})
        assert choice is not None
        selected.append(choice.project_key)

    assert selected == ["a", "a", "b", "c", "c", "c", "a", "a"]


def test_weighted_round_robin_state_preserves_mid_round_quota() -> None:
    strategy = create_watch_dispatch_strategy(
        "weighted-round-robin",
        project_order=("a", "b"),
        weights={"a": 2, "b": 1},
    )
    first = strategy.select_next({"a": "a", "b": "b"})
    restored = create_watch_dispatch_strategy(
        "weighted-round-robin",
        project_order=("a", "b"),
        state=strategy.serialize_state(),
    )

    second = restored.select_next({"a": "a", "b": "b"})
    third = restored.select_next({"a": "a", "b": "b"})

    assert first is not None
    assert first.project_key == "a"
    assert second is not None
    assert second.project_key == "a"
    assert third is not None
    assert third.project_key == "b"


def test_weighted_round_robin_skips_unavailable_weighted_quota_in_one_selection() -> None:
    strategy = create_watch_dispatch_strategy(
        "weighted-round-robin",
        project_order=("a", "b"),
        weights={"a": 10**9, "b": 2},
    )

    first = strategy.select_next({"a": None, "b": "b-1"})
    second = strategy.select_next({"a": "a-1", "b": "b-2"})
    third = strategy.select_next({"a": "a-1", "b": "b-3"})

    assert first is not None
    assert first.project_key == "b"
    assert first.candidate == "b-1"
    assert second is not None
    assert second.project_key == "b"
    assert second.candidate == "b-2"
    assert third is not None
    assert third.project_key == "a"
    assert third.candidate == "a-1"
    assert strategy.serialize_state()["cursor"] == 0
    assert strategy.serialize_state()["remaining_turns"] == 10**9 - 1


@pytest.mark.parametrize("bad_weight", [0, -1])
def test_weighted_round_robin_rejects_non_positive_weights(bad_weight: int) -> None:
    with pytest.raises(ValueError, match="must be positive"):
        create_watch_dispatch_strategy(
            "weighted-round-robin",
            project_order=("a", "b"),
            weights={"a": bad_weight, "b": 1},
        )


@pytest.mark.parametrize(
    ("state_update", "match"),
    [
        ({"version": None}, "version"),
        ({"version": 2}, "version"),
        ({"__delete__": "version"}, "version"),
        ({"__delete__": "name"}, "name"),
        ({"name": None}, "name"),
        ({"name": "round-robin"}, "belongs to"),
        ({"__delete__": "project_order"}, "project_order"),
        ({"project_order": ["b", "a"]}, "project order"),
        ({"project_order": "a,b"}, "project_order"),
        ({"weights": {"a": 2}}, "weights must match"),
        ({"weights": {"a": True, "b": 1}}, "must be an integer"),
        ({"weights": {"a": 0, "b": 1}}, "must be positive"),
        ({"__delete__": "cursor"}, "cursor"),
        ({"cursor": True}, "cursor"),
        ({"cursor": -1}, "cursor"),
        ({"cursor": 2}, "cursor"),
        ({"__delete__": "remaining_turns"}, "remaining_turns"),
        ({"remaining_turns": False}, "remaining_turns"),
        ({"remaining_turns": 0}, "remaining_turns"),
        ({"remaining_turns": 3}, "remaining_turns"),
    ],
)
def test_weighted_round_robin_rejects_malformed_restored_state(
    state_update: dict[str, object],
    match: str,
) -> None:
    state: dict[str, object] = {
        "version": watch_strategies.STRATEGY_STATE_VERSION,
        "name": "weighted-round-robin",
        "project_order": ["a", "b"],
        "weights": {"a": 2, "b": 1},
        "cursor": 0,
        "remaining_turns": 1,
    }
    if "__delete__" in state_update:
        delete_key = state_update["__delete__"]
        assert isinstance(delete_key, str)
        del state[delete_key]
        state_update = {key: value for key, value in state_update.items() if key != "__delete__"}
    state.update(state_update)

    with pytest.raises(ValueError, match=match):
        create_watch_dispatch_strategy(
            "weighted-round-robin",
            project_order=("a", "b"),
            state=state,
        )


def test_round_robin_rejects_malformed_restored_state_header() -> None:
    with pytest.raises(ValueError, match="version"):
        create_watch_dispatch_strategy(
            "round-robin",
            project_order=("a", "b"),
            state={"name": "round-robin", "project_order": ["a", "b"], "cursor": 0},
        )


def test_project_priority_rejects_malformed_restored_state_header() -> None:
    with pytest.raises(ValueError, match="project order"):
        create_watch_dispatch_strategy(
            "project-priority",
            project_order=("a", "b"),
            state={
                "version": watch_strategies.STRATEGY_STATE_VERSION,
                "name": "project-priority",
                "project_order": ["b", "a"],
            },
        )


def test_weighted_round_robin_rejects_serialized_current_weight_mismatch() -> None:
    strategy = create_watch_dispatch_strategy(
        "weighted-round-robin",
        project_order=("a", "b"),
        weights={"a": 3, "b": 1},
    )
    first = strategy.select_next({"a": "a", "b": "b"})
    assert first is not None
    assert first.project_key == "a"

    with pytest.raises(ValueError, match="weights do not match"):
        create_watch_dispatch_strategy(
            "weighted-round-robin",
            project_order=("a", "b"),
            weights={"a": 1, "b": 1},
            state=strategy.serialize_state(),
        )


def test_weighted_round_robin_rejects_stale_quota_that_would_hide_ready_later_project() -> None:
    state = {
        "version": watch_strategies.STRATEGY_STATE_VERSION,
        "name": "weighted-round-robin",
        "project_order": ["a", "b"],
        "weights": {"a": 1, "b": 1},
        "cursor": 0,
        "remaining_turns": 2,
    }

    with pytest.raises(ValueError, match="remaining_turns"):
        create_watch_dispatch_strategy(
            "weighted-round-robin",
            project_order=("a", "b"),
            weights={"a": 1, "b": 1},
            state=state,
        )


def test_project_priority_strictly_starves_later_projects_while_first_project_remains_runnable() -> None:
    strategy = create_watch_dispatch_strategy("project-priority", project_order=("incident", "backlog"))

    selected: list[str] = []
    for _ in range(4):
        choice = strategy.select_next({"incident": "incident-head", "backlog": "backlog-head"})
        assert choice is not None
        selected.append(choice.project_key)

    assert selected == ["incident", "incident", "incident", "incident"]
    metadata = watch_strategies.WATCH_STRATEGY_METADATA["project-priority"]
    assert metadata.startup_warning is not None
    assert "starve" in metadata.startup_warning


def test_strategies_do_not_inspect_or_compare_cross_project_candidate_positions() -> None:
    source = inspect.getsource(watch_strategies)
    assert "queue_position" not in source

    for strategy_name in ("round-robin", "weighted-round-robin", "project-priority"):
        strategy = create_watch_dispatch_strategy(
            strategy_name,
            project_order=("later", "earlier"),
            weights={"later": 1, "earlier": 1} if strategy_name == "weighted-round-robin" else None,
        )
        choice = strategy.select_next({"earlier": PoisonCandidate(), "later": PoisonCandidate()})
        assert choice is not None
        assert choice.project_key == "later"
