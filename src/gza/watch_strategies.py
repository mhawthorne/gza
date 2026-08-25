"""Pure cross-project watch dispatch strategies."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Generic, Protocol, TypeVar, cast

T = TypeVar("T")

STRATEGY_STATE_VERSION = 1


@dataclass(frozen=True)
class WatchStrategyChoice(Generic[T]):
    """One selected project head."""

    project_key: str
    candidate: T


@dataclass(frozen=True)
class WatchStrategyMetadata:
    """Operator-facing strategy metadata used by future startup output."""

    name: str
    description: str
    startup_warning: str | None = None


class WatchDispatchStrategy(Protocol[T]):
    """Select between current project heads without inspecting project-local queues."""

    @property
    def metadata(self) -> WatchStrategyMetadata:
        ...

    def select_next(self, heads: Mapping[str, T | None]) -> WatchStrategyChoice[T] | None:
        ...

    def serialize_state(self) -> dict[str, object]:
        ...


def _validate_project_order(project_order: Sequence[str]) -> tuple[str, ...]:
    ordered = tuple(project_order)
    if not ordered:
        raise ValueError("watch strategy requires at least one project")
    duplicates = sorted({key for key in ordered if ordered.count(key) > 1})
    if duplicates:
        raise ValueError(f"duplicate project keys are not allowed: {', '.join(duplicates)}")
    return ordered


def _validate_state_header(
    state: Mapping[str, object] | None,
    *,
    strategy_name: str,
    project_order: tuple[str, ...],
) -> None:
    if state is None:
        return

    version = state.get("version")
    if type(version) is not int or version != STRATEGY_STATE_VERSION:
        raise ValueError("strategy state field 'version' must match the current strategy state version")

    state_name = state.get("name")
    if not isinstance(state_name, str):
        raise ValueError("strategy state field 'name' must be a string")
    if state_name != strategy_name:
        raise ValueError(f"strategy state belongs to {state_name!r}, not {strategy_name!r}")

    if "project_order" not in state:
        raise ValueError("strategy state field 'project_order' is required")
    state_project_order = state["project_order"]
    if not isinstance(state_project_order, list) or not all(isinstance(key, str) for key in state_project_order):
        raise ValueError("strategy state field 'project_order' must be a list of strings")
    if tuple(state_project_order) != project_order:
        raise ValueError("strategy state project order does not match selected project order")


def _state_int(state: Mapping[str, object] | None, key: str, default: int) -> int:
    if state is None or key not in state:
        return default
    value = state[key]
    if type(value) is not int:
        raise ValueError(f"strategy state field {key!r} must be an integer")
    return value


def _state_required_int(state: Mapping[str, object], key: str) -> int:
    if key not in state:
        raise ValueError(f"strategy state field {key!r} is required")
    return _state_int(state, key, 0)


def _state_cursor(state: Mapping[str, object] | None, project_count: int) -> int:
    if state is None:
        return 0
    cursor = _state_required_int(state, "cursor")
    if cursor < 0 or cursor >= project_count:
        raise ValueError("strategy state field 'cursor' is out of range")
    return cursor


class RoundRobinWatchStrategy(Generic[T]):
    """Persistent-cursor round-robin over declared project order."""

    metadata = WatchStrategyMetadata(
        name="round-robin",
        description="Visit eligible project heads in declared project order, preserving a cursor across cycles.",
    )

    def __init__(self, project_order: Sequence[str], *, state: Mapping[str, object] | None = None) -> None:
        self._project_order = _validate_project_order(project_order)
        _validate_state_header(state, strategy_name=self.metadata.name, project_order=self._project_order)
        self._cursor = _state_cursor(state, len(self._project_order))

    def select_next(self, heads: Mapping[str, T | None]) -> WatchStrategyChoice[T] | None:
        project_count = len(self._project_order)
        for offset in range(project_count):
            index = (self._cursor + offset) % project_count
            project_key = self._project_order[index]
            candidate = heads.get(project_key)
            if candidate is None:
                continue
            self._cursor = (index + 1) % project_count
            return WatchStrategyChoice(project_key=project_key, candidate=candidate)
        return None

    def serialize_state(self) -> dict[str, object]:
        return {
            "version": STRATEGY_STATE_VERSION,
            "name": self.metadata.name,
            "project_order": list(self._project_order),
            "cursor": self._cursor,
        }


class WeightedRoundRobinWatchStrategy(Generic[T]):
    """Persistent weighted round-robin over positive project weights."""

    metadata = WatchStrategyMetadata(
        name="weighted-round-robin",
        description="Give each eligible project its positive configured turns per round.",
    )

    def __init__(
        self,
        project_order: Sequence[str],
        *,
        weights: Mapping[str, int] | None = None,
        state: Mapping[str, object] | None = None,
    ) -> None:
        self._project_order = _validate_project_order(project_order)
        _validate_state_header(state, strategy_name=self.metadata.name, project_order=self._project_order)
        configured_weights = weights or {}
        if state is not None:
            state_weights = state.get("weights")
            if not isinstance(state_weights, Mapping):
                raise ValueError("strategy state field 'weights' must be a mapping")
            if set(state_weights) != set(self._project_order):
                raise ValueError("strategy state weights must match the selected project order")
            restored_weights = self._validate_weights(cast(Mapping[str, object], state_weights))
            if weights is not None and restored_weights != self._validate_weights(weights):
                raise ValueError("strategy state weights do not match configured weights")
            configured_weights = restored_weights
        self._weights = self._validate_weights(configured_weights)
        self._cursor = _state_cursor(state, len(self._project_order))
        if state is None:
            self._remaining_turns = self._weights[self._project_order[self._cursor]]
        else:
            self._remaining_turns = _state_required_int(state, "remaining_turns")
            current_weight = self._weights[self._project_order[self._cursor]]
            if self._remaining_turns < 1 or self._remaining_turns > current_weight:
                raise ValueError(
                    "strategy state field 'remaining_turns' must be between 1 and the current cursor weight"
                )

    def _validate_weights(self, weights: Mapping[str, object]) -> dict[str, int]:
        if not all(isinstance(project_key, str) for project_key in weights):
            raise ValueError("weight project keys must be strings")
        unknown = sorted(set(weights) - set(self._project_order))
        if unknown:
            raise ValueError(f"weights provided for unknown projects: {', '.join(unknown)}")
        normalized: dict[str, int] = {}
        for project_key in self._project_order:
            weight = weights.get(project_key, 1)
            if type(weight) is not int:
                raise ValueError(f"weight for project {project_key!r} must be an integer")
            if weight <= 0:
                raise ValueError(f"weight for project {project_key!r} must be positive")
            normalized[project_key] = weight
        return normalized

    def select_next(self, heads: Mapping[str, T | None]) -> WatchStrategyChoice[T] | None:
        project_count = len(self._project_order)
        cursor = self._cursor
        remaining_turns = self._remaining_turns
        for _ in range(project_count):
            project_key = self._project_order[cursor]
            candidate = heads.get(project_key)
            if candidate is not None:
                next_cursor = cursor
                next_remaining_turns = remaining_turns - 1
                if next_remaining_turns <= 0:
                    next_cursor = (cursor + 1) % project_count
                    next_remaining_turns = self._weights[self._project_order[next_cursor]]
                self._cursor = next_cursor
                self._remaining_turns = next_remaining_turns
                return WatchStrategyChoice(project_key=project_key, candidate=candidate)
            cursor = (cursor + 1) % project_count
            remaining_turns = self._weights[self._project_order[cursor]]
        return None

    def serialize_state(self) -> dict[str, object]:
        return {
            "version": STRATEGY_STATE_VERSION,
            "name": self.metadata.name,
            "project_order": list(self._project_order),
            "weights": dict(self._weights),
            "cursor": self._cursor,
            "remaining_turns": self._remaining_turns,
        }


class ProjectPriorityWatchStrategy(Generic[T]):
    """Strict project-priority strategy over declared project order."""

    metadata = WatchStrategyMetadata(
        name="project-priority",
        description="Always choose the first eligible project head in declared project order.",
        startup_warning=(
            "project-priority can indefinitely starve later projects while an earlier project remains runnable"
        ),
    )

    def __init__(self, project_order: Sequence[str], *, state: Mapping[str, object] | None = None) -> None:
        self._project_order = _validate_project_order(project_order)
        _validate_state_header(state, strategy_name=self.metadata.name, project_order=self._project_order)

    def select_next(self, heads: Mapping[str, T | None]) -> WatchStrategyChoice[T] | None:
        for project_key in self._project_order:
            candidate = heads.get(project_key)
            if candidate is not None:
                return WatchStrategyChoice(project_key=project_key, candidate=candidate)
        return None

    def serialize_state(self) -> dict[str, object]:
        return {
            "version": STRATEGY_STATE_VERSION,
            "name": self.metadata.name,
            "project_order": list(self._project_order),
        }


WATCH_STRATEGY_METADATA: Mapping[str, WatchStrategyMetadata] = {
    RoundRobinWatchStrategy.metadata.name: RoundRobinWatchStrategy.metadata,
    WeightedRoundRobinWatchStrategy.metadata.name: WeightedRoundRobinWatchStrategy.metadata,
    ProjectPriorityWatchStrategy.metadata.name: ProjectPriorityWatchStrategy.metadata,
}


def create_watch_dispatch_strategy(
    name: str,
    *,
    project_order: Sequence[str],
    weights: Mapping[str, int] | None = None,
    state: Mapping[str, object] | None = None,
) -> WatchDispatchStrategy[T]:
    """Create a watch dispatch strategy by registry name."""

    if name == RoundRobinWatchStrategy.metadata.name:
        return RoundRobinWatchStrategy(project_order, state=state)
    if name == WeightedRoundRobinWatchStrategy.metadata.name:
        return WeightedRoundRobinWatchStrategy(project_order, weights=weights, state=state)
    if name == ProjectPriorityWatchStrategy.metadata.name:
        return ProjectPriorityWatchStrategy(project_order, state=state)
    raise ValueError(f"unknown watch dispatch strategy: {name}")


WATCH_DISPATCH_STRATEGY_REGISTRY = WATCH_STRATEGY_METADATA
