"""Watch-supervisor project lease helpers."""

import os
import uuid
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Protocol

from gza.db import ProjectLease

WATCH_SUPERVISOR_LEASE_NAME = "watch-supervisor"


class WatchLeaseStore(Protocol):
    @property
    def db_path(self) -> Path:
        """Return the resolved task database path for ownership identity."""

    @property
    def project_id(self) -> str:
        """Return the project identity for diagnostics."""

    def try_acquire_project_lease(
        self,
        *,
        lease_name: str,
        owner_pid: int,
        owner_token: str,
        acquired_at: datetime | None = None,
    ) -> ProjectLease | None:
        """Acquire a project lease, returning None on conflict."""

    def release_project_lease(
        self,
        *,
        lease_name: str,
        owner_token: str,
    ) -> bool:
        """Release a project lease when the owner token still matches."""


@dataclass(frozen=True)
class WatchLeaseTarget:
    """One project store selected for watch-supervisor leasing."""

    key: str
    store: WatchLeaseStore


@dataclass(frozen=True)
class WatchLeaseIdentity:
    """Concrete execution identity for one watch-supervisor lease."""

    db_path: Path
    project_id: str
    lease_name: str


@dataclass(frozen=True)
class WatchLeaseHeld:
    """One acquired watch-supervisor lease."""

    target: WatchLeaseTarget
    lease: ProjectLease


@dataclass(frozen=True)
class WatchLeaseRelease:
    """Release result for one acquired lease."""

    target_key: str
    released: bool


class WatchLeaseConflict(RuntimeError):
    """Raised when another live owner holds a selected watch-supervisor lease."""

    def __init__(self, target: WatchLeaseTarget) -> None:
        self.target = target
        self.target_key = target.key
        self.project_id = target.store.project_id
        self.watch_disabled_projects: tuple[object, ...] = ()
        super().__init__(
            f"watch-supervisor lease for project {self.project_id!r} "
            f"({self.target_key}) is held by another live owner"
        )


def watch_lease_target_identity(target: WatchLeaseTarget) -> WatchLeaseIdentity:
    """Return the concrete identity that determines whether a lease is already owned."""
    return WatchLeaseIdentity(
        db_path=target.store.db_path.resolve(),
        project_id=target.store.project_id,
        lease_name=WATCH_SUPERVISOR_LEASE_NAME,
    )


@dataclass(frozen=True)
class WatchLeaseReleaseFailure:
    """Release failure for one acquired lease."""

    target_key: str
    error: BaseException


class WatchLeaseReleaseError(RuntimeError):
    """Raised after best-effort watch-supervisor lease release encounters failures."""

    def __init__(
        self,
        *,
        results: tuple[WatchLeaseRelease, ...],
        failures: tuple[WatchLeaseReleaseFailure, ...],
    ) -> None:
        self.results = results
        self.failures = failures
        failed_keys = ", ".join(failure.target_key for failure in failures)
        super().__init__(f"failed to release watch-supervisor leases for: {failed_keys}")


@dataclass(frozen=True)
class WatchLeaseSet:
    """A deterministic set of acquired watch-supervisor leases."""

    owner_pid: int
    owner_token: str
    held: tuple[WatchLeaseHeld, ...]

    def release(self) -> tuple[WatchLeaseRelease, ...]:
        """Release owned leases in reverse acquisition order."""
        return release_watch_held_leases(self.held, owner_token=self.owner_token)


def generate_watch_lease_owner_token() -> str:
    """Generate the stable run token used to own watch-supervisor leases."""
    return uuid.uuid4().hex


def _release_held_best_effort(
    held: Sequence[WatchLeaseHeld],
    *,
    owner_token: str,
) -> tuple[WatchLeaseRelease, ...]:
    released: list[WatchLeaseRelease] = []
    failures: list[WatchLeaseReleaseFailure] = []
    for acquired in reversed(held):
        try:
            release_result = acquired.target.store.release_project_lease(
                lease_name=WATCH_SUPERVISOR_LEASE_NAME,
                owner_token=owner_token,
            )
        except BaseException as exc:
            release_result = False
            failures.append(WatchLeaseReleaseFailure(target_key=acquired.target.key, error=exc))
        released.append(WatchLeaseRelease(target_key=acquired.target.key, released=release_result))
    results = tuple(released)
    if failures:
        raise WatchLeaseReleaseError(results=results, failures=tuple(failures))
    return results


def release_watch_held_leases(
    held: Sequence[WatchLeaseHeld],
    *,
    owner_token: str,
) -> tuple[WatchLeaseRelease, ...]:
    """Release held watch-supervisor leases in reverse acquisition order."""
    return _release_held_best_effort(held, owner_token=owner_token)


def _raise_with_cleanup_context(primary: BaseException, cleanup_error: WatchLeaseReleaseError) -> None:
    raise BaseExceptionGroup(
        "watch-supervisor lease acquisition failed and rollback also failed",
        [primary, cleanup_error],
    )


def _release_stale_existing_leases(
    existing: Sequence[WatchLeaseHeld],
    *,
    owner_token: str,
) -> None:
    if not existing:
        return
    _release_held_best_effort(existing, owner_token=owner_token)


def acquire_watch_project_leases(
    targets: Sequence[WatchLeaseTarget],
    *,
    owner_token: str | None = None,
    owner_pid: int | None = None,
    acquired_at: datetime | None = None,
    existing_lease_set: WatchLeaseSet | None = None,
    retain_existing_target_keys: frozenset[str] = frozenset(),
) -> WatchLeaseSet:
    """Acquire selected project leases, adopting existing concrete ownership when supplied."""
    if existing_lease_set is not None:
        if owner_token is not None and owner_token != existing_lease_set.owner_token:
            raise ValueError("owner_token must match existing_lease_set.owner_token")
        if owner_pid is not None and owner_pid != existing_lease_set.owner_pid:
            raise ValueError("owner_pid must match existing_lease_set.owner_pid")
    resolved_owner_token = (
        existing_lease_set.owner_token
        if existing_lease_set is not None
        else owner_token or generate_watch_lease_owner_token()
    )
    resolved_owner_pid = (
        existing_lease_set.owner_pid
        if existing_lease_set is not None
        else owner_pid if owner_pid is not None else os.getpid()
    )
    newly_acquired: list[WatchLeaseHeld] = []
    existing_by_identity = (
        {watch_lease_target_identity(held.target): held for held in existing_lease_set.held}
        if existing_lease_set is not None
        else {}
    )
    adopted_by_identity: dict[WatchLeaseIdentity, WatchLeaseHeld] = {}
    desired_identities: set[WatchLeaseIdentity] = set()
    for target in targets:
        identity = watch_lease_target_identity(target)
        desired_identities.add(identity)
        already_owned = existing_by_identity.get(identity)
        try:
            lease = target.store.try_acquire_project_lease(
                lease_name=WATCH_SUPERVISOR_LEASE_NAME,
                owner_pid=resolved_owner_pid,
                owner_token=resolved_owner_token,
                acquired_at=acquired_at,
            )
        except BaseException as exc:
            try:
                _release_held_best_effort(newly_acquired, owner_token=resolved_owner_token)
            except WatchLeaseReleaseError as cleanup_error:
                _raise_with_cleanup_context(exc, cleanup_error)
            raise
        if lease is None:
            conflict = WatchLeaseConflict(target)
            try:
                _release_held_best_effort(newly_acquired, owner_token=resolved_owner_token)
            except WatchLeaseReleaseError as cleanup_error:
                _raise_with_cleanup_context(conflict, cleanup_error)
            raise conflict
        acquired = WatchLeaseHeld(target=target, lease=lease)
        if not already_owned:
            newly_acquired.append(acquired)
        else:
            adopted_by_identity[identity] = acquired
    if existing_lease_set is not None:
        retained_existing = [
            held_lease
            for held_lease in existing_lease_set.held
            if watch_lease_target_identity(held_lease.target) not in desired_identities
            and held_lease.target.key in retain_existing_target_keys
        ]
        stale_existing = [
            held_lease
            for held_lease in existing_lease_set.held
            if watch_lease_target_identity(held_lease.target) not in desired_identities
            and held_lease.target.key not in retain_existing_target_keys
        ]
        try:
            _release_stale_existing_leases(stale_existing, owner_token=resolved_owner_token)
        except WatchLeaseReleaseError as cleanup_error:
            try:
                _release_held_best_effort(newly_acquired, owner_token=resolved_owner_token)
            except WatchLeaseReleaseError as rollback_error:
                raise BaseExceptionGroup(
                    "watch-supervisor lease refresh failed while releasing stale ownership",
                    [cleanup_error, rollback_error],
                ) from cleanup_error
            raise
        retained_by_identity = {
            watch_lease_target_identity(held_lease.target): held_lease for held_lease in retained_existing
        }
        held = [
            adopted_by_identity[identity]
            if identity in adopted_by_identity
            else retained_by_identity[identity]
            for held_lease in existing_lease_set.held
            for identity in (watch_lease_target_identity(held_lease.target),)
            if identity in adopted_by_identity or identity in retained_by_identity
        ]
        held.extend(newly_acquired)
    else:
        held = newly_acquired
    return WatchLeaseSet(
        owner_pid=resolved_owner_pid,
        owner_token=resolved_owner_token,
        held=tuple(held),
    )
