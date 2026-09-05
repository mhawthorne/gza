from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest

from gza.config import Config
from gza.db import SqliteTaskStore
from gza.git import GitError
from gza.review_verify_state import (
    VERIFY_GATE_ARTIFACT_KIND,
    build_verify_gate_artifact_payload,
    latest_verify_result_for_epoch,
    make_verify_epoch,
    owner_task_verify_epoch,
    persist_verify_gate_artifact,
    resolve_verify_gate_decision,
    resolve_verify_read_model,
    review_task_verify_epoch,
    task_has_current_passing_verify_evidence,
    verify_epoch_matches,
    verify_result_is_timeout_origin,
)


def _config(tmp_path: Path) -> Config:
    return Config(project_dir=tmp_path, project_name="test-project")


def _result(
    *,
    command: str = "./bin/tests",
    head_sha: str = "head-1",
    tree_sha: str | None = None,
    captured_at: datetime,
) -> SimpleNamespace:
    return SimpleNamespace(
        command=command,
        status="passed",
        exit_status="0",
        captured_at=captured_at,
        reviewed_branch="feature/verify",
        reviewed_head_sha=head_sha,
        reviewed_tree_sha=tree_sha,
        reviewed_base_sha="base-1",
        working_directory="/tmp/worktree",
        failure=None,
    )


def _epoch(
    *,
    command: str = "./bin/tests",
    branch: str | None = "feature/verify",
    head_sha: str | None = "head-1",
    tree_sha: str | None = None,
    timeout_seconds: int | None = 120,
    timeout_grace_seconds: float | None = 5.0,
):
    return make_verify_epoch(
        reviewed_branch=branch,
        reviewed_head_sha=head_sha,
        reviewed_tree_sha=tree_sha,
        verify_command=command,
        verify_timeout_seconds=timeout_seconds,
        verify_timeout_grace_seconds=timeout_grace_seconds,
    )


def test_verify_epoch_matches_same_branch_equal_non_empty_tree_with_different_heads() -> None:
    expected = _epoch(head_sha="head-2", tree_sha="tree-1")
    candidate = _epoch(head_sha="head-1", tree_sha="tree-1")

    assert verify_epoch_matches(expected=expected, candidate=candidate) is True


def test_verify_epoch_matches_rejects_different_tree_and_changed_head() -> None:
    expected = _epoch(head_sha="head-2", tree_sha="tree-2")
    candidate = _epoch(head_sha="head-1", tree_sha="tree-1")

    assert verify_epoch_matches(expected=expected, candidate=candidate) is False


def test_verify_epoch_matches_requires_same_branch_even_when_tree_matches() -> None:
    expected = _epoch(branch="feature/current", head_sha="head-2", tree_sha="tree-1")
    candidate = _epoch(branch="feature/other", head_sha="head-1", tree_sha="tree-1")

    assert verify_epoch_matches(expected=expected, candidate=candidate) is False


@pytest.mark.parametrize(
    ("expected_branch", "candidate_branch"),
    [
        (None, "feature/verify"),
        ("feature/verify", None),
        (None, None),
        ("", "feature/verify"),
        ("feature/verify", ""),
        ("", ""),
        ("   ", "feature/verify"),
        ("feature/verify", "   "),
        ("   ", "   "),
    ],
)
@pytest.mark.parametrize(
    ("expected_head", "candidate_head", "expected_tree", "candidate_tree"),
    [
        ("head-1", "head-1", None, None),
        ("head-2", "head-1", "tree-1", "tree-1"),
    ],
)
def test_verify_epoch_matches_requires_concrete_non_blank_branch_proof(
    expected_branch: str | None,
    candidate_branch: str | None,
    expected_head: str,
    candidate_head: str,
    expected_tree: str | None,
    candidate_tree: str | None,
) -> None:
    expected = _epoch(branch=expected_branch, head_sha=expected_head, tree_sha=expected_tree)
    candidate = _epoch(branch=candidate_branch, head_sha=candidate_head, tree_sha=candidate_tree)

    assert verify_epoch_matches(expected=expected, candidate=candidate) is False


def test_verify_epoch_matches_does_not_treat_absent_tree_as_tree_proof() -> None:
    expected = _epoch(head_sha="head-2", tree_sha=None)
    candidate = _epoch(head_sha="head-1", tree_sha=None)

    assert verify_epoch_matches(expected=expected, candidate=candidate) is False


@pytest.mark.parametrize("candidate_tree_sha", [None, "tree-other"])
def test_verify_epoch_matches_same_head_even_when_tree_metadata_absent_or_contradictory(
    candidate_tree_sha: str | None,
) -> None:
    expected = _epoch(head_sha="head-1", tree_sha="tree-1")
    candidate = _epoch(head_sha="head-1", tree_sha=candidate_tree_sha)

    assert verify_epoch_matches(expected=expected, candidate=candidate) is True


def test_verify_epoch_matches_ignores_command_and_timeout_drift_when_source_matches() -> None:
    expected = _epoch(
        command="./bin/tests --new",
        head_sha="head-2",
        tree_sha="tree-1",
        timeout_seconds=240,
        timeout_grace_seconds=9.0,
    )
    candidate = _epoch(
        command="./bin/tests",
        head_sha="head-1",
        tree_sha="tree-1",
        timeout_seconds=120,
        timeout_grace_seconds=5.0,
    )

    assert verify_epoch_matches(expected=expected, candidate=candidate) is True


def _seed_legacy_review(
    store: SqliteTaskStore,
    *,
    impl_id: str,
    command: str = "./bin/tests",
    head_sha: str = "head-1",
    captured_at: datetime,
) -> None:
    review = store.add("Review verify state", task_type="review", based_on=impl_id, depends_on=impl_id)
    review.status = "completed"
    review.completed_at = captured_at + timedelta(seconds=1)
    review.review_verify_command = command
    review.review_verify_status = "passed"
    review.review_verify_exit_status = "0"
    review.review_verify_captured_at = captured_at
    review.review_verify_branch = "feature/verify"
    review.review_verify_head_sha = head_sha
    review.review_verify_base_sha = "base-1"
    review.review_verify_cwd = "/tmp/worktree"
    store.update(review)


def test_latest_verify_result_for_epoch_prefers_current_owner_artifact(tmp_path: Path) -> None:
    store = SqliteTaskStore(tmp_path / "test.db")
    impl = store.add("Implement verify gate owner", task_type="implement")
    assert impl.id is not None
    review = store.add("Review owner artifact", task_type="review", based_on=impl.id, depends_on=impl.id)

    persist_verify_gate_artifact(
        store,
        _config(tmp_path),
        owner_task=impl,
        source_task=review,
        result=_result(captured_at=datetime(2026, 6, 29, 12, 0, tzinfo=UTC)),
        verify_timeout_seconds=120,
        verify_timeout_grace_seconds=5.0,
        producer="review_verify",
    )

    lookup = latest_verify_result_for_epoch(store, impl, current_epoch=_epoch())

    assert lookup.source == "owner_artifact"
    assert lookup.has_owner_artifact is True
    assert lookup.is_current is True
    assert lookup.result is not None
    assert lookup.result.reviewed_head_sha == "head-1"
    assert len(store.list_artifacts(impl.id, kind=VERIFY_GATE_ARTIFACT_KIND)) == 1


def test_latest_verify_result_for_epoch_selects_same_tree_owner_artifact_after_commit_rewrite(tmp_path: Path) -> None:
    store = SqliteTaskStore(tmp_path / "test.db")
    impl = store.add("Implement tree-aware verify gate owner", task_type="implement")
    assert impl.id is not None
    review = store.add("Review tree-aware owner artifact", task_type="review", based_on=impl.id, depends_on=impl.id)

    persist_verify_gate_artifact(
        store,
        _config(tmp_path),
        owner_task=impl,
        source_task=review,
        result=_result(head_sha="head-1", tree_sha="tree-1", captured_at=datetime(2026, 6, 29, 12, 0, tzinfo=UTC)),
        verify_timeout_seconds=120,
        verify_timeout_grace_seconds=5.0,
        producer="review_verify",
    )

    lookup = latest_verify_result_for_epoch(store, impl, current_epoch=_epoch(head_sha="head-2", tree_sha="tree-1"))

    assert lookup.source == "owner_artifact"
    assert lookup.is_current is True
    assert lookup.result is not None
    assert lookup.result.reviewed_head_sha == "head-1"
    assert lookup.result.reviewed_tree_sha == "tree-1"


def test_latest_verify_result_for_epoch_keeps_legacy_head_only_artifact_stale_after_commit_rewrite(
    tmp_path: Path,
) -> None:
    store = SqliteTaskStore(tmp_path / "test.db")
    impl = store.add("Implement legacy head-only verify gate owner", task_type="implement")
    assert impl.id is not None
    review = store.add("Review legacy owner artifact", task_type="review", based_on=impl.id, depends_on=impl.id)

    captured_at = datetime(2026, 6, 29, 12, 0, tzinfo=UTC)
    metadata = build_verify_gate_artifact_payload(
        result=_result(head_sha="head-1", tree_sha=None, captured_at=captured_at),
        source_task=review,
        verify_timeout_seconds=120,
        verify_timeout_grace_seconds=5.0,
    )
    metadata["verify_epoch"].pop("reviewed_tree_sha")
    metadata["result"].pop("reviewed_tree_sha")
    store.add_artifact(
        impl.id,
        kind=VERIFY_GATE_ARTIFACT_KIND,
        label="verify_gate_result",
        path=".gza/artifacts/legacy-verify.json",
        byte_size=2,
        sha256="0" * 64,
        created_at=captured_at,
        producer="review_verify",
        command="./bin/tests",
        status="passed",
        exit_status="0",
        head_sha="head-1",
        metadata=metadata,
    )

    lookup = latest_verify_result_for_epoch(store, impl, current_epoch=_epoch(head_sha="head-2", tree_sha="tree-1"))

    assert lookup.source == "owner_artifact"
    assert lookup.is_current is False
    assert lookup.result is not None
    assert lookup.result.reviewed_head_sha == "head-1"
    assert lookup.result.reviewed_tree_sha is None


@pytest.mark.parametrize(
    "failure_origin,expected_timeout",
    [
        ("absent", True),
        (123, False),
        (None, False),
        ("", False),
        ("test_failure", False),
    ],
)
def test_artifact_verify_result_only_uses_timeout_text_when_failure_origin_is_absent(
    tmp_path: Path,
    failure_origin: object,
    expected_timeout: bool,
) -> None:
    store = SqliteTaskStore(tmp_path / "test.db")
    impl = store.add("Implement timeout origin parsing", task_type="implement")
    assert impl.id is not None
    review = store.add("Review timeout origin parsing", task_type="review", based_on=impl.id, depends_on=impl.id)
    captured_at = datetime(2026, 8, 17, 12, 0, tzinfo=UTC)
    result = SimpleNamespace(
        command="./bin/tests",
        status="failed",
        exit_status="timed out",
        captured_at=captured_at,
        reviewed_branch="feature/verify",
        reviewed_head_sha="head-1",
        reviewed_base_sha="base-1",
        working_directory="/tmp/worktree",
        failure="verify_command timed out after 120s",
    )
    metadata = build_verify_gate_artifact_payload(
        result=result,
        source_task=review,
        verify_timeout_seconds=120,
        verify_timeout_grace_seconds=5.0,
    )
    if failure_origin == "absent":
        metadata["result"].pop("failure_origin", None)
    else:
        metadata["result"]["failure_origin"] = failure_origin
    store.add_artifact(
        impl.id,
        kind=VERIFY_GATE_ARTIFACT_KIND,
        label="verify_gate_result",
        path=".gza/artifacts/verify.json",
        byte_size=2,
        sha256="0" * 64,
        created_at=captured_at,
        producer="review_verify",
        status="failed",
        exit_status="timed out",
        head_sha="head-1",
        metadata=metadata,
    )

    lookup = latest_verify_result_for_epoch(store, impl, current_epoch=_epoch())

    assert lookup.result is not None
    assert verify_result_is_timeout_origin(lookup.result) is expected_timeout


def test_persist_verify_gate_artifact_stores_provenance_and_cross_project_aggregate_details(tmp_path: Path) -> None:
    store = SqliteTaskStore(tmp_path / "test.db")
    impl = store.add("Implement verify gate owner", task_type="implement")
    assert impl.id is not None
    review = store.add("Review owner artifact", task_type="review", based_on=impl.id, depends_on=impl.id)

    persist_verify_gate_artifact(
        store,
        _config(tmp_path),
        owner_task=impl,
        source_task=review,
        result=_result(captured_at=datetime(2026, 6, 29, 12, 0, tzinfo=UTC)),
        verify_timeout_seconds=120,
        verify_timeout_grace_seconds=5.0,
        producer="review_verify",
        provenance={
            "command_identity": "./bin/tests",
            "reviewed_branch": "feature/verify",
            "reviewed_head_sha": "head-1",
            "reviewed_base_sha": "base-1",
            "working_directory": "/tmp/worktree",
            "config_identity": {
                "verify_command": "./bin/tests",
                "verify_timeout_seconds": 120,
                "verify_timeout_grace_seconds": 5.0,
                "cross_project": True,
            },
        },
        aggregate_details={
            "affected_scope_count": 2,
            "runnable_count": 2,
            "passed_count": 1,
            "failed_count": 1,
            "unavailable_count": 0,
            "skipped_count": 0,
            "scopes": [
                {
                    "scope": "services/foo",
                    "working_directory": "services/foo",
                    "status": "passed",
                    "exit_status": "0",
                    "command_identity": "./bin/foo-verify",
                    "reviewed_branch": "feature/verify",
                    "reviewed_head_sha": "head-1",
                    "reviewed_base_sha": "base-1",
                    "skip_reason": None,
                },
                {
                    "scope": "libs/bar",
                    "working_directory": "libs/bar",
                    "status": "failed",
                    "exit_status": "7",
                    "command_identity": "./bin/bar-verify",
                    "reviewed_branch": "feature/verify",
                    "reviewed_head_sha": "head-1",
                    "reviewed_base_sha": "base-1",
                    "skip_reason": None,
                },
            ],
        },
    )

    artifact = store.list_artifacts(impl.id, kind=VERIFY_GATE_ARTIFACT_KIND)[0]
    assert artifact.metadata is not None
    assert artifact.metadata["provenance"] == {
        "command_identity": "./bin/tests",
        "reviewed_branch": "feature/verify",
        "reviewed_head_sha": "head-1",
        "reviewed_base_sha": "base-1",
        "working_directory": "/tmp/worktree",
        "config_identity": {
            "verify_command": "./bin/tests",
            "verify_timeout_seconds": 120,
            "verify_timeout_grace_seconds": 5.0,
            "cross_project": True,
        },
    }
    assert artifact.metadata["aggregate_details"]["failed_count"] == 1
    assert artifact.metadata["aggregate_details"]["scopes"][1]["scope"] == "libs/bar"
    lookup = latest_verify_result_for_epoch(store, impl, current_epoch=_epoch())
    assert lookup.is_current is True
    assert lookup.source == "owner_artifact"


@pytest.mark.parametrize(
    "aggregate_details",
    [
        {
            "runnable_count": 2,
            "tree_fingerprint": None,
            "tree_fingerprint_complete": False,
            "tree_fingerprint_missing_count": 1,
            "scopes": [],
        },
        {
            "runnable_count": 2,
            "tree_fingerprint": None,
            "tree_fingerprint_complete": False,
            "tree_fingerprint_contradictory": True,
            "scopes": [],
        },
    ],
)
def test_build_verify_gate_artifact_payload_rejects_cross_project_fingerprint_fallback(
    tmp_path: Path,
    aggregate_details: dict[str, object],
) -> None:
    impl = SimpleNamespace(id="gza-1", task_type="implement")

    payload = build_verify_gate_artifact_payload(
        result=_result(captured_at=datetime(2026, 8, 29, 12, 0, tzinfo=UTC)),
        source_task=impl,
        verify_timeout_seconds=120,
        verify_timeout_grace_seconds=5.0,
        provenance={"tree_fingerprint": "f" * 64},
        aggregate_details=aggregate_details,
    )

    assert "tree_fingerprint" not in payload


def test_build_verify_gate_artifact_payload_allows_single_project_fingerprint_fallback(tmp_path: Path) -> None:
    impl = SimpleNamespace(id="gza-1", task_type="implement")

    payload = build_verify_gate_artifact_payload(
        result=_result(captured_at=datetime(2026, 8, 29, 12, 0, tzinfo=UTC)),
        source_task=impl,
        verify_timeout_seconds=120,
        verify_timeout_grace_seconds=5.0,
        provenance={"tree_fingerprint": "f" * 64},
        aggregate_details={
            "schema_version": 1,
            "phase_results": [{"name": "unit", "status": "passed", "duration_seconds": 1.0}],
            "started_phase_names": ["unit"],
            "completed_phase_names": ["unit"],
            "failed_phase_names": [],
            "expected_phase_names": ["unit"],
            "not_started_phase_names": [],
        },
    )

    assert payload["tree_fingerprint"] == "f" * 64


def test_latest_verify_result_for_epoch_marks_canonical_owner_artifact_stale(tmp_path: Path) -> None:
    store = SqliteTaskStore(tmp_path / "test.db")
    impl = store.add("Implement stale canonical verify", task_type="implement")
    assert impl.id is not None
    review = store.add("Review stale canonical verify", task_type="review", based_on=impl.id, depends_on=impl.id)

    persist_verify_gate_artifact(
        store,
        _config(tmp_path),
        owner_task=impl,
        source_task=review,
        result=_result(head_sha="old-head", captured_at=datetime(2026, 6, 29, 12, 0, tzinfo=UTC)),
        verify_timeout_seconds=120,
        verify_timeout_grace_seconds=5.0,
        producer="review_verify",
    )

    lookup = latest_verify_result_for_epoch(store, impl, current_epoch=_epoch(head_sha="new-head"))

    assert lookup.source == "owner_artifact"
    assert lookup.has_owner_artifact is True
    assert lookup.is_current is False
    assert lookup.result is not None
    assert lookup.result.reviewed_head_sha == "old-head"


def test_latest_verify_result_for_epoch_marks_different_branch_stale(tmp_path: Path) -> None:
    store = SqliteTaskStore(tmp_path / "test.db")
    impl = store.add("Implement stale branch canonical verify", task_type="implement")
    assert impl.id is not None
    review = store.add("Review stale branch canonical verify", task_type="review", based_on=impl.id, depends_on=impl.id)

    persist_verify_gate_artifact(
        store,
        _config(tmp_path),
        owner_task=impl,
        source_task=review,
        result=_result(captured_at=datetime(2026, 6, 29, 12, 0, tzinfo=UTC)),
        verify_timeout_seconds=120,
        verify_timeout_grace_seconds=5.0,
        producer="review_verify",
    )

    lookup = latest_verify_result_for_epoch(store, impl, current_epoch=_epoch(branch="feature/other"))

    assert lookup.source == "owner_artifact"
    assert lookup.has_owner_artifact is True
    assert lookup.is_current is False
    assert lookup.result is not None
    assert lookup.result.reviewed_branch == "feature/verify"


def test_resolve_verify_gate_decision_accepts_same_head_per_project_command_placeholder(
    tmp_path: Path,
) -> None:
    store = SqliteTaskStore(tmp_path / "test.db")
    config = _config(tmp_path)
    config.verify_command = "./bin/tests"
    config.autonomous_verify_timeout_seconds = 120
    config.review_verify_timeout_grace_seconds = 5.0

    impl = store.add("Implement cross-project verify gate decision", task_type="implement")
    assert impl.id is not None
    impl.branch = "feature/verify"
    store.update(impl)
    review = store.add("Review cross-project verify gate decision", task_type="review", based_on=impl.id, depends_on=impl.id)

    persist_verify_gate_artifact(
        store,
        config,
        owner_task=impl,
        source_task=review,
        result=_result(
            command="(per-project verify_command)",
            captured_at=datetime(2026, 6, 29, 12, 0, tzinfo=UTC),
        ),
        verify_timeout_seconds=120,
        verify_timeout_grace_seconds=5.0,
        producer="advance_verify_gate",
    )

    git = SimpleNamespace(rev_parse_if_exists=lambda _ref: "head-1")
    decision = resolve_verify_gate_decision(store, impl, config=config, git=git)

    assert decision.state == "passed"
    assert decision.lookup.is_current is True
    assert decision.lookup.result is not None
    assert decision.lookup.result.command == "(per-project verify_command)"


def test_latest_verify_result_for_epoch_falls_back_to_legacy_review_when_owner_artifact_absent(
    tmp_path: Path,
) -> None:
    store = SqliteTaskStore(tmp_path / "test.db")
    impl = store.add("Implement legacy verify fallback", task_type="implement")
    assert impl.id is not None

    captured_at = datetime(2026, 6, 29, 12, 0, tzinfo=UTC)
    _seed_legacy_review(store, impl_id=impl.id, captured_at=captured_at)

    lookup = latest_verify_result_for_epoch(
        store,
        impl,
        current_epoch=make_verify_epoch(
            reviewed_branch="feature/verify",
            reviewed_head_sha="head-1",
            verify_command="./bin/tests",
            verify_timeout_seconds=None,
            verify_timeout_grace_seconds=None,
        ),
    )

    assert lookup.source == "legacy_review"
    assert lookup.has_owner_artifact is False
    assert lookup.is_current is True
    assert lookup.result is not None
    assert lookup.result.captured_at == captured_at


def test_latest_verify_result_for_epoch_accepts_legacy_review_without_persisted_timeout_identity(
    tmp_path: Path,
) -> None:
    store = SqliteTaskStore(tmp_path / "test.db")
    impl = store.add("Implement legacy timeout drift", task_type="implement")
    assert impl.id is not None

    captured_at = datetime(2026, 6, 29, 12, 0, tzinfo=UTC)
    _seed_legacy_review(store, impl_id=impl.id, captured_at=captured_at)

    lookup = latest_verify_result_for_epoch(store, impl, current_epoch=_epoch())

    assert lookup.source == "legacy_review"
    assert lookup.has_owner_artifact is False
    assert lookup.is_current is True
    assert lookup.result is not None
    assert lookup.result.captured_at == captured_at


def test_latest_verify_result_for_epoch_does_not_fallback_to_legacy_when_owner_artifact_exists(
    tmp_path: Path,
) -> None:
    store = SqliteTaskStore(tmp_path / "test.db")
    impl = store.add("Implement fail closed verify fallback", task_type="implement")
    assert impl.id is not None
    review = store.add("Review fail closed verify fallback", task_type="review", based_on=impl.id, depends_on=impl.id)

    persist_verify_gate_artifact(
        store,
        _config(tmp_path),
        owner_task=impl,
        source_task=review,
        result=_result(head_sha="old-head", captured_at=datetime(2026, 6, 29, 12, 0, tzinfo=UTC)),
        verify_timeout_seconds=120,
        verify_timeout_grace_seconds=5.0,
        producer="review_verify",
    )
    _seed_legacy_review(
        store,
        impl_id=impl.id,
        head_sha="head-1",
        captured_at=datetime(2026, 6, 29, 12, 5, tzinfo=UTC),
    )

    lookup = latest_verify_result_for_epoch(store, impl, current_epoch=_epoch())

    assert lookup.source == "owner_artifact"
    assert lookup.has_owner_artifact is True
    assert lookup.is_current is False
    assert lookup.result is not None
    assert lookup.result.reviewed_head_sha == "old-head"


def test_owner_task_verify_epoch_returns_none_when_branch_probe_raises(tmp_path: Path) -> None:
    config = _config(tmp_path)
    config.verify_command = "./bin/tests"
    config.autonomous_verify_timeout_seconds = 120
    config.review_verify_timeout_grace_seconds = 5.0

    store = SqliteTaskStore(tmp_path / "test.db")
    impl = store.add("Implement probe-failure fallback", task_type="implement")
    impl.branch = "feature/verify-probe-failure"
    store.update(impl)

    git = SimpleNamespace(rev_parse_if_exists=lambda _ref: (_ for _ in ()).throw(GitError("boom")))

    assert owner_task_verify_epoch(impl, config, git) is None


def test_owner_task_verify_epoch_records_tree_probe_success(tmp_path: Path) -> None:
    config = _config(tmp_path)
    config.verify_command = "./bin/tests"
    config.autonomous_verify_timeout_seconds = 120
    config.review_verify_timeout_grace_seconds = 5.0

    store = SqliteTaskStore(tmp_path / "test.db")
    impl = store.add("Implement tree epoch", task_type="implement")
    impl.branch = "feature/verify-tree"
    store.update(impl)

    def resolve_refs(refs: list[str], peel: str = "commit") -> dict[str, str | None]:
        assert refs == ["feature/verify-tree"]
        assert peel == "tree"
        return {"feature/verify-tree": "tree-1"}

    epoch = owner_task_verify_epoch(
        impl,
        config,
        SimpleNamespace(rev_parse_if_exists=lambda _ref: "head-1", resolve_refs=resolve_refs),
    )

    assert epoch == make_verify_epoch(
        reviewed_branch="feature/verify-tree",
        reviewed_head_sha="head-1",
        reviewed_tree_sha="tree-1",
        verify_command="./bin/tests",
        verify_timeout_seconds=120,
        verify_timeout_grace_seconds=5.0,
    )


@pytest.mark.parametrize(
    "resolve_refs",
    [
        lambda _refs, peel="commit": {},
        lambda _refs, peel="commit": (_ for _ in ()).throw(GitError("tree probe failed")),
    ],
)
def test_owner_task_verify_epoch_returns_head_only_epoch_when_tree_probe_fails(
    tmp_path: Path,
    resolve_refs: object,
) -> None:
    config = _config(tmp_path)
    config.verify_command = "./bin/tests"
    config.autonomous_verify_timeout_seconds = 120
    config.review_verify_timeout_grace_seconds = 5.0

    store = SqliteTaskStore(tmp_path / "test.db")
    impl = store.add("Implement head-only tree fallback", task_type="implement")
    impl.branch = "feature/verify-tree-fallback"
    store.update(impl)

    epoch = owner_task_verify_epoch(
        impl,
        config,
        SimpleNamespace(rev_parse_if_exists=lambda _ref: "head-1", resolve_refs=resolve_refs),
    )

    assert epoch == make_verify_epoch(
        reviewed_branch="feature/verify-tree-fallback",
        reviewed_head_sha="head-1",
        reviewed_tree_sha=None,
        verify_command="./bin/tests",
        verify_timeout_seconds=120,
        verify_timeout_grace_seconds=5.0,
    )


def test_review_task_verify_epoch_preserves_legacy_timeout_identity_as_none(tmp_path: Path) -> None:
    config = _config(tmp_path)
    config.verify_command = "./bin/tests"
    config.autonomous_verify_timeout_seconds = 120
    config.review_verify_timeout_grace_seconds = 5.0

    store = SqliteTaskStore(tmp_path / "test.db")
    impl = store.add("Implement legacy review epoch", task_type="implement")
    assert impl.id is not None
    review = store.add("Review legacy review epoch", task_type="review", based_on=impl.id, depends_on=impl.id)
    review.review_verify_command = "./bin/tests"
    review.review_verify_branch = "feature/verify"
    review.review_verify_head_sha = "head-1"
    store.update(review)

    legacy_epoch = review_task_verify_epoch(review, config)
    owner_epoch = owner_task_verify_epoch(
        SimpleNamespace(branch="feature/verify"),
        config,
        SimpleNamespace(rev_parse_if_exists=lambda _ref: "head-1"),
    )

    assert legacy_epoch == make_verify_epoch(
        reviewed_branch="feature/verify",
        reviewed_head_sha="head-1",
        verify_command="./bin/tests",
        verify_timeout_seconds=None,
        verify_timeout_grace_seconds=None,
    )
    assert owner_epoch is not None
    assert legacy_epoch != owner_epoch
    assert verify_epoch_matches(expected=owner_epoch, candidate=legacy_epoch) is True


def test_review_task_verify_epoch_stays_stale_across_timeout_config_changes(tmp_path: Path) -> None:
    first_config = _config(tmp_path)
    first_config.verify_command = "./bin/tests"
    first_config.autonomous_verify_timeout_seconds = 120
    first_config.review_verify_timeout_grace_seconds = 5.0

    changed_config = _config(tmp_path)
    changed_config.verify_command = "./bin/tests"
    changed_config.autonomous_verify_timeout_seconds = 240
    changed_config.review_verify_timeout_grace_seconds = 9.0

    store = SqliteTaskStore(tmp_path / "test.db")
    impl = store.add("Implement legacy review timeout drift", task_type="implement")
    assert impl.id is not None
    review = store.add("Review legacy review timeout drift", task_type="review", based_on=impl.id, depends_on=impl.id)
    review.review_verify_command = "./bin/tests"
    review.review_verify_branch = "feature/verify"
    review.review_verify_head_sha = "head-1"
    store.update(review)

    first_epoch = review_task_verify_epoch(review, first_config)
    changed_epoch = review_task_verify_epoch(review, changed_config)
    changed_owner_epoch = owner_task_verify_epoch(
        SimpleNamespace(branch="feature/verify"),
        changed_config,
        SimpleNamespace(rev_parse_if_exists=lambda _ref: "head-1"),
    )

    assert first_epoch == changed_epoch
    assert changed_epoch == make_verify_epoch(
        reviewed_branch="feature/verify",
        reviewed_head_sha="head-1",
        verify_command="./bin/tests",
        verify_timeout_seconds=None,
        verify_timeout_grace_seconds=None,
    )
    assert changed_owner_epoch is not None
    assert changed_epoch != changed_owner_epoch
    assert verify_epoch_matches(expected=changed_owner_epoch, candidate=changed_epoch) is True


def test_resolve_verify_gate_decision_marks_current_failed_owner_artifact_red(tmp_path: Path) -> None:
    store = SqliteTaskStore(tmp_path / "test.db")
    config = _config(tmp_path)
    config.verify_command = "./bin/tests"
    config.autonomous_verify_timeout_seconds = 120
    config.review_verify_timeout_grace_seconds = 5.0

    impl = store.add("Implement verify gate decision", task_type="implement")
    assert impl.id is not None
    impl.branch = "feature/verify"
    store.update(impl)
    review = store.add("Review verify gate decision", task_type="review", based_on=impl.id, depends_on=impl.id)

    persist_verify_gate_artifact(
        store,
        config,
        owner_task=impl,
        source_task=review,
        result=SimpleNamespace(
            command="./bin/tests",
            status="failed",
            exit_status="7",
            captured_at=datetime(2026, 6, 29, 12, 0, tzinfo=UTC),
            reviewed_branch="feature/verify",
            reviewed_head_sha="head-1",
            reviewed_base_sha="base-1",
            working_directory="/tmp/worktree",
            failure=None,
        ),
        verify_timeout_seconds=120,
        verify_timeout_grace_seconds=5.0,
        producer="review_verify",
    )

    git = SimpleNamespace(rev_parse_if_exists=lambda _ref: "head-1")
    decision = resolve_verify_gate_decision(store, impl, config=config, git=git)

    assert decision.state == "failed"
    assert task_has_current_passing_verify_evidence(store, impl, config=config, git=git) is False


def test_later_current_green_verify_evidence_supersedes_stale_red_at_same_head(tmp_path: Path) -> None:
    store = SqliteTaskStore(tmp_path / "test.db")
    config = _config(tmp_path)
    config.verify_command = "./bin/tests"
    config.autonomous_verify_timeout_seconds = 120
    config.review_verify_timeout_grace_seconds = 5.0

    impl = store.add("Implement verify gate decision", task_type="implement")
    assert impl.id is not None
    impl.branch = "feature/verify"
    store.update(impl)
    review = store.add("Review verify gate decision", task_type="review", based_on=impl.id, depends_on=impl.id)

    persist_verify_gate_artifact(
        store,
        config,
        owner_task=impl,
        source_task=review,
        result=SimpleNamespace(
            command="./bin/tests",
            status="failed",
            exit_status="timed out",
            captured_at=datetime(2026, 6, 29, 12, 0, tzinfo=UTC),
            reviewed_branch="feature/verify",
            reviewed_head_sha="head-1",
            reviewed_base_sha="base-1",
            working_directory="/tmp/worktree",
            failure="verify_command timed out after 120s",
        ),
        verify_timeout_seconds=120,
        verify_timeout_grace_seconds=5.0,
        producer="review_verify",
    )
    persist_verify_gate_artifact(
        store,
        config,
        owner_task=impl,
        source_task=review,
        result=_result(captured_at=datetime(2026, 6, 29, 12, 5, tzinfo=UTC)),
        verify_timeout_seconds=120,
        verify_timeout_grace_seconds=5.0,
        producer="verify_fix",
    )

    git = SimpleNamespace(rev_parse_if_exists=lambda _ref: "head-1")
    decision = resolve_verify_gate_decision(store, impl, config=config, git=git)

    assert decision.state == "passed"
    assert decision.lookup.result is not None
    assert decision.lookup.result.status == "passed"


def test_later_current_green_verify_evidence_supersedes_red_with_absent_timeout_provenance(
    tmp_path: Path,
) -> None:
    store = SqliteTaskStore(tmp_path / "test.db")
    config = _config(tmp_path)
    config.verify_command = "./bin/tests"
    config.autonomous_verify_timeout_seconds = 120
    config.review_verify_timeout_grace_seconds = 5.0

    impl = store.add("Implement verify gate decision", task_type="implement")
    assert impl.id is not None
    impl.branch = "feature/verify"
    store.update(impl)
    review = store.add("Review verify gate decision", task_type="review", based_on=impl.id, depends_on=impl.id)

    persist_verify_gate_artifact(
        store,
        config,
        owner_task=impl,
        source_task=review,
        result=SimpleNamespace(
            command="./bin/tests",
            status="failed",
            exit_status="timed out",
            captured_at=datetime(2026, 6, 29, 12, 0, tzinfo=UTC),
            reviewed_branch="feature/verify",
            reviewed_head_sha="head-1",
            reviewed_base_sha="base-1",
            working_directory="/tmp/worktree",
            failure="verify_command timed out after 120s",
        ),
        verify_timeout_seconds=120,
        verify_timeout_grace_seconds=5.0,
        producer="review_verify",
    )
    persist_verify_gate_artifact(
        store,
        config,
        owner_task=impl,
        source_task=review,
        result=_result(captured_at=datetime(2026, 6, 29, 12, 5, tzinfo=UTC)),
        verify_timeout_seconds=None,
        verify_timeout_grace_seconds=None,
        producer="advance_verify_gate",
    )

    git = SimpleNamespace(rev_parse_if_exists=lambda _ref: "head-1")
    decision = resolve_verify_gate_decision(store, impl, config=config, git=git)

    assert decision.state == "passed"
    assert decision.lookup.result is not None
    assert decision.lookup.result.status == "passed"
    assert task_has_current_passing_verify_evidence(store, impl, config=config, git=git) is True


def test_later_current_red_verify_evidence_supersedes_older_green_at_same_head(tmp_path: Path) -> None:
    store = SqliteTaskStore(tmp_path / "test.db")
    config = _config(tmp_path)
    config.verify_command = "./bin/tests"
    config.autonomous_verify_timeout_seconds = 120
    config.review_verify_timeout_grace_seconds = 5.0

    impl = store.add("Implement verify gate decision", task_type="implement")
    assert impl.id is not None
    impl.branch = "feature/verify"
    store.update(impl)
    review = store.add("Review verify gate decision", task_type="review", based_on=impl.id, depends_on=impl.id)

    persist_verify_gate_artifact(
        store,
        config,
        owner_task=impl,
        source_task=review,
        result=_result(captured_at=datetime(2026, 6, 29, 12, 0, tzinfo=UTC)),
        verify_timeout_seconds=120,
        verify_timeout_grace_seconds=5.0,
        producer="review_verify",
    )
    failed_result = _result(captured_at=datetime(2026, 6, 29, 12, 5, tzinfo=UTC))
    failed_result.status = "failed"
    failed_result.exit_status = "1"
    failed_result.failure = "pytest failed"
    persist_verify_gate_artifact(
        store,
        config,
        owner_task=impl,
        source_task=review,
        result=failed_result,
        verify_timeout_seconds=120,
        verify_timeout_grace_seconds=5.0,
        producer="advance_verify_gate",
    )

    git = SimpleNamespace(rev_parse_if_exists=lambda _ref: "head-1")
    decision = resolve_verify_gate_decision(store, impl, config=config, git=git)

    assert decision.state == "failed"
    assert decision.lookup.result is not None
    assert decision.lookup.result.exit_status == "1"


def test_verify_result_is_timeout_origin_uses_structured_exit_status_and_failure(tmp_path: Path) -> None:
    del tmp_path
    timeout_result = _result(captured_at=datetime(2026, 6, 29, 12, 0, tzinfo=UTC))
    timeout_result.status = "failed"
    timeout_result.exit_status = "timed out"
    timeout_result.failure = "verify_command timed out after 120s"
    timeout_result.failure_origin = "timeout"

    failed_result = _result(captured_at=datetime(2026, 6, 29, 12, 1, tzinfo=UTC))
    failed_result.status = "failed"
    failed_result.exit_status = "timed out"
    failed_result.failure = "verify_command timed out after 120s"
    failed_result.failure_origin = "test_failure"

    unknown_result = _result(captured_at=datetime(2026, 6, 29, 12, 2, tzinfo=UTC))
    unknown_result.status = "failed"
    unknown_result.exit_status = "timed out"
    unknown_result.failure = "verify_command timed out after 120s"
    unknown_result.failure_origin = "unknown"

    legacy_timeout_result = _result(captured_at=datetime(2026, 6, 29, 12, 3, tzinfo=UTC))
    legacy_timeout_result.status = "failed"
    legacy_timeout_result.exit_status = "timed out"
    legacy_timeout_result.failure = "verify_command timed out after 120s"
    legacy_timeout_result.failure_origin = None

    assert verify_result_is_timeout_origin(timeout_result) is True
    assert verify_result_is_timeout_origin(failed_result) is False
    assert verify_result_is_timeout_origin(unknown_result) is False
    assert verify_result_is_timeout_origin(legacy_timeout_result) is True


def test_resolve_verify_gate_decision_marks_missing_current_verify_state(tmp_path: Path) -> None:
    store = SqliteTaskStore(tmp_path / "test.db")
    config = _config(tmp_path)
    config.verify_command = "./bin/tests"
    config.autonomous_verify_timeout_seconds = 120
    config.review_verify_timeout_grace_seconds = 5.0

    impl = store.add("Implement missing verify gate decision", task_type="implement")
    assert impl.id is not None
    impl.branch = "feature/verify"
    store.update(impl)

    git = SimpleNamespace(rev_parse_if_exists=lambda _ref: "head-1")
    decision = resolve_verify_gate_decision(store, impl, config=config, git=git)

    assert decision.state == "missing"
    assert task_has_current_passing_verify_evidence(store, impl, config=config, git=git) is False


def test_resolve_verify_read_model_prefers_owner_artifact_for_review_surface(tmp_path: Path) -> None:
    store = SqliteTaskStore(tmp_path / "test.db")
    config = _config(tmp_path)
    config.autonomous_verify_timeout_seconds = 120
    config.review_verify_timeout_grace_seconds = 5.0

    impl = store.add("Implement canonical verify owner", task_type="implement")
    assert impl.id is not None

    review = store.add("Review canonical owner artifact", task_type="review", depends_on=impl.id)
    assert review.id is not None
    review.status = "completed"
    review.review_verify_command = "./bin/tests"
    review.review_verify_status = "failed"
    review.review_verify_exit_status = "7"
    review.review_verify_captured_at = datetime(2026, 6, 29, 12, 0, tzinfo=UTC)
    review.review_verify_branch = "feature/verify"
    review.review_verify_head_sha = "head-1"
    review.review_verify_markdown = "legacy markdown"
    store.update(review)

    persist_verify_gate_artifact(
        store,
        config,
        owner_task=impl,
        source_task=review,
        result=_result(captured_at=datetime(2026, 6, 29, 12, 5, tzinfo=UTC)),
        verify_timeout_seconds=120,
        verify_timeout_grace_seconds=5.0,
        producer="review_verify",
    )

    read_model = resolve_verify_read_model(
        store,
        review,
        owner_task=impl,
        current_epoch=review_task_verify_epoch(review, config),
    )

    assert read_model is not None
    assert read_model.source == "owner_artifact"
    assert read_model.result.status == "passed"
    assert read_model.result.exit_status == "0"
    assert read_model.legacy_markdown is None


def test_resolve_verify_read_model_returns_latest_owner_artifact_when_current_epoch_unavailable(
    tmp_path: Path,
) -> None:
    store = SqliteTaskStore(tmp_path / "test.db")
    config = _config(tmp_path)
    config.autonomous_verify_timeout_seconds = 120
    config.review_verify_timeout_grace_seconds = 5.0

    impl = store.add("Implement missing git freshness probe", task_type="implement")
    assert impl.id is not None

    review = store.add("Review missing git freshness probe", task_type="review", depends_on=impl.id)
    assert review.id is not None
    review.status = "completed"
    store.update(review)

    persist_verify_gate_artifact(
        store,
        config,
        owner_task=impl,
        source_task=review,
        result=_result(captured_at=datetime(2026, 6, 29, 12, 5, tzinfo=UTC)),
        verify_timeout_seconds=120,
        verify_timeout_grace_seconds=5.0,
        producer="review_verify",
    )

    read_model = resolve_verify_read_model(
        store,
        impl,
        owner_task=impl,
        current_epoch=None,
    )

    assert read_model is not None
    assert read_model.source == "owner_artifact"
    assert read_model.is_current is False
    assert read_model.has_owner_artifact is True
    assert read_model.result.status == "passed"
