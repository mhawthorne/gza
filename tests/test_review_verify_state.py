from __future__ import annotations

from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from pathlib import Path

from gza.config import Config
from gza.db import SqliteTaskStore
from gza.review_verify_state import (
    VERIFY_GATE_ARTIFACT_KIND,
    latest_verify_result_for_epoch,
    make_verify_epoch,
    persist_verify_gate_artifact,
    resolve_verify_read_model,
    review_task_verify_epoch,
)


def _config(tmp_path: Path) -> Config:
    return Config(project_dir=tmp_path, project_name="test-project")


def _result(*, command: str = "./bin/tests", head_sha: str = "head-1", captured_at: datetime) -> SimpleNamespace:
    return SimpleNamespace(
        command=command,
        status="passed",
        exit_status="0",
        captured_at=captured_at,
        reviewed_branch="feature/verify",
        reviewed_head_sha=head_sha,
        reviewed_base_sha="base-1",
        working_directory="/tmp/worktree",
        failure=None,
    )


def _epoch(*, command: str = "./bin/tests", head_sha: str = "head-1"):
    return make_verify_epoch(
        reviewed_branch="feature/verify",
        reviewed_head_sha=head_sha,
        verify_command=command,
        verify_timeout_seconds=120,
        verify_timeout_grace_seconds=5.0,
    )


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


def test_latest_verify_result_for_epoch_falls_back_to_legacy_review_when_owner_artifact_absent(
    tmp_path: Path,
) -> None:
    store = SqliteTaskStore(tmp_path / "test.db")
    impl = store.add("Implement legacy verify fallback", task_type="implement")
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
