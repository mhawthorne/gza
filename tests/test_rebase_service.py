from __future__ import annotations

from pathlib import Path
from typing import Any

from gza.config import Config
from gza.db import DuplicateActiveChildError, SqliteTaskStore, Task
from gza.rebase_service import (
    REBASE_EXECUTION_OUTCOME_ARTIFACT_KIND,
    RebaseExecutionOutcome,
    RebaseServiceRequest,
    execute_task_backed_rebase_service,
    _rebase_outcome_key,
)
from gza.runtime_context import RuntimeExecutionContext


class FakeGit:
    def __init__(self, refs: dict[str, str | None], *, contains_target: bool = False) -> None:
        self.refs = refs
        self.contains_target = contains_target
        self.ancestor_checks: list[tuple[str, str]] = []

    def rev_parse_if_exists(self, ref: str) -> str | None:
        return self.refs.get(ref)

    def is_ancestor(self, ancestor: str, descendant: str) -> bool:
        self.ancestor_checks.append((ancestor, descendant))
        return self.contains_target


def _config(tmp_path: Path) -> Config:
    project = tmp_path / "project"
    project.mkdir()
    return Config(project_dir=project, project_name="test")


def _store(config: Config) -> SqliteTaskStore:
    return SqliteTaskStore(config.db_path)


def _create_rebase_task(
    store: SqliteTaskStore,
    parent_task_id: str,
    branch: str,
    target_branch: str,
    *,
    config: Config | None = None,
    trigger_source: str,
) -> Task:
    return store.add(
        "rebase",
        task_type="rebase",
        based_on=parent_task_id,
        branch=branch,
        base_branch=target_branch,
        same_branch=True,
        trigger_source=trigger_source,
        enforce_single_active_sibling=True,
    )


def _parent(store: SqliteTaskStore) -> Task:
    task = store.add("parent", task_type="implement", branch="feature")
    assert task.id is not None
    return task


def _persist_raw_rebase_artifact(
    store: SqliteTaskStore,
    task: Task,
    metadata: dict[str, Any],
) -> None:
    assert task.id is not None
    store.add_artifact(
        task.id,
        kind=REBASE_EXECUTION_OUTCOME_ARTIFACT_KIND,
        label="rebase_execution_outcome",
        path=f".gza/artifacts/{task.id}/raw-rebase.json",
        byte_size=2,
        sha256="0" * 64,
        producer="test",
        status=str(metadata.get("status") or ""),
        metadata=metadata,
    )


def _valid_replay_metadata(
    *,
    parent_task_id: str,
    branch: str = "feature",
    target_ref: str = "main",
    status: str = "provider_conflict_resolved",
    source_head_before: str = "src1",
    target_head_before: str = "tgt1",
    source_head_after: str = "src2",
    target_head_after: str = "tgt1",
    changed_diff: bool = False,
    provider_conflict_resolved: bool = True,
    superseded: bool = False,
) -> dict[str, Any]:
    metadata: dict[str, Any] = {
        "schema_version": 1,
        "parent_task_id": parent_task_id,
        "rebase_task_id": None,
        "branch": branch,
        "target_ref": target_ref,
        "source_head_before": source_head_before,
        "target_head_before": target_head_before,
        "source_head_after": source_head_after,
        "target_head_after": target_head_after,
        "status": status,
        "changed_diff": changed_diff,
        "provider_conflict_resolved": provider_conflict_resolved,
        "superseded": superseded,
        "completion_reason": None,
    }
    metadata["key"] = _rebase_outcome_key(metadata)
    return metadata


def test_rebase_service_skips_when_source_already_contains_target_and_persists_artifact(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path)
    store = _store(config)
    parent = _parent(store)
    git = FakeGit({"feature": "src1", "main": "tgt1"}, contains_target=True)

    result = execute_task_backed_rebase_service(
        config=config,
        store=store,
        git=git,  # type: ignore[arg-type]
        request=RebaseServiceRequest(parent.id, "feature", "main"),
        create_rebase_task=_create_rebase_task,
    )

    assert result.status == "skipped"
    assert result.changed_diff is False
    assert result.rebase_task_id is None
    assert result.artifact_id is not None
    artifacts = store.list_artifacts(parent.id, kind=REBASE_EXECUTION_OUTCOME_ARTIFACT_KIND)
    assert artifacts[0].metadata["status"] == "skipped"
    assert artifacts[0].metadata["source_head_before"] == "src1"
    assert artifacts[0].metadata["target_head_before"] == "tgt1"


def test_rebase_service_run_persists_provider_conflict_resolved_outcome(tmp_path: Path) -> None:
    config = _config(tmp_path)
    store = _store(config)
    parent = _parent(store)
    git = FakeGit({"feature": "src1", "main": "tgt1"}, contains_target=False)

    def executor(
        *,
        config: Config,
        store: SqliteTaskStore,
        rebase_task: Task,
        branch: str,
        target_branch: str,
        remote: bool = False,
        parent_task_id: str | None = None,
        failure_hint_lines: list[str] | None = None,
        runtime_context: RuntimeExecutionContext | None = None,
        outcome_callback=None,
    ) -> int:
        assert outcome_callback is not None
        outcome_callback(
            RebaseExecutionOutcome(
                status="provider_conflict_resolved",
                source_head_before="src1",
                target_head_before="tgt1",
                source_head_after="src2",
                target_head_after="tgt1",
                changed_diff=False,
                provider_conflict_resolved=True,
            )
        )
        store.mark_completed(rebase_task, branch=branch, changed_diff=False)
        return 0

    result = execute_task_backed_rebase_service(
        config=config,
        store=store,
        git=git,  # type: ignore[arg-type]
        request=RebaseServiceRequest(parent.id, "feature", "main", skip_if_target_contained=False),
        create_rebase_task=_create_rebase_task,
        executor=executor,
    )

    assert result.status == "provider_conflict_resolved"
    assert result.changed_diff is False
    assert result.artifact_key
    assert result.rebase_task_id is not None
    artifact = store.list_artifacts(result.rebase_task_id, kind=REBASE_EXECUTION_OUTCOME_ARTIFACT_KIND)[0]
    assert artifact.metadata["provider_conflict_resolved"] is True
    assert artifact.metadata["changed_diff"] is False


def test_rebase_service_reuses_exact_completed_outcome(tmp_path: Path) -> None:
    config = _config(tmp_path)
    store = _store(config)
    parent = _parent(store)
    git = FakeGit({"feature": "src1", "main": "tgt1"}, contains_target=False)
    calls = 0

    def executor(**kwargs) -> int:
        nonlocal calls
        calls += 1
        kwargs["outcome_callback"](
            RebaseExecutionOutcome(
                status="completed_mechanical",
                source_head_before="src1",
                target_head_before="tgt1",
                source_head_after="src2",
                target_head_after="tgt1",
                changed_diff=False,
            )
        )
        git.refs["feature"] = "src2"
        kwargs["store"].mark_completed(
            kwargs["rebase_task"],
            branch=kwargs["branch"],
            changed_diff=False,
        )
        return 0

    first = execute_task_backed_rebase_service(
        config=config,
        store=store,
        git=git,  # type: ignore[arg-type]
        request=RebaseServiceRequest(parent.id, "feature", "main", skip_if_target_contained=False),
        create_rebase_task=_create_rebase_task,
        executor=executor,  # type: ignore[arg-type]
    )
    git.contains_target = True
    second = execute_task_backed_rebase_service(
        config=config,
        store=store,
        git=git,  # type: ignore[arg-type]
        request=RebaseServiceRequest(parent.id, "feature", "main"),
        create_rebase_task=_create_rebase_task,
        executor=executor,  # type: ignore[arg-type]
    )

    assert calls == 1
    assert second.status == "completed_mechanical"
    assert second.rebase_task_id == first.rebase_task_id
    assert second.artifact_key == first.artifact_key
    assert second.source_head_before == "src1"
    assert second.source_head_after == "src2"


def test_rebase_service_replays_provider_outcome_before_generic_containment_skip(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path)
    store = _store(config)
    parent = _parent(store)
    git = FakeGit({"feature": "src1", "main": "tgt1"}, contains_target=False)
    calls = 0

    def executor(**kwargs) -> int:
        nonlocal calls
        calls += 1
        kwargs["outcome_callback"](
            RebaseExecutionOutcome(
                status="provider_conflict_resolved",
                source_head_before="src1",
                target_head_before="tgt1",
                source_head_after="src2",
                target_head_after="tgt1",
                changed_diff=False,
                provider_conflict_resolved=True,
            )
        )
        git.refs["feature"] = "src2"
        kwargs["store"].mark_completed(
            kwargs["rebase_task"],
            branch=kwargs["branch"],
            changed_diff=False,
        )
        return 0

    first = execute_task_backed_rebase_service(
        config=config,
        store=store,
        git=git,  # type: ignore[arg-type]
        request=RebaseServiceRequest(parent.id, "feature", "main", skip_if_target_contained=False),
        create_rebase_task=_create_rebase_task,
        executor=executor,  # type: ignore[arg-type]
    )
    git.contains_target = True

    second = execute_task_backed_rebase_service(
        config=config,
        store=store,
        git=git,  # type: ignore[arg-type]
        request=RebaseServiceRequest(parent.id, "feature", "main"),
        create_rebase_task=_create_rebase_task,
        executor=executor,  # type: ignore[arg-type]
    )

    assert calls == 1
    assert second.status == "provider_conflict_resolved"
    assert second.completed is True
    assert second.rebase_task_id == first.rebase_task_id
    assert second.artifact_key == first.artifact_key
    assert second.source_head_before == "src1"
    assert second.source_head_after == "src2"


def test_rebase_service_changed_head_invalidates_reuse_and_runs_again(tmp_path: Path) -> None:
    config = _config(tmp_path)
    store = _store(config)
    parent = _parent(store)
    calls = 0

    def executor(**kwargs) -> int:
        nonlocal calls
        calls += 1
        source_head = f"src{calls}"
        kwargs["outcome_callback"](
            RebaseExecutionOutcome(
                status="completed_mechanical",
                source_head_before=source_head,
                target_head_before="tgt1",
                source_head_after=f"{source_head}-rebased",
                target_head_after="tgt1",
                changed_diff=False,
            )
        )
        kwargs["store"].mark_completed(
            kwargs["rebase_task"],
            branch=kwargs["branch"],
            changed_diff=False,
        )
        return 0

    first_git = FakeGit({"feature": "src1", "main": "tgt1"}, contains_target=False)
    second_git = FakeGit({"feature": "src2", "main": "tgt1"}, contains_target=False)
    execute_task_backed_rebase_service(
        config=config,
        store=store,
        git=first_git,  # type: ignore[arg-type]
        request=RebaseServiceRequest(parent.id, "feature", "main", skip_if_target_contained=False),
        create_rebase_task=_create_rebase_task,
        executor=executor,  # type: ignore[arg-type]
    )
    second = execute_task_backed_rebase_service(
        config=config,
        store=store,
        git=second_git,  # type: ignore[arg-type]
        request=RebaseServiceRequest(parent.id, "feature", "main"),
        create_rebase_task=_create_rebase_task,
        executor=executor,  # type: ignore[arg-type]
    )

    assert calls == 2
    assert second.source_head_before == "src2"


def test_rebase_service_changed_target_head_invalidates_reuse_and_runs_again(tmp_path: Path) -> None:
    config = _config(tmp_path)
    store = _store(config)
    parent = _parent(store)
    git = FakeGit({"feature": "src1", "main": "tgt1"}, contains_target=False)
    calls = 0

    def executor(**kwargs) -> int:
        nonlocal calls
        calls += 1
        target_head = git.refs["main"]
        kwargs["outcome_callback"](
            RebaseExecutionOutcome(
                status="completed_mechanical",
                source_head_before=git.refs["feature"],
                target_head_before=target_head,
                source_head_after="src2",
                target_head_after=target_head,
                changed_diff=False,
            )
        )
        git.refs["feature"] = "src2"
        kwargs["store"].mark_completed(
            kwargs["rebase_task"],
            branch=kwargs["branch"],
            changed_diff=False,
        )
        return 0

    first = execute_task_backed_rebase_service(
        config=config,
        store=store,
        git=git,  # type: ignore[arg-type]
        request=RebaseServiceRequest(parent.id, "feature", "main", skip_if_target_contained=False),
        create_rebase_task=_create_rebase_task,
        executor=executor,  # type: ignore[arg-type]
    )
    git.refs["main"] = "tgt2"

    second = execute_task_backed_rebase_service(
        config=config,
        store=store,
        git=git,  # type: ignore[arg-type]
        request=RebaseServiceRequest(parent.id, "feature", "main", skip_if_target_contained=False),
        create_rebase_task=_create_rebase_task,
        executor=executor,  # type: ignore[arg-type]
    )

    assert first.target_head_after == "tgt1"
    assert calls == 2
    assert second.target_head_before == "tgt2"


def test_rebase_service_queue_result_is_non_completed(tmp_path: Path) -> None:
    config = _config(tmp_path)
    store = _store(config)
    parent = _parent(store)
    git = FakeGit({"feature": "src1", "main": "tgt1"}, contains_target=False)

    result = execute_task_backed_rebase_service(
        config=config,
        store=store,
        git=git,  # type: ignore[arg-type]
        request=RebaseServiceRequest(
            parent.id,
            "feature",
            "main",
            run=False,
            skip_if_target_contained=False,
        ),
        create_rebase_task=_create_rebase_task,
    )

    assert result.status == "queued"
    assert result.completed is False
    assert result.rebase_task_id is not None
    children = store.get_based_on_children_by_type(parent.id, "rebase")
    assert len(children) == 1
    assert children[0].id == result.rebase_task_id
    assert children[0].status == "pending"


def test_rebase_service_exact_duplicate_returns_in_progress_state(tmp_path: Path) -> None:
    config = _config(tmp_path)
    store = _store(config)
    parent = _parent(store)
    active = _create_rebase_task(store, parent.id, "feature", "main", trigger_source="manual")
    assert active.id is not None
    git = FakeGit({"feature": "src1", "main": "tgt1"}, contains_target=False)

    def duplicate_factory(*args, **kwargs) -> Task:
        raise DuplicateActiveChildError(active)

    result = execute_task_backed_rebase_service(
        config=config,
        store=store,
        git=git,  # type: ignore[arg-type]
        request=RebaseServiceRequest(parent.id, "feature", "main", skip_if_target_contained=False),
        create_rebase_task=duplicate_factory,  # type: ignore[arg-type]
        executor=lambda **kwargs: 0,  # type: ignore[arg-type]
    )

    assert result.status == "in_progress"
    assert result.completed is False
    assert result.rebase_task_id == active.id


def test_rebase_service_duplicate_branch_or_target_mismatch_returns_identity_conflict(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path)
    store = _store(config)
    parent = _parent(store)
    git = FakeGit({"feature": "src1", "main": "tgt1"}, contains_target=False)

    for active in (
        Task(id="rebase-branch", prompt="", task_type="rebase", based_on=parent.id, branch="other", base_branch="main"),
        Task(id="rebase-target", prompt="", task_type="rebase", based_on=parent.id, branch="feature", base_branch="release"),
    ):

        def duplicate_factory(*args, **kwargs) -> Task:
            raise DuplicateActiveChildError(active)

        result = execute_task_backed_rebase_service(
            config=config,
            store=store,
            git=git,  # type: ignore[arg-type]
            request=RebaseServiceRequest(parent.id, "feature", "main", skip_if_target_contained=False),
            create_rebase_task=duplicate_factory,  # type: ignore[arg-type]
            executor=lambda **kwargs: 0,  # type: ignore[arg-type]
        )

        assert result.status == "identity_conflict"
        assert result.completed is False
        assert result.rebase_task_id == active.id


def test_rebase_service_duplicate_parent_mismatch_returns_identity_conflict(tmp_path: Path) -> None:
    config = _config(tmp_path)
    store = _store(config)
    parent = _parent(store)
    other_parent = _parent(store)
    assert other_parent.id is not None
    active = Task(
        id="rebase-parent",
        prompt="",
        task_type="rebase",
        based_on=other_parent.id,
        branch="feature",
        base_branch="main",
    )
    git = FakeGit({"feature": "src1", "main": "tgt1"}, contains_target=False)

    def duplicate_factory(*args, **kwargs) -> Task:
        raise DuplicateActiveChildError(active)

    result = execute_task_backed_rebase_service(
        config=config,
        store=store,
        git=git,  # type: ignore[arg-type]
        request=RebaseServiceRequest(parent.id, "feature", "main", skip_if_target_contained=False),
        create_rebase_task=duplicate_factory,  # type: ignore[arg-type]
        executor=lambda **kwargs: 0,  # type: ignore[arg-type]
    )

    assert result.status == "identity_conflict"
    assert result.completed is False
    assert result.rebase_task_id == active.id


def test_rebase_service_malformed_replay_artifact_fails_closed_before_containment_skip(
    tmp_path: Path,
) -> None:
    config = _config(tmp_path)
    store = _store(config)

    for field, value in (
        ("schema_version", 999),
        ("key", "not-the-key"),
        ("source_head_after", None),
        ("target_head_after", None),
    ):
        case_parent = _parent(store)
        assert case_parent.id is not None
        rebase_task = store.add(
            "rebase",
            task_type="rebase",
            based_on=case_parent.id,
            branch="feature",
            base_branch="main",
            same_branch=True,
        )
        metadata = _valid_replay_metadata(parent_task_id=case_parent.id)
        metadata[field] = value
        _persist_raw_rebase_artifact(store, rebase_task, metadata)
        git = FakeGit({"feature": "src2", "main": "tgt1"}, contains_target=True)

        result = execute_task_backed_rebase_service(
            config=config,
            store=store,
            git=git,  # type: ignore[arg-type]
            request=RebaseServiceRequest(case_parent.id, "feature", "main"),
            create_rebase_task=_create_rebase_task,
            executor=lambda **kwargs: 0,  # type: ignore[arg-type]
        )

        assert result.status == "proof_unavailable"
        assert result.completed is False
        assert result.rebase_task_id is None
        assert result.fact is not None
        assert "malformed" in result.fact


def test_rebase_service_ambiguous_replay_artifacts_fail_closed(tmp_path: Path) -> None:
    config = _config(tmp_path)
    store = _store(config)
    parent = _parent(store)
    assert parent.id is not None
    git = FakeGit({"feature": "src2", "main": "tgt1"}, contains_target=True)

    for prompt in ("first rebase", "second rebase"):
        rebase_task = store.add(
            prompt,
            task_type="rebase",
            based_on=parent.id,
            branch="feature",
            base_branch="main",
            same_branch=True,
        )
        _persist_raw_rebase_artifact(
            store,
            rebase_task,
            _valid_replay_metadata(parent_task_id=parent.id),
        )

    result = execute_task_backed_rebase_service(
        config=config,
        store=store,
        git=git,  # type: ignore[arg-type]
        request=RebaseServiceRequest(parent.id, "feature", "main"),
        create_rebase_task=_create_rebase_task,
        executor=lambda **kwargs: 0,  # type: ignore[arg-type]
    )

    assert result.status == "proof_unavailable"
    assert result.completed is False
    assert result.rebase_task_id is None
    assert result.fact == "stored rebase outcome is ambiguous"


def test_rebase_service_failed_execution_persists_failed_artifact(tmp_path: Path) -> None:
    config = _config(tmp_path)
    store = _store(config)
    parent = _parent(store)
    git = FakeGit({"feature": "src1", "main": "tgt1"}, contains_target=False)

    def executor(**kwargs) -> int:
        return 1

    result = execute_task_backed_rebase_service(
        config=config,
        store=store,
        git=git,  # type: ignore[arg-type]
        request=RebaseServiceRequest(parent.id, "feature", "main", skip_if_target_contained=False),
        create_rebase_task=_create_rebase_task,
        executor=executor,  # type: ignore[arg-type]
    )

    assert result.status == "failed"
    assert result.exit_code == 1
    assert result.artifact_id is not None
    artifact = store.list_artifacts(result.rebase_task_id, kind=REBASE_EXECUTION_OUTCOME_ARTIFACT_KIND)[0]
    assert artifact.metadata["status"] == "failed"
    assert artifact.exit_status == "1"


def test_rebase_service_fails_closed_when_required_ref_proof_is_unavailable(tmp_path: Path) -> None:
    config = _config(tmp_path)
    store = _store(config)
    parent = _parent(store)
    git = FakeGit({"feature": "src1", "main": None})

    result = execute_task_backed_rebase_service(
        config=config,
        store=store,
        git=git,  # type: ignore[arg-type]
        request=RebaseServiceRequest(parent.id, "feature", "main"),
        create_rebase_task=_create_rebase_task,
    )

    assert result.status == "proof_unavailable"
    assert result.exit_code == 1
    assert result.rebase_task_id is None
    assert store.get_based_on_children_by_type(parent.id, "rebase") == []
