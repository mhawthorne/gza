from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from gza.db import SqliteTaskStore
from gza.flaky_investigations import (
    DEFAULT_FLAKY_REPRO_RUNS,
    FLAKY_VERIFY_ATTEMPT_ARTIFACT_KIND,
    FLAKY_VERIFY_INCONCLUSIVE_ARTIFACT_KIND,
    FLAKY_VERIFY_INVESTIGATION_ARTIFACT_KIND,
    build_flaky_investigation_prompt,
    build_flaky_reproduction_plan,
    create_or_reuse_flaky_investigations,
    derive_flaky_targeted_command,
    normalize_flaky_investigation_dedup_key,
    run_flaky_reproduction_plan,
)
from gza.off_topic_verify import FailingNode, PytestPassFailCounts, PytestXdistMetadata
from gza.runner import _make_review_verify_result


def _make_evidence(
    *,
    review_id: str,
    impl_id: str,
    assertion_signature: str | None,
    working_directory: str = "/workspace",
    targeted_command: str | None = "uv run pytest tests/cli/test_watch.py::test_worker_registry_race --maxfail=0",
):
    node = FailingNode(
        nodeid="tests/cli/test_watch.py::test_worker_registry_race",
        path="tests/cli/test_watch.py",
        outcome="FAILED",
        assertion_signature=assertion_signature,
        failure_path="tests/cli/test_watch.py",
        failure_line=42,
        traceback_paths=("tests/cli/test_watch.py",),
        trustworthy_attribution=True,
    )
    from gza.flaky_investigations import FlakyInvestigationEvidence

    return FlakyInvestigationEvidence(
        node=node,
        dedup_key=normalize_flaky_investigation_dedup_key(node.nodeid, node.assertion_signature),
        review_task_id=review_id,
        impl_task_id=impl_id,
        merge_unit_id=None,
        reviewed_head_sha="deadbeef",
        tree_fingerprint="f" * 64,
        observed_branch="feature/flaky-investigation",
        target_branch="main",
        verify_command="./bin/tests",
        targeted_command=targeted_command,
        working_directory=working_directory,
        branch_pass_fail_counts=PytestPassFailCounts(failed=1, passed=412),
        xdist=PytestXdistMetadata(enabled=True, worker_count=8, worker_count_raw="8"),
        branch_verify_status="failed",
        branch_verify_exit_status="1",
    )


def test_derive_flaky_targeted_command_supports_bin_tests_wrapper() -> None:
    command = derive_flaky_targeted_command(
        verify_command="./bin/tests",
        nodeids=("tests/cli/test_watch.py::test_worker_registry_race",),
    )

    assert command == "uv run pytest tests/cli/test_watch.py::test_worker_registry_race --maxfail=0"


def test_create_or_reuse_flaky_investigation_dedups_open_task_and_keeps_structured_artifact(
    tmp_path: Path,
) -> None:
    store = SqliteTaskStore(tmp_path / "test.db")
    config = SimpleNamespace(project_dir=tmp_path)

    impl = store.add("Implement slice", task_type="implement")
    review = store.add("Review slice", task_type="review", depends_on=impl.id)
    assert impl.id is not None
    assert review.id is not None

    first = create_or_reuse_flaky_investigations(
        store,
        config=config,
        review_task=review,
        impl_task=impl,
        evidences=(_make_evidence(review_id=review.id, impl_id=impl.id, assertion_signature="assert running == completed"),),
        trigger_source="advance",
    )
    assert [task.id for task in first.created] == ["gza-3"]
    assert first.reused == ()

    [created] = first.created
    assert created.id is not None
    artifacts = store.list_artifacts(created.id, kind=FLAKY_VERIFY_INVESTIGATION_ARTIFACT_KIND)
    assert len(artifacts) == 1
    assert artifacts[0].content_type == "application/json"
    assert artifacts[0].metadata is not None
    assert artifacts[0].metadata["dedup_key"].endswith("assert running == completed")
    assert artifacts[0].metadata["failing_node"]["nodeid"] == "tests/cli/test_watch.py::test_worker_registry_race"
    assert artifacts[0].metadata["reviewed_head_sha"] == "deadbeef"
    assert (tmp_path / artifacts[0].path).exists()

    second = create_or_reuse_flaky_investigations(
        store,
        config=config,
        review_task=review,
        impl_task=impl,
        evidences=(_make_evidence(review_id=review.id, impl_id=impl.id, assertion_signature="assert running == completed"),),
        trigger_source="advance",
    )
    assert second.created == ()
    assert [task.id for task in second.reused] == [created.id]
    assert len(store.list_artifacts(created.id, kind=FLAKY_VERIFY_INVESTIGATION_ARTIFACT_KIND)) == 2


def test_create_or_reuse_flaky_investigation_distinguishes_same_node_different_assertion(
    tmp_path: Path,
) -> None:
    store = SqliteTaskStore(tmp_path / "test.db")
    config = SimpleNamespace(project_dir=tmp_path)

    impl = store.add("Implement slice", task_type="implement")
    review = store.add("Review slice", task_type="review", depends_on=impl.id)
    assert impl.id is not None
    assert review.id is not None

    first = create_or_reuse_flaky_investigations(
        store,
        config=config,
        review_task=review,
        impl_task=impl,
        evidences=(_make_evidence(review_id=review.id, impl_id=impl.id, assertion_signature="assert running == completed"),),
        trigger_source="advance",
    )
    second = create_or_reuse_flaky_investigations(
        store,
        config=config,
        review_task=review,
        impl_task=impl,
        evidences=(_make_evidence(review_id=review.id, impl_id=impl.id, assertion_signature="assert registered == expected"),),
        trigger_source="advance",
    )

    assert len(first.created) == 1
    assert len(second.created) == 1
    assert first.created[0].id != second.created[0].id


def test_build_flaky_investigation_prompt_requires_reproduce_or_record_contract(tmp_path: Path) -> None:
    store = SqliteTaskStore(tmp_path / "test.db")
    impl = store.add("Implement slice", task_type="implement")
    review = store.add("Review slice", task_type="review", depends_on=impl.id)
    assert impl.id is not None
    assert review.id is not None
    evidence = _make_evidence(
        review_id=review.id,
        impl_id=impl.id,
        assertion_signature="assert running == completed",
    )

    prompt = build_flaky_investigation_prompt(
        review_task_id=review.id,
        impl_task_id=impl.id,
        evidence=evidence,
    )

    assert "Contract: REPRODUCE-OR-RECORD." in prompt
    assert "First produce red-under-stress evidence" in prompt
    assert "After any fix, rerun the same targeted harness" in prompt
    assert "structured inconclusive result" in prompt
    assert "Do not default to sleeps, blanket retries, @flaky, or broad timeout increases." in prompt
    assert "uv run gza flaky reproduce <this-task-id>" in prompt


def test_build_flaky_reproduction_plan_preserves_cwd_and_adds_harness_flags(tmp_path: Path) -> None:
    store = SqliteTaskStore(tmp_path / "test.db")
    config = SimpleNamespace(project_dir=tmp_path)
    impl = store.add("Implement slice", task_type="implement")
    review = store.add("Review slice", task_type="review", depends_on=impl.id)
    assert impl.id is not None
    assert review.id is not None

    created = create_or_reuse_flaky_investigations(
        store,
        config=config,
        review_task=review,
        impl_task=impl,
        evidences=(
            _make_evidence(
                review_id=review.id,
                impl_id=impl.id,
                assertion_signature="assert running == completed",
                working_directory=str(tmp_path),
            ),
        ),
        trigger_source="advance",
    ).created[0]
    assert created.id is not None

    with (
        patch("gza.flaky_investigations._pytest_plugin_available", return_value=True),
        patch("gza.flaky_investigations._available_randomization_plugin", return_value="pytest-randomly"),
    ):
        plan = build_flaky_reproduction_plan(
            store,
            project_dir=tmp_path,
            task_id=created.id,
        )

    assert plan.runs == DEFAULT_FLAKY_REPRO_RUNS
    assert plan.working_directory == tmp_path.resolve()
    assert "PYTHONFAULTHANDLER=1" in plan.command
    assert "tests/cli/test_watch.py::test_worker_registry_race" in plan.command
    assert " -n " in f" {plan.command} "
    assert "--randomly-seed=1729" in plan.command


def test_build_flaky_reproduction_plan_derives_targeted_command_from_bin_tests_metadata(
    tmp_path: Path,
) -> None:
    store = SqliteTaskStore(tmp_path / "test.db")
    config = SimpleNamespace(project_dir=tmp_path)
    impl = store.add("Implement slice", task_type="implement")
    review = store.add("Review slice", task_type="review", depends_on=impl.id)
    assert impl.id is not None
    assert review.id is not None

    created = create_or_reuse_flaky_investigations(
        store,
        config=config,
        review_task=review,
        impl_task=impl,
        evidences=(
            _make_evidence(
                review_id=review.id,
                impl_id=impl.id,
                assertion_signature="assert running == completed",
                working_directory=str(tmp_path),
                targeted_command=None,
            ),
        ),
        trigger_source="advance",
    ).created[0]
    assert created.id is not None

    plan = build_flaky_reproduction_plan(
        store,
        project_dir=tmp_path,
        task_id=created.id,
        enable_xdist=False,
        enable_randomization=False,
    )

    assert plan.working_directory == tmp_path.resolve()
    assert "PYTHONFAULTHANDLER=1" in plan.command
    assert "uv run pytest tests/cli/test_watch.py::test_worker_registry_race --maxfail=0" in plan.command


def test_build_flaky_reproduction_plan_omits_optional_plugins_when_unavailable(tmp_path: Path) -> None:
    store = SqliteTaskStore(tmp_path / "test.db")
    config = SimpleNamespace(project_dir=tmp_path)
    impl = store.add("Implement slice", task_type="implement")
    review = store.add("Review slice", task_type="review", depends_on=impl.id)
    assert impl.id is not None
    assert review.id is not None

    created = create_or_reuse_flaky_investigations(
        store,
        config=config,
        review_task=review,
        impl_task=impl,
        evidences=(
            _make_evidence(
                review_id=review.id,
                impl_id=impl.id,
                assertion_signature="assert running == completed",
                working_directory=str(tmp_path),
            ),
        ),
        trigger_source="advance",
    ).created[0]
    assert created.id is not None

    with (
        patch("gza.flaky_investigations._pytest_plugin_available", return_value=False),
        patch("gza.flaky_investigations._available_randomization_plugin", return_value=None),
    ):
        plan = build_flaky_reproduction_plan(
            store,
            project_dir=tmp_path,
            task_id=created.id,
        )

    assert " -n " not in f" {plan.command} "
    assert "--randomly-seed" not in plan.command
    assert "--random-order-seed" not in plan.command


def test_run_flaky_reproduction_plan_persists_attempts_and_inconclusive_record(tmp_path: Path) -> None:
    store = SqliteTaskStore(tmp_path / "test.db")
    config = SimpleNamespace(project_dir=tmp_path)
    impl = store.add("Implement slice", task_type="implement")
    review = store.add("Review slice", task_type="review", depends_on=impl.id)
    assert impl.id is not None
    assert review.id is not None

    created = create_or_reuse_flaky_investigations(
        store,
        config=config,
        review_task=review,
        impl_task=impl,
        evidences=(
            _make_evidence(
                review_id=review.id,
                impl_id=impl.id,
                assertion_signature="assert running == completed",
                working_directory=str(tmp_path),
            ),
        ),
        trigger_source="advance",
    ).created[0]
    assert created.id is not None

    with (
        patch("gza.flaky_investigations._pytest_plugin_available", return_value=False),
        patch("gza.flaky_investigations._available_randomization_plugin", return_value=None),
    ):
        plan = build_flaky_reproduction_plan(
            store,
            project_dir=tmp_path,
            task_id=created.id,
            runs=2,
        )

    green = _make_review_verify_result(
        plan.command,
        status="passed",
        exit_status="0",
        captured_at=datetime(2026, 6, 25, tzinfo=UTC),
        reviewed_branch="feature/flaky-investigation",
        reviewed_head_sha="deadbeef",
        working_directory=str(tmp_path),
        output="== 412 passed in 4.00s ==",
    )

    run = run_flaky_reproduction_plan(
        store,
        project_dir=tmp_path,
        task_id=created.id,
        plan=plan,
        timeout_seconds=120,
        timeout_grace_seconds=5.0,
        hypotheses=("race is timing-sensitive",),
        run_verify_command=lambda *args, **kwargs: green,
    )

    assert run.reproduced is False
    assert len(run.attempts) == 2
    assert run.inconclusive_artifact_id is not None

    attempt_artifacts = store.list_artifacts(created.id, kind=FLAKY_VERIFY_ATTEMPT_ARTIFACT_KIND)
    assert len(attempt_artifacts) == 2
    assert attempt_artifacts[0].metadata is not None
    assert attempt_artifacts[0].metadata["attempt_budget"] == 2
    assert attempt_artifacts[0].metadata["working_directory"] == str(tmp_path.resolve())

    inconclusive_artifacts = store.list_artifacts(created.id, kind=FLAKY_VERIFY_INCONCLUSIVE_ARTIFACT_KIND)
    assert len(inconclusive_artifacts) == 1
    assert inconclusive_artifacts[0].metadata is not None
    assert inconclusive_artifacts[0].metadata["attempt_count"] == 2
    assert inconclusive_artifacts[0].metadata["hypotheses"] == ["race is timing-sensitive"]
    assert len(inconclusive_artifacts[0].metadata["attempt_artifact_ids"]) == 2


def test_run_flaky_reproduction_plan_stops_on_matching_reproduction(tmp_path: Path) -> None:
    store = SqliteTaskStore(tmp_path / "test.db")
    config = SimpleNamespace(project_dir=tmp_path)
    impl = store.add("Implement slice", task_type="implement")
    review = store.add("Review slice", task_type="review", depends_on=impl.id)
    assert impl.id is not None
    assert review.id is not None

    created = create_or_reuse_flaky_investigations(
        store,
        config=config,
        review_task=review,
        impl_task=impl,
        evidences=(
            _make_evidence(
                review_id=review.id,
                impl_id=impl.id,
                assertion_signature="assert running == completed",
                working_directory=str(tmp_path),
            ),
        ),
        trigger_source="advance",
    ).created[0]
    assert created.id is not None

    with (
        patch("gza.flaky_investigations._pytest_plugin_available", return_value=False),
        patch("gza.flaky_investigations._available_randomization_plugin", return_value=None),
    ):
        plan = build_flaky_reproduction_plan(
            store,
            project_dir=tmp_path,
            task_id=created.id,
            runs=3,
        )

    matching_red = _make_review_verify_result(
        plan.command,
        status="failed",
        exit_status="1",
        captured_at=datetime(2026, 6, 25, tzinfo=UTC),
        reviewed_branch="feature/flaky-investigation",
        reviewed_head_sha="deadbeef",
        working_directory=str(tmp_path),
        output=(
            "________________ test_worker_registry_race ________________\n"
            "E   assert running == completed\n"
            "tests/cli/test_watch.py:42: AssertionError\n"
            "=========================== short test summary info ============================\n"
            "FAILED tests/cli/test_watch.py::test_worker_registry_race - assert running == completed\n"
            "========================= 1 failed, 412 passed in 4.00s ========================="
        ),
    )

    run = run_flaky_reproduction_plan(
        store,
        project_dir=tmp_path,
        task_id=created.id,
        plan=plan,
        timeout_seconds=120,
        timeout_grace_seconds=5.0,
        run_verify_command=lambda *args, **kwargs: matching_red,
    )

    assert run.reproduced is True
    assert len(run.attempts) == 1
    assert run.inconclusive_artifact_id is None
    assert len(store.list_artifacts(created.id, kind=FLAKY_VERIFY_ATTEMPT_ARTIFACT_KIND)) == 1
    assert store.list_artifacts(created.id, kind=FLAKY_VERIFY_INCONCLUSIVE_ARTIFACT_KIND) == []
