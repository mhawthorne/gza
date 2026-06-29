"""Tests for review_tasks helpers."""

import hashlib
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from gza.artifacts import store_command_output_artifact
from gza.config import Config
from gza.db import SqliteTaskStore
from gza.db import Task
from gza.review_scope import parse_spec_coherence_review_scope
from gza.review_tasks import (
    DuplicateReviewError,
    OFF_TOPIC_VERIFY_INVESTIGATION_ARTIFACT_KIND,
    VerifyFixContextError,
    build_deferred_blocker_prompt_prefix,
    build_followup_prompt,
    build_followup_prompt_prefix,
    build_spec_coherence_review_prompt,
    build_review_blocker_adjudication_prompt,
    build_review_blocker_adjudication_prompt_prefix,
    build_auto_review_prompt,
    build_verify_fix_prompt,
    create_or_reuse_deferred_blocker_task,
    create_or_reuse_followup_task,
    create_or_reuse_review_blocker_adjudication_task,
    create_spec_coherence_review_task,
    create_or_reuse_verify_fix_task,
    create_review_task,
    create_resolution_review_task,
    extract_deferred_blocker_prompt_parts,
    extract_followup_prompt_parts,
    extract_review_blocker_adjudication_dispute_reference,
    extract_review_blocker_adjudication_dispute_identity,
    format_verify_fix_context,
    find_existing_deferred_blocker_task,
    find_existing_followup_task,
    find_existing_review_blocker_adjudication_task,
    find_existing_verify_fix_task,
    format_blocker_finding_context,
    format_followup_finding_context,
    persist_off_topic_verify_clearance,
    resolve_verify_fix_context,
)
from gza.review_verify_state import VerifyEpoch, persist_verify_gate_artifact
from gza.review_verdict import ReviewFinding


def _task(**overrides) -> Task:
    defaults = dict(
        id=1,
        prompt="implement widgets",
        status="completed",
        task_type="implement",
        slug=None,
    )
    defaults.update(overrides)
    return Task(**defaults)


def _make_store(tmp_path: Path) -> tuple[Config, SqliteTaskStore]:
    (tmp_path / "gza.yaml").write_text("project_name: test-project\n")
    config = Config.load(tmp_path)
    db_path = tmp_path / ".gza" / "gza.db"
    db_path.parent.mkdir(parents=True, exist_ok=True)
    return config, SqliteTaskStore(db_path, prefix=config.project_prefix)


def _seed_failed_verify_evidence(
    *,
    config: Config,
    store: SqliteTaskStore,
    impl: Task,
    source_task: Task,
    epoch: VerifyEpoch,
    output: str = "phase=pytest\nAssertionError: expected green\n",
) -> str:
    stored_output = store_command_output_artifact(
        store,
        source_task,
        config,
        kind="verify_command_output",
        producer="test",
        label="verify_command_output",
        output=output,
        command=epoch.verify_command,
        status="failed",
        exit_status="1",
        head_sha=epoch.reviewed_head_sha,
        created_at=datetime(2026, 6, 29, 12, 0, tzinfo=UTC),
    )
    result = type(
        "Result",
        (),
        {
            "command": epoch.verify_command,
            "status": "failed",
            "exit_status": "1",
            "captured_at": datetime(2026, 6, 29, 12, 0, tzinfo=UTC),
            "reviewed_branch": epoch.reviewed_branch,
            "reviewed_head_sha": epoch.reviewed_head_sha,
            "reviewed_base_sha": "base-sha",
            "working_directory": str(config.project_dir / "worktrees" / "verify"),
            "failure": "pytest failed",
        },
    )()
    persist_verify_gate_artifact(
        store,
        config,
        owner_task=impl,
        source_task=source_task,
        result=result,
        verify_timeout_seconds=epoch.verify_timeout_seconds,
        verify_timeout_grace_seconds=epoch.verify_timeout_grace_seconds,
        output_artifact_id=stored_output.id,
        output_artifact_task_id=source_task.id,
        output_artifact_path=stored_output.path,
        producer="test",
    )
    return stored_output.path


# ---------------------------------------------------------------------------
# DuplicateReviewError
# ---------------------------------------------------------------------------


class TestDuplicateReviewError:
    def test_stores_active_review(self):
        task = _task(id=42, status="in_progress")
        err = DuplicateReviewError(task)
        assert err.active_review is task

    def test_message_contains_id_and_status(self):
        task = _task(id=7, status="pending")
        err = DuplicateReviewError(task)
        assert "7" in str(err)
        assert "pending" in str(err)

    def test_is_value_error(self):
        err = DuplicateReviewError(_task())
        assert isinstance(err, ValueError)


class TestVerifyFixTasks:
    def test_build_verify_fix_prompt_keys_on_epoch(self) -> None:
        prompt = build_verify_fix_prompt(
            "gza-101",
            VerifyEpoch(
                reviewed_branch="feature/test",
                reviewed_head_sha="deadbeef",
                verify_command="./bin/tests",
                verify_timeout_seconds=1800,
                verify_timeout_grace_seconds=5.0,
            ),
        )

        assert prompt == (
            "Fix verify failures for task gza-101 "
            "[branch=feature/test head=deadbeef command=./bin/tests timeout=1800 grace=5.0]"
        )

    def test_create_or_reuse_verify_fix_task_reuses_same_epoch(self, tmp_path: Path) -> None:
        config, store = _make_store(tmp_path)
        impl = store.add("Implement feature", task_type="implement")
        improve = store.add("Improve feature", task_type="improve", based_on=impl.id, same_branch=True)
        epoch = VerifyEpoch(
            reviewed_branch="feature/test",
            reviewed_head_sha="deadbeef",
            verify_command="./bin/tests",
            verify_timeout_seconds=1800,
            verify_timeout_grace_seconds=5.0,
        )
        _seed_failed_verify_evidence(
            config=config,
            store=store,
            impl=impl,
            source_task=improve,
            epoch=epoch,
        )

        created, did_create = create_or_reuse_verify_fix_task(
            store,
            config,
            impl_task=impl,
            based_on_task=improve,
            verify_epoch=epoch,
            trigger_source="advance",
        )
        reused, reused_create = create_or_reuse_verify_fix_task(
            store,
            config,
            impl_task=impl,
            based_on_task=improve,
            verify_epoch=epoch,
            trigger_source="advance",
        )

        assert did_create is True
        assert reused_create is False
        assert reused.id == created.id
        assert created.task_type == "verify_fix"
        assert created.same_branch is True
        assert created.based_on == improve.id

    def test_create_or_reuse_verify_fix_task_creates_new_lane_for_new_epoch(self, tmp_path: Path) -> None:
        config, store = _make_store(tmp_path)
        impl = store.add("Implement feature", task_type="implement")
        rebase = store.add("Rebase feature", task_type="rebase", based_on=impl.id, same_branch=True)
        first_epoch = VerifyEpoch(
            reviewed_branch="feature/test",
            reviewed_head_sha="deadbeef",
            verify_command="./bin/tests",
            verify_timeout_seconds=1800,
            verify_timeout_grace_seconds=5.0,
        )
        second_epoch = VerifyEpoch(
            reviewed_branch="feature/test",
            reviewed_head_sha="feedface",
            verify_command="./bin/tests",
            verify_timeout_seconds=1800,
            verify_timeout_grace_seconds=5.0,
        )
        _seed_failed_verify_evidence(
            config=config,
            store=store,
            impl=impl,
            source_task=rebase,
            epoch=first_epoch,
            output="first failure\n",
        )
        _seed_failed_verify_evidence(
            config=config,
            store=store,
            impl=impl,
            source_task=rebase,
            epoch=second_epoch,
            output="second failure\n",
        )

        first, first_created = create_or_reuse_verify_fix_task(
            store,
            config,
            impl_task=impl,
            based_on_task=rebase,
            verify_epoch=first_epoch,
            trigger_source="advance",
        )
        second, second_created = create_or_reuse_verify_fix_task(
            store,
            config,
            impl_task=impl,
            based_on_task=rebase,
            verify_epoch=second_epoch,
            trigger_source="advance",
        )

        assert first_created is True
        assert second_created is True
        assert second.id != first.id
        assert find_existing_verify_fix_task(store, impl_task_id=impl.id, verify_epoch=first_epoch).id == first.id
        assert find_existing_verify_fix_task(store, impl_task_id=impl.id, verify_epoch=second_epoch).id == second.id

    def test_create_or_reuse_verify_fix_task_fails_closed_without_matching_failed_evidence(
        self, tmp_path: Path
    ) -> None:
        config, store = _make_store(tmp_path)
        impl = store.add("Implement feature", task_type="implement")
        improve = store.add("Improve feature", task_type="improve", based_on=impl.id, same_branch=True)
        epoch = VerifyEpoch(
            reviewed_branch="feature/test",
            reviewed_head_sha="deadbeef",
            verify_command="./bin/tests",
            verify_timeout_seconds=1800,
            verify_timeout_grace_seconds=5.0,
        )

        with pytest.raises(VerifyFixContextError, match="no current failed verify evidence"):
            create_or_reuse_verify_fix_task(
                store,
                config,
                impl_task=impl,
                based_on_task=improve,
                verify_epoch=epoch,
                trigger_source="advance",
            )

    def test_resolve_verify_fix_context_formats_failed_verify_evidence(self, tmp_path: Path) -> None:
        config, store = _make_store(tmp_path)
        impl = store.add("Implement feature", task_type="implement")
        assert impl.id is not None
        improve = store.add("Improve feature", task_type="improve", based_on=impl.id, same_branch=True)
        epoch = VerifyEpoch(
            reviewed_branch="feature/test",
            reviewed_head_sha="deadbeef",
            verify_command="./bin/tests",
            verify_timeout_seconds=1800,
            verify_timeout_grace_seconds=5.0,
        )
        artifact_path = _seed_failed_verify_evidence(
            config=config,
            store=store,
            impl=impl,
            source_task=improve,
            epoch=epoch,
            output="setup ok\npytest failed\nAssertionError: expected green\n",
        )

        context = resolve_verify_fix_context(
            store,
            config,
            impl_task=impl,
            verify_epoch=epoch,
        )
        rendered = format_verify_fix_context(context)

        assert context.artifact_path == artifact_path
        assert "- Status: `failed`" in rendered
        assert "- Exit status: `1`" in rendered
        assert "- Command: `./bin/tests`" in rendered
        assert "- Working directory: " in rendered
        assert "- Reviewed branch: `feature/test`" in rendered
        assert "- Reviewed head: `deadbeef`" in rendered
        assert "- Reviewed base/default SHA: `base-sha`" in rendered
        assert "- Failure: pytest failed" in rendered
        assert f"- Output artifact path: `{artifact_path}`" in rendered
        assert "AssertionError: expected green" in rendered


# ---------------------------------------------------------------------------
# build_auto_review_prompt
# ---------------------------------------------------------------------------


class TestBuildAutoReviewPrompt:
    def test_slug_from_task_id(self):
        task = _task(slug="20260315-add-widget-support-1")
        result = build_auto_review_prompt(task)
        assert result == "review add-widget-support"

    def test_slug_without_trailing_revision(self):
        task = _task(slug="20260315-refactor-db")
        result = build_auto_review_prompt(task)
        assert result == "review refactor-db"

    def test_slug_strips_only_trailing_number(self):
        """Trailing ``-<digits>`` is treated as a revision suffix by design."""
        task = _task(slug="20260315-fix-bug-42")
        result = build_auto_review_prompt(task)
        assert result == "review fix-bug"

    def test_slug_strips_trailing_year_like_suffix_as_revision(self):
        """Even year-like suffixes are normalized as trailing revision numbers."""
        task = _task(slug="20260315-security-rollout-2024")
        result = build_auto_review_prompt(task)
        assert result == "review security-rollout"

    def test_slug_strips_derived_implement_prefix(self):
        task = _task(slug="20260410-1234-impl-add-authentication-system")
        result = build_auto_review_prompt(task, known_task_id_suffixes={"1234"})
        assert result == "review add-authentication-system"

    def test_slug_strips_nested_derived_implement_prefixes(self):
        task = _task(slug="20260410-12-impl-11-impl-add-feature")
        result = build_auto_review_prompt(task, known_task_id_suffixes={"12", "11"})
        assert result == "review add-feature"

    def test_slug_preserves_semantic_impl_subject_add(self):
        task = _task(slug="20260410-1234-impl-add-impl-support")
        result = build_auto_review_prompt(task, known_task_id_suffixes={"1234"})
        assert result == "review add-impl-support"

    def test_slug_preserves_semantic_impl_subject_api(self):
        task = _task(slug="20260410-1234-impl-api-impl-migration")
        result = build_auto_review_prompt(task, known_task_id_suffixes={"1234"})
        assert result == "review api-impl-migration"

    def test_slug_preserves_semantic_impl_subject_ui(self):
        task = _task(slug="20260410-1234-impl-ui-impl-refresh")
        result = build_auto_review_prompt(task, known_task_id_suffixes={"1234"})
        assert result == "review ui-impl-refresh"

    def test_slug_preserves_semantic_impl_subject_db(self):
        task = _task(slug="20260410-1234-impl-db-impl-migration")
        result = build_auto_review_prompt(task, known_task_id_suffixes={"1234"})
        assert result == "review db-impl-migration"


def test_build_spec_coherence_review_prompt_requires_standard_review_sections() -> None:
    prompt = build_spec_coherence_review_prompt(
        _task(id="gza-7392"),
        changed_paths=("specs/behavior/lifecycle-engine.md",),
    )

    assert "Run /gza-spec-coherence for implementation task gza-7392." in prompt
    assert "`## Summary`" in prompt
    assert "`## Blockers`" in prompt
    assert "`## Follow-Ups`" in prompt
    assert "`## Questions / Assumptions`" in prompt
    assert "`## Verdict`" in prompt
    assert "`APPROVED`, `CHANGES_REQUESTED`, or `NEEDS_DISCUSSION`" in prompt
    assert "write `None.`" in prompt


def test_create_spec_coherence_review_task_sets_scope_and_prompt_contract(tmp_path: Path) -> None:
    _config, store = _make_store(tmp_path)
    impl = store.add("Update behavior spec", task_type="implement")
    assert impl.id is not None
    impl.status = "completed"
    impl.tags = ("existing-tag",)
    store.update(impl)

    review = create_spec_coherence_review_task(
        store,
        impl,
        reviewed_head_sha="head123",
        changed_paths=("specs/behavior/lifecycle-engine.md",),
        trigger_source="advance",
    )

    persisted = store.get(review.id)
    assert persisted is not None
    scope = parse_spec_coherence_review_scope(persisted.review_scope)
    assert scope is not None
    assert scope.implementation_task_id == impl.id
    assert scope.reviewed_head_sha == "head123"
    assert scope.changed_paths == ("specs/behavior/lifecycle-engine.md",)
    assert persisted.tags == ("existing-tag", "spec-coherence", "specs-behavior")
    assert "`## Verdict`" in persisted.prompt

    def test_slug_preserves_semantic_impl_subject_api2(self):
        task = _task(slug="20260410-1234-impl-api2-impl-refresh")
        result = build_auto_review_prompt(task, known_task_id_suffixes={"1234"})
        assert result == "review api2-impl-refresh"

    def test_slug_preserves_semantic_impl_subject_v2(self):
        task = _task(slug="20260410-1234-impl-v2-impl-rollout")
        result = build_auto_review_prompt(task, known_task_id_suffixes={"1234"})
        assert result == "review v2-impl-rollout"

    def test_slug_preserves_digit_leading_semantic_subject_2fa(self):
        task = _task(slug="20260410-1234-impl-2fa-impl-login")
        result = build_auto_review_prompt(task, known_task_id_suffixes={"1234"})
        assert result == "review 2fa-impl-login"

    def test_slug_preserves_digit_leading_semantic_subject_3d(self):
        task = _task(slug="20260410-1234-impl-3d-impl-preview")
        result = build_auto_review_prompt(task, known_task_id_suffixes={"1234"})
        assert result == "review 3d-impl-preview"

    def test_slug_preserves_digit_leading_semantic_subject_2024(self):
        task = _task(slug="20260410-1234-impl-2024-impl-rollout")
        result = build_auto_review_prompt(task, known_task_id_suffixes={"1234"})
        assert result == "review 2024-impl-rollout"

    def test_project_prefix_stripped_after_derived_normalization(self):
        task = _task(slug="20260410-1234-impl-myproj-add-feature")
        result = build_auto_review_prompt(
            task,
            project_prefix="myproj",
            known_task_id_suffixes={"1234"},
        )
        assert result == "review add-feature"

    def test_project_prefix_not_stripped_without_exact_prefix_token(self):
        task = _task(slug="20260410-1234-impl-myproj2-add-feature")
        result = build_auto_review_prompt(
            task,
            project_prefix="myproj",
            known_task_id_suffixes={"1234"},
        )
        assert result == "review myproj2-add-feature"

    def test_fallback_when_no_task_id(self):
        task = _task(id=5, slug=None, prompt="build the thing")
        result = build_auto_review_prompt(task)
        assert result == "Review task 5"

    def test_fallback_when_task_id_has_no_dash(self):
        task = _task(id=5, slug="nodash", prompt="build the thing")
        result = build_auto_review_prompt(task)
        assert result == "Review task 5"

    def test_fallback_does_not_include_impl_prompt_excerpt(self):
        long_prompt = "x" * 200
        task = _task(id=3, slug=None, prompt=long_prompt)
        result = build_auto_review_prompt(task)
        assert result == "Review task 3"

    def test_fallback_without_prompt(self):
        task = _task(id=3, slug=None, prompt=None)
        result = build_auto_review_prompt(task)
        assert result == "Review task 3"


# ---------------------------------------------------------------------------
# create_review_task
# ---------------------------------------------------------------------------


class TestCreateReviewTask:
    def _mock_store(self, existing_reviews=None):
        store = MagicMock()
        store.get_reviews_for_task.return_value = existing_reviews or []
        store.add.return_value = _task(id=99, task_type="review")
        store.default_merge_target.return_value = "main"
        return store

    def test_rejects_non_implement_task(self):
        store = self._mock_store()
        task = _task(task_type="explore")
        with pytest.raises(ValueError, match="explore task"):
            create_review_task(store, task, trigger_source="manual")

    def test_rejects_non_completed_task(self):
        store = self._mock_store()
        task = _task(status="failed")
        with pytest.raises(ValueError, match="failed"):
            create_review_task(store, task, trigger_source="manual")

    def test_rejects_none_id(self):
        store = self._mock_store()
        task = _task(id=None, status="completed")
        with pytest.raises(ValueError, match="without an ID"):
            create_review_task(store, task, trigger_source="manual")

    def test_raises_duplicate_review_error_for_pending(self):
        active = _task(id=50, task_type="review", status="pending")
        store = self._mock_store(existing_reviews=[active])
        task = _task(id=10)
        with pytest.raises(DuplicateReviewError) as exc_info:
            create_review_task(store, task, trigger_source="manual")
        assert exc_info.value.active_review is active

    def test_raises_duplicate_review_error_for_in_progress(self):
        active = _task(id=51, task_type="review", status="in_progress")
        store = self._mock_store(existing_reviews=[active])
        task = _task(id=10)
        with pytest.raises(DuplicateReviewError):
            create_review_task(store, task, trigger_source="manual")

    def test_allows_review_when_existing_are_completed(self):
        completed = _task(id=50, task_type="review", status="completed")
        store = self._mock_store(existing_reviews=[completed])
        task = _task(id=10)
        result = create_review_task(store, task, trigger_source="manual")
        assert result is not None
        store.add.assert_called_once()

    def test_persists_comment_derived_review_scope(self, tmp_path: Path):
        store = SqliteTaskStore(tmp_path / "test.db")
        impl_task = store.add("Implement feature", task_type="implement")
        impl_task.status = "completed"
        store.update(impl_task)
        assert impl_task.id is not None
        store.add_comment(
            impl_task.id,
            "Review only the API validation slice.",
            kind="review_scope",
        )

        review_task = create_review_task(store, impl_task, trigger_source="manual")
        persisted = store.get(review_task.id)

        assert persisted is not None
        assert persisted.review_scope == "Review only the API validation slice."

    def test_persists_derived_scope_for_plan_backed_unsliced_implementation(self, tmp_path: Path):
        store = SqliteTaskStore(tmp_path / "test.db")
        plan_task = store.add("Plan bridge slices", task_type="plan")
        impl_task = store.add(
            "Implement the bridge slices for the serial rerun path.",
            task_type="implement",
            based_on=plan_task.id,
        )
        impl_task.status = "completed"
        store.update(impl_task)

        review_task = create_review_task(store, impl_task, trigger_source="manual")
        persisted = store.get(review_task.id)

        assert persisted is not None
        assert persisted.review_scope is not None
        assert persisted.review_scope.startswith(
            f"Plan-backed implementation scope from {plan_task.id}."
        )
        assert "Implementation request: Implement the bridge slices for the serial rerun path." in persisted.review_scope

    def test_auto_prompt_mode(self):
        store = self._mock_store()
        store.get.return_value = None
        task = _task(
            id="gza-1234",
            slug="20260315-1234-impl-add-feature-1",
            tags=("202606-recovery", "v0.5.0"),
            based_on=None,
        )
        create_review_task(store, task, trigger_source="manual", prompt_mode="auto")
        call_kwargs = store.add.call_args[1]
        assert call_kwargs["prompt"] == "review add-feature"
        assert call_kwargs["task_type"] == "review"
        assert call_kwargs["depends_on"] == "gza-1234"
        assert call_kwargs["tags"] == ("202606-recovery", "v0.5.0")
        assert call_kwargs["based_on"] == "gza-1234"

    def test_auto_prompt_mode_strips_nested_known_suffixes_from_lineage(self):
        store = self._mock_store()
        parent = _task(id="gza-11", slug="20260314-11-impl-add-feature", based_on=None, depends_on=None)
        impl = _task(
            id="gza-12",
            slug="20260315-12-impl-11-impl-add-feature",
            based_on="gza-11",
            depends_on=None,
        )
        store.get.side_effect = lambda task_id: parent if task_id == "gza-11" else None
        create_review_task(store, impl, trigger_source="manual", prompt_mode="auto")
        assert store.add.call_args[1]["prompt"] == "review add-feature"

    @patch("gza.review_tasks.PromptBuilder")
    def test_cli_prompt_mode(self, MockPromptBuilder):
        mock_builder = MockPromptBuilder.return_value
        mock_builder.review_task_prompt.return_value = "cli review prompt"
        store = self._mock_store()
        task = _task(id=10, prompt="implement widgets")
        create_review_task(store, task, trigger_source="manual", prompt_mode="cli")
        mock_builder.review_task_prompt.assert_called_once_with(10, "implement widgets")
        assert store.add.call_args[1]["prompt"] == "cli review prompt"

    def test_passes_model_and_provider(self):
        store = self._mock_store()
        task = _task(id=10)
        create_review_task(store, task, trigger_source="manual", model="opus-4", provider="anthropic", prompt_mode="auto")
        call_kwargs = store.add.call_args[1]
        assert call_kwargs["model"] == "opus-4"
        assert call_kwargs["provider"] == "anthropic"

    def test_inherits_parent_tags(self):
        store = self._mock_store()
        task = _task(id=10, tags=("202606-recovery", "v0.5.0"))

        create_review_task(store, task, trigger_source="manual")

        assert store.add.call_args.kwargs["tags"] == task.tags

    def test_review_creation_opts_into_singleton_guard(self):
        store = self._mock_store()
        task = _task(id=10)

        create_review_task(store, task, trigger_source="manual")

        assert store.add.call_args.kwargs["enforce_single_active_sibling"] is True

    def test_model_and_provider_default_to_none(self):
        store = self._mock_store()
        task = _task(id=10)
        create_review_task(store, task, trigger_source="manual", prompt_mode="auto")
        call_kwargs = store.add.call_args[1]
        assert call_kwargs["model"] is None
        assert call_kwargs["provider"] is None

    def test_review_is_based_on_implementation_task_id(self):
        store = self._mock_store()
        task = _task(id="gza-12", based_on="gza-11")
        create_review_task(store, task, trigger_source="manual", prompt_mode="auto")
        call_kwargs = store.add.call_args[1]
        assert call_kwargs["depends_on"] == "gza-12"
        assert call_kwargs["based_on"] == "gza-12"

    def test_review_inherits_review_scope_from_implementation(self):
        store = self._mock_store()
        task = _task(id="gza-12", review_scope="slice F-A1 + F-A2: only review the classifier slice")

        create_review_task(store, task, trigger_source="manual", prompt_mode="auto")

        assert store.add.call_args[1]["review_scope"] == task.review_scope

    def test_review_resolves_scope_from_legacy_sliced_implementation_prompt(self):
        store = self._mock_store()
        task = _task(
            id="gza-12",
            review_scope=None,
            prompt=(
                "Implement plan gza-4065, slice F-A1 + F-A2: preserve the scoped classifier path.\n\n"
                "## Scope\n"
                "1. Add the classifier.\n"
                "2. Persist the review boundary.\n\n"
                "## Out of scope\n"
                "- F-A3\n"
            ),
        )

        create_review_task(store, task, trigger_source="manual", prompt_mode="auto")

        assert store.add.call_args[1]["review_scope"] == (
            "Slice F-A1 + F-A2: preserve the scoped classifier path.\n\n"
            "1. Add the classifier.\n"
            "2. Persist the review boundary."
        )

    def test_attaches_review_to_implementation_merge_unit(self):
        store = self._mock_store()
        impl = _task(id="gza-12")
        unit = MagicMock()
        store.resolve_merge_unit_for_task.return_value = unit

        create_review_task(store, impl, trigger_source="manual", prompt_mode="auto")

        store.resolve_merge_unit_for_task.assert_called_once_with("gza-12")
        review_task = store.add.return_value
        store.get_or_create_merge_unit_for_task.assert_called_once_with(review_task)

    def test_create_resolution_review_task_persists_structured_scope(self, tmp_path: Path):
        store = SqliteTaskStore(tmp_path / "test.db")
        impl_task = store.add("Implement feature", task_type="implement")
        impl_task.status = "completed"
        store.update(impl_task)
        rebase_task = store.add("Rebase feature", task_type="rebase", based_on=impl_task.id, same_branch=True)
        rebase_task.status = "completed"
        rebase_task.review_scope = (
            "Rebase diff provenance: yes\n"
            "Pre-rebase head SHA: old-head\n"
            "Pre-rebase target SHA: old-target\n"
            "Pre-rebase merge-base SHA: old-base\n"
            "Resolved head SHA: rebased-head\n"
            "Resolved target SHA: target-head\n"
            "Recovered baseline: no"
        )
        store.update(rebase_task)

        review_task = create_resolution_review_task(
            store,
            impl_task,
            rebase_task=rebase_task,
            resolved_head_sha="rebased-head",
            resolved_target_sha="target-head",
            trigger_source="manual",
        )
        persisted = store.get(review_task.id)

        assert persisted is not None
        assert persisted.review_scope is not None
        assert "Review mode: resolution" in persisted.review_scope
        assert f"Implementation task: {impl_task.id}" in persisted.review_scope
        assert f"Rebase task: {rebase_task.id}" in persisted.review_scope
        assert "Pre-rebase head SHA: old-head" in persisted.review_scope
        assert "Pre-rebase target SHA: old-target" in persisted.review_scope
        assert "Pre-rebase merge-base SHA: old-base" in persisted.review_scope
        assert "Resolved head SHA: rebased-head" in persisted.review_scope
        assert "Resolved target SHA: target-head" in persisted.review_scope

    def test_create_resolution_review_task_rejects_mismatched_provenance(self, tmp_path: Path):
        store = SqliteTaskStore(tmp_path / "test.db")
        impl_task = store.add("Implement feature", task_type="implement")
        impl_task.status = "completed"
        store.update(impl_task)
        rebase_task = store.add("Rebase feature", task_type="rebase", based_on=impl_task.id, same_branch=True)
        rebase_task.status = "completed"
        rebase_task.review_scope = (
            "Rebase diff provenance: yes\n"
            "Pre-rebase head SHA: old-head\n"
            "Pre-rebase target SHA: target-at-rebase\n"
            "Pre-rebase merge-base SHA: old-base\n"
            "Resolved head SHA: rebased-head\n"
            "Resolved target SHA: target-at-rebase\n"
            "Recovered baseline: no"
        )
        store.update(rebase_task)

        with pytest.raises(
            ValueError,
            match="Resolution review metadata must match the completed rebase provenance.",
        ):
            create_resolution_review_task(
                store,
                impl_task,
                rebase_task=rebase_task,
                resolved_head_sha="rebased-head",
                resolved_target_sha="target-now",
                trigger_source="manual",
            )


@pytest.mark.parametrize("terminal_status", ["completed", "failed", "dropped"])
def test_persist_off_topic_verify_clearance_creates_new_investigation_when_only_terminal_match_exists(
    tmp_path: Path,
    terminal_status: str,
) -> None:
    config, store = _make_store(tmp_path)

    impl = store.add("Implement feature", task_type="implement")
    assert impl.id is not None
    impl.status = "completed"
    impl.branch = "feat/review-task-reuse"
    store.update(impl)

    review = store.add("Review feature", task_type="review", depends_on=impl.id, based_on=impl.id)
    assert review.id is not None
    review.status = "completed"
    store.update(review)

    terminal = store.add(
        "Terminal investigation",
        task_type="explore",
        depends_on=impl.id,
        based_on=review.id,
        same_branch=True,
    )
    assert terminal.id is not None
    terminal.status = terminal_status
    store.update(terminal)

    failing_node = {
        "nodeid": "tests/cli/test_query.py::test_worker_registry",
        "assertion_signature": "AssertionError: assert 'running' == 'completed'",
        "path": "tests/cli/test_query.py",
        "failure_path": "tests/cli/test_query.py",
        "failure_line": 42,
        "traceback_paths": ["tests/cli/test_query.py"],
    }
    signature_key = hashlib.sha256(
        f"{failing_node['nodeid']}\n{failing_node['assertion_signature']}".encode("utf-8")
    ).hexdigest()
    store_command_output_artifact(
        store,
        terminal,
        config,
        kind=OFF_TOPIC_VERIFY_INVESTIGATION_ARTIFACT_KIND,
        producer="test",
        label="off_topic_verify_investigation",
        output="{}",
        status="queued",
        metadata={
            "signature_key": signature_key,
            "nodeid": failing_node["nodeid"],
            "assertion_signature": failing_node["assertion_signature"],
        },
    )

    payload = {
        "reason": "off_topic_verify_failure",
        "implementation_task_id": impl.id,
        "review_task_id": review.id,
        "green_task_id": "gza-100",
        "red_task_id": "gza-101",
        "head_sha": "same-head-sha",
        "tree_fingerprint": "f" * 64,
        "verify_command": "uv run pytest tests/ -q --maxfail=0",
        "target_branch": "main",
        "target_head_sha": "main-head-sha",
        "target_tree_fingerprint": "a" * 64,
        "baseline_mode": "single",
        "failing_nodes": [failing_node],
    }

    result = persist_off_topic_verify_clearance(
        store,
        config=config,
        review_task=review,
        impl_task=impl,
        payload=payload,
        trigger_source="advance_off_topic_verify_unblock",
        review_clearance_artifact_kind="review_clearance",
        review_clearance_artifact_label="review_clearance",
        review_clearance_artifact_producer="advance_off_topic_verify_unblock",
    )

    assert [task.id for task in result.reused_tasks] == []
    assert len(result.created_tasks) == 1
    assert result.created_tasks[0].id != terminal.id
    clearance_artifacts = store.list_artifacts(impl.id, kind="review_clearance")
    assert len(clearance_artifacts) == 1
    assert clearance_artifacts[0].metadata["created_investigation_task_ids"] == [result.created_tasks[0].id]
    assert clearance_artifacts[0].metadata["reused_investigation_task_ids"] == []


class TestFollowupTasks:
    def test_build_followup_prompt_prefix(self):
        assert (
            build_followup_prompt_prefix("gza-200", "gza-101", "F1")
            == "Follow-up F1 from review gza-200 for task gza-101:"
        )

    def test_build_followup_prompt(self):
        finding = ReviewFinding(
            id="F1",
            severity="FOLLOWUP",
            title="Validate malformed input handling",
            body=(
                "Evidence: malformed optional value is currently accepted.\n"
                "Impact: Low-risk hardening opportunity.\n"
                "Recommended follow-up: add malformed input validation.\n"
                "Recommended tests: add malformed input CLI regression."
            ),
            evidence="malformed optional value is currently accepted.",
            impact="Low-risk hardening opportunity.",
            fix_or_followup="add malformed input validation",
            tests="add malformed input CLI regression.",
        )
        assert (
            build_followup_prompt("gza-200", "gza-101", finding)
            == "Follow-up F1 from review gza-200 for task gza-101: add malformed input validation\n\n"
            "## Follow-up finding to implement:\n\n"
            "### F1 Validate malformed input handling\n"
            "Evidence: malformed optional value is currently accepted.\n"
            "Impact: Low-risk hardening opportunity.\n"
            "Recommended follow-up: add malformed input validation.\n"
            "Recommended tests: add malformed input CLI regression."
        )

    def test_format_followup_finding_context_falls_back_to_structured_fields(self):
        finding = ReviewFinding(
            id="F2",
            severity="FOLLOWUP",
            title="",
            body="",
            evidence="legacy path misses normalization.",
            impact="untrusted optional input can slip through.",
            fix_or_followup="normalize optional claims",
            tests="add malformed-claim regression",
        )
        assert format_followup_finding_context(finding) == (
            "### F2\n"
            "Evidence: legacy path misses normalization.\n"
            "Impact: untrusted optional input can slip through.\n"
            "Recommended follow-up: normalize optional claims\n"
            "Recommended tests: add malformed-claim regression"
        )

    def test_extract_followup_prompt_parts(self):
        assert extract_followup_prompt_parts(
            "Follow-up F1 from review gza-200 for task gza-101: add malformed input validation"
        ) == ("F1", "gza-200", "gza-101")
        assert extract_followup_prompt_parts("Implement feature") is None

    def test_extract_deferred_blocker_prompt_parts(self):
        assert extract_deferred_blocker_prompt_parts(
            "Deferred blocker B1 from review gza-200 for task gza-101: fix flaky gate"
        ) == ("B1", "gza-200", "gza-101")
        assert extract_deferred_blocker_prompt_parts("Implement feature") is None

    def test_find_existing_followup_task_matches_prefix(self):
        store = MagicMock()
        existing = _task(
            id="gza-301",
            task_type="implement",
            prompt="Follow-up F1 from review gza-200 for task gza-101: add validation",
        )
        store.get_based_on_children.return_value = [existing]

        found = find_existing_followup_task(
            store,
            review_task_id="gza-200",
            impl_task_id="gza-101",
            finding_id="F1",
        )
        assert found is existing

    def test_find_existing_deferred_blocker_task_matches_prefix(self):
        store = MagicMock()
        existing = _task(
            id="gza-302",
            task_type="implement",
            prompt="Deferred blocker B1 from review gza-200 for task gza-101: fix flaky gate",
        )
        store.get_based_on_children.return_value = [existing]

        found = find_existing_deferred_blocker_task(
            store,
            review_task_id="gza-200",
            impl_task_id="gza-101",
            finding_id="B1",
        )
        assert found is existing

    def test_create_or_reuse_followup_task_is_idempotent(self):
        store = MagicMock()
        review_task = _task(id="gza-200", task_type="review")
        impl_task = _task(id="gza-101", task_type="implement", tags=("202606-recovery",))
        finding = ReviewFinding(
            id="F1",
            severity="FOLLOWUP",
            title="Title",
            body="Body",
            evidence=None,
            impact=None,
            fix_or_followup="add validation",
            tests=None,
        )

        existing = _task(
            id="gza-401",
            task_type="implement",
            prompt="Follow-up F1 from review gza-200 for task gza-101: add validation",
        )
        store.get_based_on_children.return_value = [existing]
        reused, created_now = create_or_reuse_followup_task(
            store,
            review_task=review_task,
            impl_task=impl_task,
            finding=finding,
            trigger_source="manual",
        )
        assert reused is existing
        assert created_now is False
        store.add.assert_not_called()

    def test_create_or_reuse_followup_task_reuse_does_not_retroactively_mutate_existing_child(self):
        store = MagicMock()
        review_task = _task(id="gza-200", task_type="review")
        impl_task = _task(id="gza-101", task_type="implement", tags=("202606-recovery", "v0.5.0"))
        finding = ReviewFinding(
            id="F1",
            severity="FOLLOWUP",
            title="Title",
            body="Body",
            evidence=None,
            impact=None,
            fix_or_followup="add validation",
            tests=None,
        )
        existing = _task(
            id="gza-401",
            task_type="implement",
            tags=("legacy-only",),
            prompt="Follow-up F1 from review gza-200 for task gza-101: add validation",
        )
        store.get_based_on_children.return_value = [existing]

        reused, created_now = create_or_reuse_followup_task(
            store,
            review_task=review_task,
            impl_task=impl_task,
            finding=finding,
            trigger_source="manual",
        )

        assert reused.tags == ("legacy-only",)
        assert created_now is False
        store.add.assert_not_called()

    def test_create_or_reuse_followup_task_creates_when_missing(self):
        store = MagicMock()
        review_task = _task(id="gza-200", task_type="review")
        impl_task = _task(
            id="gza-101",
            task_type="implement",
            tags=("202606-recovery", "v0.5.0"),
            review_scope="slice F-A1 + F-A2: preserve scoped review boundaries",
        )
        finding = ReviewFinding(
            id="F2",
            severity="FOLLOWUP",
            title="Title",
            body="Body",
            evidence=None,
            impact=None,
            fix_or_followup="update docs",
            tests=None,
        )
        created_task = _task(
            id="gza-402",
            task_type="implement",
            prompt=(
                "Follow-up F2 from review gza-200 for task gza-101: update docs\n\n"
                "## Follow-up finding to implement:\n\n"
                "### F2 Title\n"
                "Body"
            ),
        )
        store.get_based_on_children.return_value = []
        store.add.return_value = created_task

        created, created_now = create_or_reuse_followup_task(
            store,
            review_task=review_task,
            impl_task=impl_task,
            finding=finding,
            trigger_source="manual",
        )
        assert created is created_task
        assert created_now is True
        kwargs = store.add.call_args.kwargs
        assert kwargs["task_type"] == "implement"
        assert kwargs["based_on"] == "gza-200"
        assert kwargs["depends_on"] == "gza-101"
        assert kwargs["review_scope"] == format_followup_finding_context(finding)
        assert kwargs["tags"] == impl_task.tags

    def test_create_or_reuse_followup_task_sets_review_scope_to_finding_text(self):
        store = MagicMock()
        review_task = _task(id="gza-200", task_type="review")
        impl_task = _task(
            id="gza-101",
            task_type="implement",
            prompt=(
                "Implement plan gza-4065, slice F-A1 + F-A2: introduce the scoped classifier path.\n\n"
                "## Scope\n"
                "1. Add the classifier.\n"
                "2. Persist the review boundary.\n\n"
                "## Out of scope\n"
                "- F-A3\n"
            ),
        )
        finding = ReviewFinding(
            id="F3",
            severity="FOLLOWUP",
            title="Title",
            body="Body",
            evidence=None,
            impact=None,
            fix_or_followup="carry scope through follow-ups",
            tests=None,
        )
        created_task = _task(id="gza-403", task_type="implement")
        store.get_based_on_children.return_value = []
        store.add.return_value = created_task

        created, created_now = create_or_reuse_followup_task(
            store,
            review_task=review_task,
            impl_task=impl_task,
            finding=finding,
            trigger_source="manual",
        )

        assert created is created_task
        assert created_now is True
        assert store.add.call_args.kwargs["review_scope"] == format_followup_finding_context(finding)

    def test_followup_implement_creation_does_not_opt_into_singleton_guard(self):
        store = MagicMock()
        review_task = _task(id="gza-200", task_type="review")
        impl_task = _task(id="gza-101", task_type="implement")
        finding = ReviewFinding(
            id="F3",
            severity="FOLLOWUP",
            title="Title",
            body="Body",
            evidence=None,
            impact=None,
            fix_or_followup="carry scope through follow-ups",
            tests=None,
        )
        created_task = _task(id="gza-403", task_type="implement")
        store.get_based_on_children.return_value = []
        store.add.return_value = created_task

        create_or_reuse_followup_task(
            store,
            review_task=review_task,
            impl_task=impl_task,
            finding=finding,
            trigger_source="manual",
        )

        assert "enforce_single_active_sibling" not in store.add.call_args.kwargs

    def test_followup_implement_fanout_remains_allowed_for_distinct_findings(self, tmp_path: Path):
        _config, store = _make_store(tmp_path)
        impl_task = store.add("Implement feature", task_type="implement")
        assert impl_task.id is not None
        review_task = store.add(
            "Review feature",
            task_type="review",
            depends_on=impl_task.id,
            based_on=impl_task.id,
        )
        assert review_task.id is not None
        first_finding = ReviewFinding(
            id="F1",
            severity="FOLLOWUP",
            title="First follow-up",
            body="First body",
            evidence=None,
            impact=None,
            fix_or_followup="first fix",
            tests=None,
        )
        second_finding = ReviewFinding(
            id="F2",
            severity="FOLLOWUP",
            title="Second follow-up",
            body="Second body",
            evidence=None,
            impact=None,
            fix_or_followup="second fix",
            tests=None,
        )

        first_task, first_created = create_or_reuse_followup_task(
            store,
            review_task=review_task,
            impl_task=impl_task,
            finding=first_finding,
            trigger_source="manual",
        )
        second_task, second_created = create_or_reuse_followup_task(
            store,
            review_task=review_task,
            impl_task=impl_task,
            finding=second_finding,
            trigger_source="manual",
        )

        assert first_created is True
        assert second_created is True
        assert first_task.id is not None
        assert second_task.id is not None
        active_followups = store.get_active_children_of_type(review_task.id, "implement")
        assert {child.id for child in active_followups} == {first_task.id, second_task.id}

    def test_create_or_reuse_deferred_blocker_task_is_idempotent(self):
        store = MagicMock()
        review_task = _task(id="gza-200", task_type="review")
        impl_task = _task(id="gza-101", task_type="implement", tags=("202606-recovery",))
        finding = ReviewFinding(
            id="B1",
            severity="BLOCKER",
            title="Fix flaky verify gate",
            body="",
            evidence="`./bin/tests` timed out after 120s",
            impact="Merge would drop the flaky verify evidence",
            fix_or_followup="Stabilize the verify command or adjust the failing phase",
            tests="Add a targeted regression for the flaky phase",
            open_state_citation="status: open",
        )
        existing = _task(
            id="gza-501",
            task_type="implement",
            prompt="Deferred blocker B1 from review gza-200 for task gza-101: fix flaky verify gate",
        )
        store.get_based_on_children.return_value = [existing]

        reused, created_now = create_or_reuse_deferred_blocker_task(
            store,
            review_task=review_task,
            impl_task=impl_task,
            finding=finding,
            trigger_source="manual",
        )

        assert reused is existing
        assert created_now is False
        store.add.assert_not_called()

    def test_create_or_reuse_deferred_blocker_task_creates_expected_shape(self):
        store = MagicMock()
        review_task = _task(id="gza-200", task_type="review")
        impl_task = _task(
            id="gza-101",
            task_type="implement",
            tags=("202606-recovery", "v0.5.0"),
            review_scope="Slice F-A1",
            group="legacy-group",
        )
        finding = ReviewFinding(
            id="B2",
            severity="BLOCKER",
            title="Missing persistence guard",
            body="Canonical blocker context.",
            evidence="Null state can escape the gate",
            impact="Manual merge could lose a required data fix",
            fix_or_followup="Persist the deferred blocker record first",
            tests="Add merge override coverage",
            open_state_citation="finding B2 remains open",
        )
        created_task = _task(id="gza-502", task_type="implement")
        store.get_based_on_children.return_value = []
        store.add.return_value = created_task

        created, created_now = create_or_reuse_deferred_blocker_task(
            store,
            review_task=review_task,
            impl_task=impl_task,
            finding=finding,
            trigger_source="manual",
        )

        assert created is created_task
        assert created_now is True
        kwargs = store.add.call_args.kwargs
        assert kwargs["task_type"] == "implement"
        assert kwargs["based_on"] == "gza-200"
        assert kwargs["depends_on"] == "gza-101"
        assert kwargs["review_scope"] == format_blocker_finding_context(finding)
        assert kwargs["tags"] == impl_task.tags
        assert kwargs["create_pr"] is True
        assert kwargs["urgent"] is True

    def test_find_existing_review_blocker_adjudication_task_requires_current_dispute_identity(self):
        store = MagicMock()
        existing = _task(
            id="gza-601",
            task_type="internal",
            prompt=(
                "Adjudicate blocker B1 from review gza-200 for task gza-101: Missing guard\n\n"
                "Dispute source task: gza-301\n"
                "Dispute source head SHA: old-sha\n"
            ),
        )
        store.get_based_on_children.return_value = [existing]

        found = find_existing_review_blocker_adjudication_task(
            store,
            review_task_id="gza-200",
            impl_task_id="gza-101",
            finding_id="B1",
            dispute_source_task_id="gza-302",
            dispute_head_sha="new-sha",
        )

        assert found is None

    def test_find_existing_review_blocker_adjudication_task_rejects_same_source_when_artifact_id_is_stale(self):
        store = MagicMock()
        existing = _task(
            id="gza-601",
            task_type="internal",
            prompt=(
                "Adjudicate blocker B1 from review gza-200 for task gza-101: Missing guard\n\n"
                "Dispute source task: gza-301\n"
                "Dispute artifact id: 11\n"
                "Dispute source head SHA: same-sha\n"
            ),
        )
        store.get_based_on_children.return_value = [existing]

        found = find_existing_review_blocker_adjudication_task(
            store,
            review_task_id="gza-200",
            impl_task_id="gza-101",
            finding_id="B1",
            dispute_metadata={
                "source_task_id": "gza-301",
                "head_sha": "same-sha",
                "disputed_artifact_id": 12,
            },
        )

        assert found is None

    def test_find_existing_review_blocker_adjudication_task_rejects_source_only_prompt_when_current_dispute_has_artifact_id(self):
        store = MagicMock()
        existing = _task(
            id="gza-601",
            task_type="internal",
            prompt=(
                "Adjudicate blocker B1 from review gza-200 for task gza-101: Missing guard\n\n"
                "Dispute source task: gza-301\n"
                "Dispute source head SHA: same-sha\n"
            ),
        )
        store.get_based_on_children.return_value = [existing]

        found = find_existing_review_blocker_adjudication_task(
            store,
            review_task_id="gza-200",
            impl_task_id="gza-101",
            finding_id="B1",
            dispute_metadata={
                "source_task_id": "gza-301",
                "head_sha": "same-sha",
                "disputed_artifact_id": 12,
            },
        )

        assert found is None

    def test_create_or_reuse_review_blocker_adjudication_task_does_not_reuse_stale_dispute(self):
        store = MagicMock()
        review_task = _task(id="gza-200", task_type="review")
        impl_task = _task(id="gza-101", task_type="implement", tags=("202606-recovery",))
        finding = ReviewFinding(
            id="B1",
            severity="BLOCKER",
            title="Missing API guard",
            body="Body",
            evidence="Evidence",
            impact="Impact",
            fix_or_followup="Required fix",
            tests="Required tests",
            open_state_citation="`src/api.py:12-18`",
        )
        existing = _task(
            id="gza-602",
            task_type="internal",
            prompt=(
                "Adjudicate blocker B1 from review gza-200 for task gza-101: Missing API guard\n\n"
                "Dispute source task: gza-301\n"
                "Dispute source head SHA: old-sha\n"
            ),
        )
        created_task = _task(id="gza-603", task_type="internal")
        store.get_based_on_children.return_value = [existing]
        store.add.return_value = created_task

        created, created_now = create_or_reuse_review_blocker_adjudication_task(
            store,
            review_task=review_task,
            impl_task=impl_task,
            finding=finding,
            dispute_metadata={
                "source_task_id": "gza-302",
                "head_sha": "new-sha",
                "reason": "already_satisfied",
                "evidence": "Current code already rejects empty IDs.",
                "current_state_citation": "`src/api.py:12-18`",
            },
            trigger_source="manual",
        )

        assert created is created_task
        assert created_now is True
        prompt = store.add.call_args.kwargs["prompt"]
        assert "Dispute source task: gza-302" in prompt
        assert "Dispute source head SHA: new-sha" in prompt

    def test_create_or_reuse_review_blocker_adjudication_task_does_not_reuse_same_source_with_stale_head(self):
        store = MagicMock()
        review_task = _task(id="gza-200", task_type="review")
        impl_task = _task(id="gza-101", task_type="implement", tags=("202606-recovery",))
        finding = ReviewFinding(
            id="B1",
            severity="BLOCKER",
            title="Missing API guard",
            body="Body",
            evidence="Evidence",
            impact="Impact",
            fix_or_followup="Required fix",
            tests="Required tests",
            open_state_citation="`src/api.py:12-18`",
        )
        existing = _task(
            id="gza-602",
            task_type="internal",
            prompt=(
                "Adjudicate blocker B1 from review gza-200 for task gza-101: Missing API guard\n\n"
                "Dispute source task: gza-301\n"
                "Dispute source head SHA: old-sha\n"
            ),
        )
        created_task = _task(id="gza-603", task_type="internal")
        store.get_based_on_children.return_value = [existing]
        store.add.return_value = created_task

        created, created_now = create_or_reuse_review_blocker_adjudication_task(
            store,
            review_task=review_task,
            impl_task=impl_task,
            finding=finding,
            dispute_metadata={
                "source_task_id": "gza-301",
                "head_sha": "new-sha",
                "reason": "repeated_reviewer_request",
                "evidence": "The same blocker repeated across review cycles.",
                "current_state_citation": "`src/api.py:12-18`",
            },
            trigger_source="manual",
        )

        assert created is created_task
        assert created_now is True
        prompt = store.add.call_args.kwargs["prompt"]
        assert "Dispute source task: gza-301" in prompt
        assert "Dispute source head SHA: new-sha" in prompt

    def test_extract_review_blocker_adjudication_dispute_identity_reads_current_prompt_lines(self):
        finding = ReviewFinding(
            id="B1",
            severity="BLOCKER",
            title="Missing API guard",
            body="Body",
            evidence="Evidence",
            impact="Impact",
            fix_or_followup="Required fix",
            tests="Required tests",
            open_state_citation="`src/api.py:12-18`",
        )
        prompt = build_review_blocker_adjudication_prompt(
            "gza-200",
            "gza-101",
            finding,
            {
                "source_task_id": "gza-301",
                "disputed_artifact_id": 11,
                "source_branch": "feature/dispute",
                "head_sha": "abc123",
                "reason": "already_satisfied",
                "evidence": "Current code already rejects empty IDs.",
                "current_state_citation": "`src/api.py:12-18`",
            },
        )

        assert extract_review_blocker_adjudication_dispute_reference(prompt) == (11, "gza-301", "abc123")
        assert extract_review_blocker_adjudication_dispute_identity(prompt) == ("gza-301", "abc123")

    def test_deferred_blocker_task_does_not_collide_with_followup_marker(self):
        store = MagicMock()
        review_task = _task(id="gza-200", task_type="review")
        impl_task = _task(id="gza-101", task_type="implement", tags=("202606-recovery",))
        finding = ReviewFinding(
            id="F1",
            severity="BLOCKER",
            title="Real blocker despite shared id",
            body="Body",
            evidence=None,
            impact=None,
            fix_or_followup="apply real fix",
            tests=None,
            open_state_citation=None,
        )
        existing_followup = _task(
            id="gza-503",
            task_type="implement",
            prompt="Follow-up F1 from review gza-200 for task gza-101: cosmetic docs",
        )
        created_task = _task(id="gza-504", task_type="implement")
        store.get_based_on_children.return_value = [existing_followup]
        store.add.return_value = created_task

        created, created_now = create_or_reuse_deferred_blocker_task(
            store,
            review_task=review_task,
            impl_task=impl_task,
            finding=finding,
            trigger_source="manual",
        )

        assert created is created_task
        assert created_now is True
        assert store.add.call_args.kwargs["prompt"].startswith(
            build_deferred_blocker_prompt_prefix("gza-200", "gza-101", "F1")
        )


class TestCreateOrReuseReviewBlockerAdjudicationTask:
    def test_dispute_aware_lookup_prefers_newer_reusable_match(self):
        store = MagicMock()
        stale = _task(
            id="gza-501",
            task_type="internal",
            prompt=(
                f"{build_review_blocker_adjudication_prompt_prefix('gza-200', 'gza-101', 'B1')} "
                "Missing API guard\n\n"
                "Dispute source task: gza-400"
            ),
            status="completed",
            completed_at=datetime(2026, 5, 14, 12, 0, tzinfo=UTC),
        )
        replacement = _task(
            id="gza-502",
            task_type="internal",
            prompt=(
                f"{build_review_blocker_adjudication_prompt_prefix('gza-200', 'gza-101', 'B1')} "
                "Missing API guard\n\n"
                "Dispute source task: gza-401"
            ),
            status="pending",
        )
        dispute_source = _task(
            id="gza-401",
            task_type="improve",
            status="completed",
            completed_at=datetime(2026, 5, 14, 13, 0, tzinfo=UTC),
        )
        store.get_based_on_children.return_value = [stale, replacement]
        store.get.side_effect = lambda task_id: dispute_source if task_id == "gza-401" else None

        existing = find_existing_review_blocker_adjudication_task(
            store,
            review_task_id="gza-200",
            impl_task_id="gza-101",
            finding_id="B1",
            dispute_metadata={
                "source_task_id": "gza-401",
                "reason": "unreproducible",
                "evidence": "The current branch already rejects empty IDs.",
                "current_state_citation": "src/api.py:12-18",
            },
        )

        assert existing is replacement

    def test_completed_stale_adjudication_is_not_reused_for_newer_dispute(self):
        store = MagicMock()
        review_task = _task(id="gza-200", task_type="review")
        impl_task = _task(id="gza-101", task_type="implement", tags=("202606-recovery",))
        finding = ReviewFinding(
            id="B1",
            severity="BLOCKER",
            title="Missing API guard",
            body="Canonical blocker context.",
            evidence="The current code still accepts empty IDs.",
            impact="Invalid requests can crash the handler.",
            fix_or_followup="Reject empty IDs before calling the service.",
            tests="Add a regression for empty IDs.",
            open_state_citation="src/api.py:12-18",
        )
        existing = _task(
            id="gza-501",
            task_type="internal",
            prompt=(
                f"{build_review_blocker_adjudication_prompt_prefix('gza-200', 'gza-101', 'B1')} "
                "Missing API guard\n\n"
                "Dispute source task: gza-400"
            ),
            status="completed",
            completed_at=datetime(2026, 5, 14, 12, 0, tzinfo=UTC),
        )
        dispute_source = _task(
            id="gza-401",
            task_type="improve",
            status="completed",
            completed_at=datetime(2026, 5, 14, 13, 0, tzinfo=UTC),
        )
        created_task = _task(id="gza-502", task_type="internal")
        store.get_based_on_children.return_value = [existing]
        store.get.side_effect = lambda task_id: dispute_source if task_id == "gza-401" else None
        store.add.return_value = created_task

        created, created_now = create_or_reuse_review_blocker_adjudication_task(
            store,
            review_task=review_task,
            impl_task=impl_task,
            finding=finding,
            dispute_metadata={
                "source_task_id": "gza-401",
                "reason": "unreproducible",
                "evidence": "The current branch already rejects empty IDs.",
                "current_state_citation": "src/api.py:12-18",
            },
            trigger_source="manual",
        )

        assert created is created_task
        assert created_now is True
        store.add.assert_called_once()
