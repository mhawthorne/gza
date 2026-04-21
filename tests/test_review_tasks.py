"""Tests for review_tasks helpers."""

from unittest.mock import MagicMock, patch

import pytest

from gza.db import Task
from gza.review_tasks import (
    DuplicateReviewError,
    build_followup_prompt,
    build_followup_prompt_prefix,
    build_auto_review_prompt,
    create_or_reuse_followup_task,
    create_review_task,
    extract_followup_prompt_parts,
    find_existing_followup_task,
    format_followup_finding_context,
)
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
        return store

    def test_rejects_non_implement_task(self):
        store = self._mock_store()
        task = _task(task_type="explore")
        with pytest.raises(ValueError, match="explore task"):
            create_review_task(store, task)

    def test_rejects_non_completed_task(self):
        store = self._mock_store()
        task = _task(status="failed")
        with pytest.raises(ValueError, match="failed"):
            create_review_task(store, task)

    def test_rejects_none_id(self):
        store = self._mock_store()
        task = _task(id=None, status="completed")
        with pytest.raises(ValueError, match="without an ID"):
            create_review_task(store, task)

    def test_raises_duplicate_review_error_for_pending(self):
        active = _task(id=50, task_type="review", status="pending")
        store = self._mock_store(existing_reviews=[active])
        task = _task(id=10)
        with pytest.raises(DuplicateReviewError) as exc_info:
            create_review_task(store, task)
        assert exc_info.value.active_review is active

    def test_raises_duplicate_review_error_for_in_progress(self):
        active = _task(id=51, task_type="review", status="in_progress")
        store = self._mock_store(existing_reviews=[active])
        task = _task(id=10)
        with pytest.raises(DuplicateReviewError):
            create_review_task(store, task)

    def test_allows_review_when_existing_are_completed(self):
        completed = _task(id=50, task_type="review", status="completed")
        store = self._mock_store(existing_reviews=[completed])
        task = _task(id=10)
        result = create_review_task(store, task)
        assert result is not None
        store.add.assert_called_once()

    def test_auto_prompt_mode(self):
        store = self._mock_store()
        store.get.return_value = None
        task = _task(
            id="gza-1234",
            slug="20260315-1234-impl-add-feature-1",
            group="mygroup",
            based_on=None,
        )
        create_review_task(store, task, prompt_mode="auto")
        call_kwargs = store.add.call_args[1]
        assert call_kwargs["prompt"] == "review add-feature"
        assert call_kwargs["task_type"] == "review"
        assert call_kwargs["depends_on"] == "gza-1234"
        assert call_kwargs["group"] == "mygroup"
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
        create_review_task(store, impl, prompt_mode="auto")
        assert store.add.call_args[1]["prompt"] == "review add-feature"

    @patch("gza.review_tasks.PromptBuilder")
    def test_cli_prompt_mode(self, MockPromptBuilder):
        mock_builder = MockPromptBuilder.return_value
        mock_builder.review_task_prompt.return_value = "cli review prompt"
        store = self._mock_store()
        task = _task(id=10, prompt="implement widgets")
        create_review_task(store, task, prompt_mode="cli")
        mock_builder.review_task_prompt.assert_called_once_with(10, "implement widgets")
        assert store.add.call_args[1]["prompt"] == "cli review prompt"

    def test_passes_model_and_provider(self):
        store = self._mock_store()
        task = _task(id=10)
        create_review_task(store, task, model="opus-4", provider="anthropic", prompt_mode="auto")
        call_kwargs = store.add.call_args[1]
        assert call_kwargs["model"] == "opus-4"
        assert call_kwargs["provider"] == "anthropic"

    def test_model_and_provider_default_to_none(self):
        store = self._mock_store()
        task = _task(id=10)
        create_review_task(store, task, prompt_mode="auto")
        call_kwargs = store.add.call_args[1]
        assert call_kwargs["model"] is None
        assert call_kwargs["provider"] is None

    def test_review_is_based_on_implementation_task_id(self):
        store = self._mock_store()
        task = _task(id="gza-12", based_on="gza-11")
        create_review_task(store, task, prompt_mode="auto")
        call_kwargs = store.add.call_args[1]
        assert call_kwargs["depends_on"] == "gza-12"
        assert call_kwargs["based_on"] == "gza-12"


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

    def test_create_or_reuse_followup_task_is_idempotent(self):
        store = MagicMock()
        review_task = _task(id="gza-200", task_type="review")
        impl_task = _task(id="gza-101", task_type="implement", group="grp-a")
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
        )
        assert reused is existing
        assert created_now is False
        store.add.assert_not_called()

    def test_create_or_reuse_followup_task_creates_when_missing(self):
        store = MagicMock()
        review_task = _task(id="gza-200", task_type="review")
        impl_task = _task(id="gza-101", task_type="implement", group="grp-a")
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
        )
        assert created is created_task
        assert created_now is True
        kwargs = store.add.call_args.kwargs
        assert kwargs["task_type"] == "implement"
        assert kwargs["based_on"] == "gza-200"
        assert kwargs["depends_on"] == "gza-101"
        assert kwargs["group"] == "grp-a"
