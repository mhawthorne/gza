"""Tests for review_tasks helpers."""

from unittest.mock import MagicMock, patch

import pytest

from gza.db import Task
from gza.review_tasks import (
    DuplicateReviewError,
    build_auto_review_prompt,
    create_review_task,
)


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
        assert "#7" in str(err)
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
        task = _task(slug="20260315-fix-bug-42")
        result = build_auto_review_prompt(task)
        assert result == "review fix-bug"

    def test_slug_strips_derived_implement_prefix(self):
        task = _task(slug="20260410-0000ab-impl-add-authentication-system")
        result = build_auto_review_prompt(task)
        assert result == "review add-authentication-system"

    def test_slug_strips_nested_derived_implement_prefixes(self):
        task = _task(slug="20260410-b2-impl-a1-impl-add-feature")
        result = build_auto_review_prompt(task)
        assert result == "review add-feature"

    def test_slug_preserves_semantic_impl_subject_add(self):
        task = _task(slug="20260410-0000ab-impl-add-impl-support")
        result = build_auto_review_prompt(task)
        assert result == "review add-impl-support"

    def test_slug_preserves_semantic_impl_subject_api(self):
        task = _task(slug="20260410-0000ab-impl-api-impl-migration")
        result = build_auto_review_prompt(task)
        assert result == "review api-impl-migration"

    def test_slug_preserves_semantic_impl_subject_ui(self):
        task = _task(slug="20260410-0000ab-impl-ui-impl-refresh")
        result = build_auto_review_prompt(task)
        assert result == "review ui-impl-refresh"

    def test_slug_preserves_semantic_impl_subject_db(self):
        task = _task(slug="20260410-0000ab-impl-db-impl-migration")
        result = build_auto_review_prompt(task)
        assert result == "review db-impl-migration"

    def test_project_prefix_stripped_after_derived_normalization(self):
        task = _task(slug="20260410-0000ab-impl-myproj-add-feature")
        result = build_auto_review_prompt(task, project_prefix="myproj")
        assert result == "review add-feature"

    def test_fallback_when_no_task_id(self):
        task = _task(id=5, slug=None, prompt="build the thing")
        result = build_auto_review_prompt(task)
        assert result == "Review task #5: build the thing"

    def test_fallback_when_task_id_has_no_dash(self):
        task = _task(id=5, slug="nodash", prompt="build the thing")
        result = build_auto_review_prompt(task)
        assert result == "Review task #5: build the thing"

    def test_fallback_includes_truncated_prompt(self):
        long_prompt = "x" * 200
        task = _task(id=3, slug=None, prompt=long_prompt)
        result = build_auto_review_prompt(task)
        assert result == f"Review task #3: {'x' * 100}"

    def test_fallback_without_prompt(self):
        task = _task(id=3, slug=None, prompt=None)
        result = build_auto_review_prompt(task)
        assert result == "Review task #3"


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
        task = _task(id=10, slug="20260315-0000ab-impl-add-feature-1", group="mygroup", based_on=5)
        create_review_task(store, task, prompt_mode="auto")
        call_kwargs = store.add.call_args[1]
        assert call_kwargs["prompt"] == "review add-feature"
        assert call_kwargs["task_type"] == "review"
        assert call_kwargs["depends_on"] == 10
        assert call_kwargs["group"] == "mygroup"
        assert call_kwargs["based_on"] == 5

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
