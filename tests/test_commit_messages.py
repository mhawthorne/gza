"""Tests for commit message formatting helpers."""

from gza.commit_messages import build_task_commit_message, format_commit_subject
from gza.console import MAX_PR_TITLE_LENGTH


def test_format_commit_subject_truncates_at_word_boundary() -> None:
    prompt = "Implement a robust parser for nested configuration with validation and clear errors"
    subject = format_commit_subject(prompt, max_len=40)
    assert subject == "Implement a robust parser for..."
    assert len(subject) <= 40


def test_format_commit_subject_normalizes_whitespace() -> None:
    prompt = "Implement feature X\n\nwith careful   edge-case handling"
    subject = format_commit_subject(prompt)
    assert "\n" not in subject
    assert "  " not in subject


def test_build_task_commit_message_uses_canonical_trailers() -> None:
    message = build_task_commit_message(
        "Implement API retries for transient failures",
        task_id=42,
        task_slug="20260401-impl-api-retries",
        review_task_id=77,
    )
    assert "\n\nTask #42\nSlug: 20260401-impl-api-retries\nGza-Review: #77" in message


def test_build_task_commit_message_respects_72_char_subject_with_prefix() -> None:
    prompt = "Implement a long running operation that will definitely exceed the normal limit"
    message = build_task_commit_message(
        prompt,
        task_id=5,
        task_slug="20260401-long-op",
        subject_prefix="Squash merge: ",
    )
    subject = message.splitlines()[0]
    assert len(subject) <= MAX_PR_TITLE_LENGTH
